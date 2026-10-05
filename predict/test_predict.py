import importlib
import numpy as np
import pytest
from httpx import AsyncClient, ASGITransport
from predict import app, normalise_sncf_data

FEATURES = ["predictedDestination", "scheduledDestination", "trainNumber", "scheduledTime"]


@pytest.fixture(autouse=True)
def model_stub(monkeypatch):
    class Model:
        feature_names_ = FEATURES
        classes_ = ["1", "2"]

        def predict(self, df):
            return np.array([["1"]] * df.height)

        def predict_proba(self, df):
            return np.tile([0.75, 0.25], (df.height, 1))

    module = importlib.import_module("predict.predict")
    monkeypatch.setattr(module, "get_model", lambda station: Model())


@pytest.fixture
def sample_payload():
    return {
        "ts": "2026-04-17T12:51:57.985068453+00:00",
        "station": "0087756056",
        "data": [
            {
                "TrafficDetailsUrl": "https://www.sncf-voyageurs.com/fr/voyagez-avec-nous/horaires-et-itineraires/recherche-de-train/detail-train/?numeroCirculation=881227&dateCirculation=2026-04-17&destinationCode=87756056",
                "actualTime": "2026-04-17T13:33:00+00:00",
                "alternativeMeans": None,
                "direction": "Departure",
                "informationStatus": {
                    "delay": None,
                    "eventLevel": "Warning",
                    "trainStatus": "SUPPRESSION_TOTALE",
                },
                "isGL": False,
                "missionCode": None,
                "platform": {
                    "backgroundColor": None,
                    "isTrackactive": False,
                    "track": "",
                    "trackGroupTitle": None,
                    "trackGroupValue": None,
                    "trackPosition": None,
                },
                "presentation": {"colorCode": "#0749ff", "textColorCode": "#FFFFFF"},
                "scheduledTime": "2026-04-17T12:03:00+00:00",
                "shortTermInformations": [],
                "stationName": "Nice",
                "statusModification": None,
                "stops": [],
                "traffic": {
                    "destination": "Menton",
                    "eventLevel": "Warning",
                    "eventStatus": "SUPPRESSION",
                    "oldDestination": "",
                    "oldOrigin": "",
                    "origin": "Les Arcs - Draguignan",
                },
                "trainLine": None,
                "trainMode": "TRAIN",
                "trainNumber": "881227",
                "trainType": "ZOU !",
                "uic": "0087756056",
            },
            {
                "TrafficDetailsUrl": "https://www.sncf-voyageurs.com/fr/voyagez-avec-nous/horaires-et-itineraires/recherche-de-train/detail-train/?numeroCirculation=86045&dateCirculation=2026-04-17&destinationCode=87756056",
                "actualTime": "2026-04-17T12:54:00+00:00",
                "alternativeMeans": None,
                "direction": "Departure",
                "informationStatus": {
                    "delay": None,
                    "eventLevel": "Normal",
                    "trainStatus": "Ontime",
                },
                "isGL": False,
                "missionCode": None,
                "platform": {
                    "backgroundColor": None,
                    "isTrackactive": True,
                    "track": "D",
                    "trackGroupTitle": None,
                    "trackGroupValue": None,
                    "trackPosition": None,
                },
                "presentation": {"colorCode": "#0749ff", "textColorCode": "#FFFFFF"},
                "scheduledTime": "2026-04-17T12:54:00+00:00",
                "shortTermInformations": [],
                "stationName": "Nice",
                "statusModification": None,
                "stops": [],
                "traffic": {
                    "destination": "Ventimiglia",
                    "eventLevel": "Normal",
                    "eventStatus": "Ontime",
                    "oldDestination": "",
                    "oldOrigin": "",
                    "origin": "Grasse",
                },
                "trainLine": None,
                "trainMode": "TRAIN",
                "trainNumber": "86045",
                "trainType": "ZOU !",
                "uic": "0087756056",
            },
        ],
    }


@pytest.mark.asyncio
async def test_health():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


@pytest.mark.asyncio
@pytest.mark.parametrize("url", ["/predict", "/predict?operator=sncf"])
async def test_predict(sample_payload, url):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(url, json=sample_payload)
        assert response.status_code == 200
        data = response.json()
        assert "predictions" in data
        assert isinstance(data["predictions"], list)
        assert len(data["predictions"]) == 2
        for pred in data["predictions"]:
            assert set(pred) == {"platform", "confidence", "probabilities"}
            total_prob = sum(p["prob"] for p in pred["probabilities"])
            assert 0.99 <= total_prob <= 1.01


def test_normalise_sncf_data(sample_payload):
    df = normalise_sncf_data(sample_payload, FEATURES)
    assert df.shape[0] == 2
    assert "scheduledDestination" in df.columns
