import numpy as np
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from adif import server
from predict.board_features import FEATURES

CLIENT = TestClient(server.app)


@pytest.fixture
def payload():
    return {"ts": "2026-04-01T10:00:00Z", "station": "51003", "data": [{
        "trainId": "00123|2026-04-01T12:30:00+02:00", "trainNumber": "00123",
        "scheduledTime": "2026-04-01T12:30:00+02:00", "stopType": "origin",
        "destination": "71801", "carrier": "Renfe", "category": "AVE", "platform": "20B",
    }]}


@pytest.fixture(autouse=True)
def model_stub(monkeypatch):
    class Model:
        feature_names_ = FEATURES
        classes_ = ["2EST", "20B"]

        def predict_proba(self, df):
            assert df["predictedPlatform"].to_list() == ["20B"]
            assert df["trainNumber"].to_list() == ["00123"]
            assert df["scheduledMinute"].to_list() == [750]
            assert df["dayOfWeek"].to_list() == [3]
            assert df["month"].to_list() == [4]
            assert df["leadMinutes"].to_list() == [30]
            return np.tile([0.6, 0.4], (df.height, 1))
    monkeypatch.setattr(server, "get_model", lambda *args: Model())


@pytest.mark.parametrize("url", ["/predict?operator=adif", "/adif/predict"])
def test_adif_identity_official_input_and_full_distribution(payload, url):
    response = CLIENT.post(url, json=payload)
    assert response.status_code == 200
    prediction = response.json()["predictions"][0]
    assert prediction["trainId"] == payload["data"][0]["trainId"]
    assert prediction["clock"] == "12:30"
    assert prediction["probabilities"] == [{"platform": "2EST", "prob": 0.6}, {"platform": "20B", "prob": 0.4}]


@pytest.mark.parametrize("change", [
    {"stopType": "destination"}, {"cancelled": True}, {"status": "cancelled"},
    {"category": "AUTOBUS"}, {"trafficType": "B"}, {"trainNumber": "BUS00123"}, {"platform": "BUS"},
])
def test_arrivals_cancelled_trains_and_buses_are_skipped(payload, monkeypatch, change):
    payload["data"][0].update(change)
    monkeypatch.setattr(server, "get_model", lambda *args: pytest.fail("Unneeded model load"))
    assert CLIENT.post("/predict?operator=adif", json=payload).json() == {"predictions": []}


@pytest.mark.parametrize("clock", ["11:59", "12:00", "12:05", "12:30", "16:00"])
@pytest.mark.parametrize("platform", ["", "--", "20B", "2EST"])
def test_every_lead_time_and_official_platform_is_predicted(payload, monkeypatch, clock, platform):
    payload["data"][0].update(scheduledTime=f"2026-04-01T{clock}:00+02:00", platform=platform)
    class Model:
        feature_names_ = FEATURES
        classes_ = ["2EST", "20B"]

        def predict_proba(self, df):
            assert df["predictedPlatform"].to_list() == [platform if platform not in ("", "--") else "MISSING"]
            return np.tile([0.6, 0.4], (df.height, 1))
    monkeypatch.setattr(server, "get_model", lambda *args: Model())
    response = CLIENT.post("/predict?operator=adif", json=payload)
    assert response.status_code == 200
    assert len(response.json()["predictions"]) == 1
    assert response.json()["predictions"][0]["clock"] == clock


@pytest.mark.parametrize("change", [
    {"station": "../51003"}, {"ts": "2026-04-01T10:00:00"},
])
def test_bad_envelopes_are_rejected(payload, change):
    payload.update(change)
    assert CLIENT.post("/predict?operator=adif", json=payload).status_code == 422


def test_departure_times_require_an_offset(payload):
    payload["data"][0]["scheduledTime"] = "2026-04-01T12:30:00"
    assert CLIENT.post("/predict?operator=adif", json=payload).status_code == 422


def test_missing_models_and_old_schemas_fail_closed(payload, monkeypatch):
    def absent(*args):
        raise HTTPException(404, "No model")
    monkeypatch.setattr(server, "get_model", absent)
    assert CLIENT.post("/predict?operator=adif", json=payload).status_code == 404
    monkeypatch.setattr(server, "get_model", lambda *args: type("Old", (), {"feature_names_": []})())
    assert CLIENT.post("/predict?operator=adif", json=payload).status_code == 409


@pytest.mark.parametrize("url", ["/stations?operator=adif", "/adif/stations"])
def test_shared_catalog_lists_only_station_models(tmp_path, monkeypatch, url):
    from predict.server import app
    assert app is server.app
    monkeypatch.setattr(server, "MODELS_DIR", tmp_path)
    (tmp_path / "05123.cbm").touch()
    (tmp_path / "report.json").touch()
    assert CLIENT.get(url).json() == {"stations": ["05123"]}
