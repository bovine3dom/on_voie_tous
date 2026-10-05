from datetime import datetime
import importlib

import numpy as np
import pytest
from fastapi.testclient import TestClient
from fastapi import HTTPException

from rfi import server
from rfi.features import FEATURES, live_features, scheduled_time

CLIENT = TestClient(server.app)


@pytest.fixture
def payload():
    return {"ts": "2026-04-01T10:00:00Z", "station": "1728",
            "data": [{"trainId": "20260331100123", "trainNumber": "123", "clock": "12:30",
                      "destination": "ROMA", "carrier": "TRENITALIA", "category": "Categoria REG",
                      "delayMinutes": 0, "platform": ""}]}


@pytest.fixture(autouse=True)
def model_stub(monkeypatch):
    class Model:
        feature_names_ = FEATURES
        classes_ = ["2EST", "20B"]

        def predict_proba(self, df):
            return np.tile([0.9, 0.1], (df.height, 1))

    monkeypatch.setattr(server, "get_model", lambda station, directory: Model())


def test_identity_string_labels_and_features(payload):
    response = CLIENT.post("/rfi/predict", json=payload)
    assert response.status_code == 200
    result = response.json()["predictions"][0]
    assert result["trainId"] == payload["data"][0]["trainId"]
    assert result["clock"] == "12:30"
    assert result["platform"] == "2EST"
    assert result["confidence"] == 0.9
    assert sum(p["prob"] for p in result["probabilities"]) == pytest.approx(1)
    features = live_features(datetime.fromisoformat(payload["ts"].replace("Z", "+00:00")),
                             server.Train(**payload["data"][0]))
    assert features["scheduledMinute"] == 750
    assert features["dayOfWeek"] == 3
    assert features["month"] == 4
    assert features["leadMinutes"] == 30


@pytest.mark.parametrize("change", [
    {"platform": "20B"}, {"cancelled": True}, {"category": "AUTOBUS"},
    {"clock": "16:00"}, {"clock": "12:05"}])
def test_official_and_out_of_scope_rows_are_not_predicted(payload, change, monkeypatch):
    payload["data"][0].update(change)
    monkeypatch.setattr(server, "get_model", lambda *args: pytest.fail("Unneeded model load"))
    assert CLIENT.post("/rfi/predict", json=payload).json() == {"predictions": []}


def test_abstention_empty_batches_missing_models_and_schema(payload, monkeypatch):
    monkeypatch.setattr(server, "MIN_CONFIDENCE", 0.99)
    assert CLIENT.post("/rfi/predict", json=payload).json() == {"predictions": []}
    payload["data"] = []
    assert CLIENT.post("/rfi/predict", json=payload).json() == {"predictions": []}
    payload["data"] = [{"trainId": "x", "trainNumber": "1", "clock": "12:30", "destination": "ROMA"}]
    def absent(*args):
        raise HTTPException(404, "No model")
    monkeypatch.setattr(server, "get_model", absent)
    assert CLIENT.post("/rfi/predict", json=payload).status_code == 404
    monkeypatch.setattr(server, "get_model", lambda *args: type("Bad", (), {"feature_names_": []})())
    assert CLIENT.post("/rfi/predict", json=payload).status_code == 409


def test_aware_timestamps_and_station_validation(payload):
    payload["ts"] = "2026-04-01T10:00:00"
    assert CLIENT.post("/rfi/predict", json=payload).status_code == 422
    payload["ts"] += "Z"
    payload["station"] = "../1728"
    assert CLIENT.post("/rfi/predict", json=payload).status_code == 422


def test_midnight_and_dst_are_consistent_with_julia():
    at = datetime.fromisoformat("2026-04-01T21:55:00+00:00")
    assert scheduled_time(at, "00:30").isoformat() == "2026-04-02T00:30:00+02:00"
    for at in ("2026-10-25T00:00:00+00:00", "2026-03-29T00:00:00+00:00"):
        with pytest.raises(ValueError):
            scheduled_time(datetime.fromisoformat(at), "02:30")


def test_sncf_and_rfi_model_caches_are_separate(tmp_path, monkeypatch):
    module = importlib.import_module("predict.predict")
    class Model:
        def load_model(self, path):
            self.path = path
    monkeypatch.setattr(module, "CatBoostClassifier", Model)
    monkeypatch.setattr(module, "_model_cache", {})
    first, second = tmp_path / "sncf", tmp_path / "rfi"
    for directory in (first, second):
        directory.mkdir()
        (directory / "1728.cbm").touch()
    sncf = module.get_model("1728", first)
    rfi = module.get_model("1728", second)
    assert sncf is not rfi
    assert module.get_model("1728", first) is sncf
    with pytest.raises(HTTPException):
        module.get_model("../1728", first)
