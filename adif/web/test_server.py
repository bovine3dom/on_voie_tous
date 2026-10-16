import pytest
from fastapi.testclient import TestClient

from predict.board_models import PlatformPrior
from . import server
from .features import SCHEMA
from .test_pipeline import board, row

CLIENT = TestClient(server.app)


@pytest.fixture
def payload():
    at = "2026-10-16T12:20:32Z"
    return {"ts": at, "station": "79400", "traffic": "cercanias",
            "data": board(at, [row("unknown"), row("3 min", "8")])}


@pytest.fixture(autouse=True)
def models(tmp_path, monkeypatch):
    monkeypatch.setattr(server, "MODELS_DIR", tmp_path)
    PlatformPrior(["8"], [1], SCHEMA.features, SCHEMA.version).save_model(tmp_path / "79400.prior.json")


def test_catalog_and_prediction(payload):
    from predict.server import app
    assert app is server.app
    assert CLIENT.get("/adif-web/stations").json() == {"stations": ["79400"]}
    response = CLIENT.post("/adif-web/predict", json=payload)
    assert response.status_code == 200
    assert response.json() == {"predictions": [{"rowIndex": 1, "trainNumber": "R2S",
        "departureTime": "2026-10-16T14:23:00+02:00", "platform": "8", "confidence": 1.0,
        "probabilities": [{"platform": "8", "prob": 1.0}]}], "issues": {"invalid_time": 1}}


@pytest.mark.parametrize("change", [
    {"station": "../79400"}, {"traffic": "arrivals"}, {"ts": "2026-10-16T12:20:32"},
    {"ts": "2026-10-16T12:40:32Z"},
])
def test_invalid_envelope(payload, change):
    payload.update(change)
    assert CLIENT.post("/adif-web/predict", json=payload).status_code == 422


@pytest.mark.parametrize("change", [
    {"date": "bad"}, {"error": True}, {"horarios": [row()] * 129},
    {"horarios": [row(train="x" * 4097)]},
])
def test_invalid_board(payload, change):
    payload["data"].update(change)
    assert CLIENT.post("/adif-web/predict", json=payload).status_code == 422


def test_missing_and_incompatible_models(payload, monkeypatch):
    payload["station"] = "17000"
    assert CLIENT.post("/adif-web/predict", json=payload).status_code == 404
    monkeypatch.setattr(server, "get_model", lambda *args: PlatformPrior(["8"], [1]))
    assert CLIENT.post("/adif-web/predict", json=payload).status_code == 409


def test_empty_board_needs_no_model(payload):
    payload["station"] = "17000"
    payload["data"]["horarios"] = []
    assert CLIENT.post("/adif-web/predict", json=payload).json() == {"predictions": [], "issues": {}}


def test_cross_origin():
    response = CLIENT.options("/adif-web/predict", headers={
        "Origin": "https://www.adif.es", "Access-Control-Request-Method": "POST",
        "Access-Control-Request-Headers": "content-type"})
    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] in ("*", "https://www.adif.es")
