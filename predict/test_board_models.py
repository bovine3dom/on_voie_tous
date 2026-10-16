import importlib

import numpy as np
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from predict.board_features import FEATURES
from predict.board_models import PlatformPrior, station_models
from predict.predict import get_model
from predict.server import app


def test_prior_round_trip_catalog_and_shared_cache(tmp_path):
    prior = PlatformPrior(["2EST", "20B"], [0.25, 0.75])
    prior.save_model(tmp_path / "05123.prior.json")
    (tmp_path / "05123.cbm").touch()
    (tmp_path / "999.cbm").touch()
    (tmp_path / "report.json").touch()
    assert station_models(tmp_path) == ["05123", "999"]
    (tmp_path / "05123.cbm").unlink()
    model = get_model("05123", tmp_path)
    assert model is get_model("05123", tmp_path)
    assert list(model.feature_names_) == FEATURES
    assert list(model.classes_) == ["2EST", "20B"]
    assert np.array_equal(model.probabilities, [0.25, 0.75])


@pytest.mark.parametrize("operator", ["rfi", "adif"])
@pytest.mark.parametrize("compatible", [True, False])
def test_prior_models_use_the_existing_operator_routes(tmp_path, monkeypatch, operator, compatible):
    server = importlib.import_module(f"{operator}.server")
    monkeypatch.setattr(server, "MODELS_DIR", tmp_path)
    PlatformPrior(["2EST"], [1.0], FEATURES if compatible else []).save_model(tmp_path / "51003.prior.json")
    train = {"trainId": "x", "trainNumber": "00123", "destination": "ROMA", "platform": "20B"}
    if operator == "rfi":
        train["clock"] = "12:30"
    else:
        train.update(scheduledTime="2026-04-01T12:30:00+02:00", stopType="origin")
    client = TestClient(app)
    assert client.get(f"/stations?operator={operator}").json() == {"stations": ["51003"]}
    response = client.post(f"/predict?operator={operator}", json={
        "ts": "2026-04-01T10:00:00Z", "station": "51003", "data": [train]})
    assert response.status_code == (200 if compatible else 409)
    if compatible:
        prediction = response.json()["predictions"][0]
        assert prediction["trainId"] == "x"
        assert prediction["probabilities"] == [{"platform": "2EST", "prob": 1.0}]


def test_custom_schema_reuses_board_training_and_prediction(tmp_path):
    import polars as pl
    from predict.board_features import FeatureSchema
    from predict.board_api import board_predictions
    from rfi.train import train_station

    schema = FeatureSchema(3, ("predictedPlatform", "trainNumber"), ("leadMinutes",), "departureTime")
    rows = pl.DataFrame({"serviceDate": ["2026-10-16"] * 2, "departureId": ["a", "b"],
                         "timestamp": [1, 2], "predictedPlatform": ["MISSING"] * 2,
                         "trainNumber": ["123"] * 2, "leadMinutes": [10.0] * 2,
                         "weight": [1.0, 1.0], "actualPlatform": ["1", "2"]})
    report = train_station("12345", rows, tmp_path, schema=schema)
    assert report["schema_version"] == 3
    model = PlatformPrior.load_model(tmp_path / "12345.prior.json")
    predictions = board_predictions(model, rows.select(schema.features).to_dicts(), [{"id": "a"}, {"id": "b"}], schema)
    assert len(predictions) == 2
    assert predictions[0]["confidence"] == 0.5
    assert model.schema_version == 3


def test_default_sncf_route_does_not_load_board_priors(tmp_path, monkeypatch):
    module = importlib.import_module("predict.predict")
    monkeypatch.setattr(module, "MODELS_DIR", str(tmp_path))
    PlatformPrior(["20B"], [1.0]).save_model(tmp_path / "51003.prior.json")
    assert module.available_stations() == {"stations": []}
    assert isinstance(get_model("51003", tmp_path), PlatformPrior)
    with pytest.raises(HTTPException) as error:
        get_model("51003")
    assert error.value.status_code == 404
