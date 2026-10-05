import importlib

import numpy as np
import pytest
from fastapi.testclient import TestClient

from predict.board_features import FEATURES
from predict.server import app


@pytest.mark.parametrize("operator", ["rfi", "adif"])
@pytest.mark.parametrize("legacy_route", [False, True])
def test_large_boards_and_signed_delays(operator, legacy_route, monkeypatch):
    trains = [dict(trainId=str(i) * 201, trainNumber=str(i), destination="TEST",
                   delayMinutes=[1440, -15, -1][i % 3], platform="1") for i in range(101)]
    for train in trains:
        train.update(clock="12:30") if operator == "rfi" else train.update(
            scheduledTime="2026-04-01T12:30:00+02:00", stopType="origin")

    class Model:
        feature_names_ = FEATURES
        classes_ = ["1", "2"]

        def predict_proba(self, df):
            assert df["delayMinutes"].to_list() == [t["delayMinutes"] for t in trains]
            return np.tile([0.75, 0.25], (df.height, 1))

    monkeypatch.setattr(importlib.import_module(f"{operator}.server"), "get_model", lambda *args: Model())
    url = f"/{operator}/predict" if legacy_route else f"/predict?operator={operator}"
    response = TestClient(app).post(url, json={"ts": "2026-04-01T10:00:00Z", "station": "123", "data": trains})
    assert response.status_code == 200
    assert [p["trainId"] for p in response.json()["predictions"]] == [t["trainId"] for t in trains]
