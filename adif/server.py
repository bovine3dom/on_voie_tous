import os
import re
from pathlib import Path
from typing import Literal

from pydantic import AwareDatetime, BaseModel, Field

from predict.predict import app, get_model, PredictionOutput
from predict.board_api import board_predictions
from predict.board_models import station_models
from .features import live_features

MODELS_DIR = Path(os.getenv("ADIF_MODELS_DIR", Path(__file__).resolve().parent / "models"))


class Train(BaseModel):
    trainId: str = Field(min_length=1)
    trainNumber: str
    scheduledTime: AwareDatetime
    stopType: Literal["origin", "intermediate", "destination"]
    destination: str
    carrier: str = ""
    category: str = ""
    trafficType: str = ""
    status: str = ""
    delayMinutes: int = 0
    platform: str = ""
    cancelled: bool = False


class Input(BaseModel):
    ts: AwareDatetime
    station: str = Field(pattern=r"^[0-9]{1,12}$")
    data: list[Train]


@app.get("/adif/stations")
def available_stations():
    return {"stations": station_models(MODELS_DIR)}


@app.post("/adif/predict", response_model=PredictionOutput, response_model_exclude_none=True)
def adif_predict(payload: Input):
    rows, identities = [], []
    for train in payload.data:
        if train.cancelled or train.stopType == "destination" or re.search(r"cancel|suprimid|anulad", train.status, re.I):
            continue
        if (train.trafficType == "B" or train.trainNumber.upper().startswith("BUS")
                or train.platform.strip().upper() == "BUS"
                or re.search(r"\bbus|autobus|pullman|autoserv|autocors", train.carrier + " " + train.category, re.I)):
            continue
        features = live_features(payload.ts, train)
        rows.append(features)
        minute = features["scheduledMinute"]
        identities.append({"trainId": train.trainId, "clock": f"{minute//60:02d}:{minute%60:02d}"})
    if not rows:
        return PredictionOutput(predictions=[])
    return PredictionOutput(predictions=board_predictions(get_model(payload.station, MODELS_DIR), rows, identities))
