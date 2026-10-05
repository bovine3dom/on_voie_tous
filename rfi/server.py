import os
import re
from pathlib import Path

from pydantic import AwareDatetime, BaseModel, Field
import uvicorn

from predict.predict import app, get_model, HOST, PORT, TrainPrediction
from predict.board_api import board_predictions
from predict.board_models import station_models
from .features import live_features

MODELS_DIR = Path(os.getenv("RFI_MODELS_DIR", Path(__file__).resolve().parent / "models"))


class Train(BaseModel):
    trainId: str = Field(min_length=1)
    trainNumber: str
    clock: str = Field(pattern=r"^(?:[01]\d|2[0-3]):[0-5]\d$")
    destination: str
    carrier: str = ""
    category: str = ""
    delayMinutes: int = 0
    platform: str = ""
    cancelled: bool = False


class Input(BaseModel):
    ts: AwareDatetime
    station: str = Field(pattern=r"^[0-9]{1,12}$")
    data: list[Train]


class Prediction(TrainPrediction):
    trainId: str
    clock: str


class Output(BaseModel):
    predictions: list[Prediction]


@app.get("/rfi/stations")
def available_stations():
    return {"stations": station_models(MODELS_DIR)}


@app.post("/rfi/predict", response_model=Output)
def rfi_predict(payload: Input):
    selected, rows = [], []
    for train in payload.data:
        if train.cancelled:
            continue
        if re.search(r"\bbus|autobus|pullman|autoserv|autocors", train.carrier + " " + train.category, re.I):
            continue
        try:
            features = live_features(payload.ts, train)
        except ValueError:
            continue
        selected.append(train)
        rows.append(features)
    if not rows:
        return Output(predictions=[])
    identities = [{"trainId": train.trainId, "clock": train.clock} for train in selected]
    return Output(predictions=board_predictions(get_model(payload.station, MODELS_DIR), rows, identities))


if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT, access_log=False)
