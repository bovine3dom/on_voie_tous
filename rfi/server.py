import os
import re
from pathlib import Path

import polars as pl
from fastapi import HTTPException
from pydantic import AwareDatetime, BaseModel, Field
import uvicorn

from predict.predict import app, get_model, HOST, PORT, TrainPrediction
from .features import FEATURES, feature_frame, live_features

MODELS_DIR = Path(os.getenv("RFI_MODELS_DIR", Path(__file__).resolve().parent / "models"))


class Train(BaseModel):
    trainId: str = Field(min_length=1, max_length=200)
    trainNumber: str
    clock: str = Field(pattern=r"^(?:[01]\d|2[0-3]):[0-5]\d$")
    destination: str
    carrier: str = ""
    category: str = ""
    delayMinutes: int = Field(default=0, ge=-1, le=719)
    platform: str = ""
    cancelled: bool = False


class Input(BaseModel):
    ts: AwareDatetime
    station: str = Field(pattern=r"^[0-9]{1,12}$")
    data: list[Train] = Field(max_length=100)


class Prediction(TrainPrediction):
    trainId: str
    clock: str


class Output(BaseModel):
    predictions: list[Prediction]


@app.get("/rfi/stations")
def available_stations():
    return {"stations": sorted(path.stem for path in MODELS_DIR.glob("*.cbm") if path.stem.isdecimal()),
            "leadMinutes": [15, 130]}


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
        if not 15 <= features["leadMinutes"] <= 130:
            continue
        selected.append(train)
        rows.append(features)
    if not rows:
        return Output(predictions=[])
    model = get_model(payload.station, MODELS_DIR)
    if list(model.feature_names_) != FEATURES:
        raise HTTPException(status_code=409, detail="Incompatible RFI feature schema")
    probabilities = model.predict_proba(feature_frame(pl.DataFrame(rows)))
    predictions = []
    for train, scores in zip(selected, probabilities):
        ranked = sorted(
            [{"platform": str(platform), "prob": float(score)}
             for platform, score in zip(model.classes_, scores)],
            key=lambda item: item["prob"], reverse=True,
        )
        if not ranked:
            continue
        predictions.append(Prediction(trainId=train.trainId, clock=train.clock,
                                      platform=ranked[0]["platform"], confidence=ranked[0]["prob"],
                                      probabilities=ranked))
    return Output(predictions=predictions)


if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT, access_log=False)
