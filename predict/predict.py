import os
import polars as pl
from pydantic import BaseModel, ValidationError
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from catboost import CatBoostClassifier
from datetime import datetime
from typing import Literal
from pathlib import Path
import uvicorn

if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from predict.server import app, HOST, PORT
    uvicorn.run(app, host=HOST, port=PORT, access_log=False)
    raise SystemExit

from .board_models import PlatformPrior

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

MODELS_DIR = os.getenv("SNCF_MODELS_DIR", os.path.join(os.path.dirname(__file__), "models"))
_model_cache: dict[tuple[str, str], CatBoostClassifier | PlatformPrior] = {}

CATEGORICAL_COLS = [
    "station",
    "predictedPlatform",
    "predictedTrackGroupValue",
    "predictedTrackGroupTitle",
    "predictedDestination",
    "predictedOrigin",
    "scheduledDestination",
    "scheduledOrigin",
    "trainLine",
    "trainMode",
    "trainNumber",
    "trainType",
    "trainStatus",
]

UNKNOWN_PLATFORM = "???"


def get_model(station_id: str, models_dir=None) -> CatBoostClassifier | PlatformPrior:
    if not station_id.isascii() or not station_id.isdecimal():
        raise HTTPException(status_code=400, detail="Invalid station ID")
    directory = os.path.abspath(MODELS_DIR if models_dir is None else models_dir)
    key = (directory, station_id)
    if key in _model_cache:
        return _model_cache[key]

    model_path = os.path.join(directory, f"{station_id}.cbm")
    prior_path = os.path.join(directory, f"{station_id}.prior.json")
    if os.path.exists(model_path):
        model = CatBoostClassifier()
        model.load_model(model_path)
    elif models_dir is not None and os.path.exists(prior_path):
        model = PlatformPrior.load_model(prior_path)
    else:
        raise HTTPException(status_code=404, detail=f"No model found for station {station_id}")
    _model_cache[key] = model
    return model


HOST = os.getenv("PREDICT_HOST", "0.0.0.0")
PORT = int(os.getenv("PREDICT_PORT", "8000"))
Operator = Literal["sncf", "rfi", "adif"]


def normalise_sncf_data(raw_payload: dict, feature_names: list[str]) -> pl.DataFrame:
    df = pl.DataFrame(raw_payload["data"])
    if "station" in feature_names:
        df = df.with_columns(pl.lit(raw_payload["station"]).alias("station"))
    root_ts = int(datetime.fromisoformat(raw_payload["ts"]).timestamp())
    df = df.with_columns(pl.lit(root_ts).alias("timestamp"))
    df = df.unnest("platform", "traffic").drop("eventLevel").unnest("informationStatus")
    df = df.rename(
        {
            "track": "predictedPlatform",
            "trackGroupValue": "predictedTrackGroupValue",
            "trackGroupTitle": "predictedTrackGroupTitle",
            "destination": "predictedDestination",
            "origin": "predictedOrigin",
            "actualTime": "predictedTime",
        }
    )
    for col in ["predictedTime", "scheduledTime"]:
        df = df.with_columns(
            (pl.col(col).str.to_datetime(time_zone="UTC").dt.epoch() // 1_000_000).cast(
                pl.UInt32
            )
        )
    df = df.with_columns(
        pl.lit("MISSING").alias(c) for c in feature_names if c not in df.columns
    )
    for col in CATEGORICAL_COLS:
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.String))
    return df.select(feature_names)


class PlatformProbability(BaseModel):
    platform: str
    prob: float


class PredictionInput(BaseModel):
    ts: str
    station: str
    data: list


class TrainPrediction(BaseModel):
    platform: str
    confidence: float
    probabilities: list[PlatformProbability]
    trainId: str | None = None
    clock: str | None = None


class PredictionOutput(BaseModel):
    predictions: list[TrainPrediction]


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/stations")
def available_stations(operator: Operator = "sncf"):
    if operator == "rfi":
        from rfi.server import available_stations as catalog
        return catalog()
    if operator == "adif":
        from adif.server import available_stations as catalog
        return catalog()
    return {"stations": sorted(path.stem for path in Path(MODELS_DIR).glob("*.cbm") if path.stem.isdecimal())}


@app.post("/predict", response_model=PredictionOutput, response_model_exclude_none=True)
def predict(payload: PredictionInput, operator: Operator = "sncf"):
    if operator != "sncf":
        if operator == "rfi":
            from rfi.server import Input, rfi_predict as forecast
        else:
            from adif.server import Input, adif_predict as forecast
        try:
            return forecast(Input.model_validate(payload.model_dump()))
        except ValidationError as error:
            raise RequestValidationError(error.errors()) from error
    payload_dict = payload.model_dump()
    station_id = payload_dict["station"]

    model = get_model(station_id)
    feature_names = list(model.feature_names_)

    df = normalise_sncf_data(payload_dict, feature_names).fill_null("MISSING")
    df = df[feature_names]

    num_trains = df.height
    predictions = []

    batch_prediction = model.predict(df)
    batch_probability = model.predict_proba(df)

    for i in range(num_trains):
        probs = []
        for j, cls in enumerate(model.classes_):
            probs.append({"platform": str(cls), "prob": float(batch_probability[i][j])})

        probs.sort(key=lambda x: x["prob"], reverse=True)
        probs = [p for p in probs if p["platform"] != UNKNOWN_PLATFORM]

        predictions.append(
            TrainPrediction(
                platform=batch_prediction[i][0],
                confidence=float(batch_probability[i].max()),
                probabilities=probs,
            )
        )

    return PredictionOutput(predictions=predictions)
