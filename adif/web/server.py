import os
from pathlib import Path
from typing import Literal

from fastapi import HTTPException
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from predict.board_models import station_models
from predict.predict import app, get_model, TrainPrediction
from .predict import predict_board

MODELS_DIR = Path(os.getenv("ADIF_WEB_MODELS_DIR", Path(__file__).resolve().parent / "models"))


class Row(BaseModel):
    model_config = ConfigDict(str_max_length=4096)
    hora: str
    tren: str
    estacion: str
    via: str
    horaEstado: str = ""
    trenDatosOp: str = ""
    estacionLink: str = ""
    observation: str = ""
    immediate: bool = False


class Board(BaseModel):
    date: str = Field(max_length=64)
    time: str = Field(max_length=16)
    horarios: list[Row] = Field(max_length=128)
    error: bool = False


class Input(BaseModel):
    ts: AwareDatetime
    station: str = Field(pattern=r"^[0-9]{5}$")
    traffic: Literal["cercanias", "avldmd"]
    data: Board


class Prediction(TrainPrediction):
    rowIndex: int = Field(ge=0)
    trainNumber: str
    departureTime: AwareDatetime


class Output(BaseModel):
    predictions: list[Prediction]
    issues: dict[str, int]


@app.get("/adif-web/stations")
def available_stations():
    return {"stations": station_models(MODELS_DIR)}


@app.post("/adif-web/predict", response_model=Output, response_model_exclude_none=True)
def predict(payload: Input):
    if payload.data.error:
        raise HTTPException(422, "ADIF returned an error")
    if not payload.data.horarios:
        return {"predictions": [], "issues": {}}
    model = get_model(payload.station, MODELS_DIR)
    try:
        return predict_board(model, payload.station, payload.traffic, payload.data.model_dump(), payload.ts)
    except (ValueError, KeyError, TypeError) as error:
        raise HTTPException(422, "Invalid ADIF website board") from error
