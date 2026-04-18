import os
import polars as pl
from pydantic import BaseModel
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from catboost import CatBoostClassifier
from datetime import datetime
from typing import Optional
import uvicorn

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

model = CatBoostClassifier()
model.load_model("sncf_model.cbm")
feature_names = model.feature_names_

HOST = os.getenv("PREDICT_HOST", "0.0.0.0")
PORT = int(os.getenv("PREDICT_PORT", "8000"))


def normalise_sncf_data(raw_payload: dict) -> pl.DataFrame:
    df = pl.DataFrame(raw_payload["data"])
    df = df.with_columns(pl.lit(raw_payload["station"]).alias("station"))
    root_ts = int(datetime.fromisoformat(raw_payload["ts"]).timestamp())
    df = df.with_columns(pl.lit(root_ts).alias("timestamp"))
    df = df.unnest("platform", "traffic").drop("eventLevel").unnest("informationStatus")
    df = df.rename(
        {
            "track": "predictedPlatform",
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
    categorical_cols = [
        "station",
        "predictedPlatform",
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
    for col in categorical_cols:
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


class PredictionOutput(BaseModel):
    predictions: list[TrainPrediction]


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/predict", response_model=PredictionOutput)
def predict(payload: PredictionInput):
    payload_dict = payload.model_dump()
    df = normalise_sncf_data(payload_dict).fill_null("MISSING")
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

        predictions.append(
            TrainPrediction(
                platform=batch_prediction[i][0],
                confidence=float(batch_probability[i].max()),
                probabilities=probs,
            )
        )

    return PredictionOutput(predictions=predictions)


if __name__ == "__main__":
    import json

    raw_payload = """
{"ts":"2026-04-17T12:51:57.985068453+00:00","station":"0087756056","data":[{"TrafficDetailsUrl":"https://www.sncf-voyageurs.com/fr/voyagez-avec-nous/horaires-et-itineraires/recherche-de-train/detail-train/?numeroCirculation=881227&dateCirculation=2026-04-17&destinationCode=87756056","actualTime":"2026-04-17T13:33:00+00:00","alternativeMeans":null,"direction":"Departure","informationStatus":{"delay":null,"eventLevel":"Warning","trainStatus":"SUPPRESSION_TOTALE"},"isGL":false,"missionCode":null,"platform":{"backgroundColor":null,"isTrackactive":false,"track":"","trackGroupTitle":null,"trackGroupValue":null,"trackPosition":null},"presentation":{"colorCode":"#0749ff","textColorCode":"#FFFFFF"},"scheduledTime":"2026-04-17T12:03:00+00:00","shortTermInformations":[],"stationName":"Nice","statusModification":null,"stops":[],"traffic":{"destination":"Menton","eventLevel":"Warning","eventStatus":"SUPPRESSION","oldDestination":"","oldOrigin":"","origin":"Les Arcs - Draguignan"},"trainLine":null,"trainMode":"TRAIN","trainNumber":"881227","trainType":"ZOU !","uic":"0087756056"},{"TrafficDetailsUrl":"https://www.sncf-voyageurs.com/fr/voyagez-avec-nous/horaires-et-itineraires/recherche-de-train/detail-train/?numeroCirculation=86045&dateCirculation=2026-04-17&destinationCode=87756056","actualTime":"2026-04-17T12:54:00+00:00","alternativeMeans":null,"direction":"Departure","informationStatus":{"delay":null,"eventLevel":"Normal","trainStatus":"Ontime"},"isGL":false,"missionCode":null,"platform":{"backgroundColor":null,"isTrackactive":true,"track":"D","trackGroupTitle":null,"trackGroupValue":null,"trackPosition":null},"presentation":{"colorCode":"#0749ff","textColorCode":"#FFFFFF"},"scheduledTime":"2026-04-17T12:54:00+00:00","shortTermInformations":[],"stationName":"Nice","statusModification":null,"stops":[],"traffic":{"destination":"Ventimiglia","eventLevel":"Normal","eventStatus":"Ontime","oldDestination":"","oldOrigin":"","origin":"Grasse"},"trainLine":null,"trainMode":"TRAIN","trainNumber":"86045","trainType":"ZOU !","uic":"0087756056"}]}
"""
    payload = json.loads(raw_payload)
    df2 = normalise_sncf_data(payload).fill_null("MISSING")
    df2 = df2[feature_names]
    prediction = model.predict(df2)
    probability = model.predict_proba(df2)
    prob_df = pl.DataFrame(probability, schema=[str(c) for c in model.classes_])
    df_with_probs = pl.concat([df2, prob_df], how="horizontal")
    ranked_probs = pl.concat_list(
        [
            pl.struct(pl.col(str(cls)).alias("prob"), pl.lit(str(cls)).alias("class"))
            for cls in model.classes_
        ]
    ).list.sort(descending=True)
    pl.Config(tbl_rows=20)
    dfpls = df_with_probs.with_columns(rankings=ranked_probs)
    print(dfpls[["predictedDestination", "predictedPlatform", "rankings"]])

    uvicorn.run(app, host=HOST, port=PORT)
