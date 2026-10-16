from fastapi import HTTPException
import polars as pl

from .board_features import SCHEMA


def board_predictions(model, rows, identities, schema=SCHEMA):
    if list(model.feature_names_) != schema.features:
        raise HTTPException(status_code=409, detail="Incompatible board feature schema")
    probabilities = model.predict_proba(schema.frame(pl.DataFrame(rows)))
    predictions = []
    for identity, scores in zip(identities, probabilities):
        ranked = sorted(
            [{"platform": str(platform), "prob": float(score)}
             for platform, score in zip(model.classes_, scores)],
            key=lambda item: item["prob"], reverse=True,
        )
        if ranked:
            predictions.append(identity | {"platform": ranked[0]["platform"],
                                           "confidence": ranked[0]["prob"], "probabilities": ranked})
    return predictions
