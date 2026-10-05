import argparse
import json
from collections import Counter
from pathlib import Path

import numpy as np
import polars as pl
from catboost import CatBoostClassifier, Pool

from predict.model import get_station_folders, extract_station_id, load_station_data
from .features import CAT_COLS, FEATURES, SCHEMA_VERSION, feature_frame

ROOT = Path(__file__).resolve().parent


def chronological_split(df: pl.DataFrame):
    days = sorted(df["serviceDate"].unique().to_list())
    if len(days) < 3:
        return None
    first = min(max(1, int(len(days) * 0.7)), len(days) - 2)
    second = min(max(first + 1, int(len(days) * 0.85)), len(days) - 1)
    train = df.filter(pl.col("serviceDate") < days[first])
    validation = df.filter((pl.col("serviceDate") >= days[first]) & (pl.col("serviceDate") < days[second]))
    test = df.filter(pl.col("serviceDate") >= days[second])
    # A target must be available before the next evaluation period starts.
    train = train.filter(pl.col("labelTimestamp") < validation["scheduledTime"].min())
    validation = validation.filter(pl.col("labelTimestamp") < test["scheduledTime"].min())
    return train, validation, test


def pool(df: pl.DataFrame) -> Pool:
    return Pool(feature_frame(df), label=df["actualPlatform"].to_list(),
                cat_features=CAT_COLS, weight=df["weight"].to_list())


def baseline(train: pl.DataFrame, evaluation: pl.DataFrame):
    keys = CAT_COLS
    departures = train.unique("departureId")
    common = Counter(departures["actualPlatform"].to_list()).most_common(1)[0][0]
    grouped = {}
    for row in departures.iter_rows(named=True):
        grouped.setdefault(tuple(row[k] for k in keys), Counter())[row["actualPlatform"]] += 1
    return np.array([
        grouped[key].most_common(1)[0][0] if key in grouped else common
        for key in evaluation.select(keys).iter_rows()
    ])


def metrics(model, train, evaluation, confidence=0.8):
    # The selection audit and primary usefulness measure use 30-minute cases.
    evaluation = evaluation.filter(pl.col("horizon") == 30)
    if evaluation.is_empty():
        return {"departures": 0}
    probabilities = model.predict_proba(feature_frame(evaluation))
    predicted = np.asarray(model.classes_)[probabilities.argmax(axis=1)].astype(str)
    labels = np.array(evaluation["actualPlatform"].to_list())
    correct = predicted == labels
    accepted = probabilities.max(axis=1) >= confidence
    return {
        "departures": evaluation.height,
        "accuracy": float(correct.mean()),
        "baseline_accuracy": float((baseline(train, evaluation) == labels).mean()),
        "confidence_threshold": confidence,
        "coverage": float(accepted.mean()),
        "selective_accuracy": float(correct[accepted].mean()) if accepted.any() else None,
        "unseen_platforms": int((~np.isin(labels, model.classes_)).sum()),
    }


def train_station(station, df, output, iterations=300, threads=4, minimum=50):
    report = {"station_id": station, "rows": df.height,
              "departures": df["departureId"].n_unique()}
    split = chronological_split(df)
    if split is None:
        return report | {"status": "insufficient_days"}
    train, validation, test = split
    report["split_departures"] = {name: part["departureId"].n_unique()
                                  for name, part in zip(("train", "validation", "test"), split)}
    if any(part.is_empty() for part in split) or train["departureId"].n_unique() < minimum:
        return report | {"status": "insufficient_training_data"}
    classes = train["actualPlatform"].unique().to_list()
    if len(classes) < 2:
        return report | {"status": "single_platform"}
    known_validation = validation.filter(pl.col("actualPlatform").is_in(classes))
    model = CatBoostClassifier(iterations=iterations, learning_rate=0.1, depth=6,
                              loss_function="MultiClass", eval_metric="Accuracy",
                              random_seed=1337, verbose=False, thread_count=threads,
                              allow_writing_files=False, has_time=True)
    kwargs = {"eval_set": pool(known_validation), "early_stopping_rounds": 40,
              "use_best_model": True} if not known_validation.is_empty() else {}
    model.fit(pool(train.sort("timestamp")), **kwargs)
    report.update(status="trained", schema_version=SCHEMA_VERSION,
                  features=FEATURES, classes=[str(x) for x in model.classes_],
                  trees=model.tree_count_, training_last_date=train["serviceDate"].max(),
                  validation=metrics(model, train, validation), test=metrics(model, train, test))
    output.mkdir(parents=True, exist_ok=True)
    temporary = output / f"{station}.cbm.tmp"
    model.save_model(str(temporary), format="cbm")
    temporary.replace(output / f"{station}.cbm")
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=ROOT / "hive")
    parser.add_argument("--models", type=Path, default=ROOT / "models")
    parser.add_argument("--stations", nargs="*", help="Optional station IDs")
    parser.add_argument("--iterations", type=int, default=300)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--min-trains", type=int, default=50)
    args = parser.parse_args()
    if min(args.iterations, args.threads, args.min_trains) <= 0:
        parser.error("Training limits must be positive")
    dataset = json.loads((args.data / "dataset.json").read_text())
    if dataset["schema_version"] != SCHEMA_VERSION:
        parser.error("Unsupported dataset schema")
    args.models.mkdir(parents=True, exist_ok=True)
    reports = []
    for folder in get_station_folders(str(args.data)):
        station = extract_station_id(folder)
        if not dataset["station_rows"].get(station) or (args.stations and station not in args.stations):
            continue
        df = load_station_data(folder)
        report = train_station(station, df, args.models, args.iterations, args.threads, args.min_trains)
        if report["status"] != "trained":
            (args.models / f"{station}.cbm").unlink(missing_ok=True)
        reports.append(report)
        print(json.dumps(report), flush=True)
    summary = {"schema_version": SCHEMA_VERSION, "dataset": dataset, "stations": reports}
    temporary = args.models / "report.json.tmp"
    temporary.write_text(json.dumps(summary, indent=2) + "\n")
    temporary.replace(args.models / "report.json")
    print(f"Trained {sum(r['status'] == 'trained' for r in reports)} of {len(reports)} stations")


if __name__ == "__main__":
    main()
