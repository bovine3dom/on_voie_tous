import argparse
import json
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context
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
    keys = [name for name in CAT_COLS if name != "predictedPlatform"]
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
    # Keep blank, published, and revised platforms separate at the audit horizon.
    evaluation = evaluation.filter(pl.col("horizon") == 30)
    if evaluation.is_empty():
        return {"departures": 0}
    probabilities = model.predict_proba(feature_frame(evaluation))
    predicted = np.asarray(model.classes_)[probabilities.argmax(axis=1)].astype(str)
    labels = np.array(evaluation["actualPlatform"].to_list())
    correct = predicted == labels
    accepted = probabilities.max(axis=1) >= confidence
    historical = baseline(train, evaluation) == labels
    published = np.array(evaluation["predictedPlatform"].to_list()) != "MISSING"
    changed = published & (np.array(evaluation["predictedPlatform"].to_list()) != labels)
    unseen = ~np.isin(labels, model.classes_)

    def measure(mask):
        n = int(mask.sum())
        if not n:
            return {"departures": 0}
        selected = mask & accepted
        return {
            "departures": n,
            "accuracy": float(correct[mask].mean()),
            "baseline_accuracy": float(historical[mask].mean()),
            "confidence_threshold": confidence,
            "coverage": float(accepted[mask].mean()),
            "selective_accuracy": float(correct[selected].mean()) if selected.any() else None,
            "unseen_platforms": int(unseen[mask].sum()),
        }

    return measure(np.ones(len(labels), dtype=bool)) | {
        "blank": measure(~published),
        "published": measure(published),
        "changed": measure(changed),
        "official_accuracy": float((~changed[published]).mean()) if published.any() else None,
    }


def train_station(station, df, output, iterations=100, threads=4, minimum=50, refit=False):
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
                              loss_function="MultiClass", eval_metric="MultiClass",
                              random_seed=1337, verbose=False, thread_count=threads,
                              allow_writing_files=False, has_time=True)
    kwargs = {"eval_set": pool(known_validation), "early_stopping_rounds": 40,
              "use_best_model": True} if not known_validation.is_empty() else {}
    model.fit(pool(train.sort("timestamp")), **kwargs)
    report.update(status="trained", schema_version=SCHEMA_VERSION,
                  features=FEATURES, classes=[str(x) for x in model.classes_],
                  trees=model.tree_count_, training_last_date=train["serviceDate"].max(),
                  validation=metrics(model, train, validation), test=metrics(model, train, test))
    if refit:
        params = model.get_params() | {"iterations": max(1, model.tree_count_)}
        model = CatBoostClassifier(**params)
        model.fit(pool(df.sort("timestamp")))
    report["refitted"] = refit
    report["model_last_date"] = df["serviceDate"].max() if refit else report["training_last_date"]
    report["model_classes"] = [str(x) for x in model.classes_]
    output.mkdir(parents=True, exist_ok=True)
    temporary = output / f"{station}.cbm.tmp"
    model.save_model(str(temporary), format="cbm")
    temporary.replace(output / f"{station}.cbm")
    return report


def train_folder(job):
    folder, output, iterations, threads, minimum, refit = job
    station = extract_station_id(folder)
    report = train_station(station, load_station_data(folder), output, iterations, threads, minimum, refit)
    if report["status"] != "trained":
        (output / f"{station}.cbm").unlink(missing_ok=True)
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=ROOT / "hive")
    parser.add_argument("--models", type=Path, default=ROOT / "models")
    parser.add_argument("--stations", nargs="*", help="Optional station IDs")
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--min-trains", type=int, default=50)
    parser.add_argument("--refit", action="store_true", help="Refit on all data after held-out evaluation")
    args = parser.parse_args()
    if min(args.iterations, args.threads, args.min_trains, args.workers) <= 0:
        parser.error("Training limits must be positive")
    dataset = json.loads((args.data / "dataset.json").read_text())
    if dataset["schema_version"] != SCHEMA_VERSION:
        parser.error("Unsupported dataset schema")
    args.models.mkdir(parents=True, exist_ok=True)
    if not args.stations:
        for path in args.models.glob("*.cbm"):
            if not dataset["station_rows"].get(path.stem):
                path.unlink()
    jobs = [(folder, args.models, args.iterations, args.threads, args.min_trains, args.refit)
            for folder in get_station_folders(str(args.data))
            if dataset["station_rows"].get(extract_station_id(folder))
            and (not args.stations or extract_station_id(folder) in args.stations)]
    reports = []
    with ProcessPoolExecutor(max_workers=args.workers, mp_context=get_context("spawn")) as executor:
        for report in executor.map(train_folder, jobs):
            reports.append(report)
            print(json.dumps(report), flush=True)
    summary = {"schema_version": SCHEMA_VERSION, "dataset": dataset, "stations": reports}
    temporary = args.models / "report.json.tmp"
    temporary.write_text(json.dumps(summary, indent=2) + "\n")
    temporary.replace(args.models / "report.json")
    print(f"Trained {sum(r['status'] == 'trained' for r in reports)} of {len(reports)} stations")


if __name__ == "__main__":
    main()
