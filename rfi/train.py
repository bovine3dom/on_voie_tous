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
from predict.board_models import MODEL_SUFFIXES, PlatformPrior
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


def fit_model(df, iterations, threads, validation=None):
    classes = df["actualPlatform"].unique().to_list()
    if len(classes) == 1 or feature_frame(df).n_unique() == 1:
        counts = df.group_by("actualPlatform").agg(pl.col("weight").sum()).sort("actualPlatform")
        return PlatformPrior(counts["actualPlatform"].to_list(), counts["weight"].to_numpy() / counts["weight"].sum())
    model = CatBoostClassifier(iterations=iterations, learning_rate=0.1, depth=6,
                              loss_function="MultiClass", eval_metric="MultiClass",
                              random_seed=1337, verbose=False, thread_count=threads,
                              allow_writing_files=False, has_time=True)
    known = validation.filter(pl.col("actualPlatform").is_in(classes)) if validation is not None else None
    kwargs = {"eval_set": pool(known), "early_stopping_rounds": 40,
              "use_best_model": True} if known is not None and not known.is_empty() else {}
    model.fit(pool(df.sort("timestamp")), **kwargs)
    return model


def train_station(station, df, output, iterations=100, threads=4, refit=False):
    report = {"station_id": station, "rows": df.height,
              "departures": df["departureId"].n_unique()}
    if df.is_empty():
        return report | {"status": "no_labels"}
    split = chronological_split(df)
    if split is not None:
        report["split_departures"] = {name: part["departureId"].n_unique()
                                      for name, part in zip(("train", "validation", "test"), split)}
    available = split is not None and all(not part.is_empty() for part in split)
    model = None
    if available:
        train, validation, test = split
        model = fit_model(train, iterations, threads, validation)
        report.update(backtest_status="available", classes=[str(x) for x in model.classes_],
                      training_last_date=train["serviceDate"].max(),
                      validation=metrics(model, train, validation), test=metrics(model, train, test))
    else:
        report.update(backtest_status="unavailable", backtest_reason="insufficient_chronological_periods")
    fitted = df if refit or not available else split[0]
    if refit or not available:
        model = fit_model(fitted, (model.tree_count_ or iterations) if model is not None else iterations, threads)
    report.update(status="trained", schema_version=SCHEMA_VERSION, features=FEATURES,
                  trees=model.tree_count_, model_kind="platform_prior" if isinstance(model, PlatformPrior) else "catboost",
                  refitted=refit and available, model_last_date=fitted["serviceDate"].max(),
                  model_departures=fitted["departureId"].n_unique(), model_classes=[str(x) for x in model.classes_])
    output.mkdir(parents=True, exist_ok=True)
    suffix = ".prior.json" if isinstance(model, PlatformPrior) else ".cbm"
    temporary = output / f"{station}{suffix}.tmp"
    model.save_model(str(temporary))
    temporary.replace(output / f"{station}{suffix}")
    for old in MODEL_SUFFIXES:
        if old != suffix:
            (output / f"{station}{old}").unlink(missing_ok=True)
    return report


def train_folder(job):
    folder, output, iterations, threads, refit = job
    station = extract_station_id(folder)
    report = train_station(station, load_station_data(folder), output, iterations, threads, refit)
    if report["status"] != "trained":
        for suffix in MODEL_SUFFIXES:
            (output / f"{station}{suffix}").unlink(missing_ok=True)
    return report


def main(root=ROOT):
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=root / "hive")
    parser.add_argument("--models", type=Path, default=root / "models")
    parser.add_argument("--stations", nargs="*", help="Optional station IDs")
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--refit", action="store_true", help="Refit on all data after held-out evaluation")
    args = parser.parse_args()
    if min(args.iterations, args.threads, args.workers) <= 0:
        parser.error("Training limits must be positive")
    dataset = json.loads((args.data / "dataset.json").read_text())
    if dataset["schema_version"] != SCHEMA_VERSION:
        parser.error("Unsupported dataset schema")
    args.models.mkdir(parents=True, exist_ok=True)
    if not args.stations:
        for suffix in MODEL_SUFFIXES:
            for path in args.models.glob("*" + suffix):
                if not dataset["station_rows"].get(path.name.removesuffix(suffix)):
                    path.unlink()
    jobs = [(folder, args.models, args.iterations, args.threads, args.refit)
            for folder in get_station_folders(str(args.data))
            if dataset["station_rows"].get(extract_station_id(folder))
            and (not args.stations or extract_station_id(folder) in args.stations)]
    reports = []
    with ProcessPoolExecutor(max_workers=args.workers, mp_context=get_context("spawn")) as executor:
        for report in executor.map(train_folder, jobs):
            reports.append(report)
            print(json.dumps(report), flush=True)
    reports.extend({"station_id": station, "rows": 0, "departures": 0, "status": "no_labels"}
                   for station, count in dataset["station_rows"].items()
                   if not count and (not args.stations or station in args.stations))
    summary = {"schema_version": SCHEMA_VERSION, "dataset": dataset, "stations": reports}
    temporary = args.models / "report.json.tmp"
    temporary.write_text(json.dumps(summary, indent=2) + "\n")
    temporary.replace(args.models / "report.json")
    print(f"Trained {sum(r['status'] == 'trained' for r in reports)} of {len(reports)} stations")


if __name__ == "__main__":
    main()
