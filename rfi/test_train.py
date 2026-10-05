import json
from pathlib import Path
import subprocess
import sys

import polars as pl
from catboost import CatBoostClassifier

from rfi.features import FEATURES, SCHEMA_VERSION, feature_frame
from rfi.train import chronological_split, train_station


def data(days=12):
    rows = []
    for day in range(1, days + 1):
        for train in range(10):
            for horizon in (15, 30):
                scheduled = 1775000000 + day * 86400 + train * 60
                rows.append(dict(
                    departureId=f"{day}|{train}", serviceDate=f"2026-04-{day:02d}",
                    timestamp=scheduled-horizon*60, scheduledTime=scheduled,
                    labelTimestamp=scheduled, horizon=horizon, trainNumber=str(train),
                    predictedPlatform=("2EST" if train % 2 else "20B") if train < 5 else "MISSING",
                    predictedDestination="ROMA" if train % 2 else "MILANO",
                    carrier="TRENITALIA", trainType="REG", delayMinutes=0,
                    scheduledMinute=700+train, dayOfWeek=(day-1) % 7+1, month=4,
                    leadMinutes=float(horizon), weight=0.5,
                    actualPlatform="2EST" if train % 2 else "20B"))
    return pl.DataFrame(rows)


def test_split_keeps_departures_and_dates_together():
    split = chronological_split(data())
    ids = [set(part["departureId"]) for part in split]
    assert all(not ids[i] & ids[j] for i in range(3) for j in range(i+1, 3))
    assert split[0]["serviceDate"].max() < split[1]["serviceDate"].min()
    assert split[1]["serviceDate"].max() < split[2]["serviceDate"].min()
    assert chronological_split(data(2)) is None


def test_training_and_string_platform_round_trip(tmp_path):
    report = train_station("1728", data(), tmp_path, iterations=30, threads=1, minimum=10)
    assert report["status"] == "trained"
    assert set(report["classes"]) == {"2EST", "20B"}
    assert report["test"]["accuracy"] == 1
    assert report["test"]["baseline_accuracy"] == 1
    assert report["test"]["blank"]["departures"] == 10
    assert report["test"]["published"]["departures"] == 10
    assert report["test"]["official_accuracy"] == 1
    model = CatBoostClassifier()
    model.load_model(str(tmp_path / "1728.cbm"))
    assert list(model.feature_names_) == FEATURES
    assert model.tree_count_ > 1
    assert model.predict_proba(feature_frame(data())).shape == (240, 2)


def test_unknown_test_platform_is_counted_as_wrong(tmp_path):
    df = data().with_columns(pl.when(pl.col("serviceDate") >= "2026-04-11")
                            .then(pl.lit("NEW")).otherwise(pl.col("actualPlatform"))
                            .alias("actualPlatform"))
    report = train_station("1728", df, tmp_path, iterations=10, threads=1, minimum=10)
    assert report["test"]["accuracy"] == 0
    assert report["test"]["unseen_platforms"] == report["test"]["departures"]


def test_refit_preserves_backtest_and_learns_later_platforms(tmp_path):
    df = data().with_columns(pl.when(pl.col("serviceDate") >= "2026-04-11")
                            .then(pl.lit("NEW")).otherwise(pl.col("actualPlatform"))
                            .alias("actualPlatform"))
    report = train_station("1728", df, tmp_path, iterations=10, threads=1, minimum=10, refit=True)
    assert report["test"]["accuracy"] == 0
    assert report["refitted"]
    assert report["model_last_date"] == "2026-04-12"
    assert "NEW" not in report["classes"]
    assert "NEW" in report["model_classes"]


def test_single_platform_and_small_training_sets_are_not_saved(tmp_path):
    report = train_station("1", data().with_columns(pl.lit("20B").alias("actualPlatform")),
                           tmp_path, minimum=10)
    assert report["status"] == "single_platform"
    assert not (tmp_path / "1.cbm").exists()
    assert train_station("1", data(), tmp_path, minimum=1000)["status"] == "insufficient_training_data"


def test_parallel_cli_and_stale_model_cleanup(tmp_path):
    hive, models = tmp_path / "hive", tmp_path / "models"
    hive.mkdir()
    models.mkdir()
    (models / "999.cbm").touch()
    for station in ("1", "2"):
        folder = hive / f"station={station}"
        folder.mkdir()
        data().write_ipc(folder / "part0.arrow")
    (hive / "dataset.json").write_text(json.dumps({"schema_version": SCHEMA_VERSION, "station_rows": {"1": 240, "2": 240}}))
    subprocess.run([sys.executable, "-m", "rfi.train", "--data", str(hive), "--models", str(models),
                    "--iterations=5", "--threads=1", "--workers=2", "--min-trains=10", "--refit"],
                   cwd=Path(__file__).resolve().parents[1], check=True, capture_output=True, timeout=45)
    assert {path.stem for path in models.glob("*.cbm")} == {"1", "2"}
    assert len(json.loads((models / "report.json").read_text())["stations"]) == 2


def test_late_training_labels_do_not_cross_validation_boundary():
    df = data()
    boundary = df.filter(pl.col("serviceDate") == "2026-04-09")["scheduledTime"].min()
    df = df.with_columns(pl.when(pl.col("serviceDate") == "2026-04-08")
                         .then(pl.lit(boundary+1)).otherwise(pl.col("labelTimestamp"))
                         .alias("labelTimestamp"))
    train, _, _ = chronological_split(df)
    assert "2026-04-08" not in train["serviceDate"].to_list()
