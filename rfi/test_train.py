import json
from pathlib import Path
import subprocess
import sys

import numpy as np
import polars as pl
import pytest
from catboost import CatBoostClassifier

from rfi.features import FEATURES, SCHEMA_VERSION, feature_frame
from rfi.train import chronological_split, train_station
from predict.board_models import PlatformPrior


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
    report = train_station("1728", data(), tmp_path, iterations=30, threads=1)
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
    report = train_station("1728", df, tmp_path, iterations=10, threads=1)
    assert report["test"]["accuracy"] == 0
    assert report["test"]["unseen_platforms"] == report["test"]["departures"]


def test_refit_preserves_backtest_and_learns_later_platforms(tmp_path):
    df = data().with_columns(pl.when(pl.col("serviceDate") >= "2026-04-11")
                            .then(pl.lit("NEW")).otherwise(pl.col("actualPlatform"))
                            .alias("actualPlatform"))
    report = train_station("1728", df, tmp_path, iterations=10, threads=1, refit=True)
    assert report["test"]["accuracy"] == 0
    assert report["refitted"]
    assert report["model_last_date"] == "2026-04-12"
    assert "NEW" not in report["classes"]
    assert "NEW" in report["model_classes"]


@pytest.mark.parametrize("days", [1, 2])
def test_short_histories_train_without_a_backtest(tmp_path, days):
    report = train_station("1", data(days), tmp_path, iterations=5, threads=1)
    assert report["status"] == "trained"
    assert report["model_kind"] == "catboost"
    assert report["backtest_status"] == "unavailable"
    assert "test" not in report
    assert report["model_departures"] == 10 * days
    assert (tmp_path / "1.cbm").exists()


def test_small_training_periods_are_evaluated_not_rejected(tmp_path):
    report = train_station("1", data(3), tmp_path, iterations=5, threads=1)
    assert report["status"] == "trained"
    assert report["backtest_status"] == "available"
    assert report["split_departures"]["train"] == 10


@pytest.mark.parametrize("df", [data().head(1), data().with_columns(pl.lit("20B").alias("actualPlatform"))])
def test_one_platform_is_saved_without_an_invented_second_platform(tmp_path, df):
    report = train_station("1", df, tmp_path, iterations=5, threads=1)
    assert report["status"] == "trained"
    assert report["model_kind"] == "platform_prior"
    assert not (tmp_path / "1.cbm").exists()
    model = PlatformPrior.load_model(tmp_path / "1.prior.json")
    assert list(model.classes_) == ["20B"]
    assert list(model.feature_names_) == FEATURES
    assert np.all(model.predict_proba(feature_frame(df)) == 1)


def test_constant_inputs_use_weighted_platform_frequencies(tmp_path):
    df = pl.concat([data().head(1)] * 3).with_columns(
        pl.Series("departureId", ["a", "b", "c"]), pl.Series("actualPlatform", ["20B", "20B", "2EST"]),
        pl.Series("weight", [0.5, 0.5, 1.0]))
    report = train_station("1", df, tmp_path, iterations=5, threads=1)
    assert report["model_kind"] == "platform_prior"
    model = PlatformPrior.load_model(tmp_path / "1.prior.json")
    assert dict(zip(model.classes_, model.probabilities)) == {"20B": 0.5, "2EST": 0.5}


def test_refit_learns_classes_missing_from_the_initial_single_platform_period(tmp_path):
    df = data(3).with_columns(pl.when(pl.col("serviceDate") == "2026-04-01")
                            .then(pl.lit("20B")).otherwise(pl.lit("2EST")).alias("actualPlatform"))
    report = train_station("1", df, tmp_path, iterations=5, threads=1, refit=True)
    assert report["classes"] == ["20B"]
    assert report["test"]["accuracy"] == 0
    assert report["model_kind"] == "catboost"
    assert set(report["model_classes"]) == {"20B", "2EST"}
    assert (tmp_path / "1.cbm").exists()


def test_model_kinds_replace_stale_artifacts_and_empty_data_is_not_fabricated(tmp_path):
    train_station("1", data(1).head(1), tmp_path, iterations=5, threads=1)
    train_station("1", data(1), tmp_path, iterations=5, threads=1)
    assert not (tmp_path / "1.prior.json").exists()
    train_station("1", data(1).head(1), tmp_path, iterations=5, threads=1)
    assert not (tmp_path / "1.cbm").exists()
    assert train_station("2", data().head(0), tmp_path)["status"] == "no_labels"
    assert not list(tmp_path.glob("2.*"))


def test_empty_evaluation_period_does_not_block_training(tmp_path):
    df = data(3).with_columns(pl.lit(2**40).alias("labelTimestamp"))
    report = train_station("1", df, tmp_path, iterations=5, threads=1)
    assert report["status"] == "trained"
    assert report["backtest_status"] == "unavailable"
    assert report["model_departures"] == 30


def test_parallel_cli_and_stale_model_cleanup(tmp_path):
    hive, models = tmp_path / "hive", tmp_path / "models"
    hive.mkdir()
    models.mkdir()
    (models / "999.cbm").touch()
    (models / "999.prior.json").touch()
    for station in ("1", "2"):
        folder = hive / f"station={station}"
        folder.mkdir()
        data().write_ipc(folder / "part0.arrow")
    (hive / "dataset.json").write_text(json.dumps({"schema_version": SCHEMA_VERSION, "station_rows": {"1": 240, "2": 240}}))
    subprocess.run([sys.executable, "-m", "rfi.train", "--data", str(hive), "--models", str(models),
                    "--iterations=5", "--threads=1", "--workers=2", "--refit"],
                   cwd=Path(__file__).resolve().parents[1], check=True, capture_output=True, timeout=45)
    assert {path.stem for path in models.glob("*.cbm")} == {"1", "2"}
    assert len(json.loads((models / "report.json").read_text())["stations"]) == 2
    assert not (models / "999.prior.json").exists()


def test_late_training_labels_do_not_cross_validation_boundary():
    df = data()
    boundary = df.filter(pl.col("serviceDate") == "2026-04-09")["scheduledTime"].min()
    df = df.with_columns(pl.when(pl.col("serviceDate") == "2026-04-08")
                         .then(pl.lit(boundary+1)).otherwise(pl.col("labelTimestamp"))
                         .alias("labelTimestamp"))
    train, _, _ = chronological_split(df)
    assert "2026-04-08" not in train["serviceDate"].to_list()
