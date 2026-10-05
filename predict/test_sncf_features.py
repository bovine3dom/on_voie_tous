from datetime import datetime

import polars as pl
import pytest

from predict.sncf_features import CAT_COLS, FEATURES, feature_frame, time_features
from predict.model import train_station_model


def epoch(value):
    return int(datetime.fromisoformat(value).timestamp())


def times(scheduled, observed, predicted):
    return pl.DataFrame({
        "scheduledTime": [epoch(scheduled)],
        "timestamp": [epoch(observed)],
        "predictedTime": [epoch(predicted)],
    }, schema={name: pl.UInt32 for name in ("scheduledTime", "timestamp", "predictedTime")})


@pytest.mark.parametrize("scheduled", [
    "2026-03-28T07:00:00Z", "2026-03-29T06:00:00Z",
    "2026-10-24T06:00:00Z", "2026-10-25T07:00:00Z",
])
def test_same_local_clock_across_dst(scheduled):
    row = time_features(times(scheduled, scheduled, scheduled)).row(0, named=True)
    assert row["scheduledMinute"] == 480
    assert row["leadMinutes"] == row["delayMinutes"] == 0


@pytest.mark.parametrize("scheduled,observed,predicted,minute,weekday,month,lead,delay", [
    ("2026-03-29T01:15:00Z", "2026-03-29T00:45:00Z", "2026-03-29T01:10:00Z", 195, 7, 3, 30, -5),
    ("2026-10-25T01:15:00Z", "2026-10-25T00:45:00Z", "2026-10-25T01:20:00Z", 135, 7, 10, 30, 5),
    ("2026-06-30T22:15:00Z", "2026-06-30T22:30:00Z", "2026-06-30T22:15:00Z", 15, 3, 7, -15, 0),
])
def test_local_calendar_and_absolute_durations(scheduled, observed, predicted, minute, weekday, month, lead, delay):
    row = time_features(times(scheduled, observed, predicted)).row(0, named=True)
    assert [row[c] for c in FEATURES[-5:]] == [minute, weekday, month, lead, delay]


def test_repeated_hour_retains_distinct_instants():
    frames = [times(s, "2026-10-24T23:45:00Z", s) for s in
              ("2026-10-25T00:15:00Z", "2026-10-25T01:15:00Z")]
    df = time_features(pl.concat(frames))
    assert df["scheduledMinute"].to_list() == [135, 135]
    assert df["leadMinutes"].to_list() == [30, 90]


def test_training_schema_and_model_roundtrip(tmp_path):
    from catboost import CatBoostClassifier

    df = times("2026-03-29T06:00:00Z", "2026-03-29T05:30:00Z", "2026-03-29T06:00:00Z")
    df = df.with_columns(pl.lit(None, dtype=pl.String).alias(c) for c in CAT_COLS)
    df = pl.concat([df.with_columns(pl.lit(p).alias("actualPlatform"), pl.lit(p).alias("trainNumber")) for p in ["1", "2"]])
    model = train_station_model("0087271734", df)
    path = str(tmp_path / "model.cbm")
    model.save_model(path)
    loaded = CatBoostClassifier()
    loaded.load_model(path)
    assert loaded.feature_names_ == FEATURES
    assert loaded.get_metadata()["sncf_schema_version"] == "2"
    assert loaded.predict(feature_frame(df)).shape == (2, 1)
