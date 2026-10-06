import numpy as np
import polars as pl
import pytest
from polars.testing import assert_frame_equal

import rfi.train as trainer
from rfi.test_train import data


def repeated_data(days=12):
    df = data(days)
    # Both source observations stay inside the same minute bucket.
    df = df.with_columns((pl.col("timestamp") // 60 * 60).alias("timestamp"),
                         pl.lit(0.25).alias("weight"))
    later = df.with_columns((pl.col("timestamp") + 10).alias("timestamp"),
                            (pl.col("leadMinutes") - 1/6).alias("leadMinutes"))
    return pl.concat([later, df])


def test_compaction_preserves_real_rows_labels_states_and_departure_weights():
    df = repeated_data(1)
    # A changed feature inside one bucket must remain a separate row.
    changed = df.head(1).with_columns(pl.lit(99, dtype=pl.Int64).alias("delayMinutes"))
    df = pl.concat([df, changed])
    compacted, report = trainer.compact_rows(df, 60)
    assert report["raw_rows"] == 41
    assert report["fit_rows"] == 21
    assert report["departures"] == 10
    assert report["max_departure_weight_error"] <= 1e-9
    assert compacted["timestamp"].is_sorted()
    assert compacted.filter(pl.col("delayMinutes") == 99).height == 1
    assert compacted.filter(pl.col("delayMinutes") == 0)["weight"].to_list() == [0.5] * 20
    original = df.select(pl.exclude("weight")).rows()
    assert all(row in original for row in compacted.select(pl.exclude("weight")).rows())
    assert set(compacted["actualPlatform"]) == set(df["actualPlatform"])
    assert compacted.filter(pl.col("delayMinutes") == 0)["timestamp"].to_list() == sorted(set(df["timestamp"] // 60 * 60))


def test_disabled_empty_invalid_and_bucket_boundaries():
    df = repeated_data(1)
    result, report = trainer.compact_rows(df, 0)
    assert result is df
    assert report["fit_rows"] == report["raw_rows"]
    assert trainer.compact_rows(df.head(0), 60)[0].is_empty()
    with pytest.raises(ValueError):
        trainer.compact_rows(df, -1)
    assert trainer.compact_rows(df, 1)[0].height == df.height
    assert trainer.compact_rows(df, 60)[0].height == df.height // 2


@pytest.mark.parametrize("days,refit", [(12, False), (12, True), (2, False)])
def test_only_fit_inputs_are_compacted_after_split(tmp_path, monkeypatch, days, refit):
    df = repeated_data(days)
    original = df.clone()
    fits, evaluations = [], []
    fit_model, metrics = trainer.fit_model, trainer.metrics

    def fit(part, iterations, threads, validation=None):
        fits.append((part, validation))
        return fit_model(part, iterations, threads, validation)

    def evaluate(model, train, evaluation):
        evaluations.append((train, evaluation))
        return metrics(model, train, evaluation)

    monkeypatch.setattr(trainer, "fit_model", fit)
    monkeypatch.setattr(trainer, "metrics", evaluate)
    report = trainer.train_station("1", df, tmp_path, iterations=5, threads=1,
                                   refit=refit, compact_seconds=60)
    assert_frame_equal(df, original)
    split = trainer.chronological_split(df)
    if split:
        train, validation, test = split
        assert fits[0][0].height == train.height // 2
        assert fits[0][1].height == validation.height // 2
        assert_frame_equal(evaluations[0][0], train)
        assert_frame_equal(evaluations[0][1], validation)
        assert_frame_equal(evaluations[1][1], test)
        assert report["compaction"]["train"]["raw_rows"] == train.height
    else:
        assert evaluations == []
    if refit or not split:
        assert fits[-1][0].height == df.height // 2
        assert report["compaction"]["full"]["raw_rows"] == df.height
    for part, _ in fits:
        totals = part.group_by("departureId").agg(pl.col("weight").sum())["weight"]
        assert np.allclose(totals.to_numpy(), 1)
