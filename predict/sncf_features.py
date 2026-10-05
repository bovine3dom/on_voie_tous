import polars as pl

SCHEMA_VERSION = 2
CAT_COLS = [
    "predictedPlatform", "predictedTrackGroupValue", "predictedTrackGroupTitle",
    "predictedDestination", "predictedOrigin", "scheduledDestination",
    "scheduledOrigin", "trainLine", "trainMode", "trainNumber", "trainType",
    "trainStatus",
]
TIME_FEATURES = ["scheduledMinute", "dayOfWeek", "month", "leadMinutes", "delayMinutes"]
FEATURES = CAT_COLS + TIME_FEATURES


def time_features(df: pl.DataFrame) -> pl.DataFrame:
    # Input times are Unix seconds. Subtract signed values to retain negative delays.
    scheduled = pl.col("scheduledTime").cast(pl.Int64)
    local = pl.from_epoch(scheduled, time_unit="s").dt.convert_time_zone("Europe/Paris")
    return df.with_columns(
        (local.dt.hour().cast(pl.Int32) * 60 + local.dt.minute()).alias("scheduledMinute"),
        local.dt.weekday().alias("dayOfWeek"),
        local.dt.month().alias("month"),
        ((scheduled - pl.col("timestamp").cast(pl.Int64)) / 60).alias("leadMinutes"),
        ((pl.col("predictedTime").cast(pl.Int64) - scheduled) / 60).alias("delayMinutes"),
    )


def feature_frame(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(pl.lit(None, dtype=pl.String).alias(c) for c in CAT_COLS if c not in df.columns)
    return time_features(df).select(FEATURES).with_columns(
        pl.col(CAT_COLS).cast(pl.String).fill_null("MISSING"),
        pl.col(TIME_FEATURES).cast(pl.Float64),
    )
