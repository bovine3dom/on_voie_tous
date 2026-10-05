import polars as pl

SCHEMA_VERSION = 2
CAT_COLS = ["predictedPlatform", "trainNumber", "predictedDestination", "carrier", "trainType"]
FEATURES = CAT_COLS + [
    "delayMinutes", "scheduledMinute", "dayOfWeek", "month", "leadMinutes"
]


def feature_frame(df: pl.DataFrame) -> pl.DataFrame:
    return df.select(FEATURES).with_columns(
        pl.col(CAT_COLS).cast(pl.String).fill_null("MISSING"),
        pl.col(FEATURES[len(CAT_COLS):]).cast(pl.Float64),
    )
