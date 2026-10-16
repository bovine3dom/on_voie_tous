from dataclasses import dataclass

import polars as pl

SCHEMA_VERSION = 2
CAT_COLS = ["predictedPlatform", "trainNumber", "predictedDestination", "carrier", "trainType"]
FEATURES = CAT_COLS + [
    "delayMinutes", "scheduledMinute", "dayOfWeek", "month", "leadMinutes"
]


@dataclass(frozen=True)
class FeatureSchema:
    version: int
    categorical: tuple[str, ...]
    numeric: tuple[str, ...]
    time_column: str = "scheduledTime"

    @property
    def features(self):
        return list(self.categorical + self.numeric)

    def frame(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.select(self.features).with_columns(
            pl.col(list(self.categorical)).cast(pl.String).fill_null("MISSING"),
            pl.col(list(self.numeric)).cast(pl.Float64),
        )


SCHEMA = FeatureSchema(SCHEMA_VERSION, tuple(CAT_COLS), tuple(FEATURES[len(CAT_COLS):]))
feature_frame = SCHEMA.frame
