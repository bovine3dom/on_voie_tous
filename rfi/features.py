from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import polars as pl

SCHEMA_VERSION = 2
CAT_COLS = ["predictedPlatform", "trainNumber", "predictedDestination", "carrier", "trainType"]
FEATURES = CAT_COLS + [
    "delayMinutes", "scheduledMinute", "dayOfWeek", "month", "leadMinutes"
]
ROME = ZoneInfo("Europe/Rome")


def scheduled_time(at: datetime, clock: str) -> datetime:
    local = at.astimezone(ROME)
    hour, minute = map(int, clock.split(":"))
    difference = hour * 60 + minute - local.hour * 60 - local.minute
    day = local.date() + timedelta(days=1 if difference < -720 else -1 if difference > 720 else 0)
    result = datetime(day.year, day.month, day.day, hour, minute, tzinfo=ROME)
    if result.utcoffset() != result.replace(fold=1).utcoffset():
        raise ValueError("Ambiguous or nonexistent local departure time")
    return result


def platform_known(value: str) -> bool:
    return value.strip().upper() not in {"", "-", "--", "—", "?", "N.D.", "ND", "NON DISPONIBILE"}


def live_features(at: datetime, train) -> dict:
    local = scheduled_time(at, train.clock)
    return {
        "predictedPlatform": train.platform.strip() if platform_known(train.platform) else "MISSING",
        "trainNumber": train.trainNumber,
        "predictedDestination": train.destination,
        "carrier": train.carrier,
        "trainType": train.category,
        "delayMinutes": train.delayMinutes,
        "scheduledMinute": local.hour * 60 + local.minute,
        "dayOfWeek": local.isoweekday(),
        "month": local.month,
        "leadMinutes": (local.timestamp() - at.timestamp()) / 60,
    }


def feature_frame(df: pl.DataFrame) -> pl.DataFrame:
    return df.select(FEATURES).with_columns(
        pl.col(CAT_COLS).cast(pl.String).fill_null("MISSING"),
        pl.col(FEATURES[len(CAT_COLS):]).cast(pl.Float64),
    )
