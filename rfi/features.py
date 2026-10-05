from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from predict.board_features import CAT_COLS, FEATURES, SCHEMA_VERSION, feature_frame
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
