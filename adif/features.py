from zoneinfo import ZoneInfo

from predict.board_features import FEATURES, SCHEMA_VERSION, feature_frame

MADRID = ZoneInfo("Europe/Madrid")
UNKNOWN = {"", "-", "--", "—", "?", "N/A", "ND", "N.D.", "SIN ASIGNAR"}


def live_features(at, train):
    local = train.scheduledTime.astimezone(MADRID)
    value = train.platform.strip()
    return {
        "predictedPlatform": "MISSING" if value.upper() in UNKNOWN else value,
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
