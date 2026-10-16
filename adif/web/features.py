from predict.board_features import FeatureSchema

SCHEMA = FeatureSchema(
    3,
    ("predictedPlatform", "trainNumber", "predictedDestination", "carrier", "trainType", "timeKind"),
    ("reportedDelayMinutes", "departureMinute", "dayOfWeek", "month", "leadMinutes", "boardAgeSeconds"),
    time_column="timestamp",
)


def live_features(observation):
    local = observation.departure.astimezone(observation.board_time.tzinfo)
    return {
        "predictedPlatform": observation.platform if observation.single_platform else "MISSING",
        "trainNumber": observation.train,
        "predictedDestination": observation.destination,
        "carrier": observation.carrier,
        "trainType": observation.category,
        "timeKind": observation.time_kind,
        "reportedDelayMinutes": observation.delay,
        "departureMinute": local.hour * 60 + local.minute,
        "dayOfWeek": local.isoweekday(),
        "month": local.month,
        "leadMinutes": (observation.departure.timestamp() - observation.collected.timestamp()) / 60,
        "boardAgeSeconds": observation.collected.timestamp() - observation.board_time.timestamp(),
    }
