import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html import unescape
from zoneinfo import ZoneInfo

from adif.features import UNKNOWN

MADRID = ZoneInfo("Europe/Madrid")


def text(value):
    return " ".join(unescape(str(value or "")).split())


def instant(value):
    at = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if at.utcoffset() is None:
        raise ValueError("collection timestamp must include a timezone")
    return at


def local_clock(day, clock):
    naive = datetime.combine(day, datetime.strptime(clock, "%H:%M").time())
    candidates = {naive.replace(tzinfo=MADRID, fold=fold).astimezone(timezone.utc) for fold in (0, 1)}
    valid = [at for at in candidates if at.astimezone(MADRID).replace(tzinfo=None) == naive]
    if len(valid) != 1:
        raise ValueError("ambiguous or nonexistent local time")
    return valid[0].astimezone(MADRID)


def clock_after(reference, clock):
    at = local_clock(reference.date(), clock)
    if at.timestamp() < reference.timestamp() - 12 * 3600:
        at = local_clock(reference.date() + timedelta(days=1), clock)
    return at


@dataclass(frozen=True)
class Observation:
    station: str
    traffic: str
    collected: datetime
    board_time: datetime
    departure: datetime
    display_time: datetime
    train: str
    destination: str
    destination_key: str
    carrier: str
    category: str
    platform: str
    time_kind: str
    delay: float | None
    row_index: int
    excluded: bool
    snapshot: datetime | None = None

    @property
    def key(self):
        return (self.station, self.traffic, self.train.casefold(), self.destination_key,
                self.carrier.casefold(), self.category.casefold())

    @property
    def single_platform(self):
        return bool(re.fullmatch(r"(?:\d+[A-Za-z]?|[A-Za-z])", self.platform))


def parse_board(station, traffic, board, collected):
    """Decode a raw page without changing it. Return observations and issue counts."""
    if not re.fullmatch(r"\d{5}", station) or traffic not in ("cercanias", "avldmd"):
        raise ValueError("invalid station or traffic filter")
    if not isinstance(board, dict) or board.get("error") or not isinstance(board.get("horarios"), list):
        raise ValueError("invalid board or API error")
    if collected.utcoffset() is None:
        raise ValueError("collection timestamp must include a timezone")
    if not board["horarios"]:
        return [], Counter()
    match = re.search(r"\b\d{2}/\d{2}/\d{4}\b", board.get("date", ""))
    if not match:
        raise ValueError("invalid board date")
    day = datetime.strptime(match[0], "%d/%m/%Y").date()
    observed = local_clock(day, board["time"])
    if not -60 <= collected.timestamp() - observed.timestamp() <= 300:
        raise ValueError("stale board timestamp")
    result, issues, previous = [], Counter(), None
    for index, row in enumerate(board["horarios"]):
        try:
            if not isinstance(row, dict) or any(not isinstance(row.get(k), str) for k in ("hora", "tren", "estacion", "via")):
                raise ValueError("invalid_row")
            clock = row["hora"].strip()
            relative = re.fullmatch(r"(\d+)\s*min", clock)
            if relative:
                minutes = int(relative[1])
                if minutes > 1440:
                    raise ValueError("invalid_relative_time")
                at, kind = (observed.astimezone(timezone.utc) + timedelta(minutes=minutes)).astimezone(MADRID), "countdown"
            elif not clock and row.get("immediate"):
                at, kind = observed, "immediate"
            else:
                at, kind = clock_after(previous or observed, clock), "clock"
                if previous is None and at.timestamp() > observed.timestamp() + 12 * 3600:
                    raise ValueError("uncertain_date")
            if previous and at.timestamp() < previous.timestamp():
                # Do not infer service dates from a board whose ordering has changed.
                raise ValueError("unordered_board")
            previous = at
            delay = None
            if text(row.get("horaEstado")):
                actual = clock_after(at, text(row["horaEstado"]))
                if actual.timestamp() < at.timestamp():
                    raise ValueError("invalid_estimate")
                if kind == "clock":
                    delay = (actual.timestamp() - at.timestamp()) / 60
                at = actual
            operator = text(row.get("trenDatosOp"))
            carrier, separator, category = operator.partition(" - ")
            if not separator:
                carrier, category = "", operator
            destination = text(row["estacion"])
            link = re.search(r"/w/(\d{5})-", text(row.get("estacionLink")))
            destination_key = link[1] if link else destination.casefold()
            platform = text(row["via"])
            if platform.upper() in UNKNOWN:
                platform = ""
            train = text(row["tren"])
            if not train or not destination:
                raise ValueError("missing_identity")
            excluded = bool(re.search(r"cancelad|suprimid|anulad", text(row.get("observation")), re.I)
                            or re.search(r"\bbus\b|autob[uú]s", f"{train} {operator} {platform}", re.I))
            result.append(Observation(station, traffic, collected, observed, at, previous, train, destination,
                                      destination_key, carrier, category, platform, kind, delay, index, excluded))
        except (ValueError, TypeError, KeyError) as error:
            issues[str(error) if str(error) in {"invalid_row", "invalid_relative_time", "uncertain_date",
                   "unordered_board", "invalid_estimate", "missing_identity"} else "invalid_time"] += 1
            if str(error) == "unordered_board":
                return [], issues
    return result, issues
