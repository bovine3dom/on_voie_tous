import argparse
from collections import Counter, defaultdict
from dataclasses import dataclass, field, replace
import hashlib
import json
from pathlib import Path

import polars as pl

from .features import SCHEMA, live_features
from .parse import instant, parse_board

ROOT = Path(__file__).resolve().parent


def read_observations(paths, audit):
    observations, seen = [], set()
    logs = sorted({log.resolve() for path in paths for log in
                   ([path] if path.is_file() else path.rglob("events.jsonl"))})
    if not logs:
        raise ValueError("no events.jsonl files found")
    for log in logs:
        active, starts = {}, {}
        with log.open() as stream:
            for line in stream:
                if not line.endswith("\n"):
                    audit["unfinished_event_lines"] += 1
                    continue
                event = json.loads(line)
                if event.get("kind") != "board":
                    continue
                if event.get("outcome") not in ("board", "empty"):
                    audit["failed_requests"] += 1
                    continue
                key = (event.get("station"), event.get("traffic"))
                page = event.get("page")
                if page == 0:
                    active[key] = True
                    starts[key] = instant(event["ts"])
                if not active.get(key):
                    audit["pages_outside_window_or_without_start"] += 1
                    continue
                name = event.get("raw", "")
                if not name or Path(name).name != name or not name.endswith(".body"):
                    raise ValueError("invalid raw response filename")
                raw = log.parent / name
                if raw.stat().st_size > 8 * 1024 * 1024:
                    raise ValueError("raw response exceeds 8 MiB")
                try:
                    board = json.loads(raw.read_bytes())
                    rows, issues = parse_board(*key, board, instant(event["ts"]))
                except (ValueError, KeyError, TypeError):
                    audit["invalid_boards"] += 1
                    active[key] = False
                    continue
                audit.update(issues)
                audit["boards"] += 1
                if not rows or any((r.display_time.timestamp() - r.board_time.timestamp()) / 60 > 65 for r in rows):
                    active[key] = False
                for row in rows:
                    lead = live_features(row)["leadMinutes"]
                    if not -2 <= lead <= 65:
                        audit["rows_outside_window"] += 1
                        continue
                    identity = (row.key, row.collected, row.departure, row.platform, row.time_kind, row.excluded)
                    if identity in seen:
                        audit["duplicate_observations"] += 1
                        continue
                    seen.add(identity)
                    observations.append(replace(row, snapshot=starts[key]))
    return observations, logs


@dataclass
class Track:
    samples: list = field(default_factory=list)
    ambiguous: bool = False


def match_departures(observations, audit, tolerance=120):
    # Join pages from one poll. A minute-resolution board clock is not a snapshot ID.
    batches = defaultdict(dict)
    for row in sorted(observations, key=lambda r: (r.collected, r.key, r.row_index, r.platform)):
        batch = ((row.snapshot or row.collected).timestamp(), row.station, row.traffic)
        # Conflicting platforms must remain separate candidates, not overwrite each other.
        batches[batch][(row.key, row.departure.timestamp(), row.platform, row.excluded)] = row
    tracks, by_key = [], defaultdict(list)
    for batch in sorted(batches):
        rows = list(batches[batch].values())
        candidates, uses = [], Counter()
        for row in rows:
            possible = [i for i in by_key[row.key] if not tracks[i].ambiguous
                        and 0 <= (row.collected - tracks[i].samples[-1].collected).total_seconds() <= 3600
                        and abs(row.departure.timestamp() - tracks[i].samples[-1].departure.timestamp()) <= tolerance]
            candidates.append(possible)
            uses.update(possible)
        for row, possible in zip(rows, candidates):
            if len(possible) > 1 or any(uses[i] > 1 for i in possible):
                audit["ambiguous_observations"] += 1
                for i in possible:
                    tracks[i].ambiguous = True
                continue
            if possible:
                tracks[possible[0]].samples.append(row)
                audit["matched_observations"] += 1
            else:
                by_key[row.key].append(len(tracks))
                tracks.append(Track([row]))
                audit["new_departures"] += 1
    return tracks


def training_rows(tracks, audit):
    rows = []
    for track in tracks:
        samples = sorted(track.samples, key=lambda row: row.collected)
        if track.ambiguous or any(row.excluded for row in samples):
            audit["ambiguous_departures" if track.ambiguous else "cancelled_or_bus_departures"] += 1
            continue
        # A published single platform near departure is a proxy label, not ground truth.
        known = [row for row in samples if row.single_platform and
                 -2 <= live_features(row)["leadMinutes"] <= 5]
        if not known:
            audit["unlabelled_departures"] += 1
            continue
        label = max(known, key=lambda row: row.collected)
        earlier = [row for row in samples if row.collected < label.collected and
                   0 <= live_features(row)["leadMinutes"] <= 60]
        if not earlier:
            audit["labels_without_earlier_observations"] += 1
            continue
        first = samples[0]
        token = json.dumps([first.key, first.departure.isoformat(), first.collected.isoformat()])
        departure_id = hashlib.sha256(token.encode()).hexdigest()[:24]
        selected = {}
        for horizon in (5, 15, 30, 60):
            eligible = [row for row in earlier if horizon <= live_features(row)["leadMinutes"] <= horizon + 2]
            if eligible:
                selected[max(eligible, key=lambda row: row.collected).collected] = horizon
        audit["labelled_departures"] += 1
        for row in earlier:
            rows.append({"station": row.station, "departureId": departure_id,
                         "serviceDate": first.departure.date().isoformat(),
                         "timestamp": row.collected.timestamp(),
                         "departureTime": first.departure.timestamp(),
                         "labelTimestamp": label.collected.timestamp(),
                         "horizon": selected.get(row.collected, -1),
                         **live_features(row), "weight": 1 / len(earlier), "actualPlatform": label.platform})
    return rows


def prepare(paths, output, tolerance=120):
    if not 0 < tolerance <= 300:
        raise ValueError("matching tolerance must be in (0, 300] seconds")
    if output.exists():
        raise ValueError("use a new output directory")
    audit = Counter()
    observations, logs = read_observations(paths, audit)
    tracks = match_departures(observations, audit, tolerance)
    rows = training_rows(tracks, audit)
    stations = sorted({row.station for row in observations})
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["station"]].append(row)
    output.mkdir(parents=True)
    for station, values in grouped.items():
        directory = output / f"station={station}"
        directory.mkdir()
        pl.DataFrame(values, schema_overrides={name: pl.Float64 for name in SCHEMA.numeric}).sort("timestamp").write_ipc(directory / "part0.arrow")
    metadata = {"operator": "adif-web", "schema_version": SCHEMA.version, "features": SCHEMA.features,
                "parser_version": 1, "match_tolerance_seconds": tolerance,
                "label_policy": "last_single_platform_within_5_minutes_before_or_2_after",
                "sources": [str(log) for log in logs], "observations": len(observations),
                "departures": len(tracks), "rows": len(rows),
                "station_rows": {station: len(grouped[station]) for station in stations},
                "audit": dict(sorted(audit.items()))}
    (output / "dataset.json").write_text(json.dumps(metadata, indent=2) + "\n")
    return metadata


def main():
    parser = argparse.ArgumentParser(description="Prepare ADIF website observations without changing raw files.")
    parser.add_argument("inputs", type=Path, nargs="+")
    parser.add_argument("--output", type=Path, default=ROOT / "hive")
    parser.add_argument("--match-tolerance", type=int, default=120)
    args = parser.parse_args()
    print(json.dumps(prepare(args.inputs, args.output, args.match_tolerance), indent=2))


if __name__ == "__main__":
    main()
