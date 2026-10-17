from collections import Counter
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
import json
import subprocess

import polars as pl
import pytest
from fastapi import HTTPException

from predict.board_models import PlatformPrior
from predict.board_features import SCHEMA as OLD_SCHEMA
from rfi.train import chronological_split, train_station
from .features import SCHEMA, live_features
from .parse import MADRID, instant, parse_board
from .predict import predict_board
from .prepare import match_departures, prepare, read_observations, training_rows


def board(at, rows):
    local = instant(at).astimezone(MADRID)
    return {"date": local.strftime("%d/%m/%Y"), "time": local.strftime("%H:%M"), "horarios": rows}


def row(clock="14:30", platform="", train="R2S", **extra):
    return {"hora": clock, "via": platform, "tren": train, "estacion": "Destination",
            "estacionLink": "https://www.adif.es/w/71600-destination", **extra}


def observations(at, rows):
    result, issues = parse_board("79400", "cercanias", board(at, rows), instant(at))
    assert not issues
    return result


def samples():
    return (observations("2026-10-16T12:00:00Z", [row(), row(train="R3", platform="4")]) +
            observations("2026-10-16T12:20:00Z", [row("10 min"), row("10 min", train="R3", platform="4")]) +
            observations("2026-10-16T12:27:00Z", [row("3 min", "8"), row("3 min", "5", "R3")]))


def test_countdown_normalization_does_not_change_raw_data():
    at = "2026-10-16T12:20:32Z"
    raw = board(at, [row("3 min")])
    before = deepcopy(raw)
    decoded, issues = parse_board("79400", "cercanias", raw, instant(at))
    assert not issues and raw == before
    assert decoded[0].departure.isoformat() == "2026-10-16T14:23:00+02:00"
    features = live_features(decoded[0])
    assert features["leadMinutes"] == pytest.approx(3 - 32 / 60)
    assert features["boardAgeSeconds"] == 32
    assert features["departureMinute"] == 14 * 60 + 23
    assert features["timeKind"] == "countdown"
    assert features["reportedDelayMinutes"] is None


def test_clock_delay_immediate_and_unknown_times():
    decoded = observations("2026-10-16T12:00:00Z", [row("", immediate=True), row("14:30", horaEstado="14:35")])
    assert decoded[0].time_kind == "immediate"
    assert live_features(decoded[1])["reportedDelayMinutes"] == 5
    assert decoded[1].departure.minute == 35
    decoded, issues = parse_board("79400", "cercanias", board("2026-10-16T12:00:00Z", [row("unknown")]), instant("2026-10-16T12:00:00Z"))
    assert not decoded and issues["invalid_time"] == 1


def test_midnight_dst_stale_and_unordered_pages():
    decoded = observations("2026-10-16T21:58:00Z", [row("23:59"), row("00:10")])
    assert decoded[1].departure.date().isoformat() == "2026-10-17"
    for date in ("25/10/2026", "29/03/2026"):
        with pytest.raises(ValueError):
            parse_board("79400", "cercanias", {"date": date, "time": "02:30", "horarios": [row()]}, instant("2026-10-16T12:00:00Z"))
    with pytest.raises(ValueError, match="stale"):
        parse_board("79400", "cercanias", board("2026-10-16T12:00:00Z", [row()]), instant("2026-10-16T12:10:00Z"))
    decoded, issues = parse_board("79400", "cercanias", board("2026-10-16T12:00:00Z", [row("15:00"), row("14:30")]), instant("2026-10-16T12:00:00Z"))
    assert not decoded and issues["unordered_board"] == 1
    with pytest.raises(ValueError):
        parse_board("79400", "cercanias", {"error": True}, instant("2026-10-16T12:00:00Z"))


def test_clock_and_countdown_match_without_platform_identity():
    audit = Counter()
    tracks = match_departures(samples(), audit)
    assert len(tracks) == 2
    assert all(len(track.samples) == 3 for track in tracks)
    result = training_rows(tracks, audit)
    assert len(result) == 4
    assert {r["actualPlatform"] for r in result} == {"8", "5"}
    assert all(r["timestamp"] < r["labelTimestamp"] for r in result)
    assert {r["predictedPlatform"] for r in result if r["actualPlatform"] == "8"} == {"MISSING"}
    assert {r["predictedPlatform"] for r in result if r["actualPlatform"] == "5"} == {"4"}
    for departure in {r["departureId"] for r in result}:
        assert sum(r["weight"] for r in result if r["departureId"] == departure) == 1


def test_rounding_drift_and_repeated_polls_in_same_board_minute():
    first = observations("2026-10-16T12:27:01Z", [row("3 min", "8")])
    second = observations("2026-10-16T12:27:40Z", [row("2 min", "9")])
    tracks = match_departures(first + second, Counter())
    assert len(tracks) == 1 and len(tracks[0].samples) == 2


def test_ambiguous_matching_excludes_all_candidate_departures():
    first = observations("2026-10-16T12:00:00Z", [row("14:30"), row("14:32")])
    second = observations("2026-10-16T12:28:00Z", [row("3 min", "8")])
    audit = Counter()
    tracks = match_departures(first + second, audit)
    assert audit["ambiguous_observations"] == 1
    assert all(t.ambiguous for t in tracks)
    assert training_rows(tracks, audit) == []


def test_conflicting_rows_in_one_snapshot_are_not_resolved_by_platform():
    first = observations("2026-10-16T12:00:00Z", [row()])
    second = observations("2026-10-16T12:27:00Z", [row("3 min", "8"), row("3 min", "9")])
    audit = Counter()
    tracks = match_departures(first + second, audit)
    assert tracks[0].ambiguous
    assert training_rows(tracks, audit) == []
    assert audit["ambiguous_observations"] == 2


def test_destination_and_operator_separate_simultaneous_trains():
    base = samples()[0]
    variants = [base, replace(base, destination="Other", destination_key="99999"), replace(base, carrier="IL")]
    later = [replace(r, collected=r.collected + timedelta(minutes=1)) for r in variants]
    tracks = match_departures(variants + later, Counter())
    assert len(tracks) == 3
    assert all(len(t.samples) == 2 for t in tracks)


def test_ranges_early_platforms_and_cancelled_services_are_not_labels():
    for replacement in (replace(samples()[4], platform="1-6"),
                        replace(samples()[4], excluded=True),
                        replace(samples()[4], departure=samples()[4].departure + timedelta(minutes=10))):
        audit = Counter()
        tracks = match_departures([samples()[0], replacement], audit)
        assert training_rows(tracks, audit) == []


def write_archive(path):
    path.mkdir()
    events = []
    for i, (at, rows) in enumerate([
        ("2026-10-16T12:00:00Z", [row(), row(train="R3")]),
        ("2026-10-16T12:20:00Z", [row("10 min"), row("10 min", train="R3")]),
        ("2026-10-16T12:27:00Z", [row("3 min", "8"), row("3 min", "5", "R3")]),
    ]):
        name = f"{i}.body"
        (path / name).write_text(json.dumps(board(at, rows)))
        events.append({"kind": "board", "outcome": "board", "station": "79400", "traffic": "cercanias", "ts": at, "page": 0, "raw": name})
    (path / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in events))
    return events


def test_prepare_train_predict_roundtrip_and_schema_isolation(tmp_path):
    archive = tmp_path / "raw"
    events = write_archive(archive)
    before = {path: path.read_bytes() for path in archive.iterdir()}
    output = tmp_path / "hive"
    metadata = prepare([archive, archive / "events.jsonl"], output)
    assert metadata["audit"]["labelled_departures"] == 2
    assert metadata["rows"] == 4
    assert before == {path: path.read_bytes() for path in archive.iterdir()}
    frame = pl.read_ipc(output / "station=79400" / "part0.arrow")
    report = train_station("79400", frame, tmp_path / "models", iterations=5, threads=1, schema=SCHEMA)
    assert report["backtest_status"] == "unavailable"
    from catboost import CatBoostClassifier
    model = CatBoostClassifier()
    model.load_model(str(tmp_path / "models" / "79400.cbm"))
    raw = json.loads((archive / "0.body").read_bytes())
    prediction = predict_board(model, "79400", "cercanias", raw, instant(events[0]["ts"]))
    assert len(prediction["predictions"]) == 2
    assert prediction["predictions"][0]["rowIndex"] == 0
    assert sum(p["prob"] for p in prediction["predictions"][0]["probabilities"]) == pytest.approx(1)
    with pytest.raises(HTTPException) as error:
        predict_board(PlatformPrior(["8"], [1]), "79400", "cercanias", raw, instant(events[0]["ts"]))
    assert error.value.status_code == 409
    prior = PlatformPrior(["8"], [1], SCHEMA.features, SCHEMA.version)
    prior.save_model(tmp_path / "prior.json")
    loaded = PlatformPrior.load_model(tmp_path / "prior.json")
    assert loaded.schema_version == 3 and loaded.feature_names_ == SCHEMA.features
    assert OLD_SCHEMA.version == 2


def test_collector_archives_match_probe_data_and_ignore_current(tmp_path):
    probe = tmp_path / "probe"
    events = write_archive(probe)
    collector = tmp_path / "collector"
    collector.mkdir()
    lines = []
    for event in events:
        data = dict(event, response=json.loads((probe / event["raw"]).read_bytes()))
        del data["raw"]
        lines.append(json.dumps({"ts": event["ts"], "station": event["station"], "data": data}) + "\n")
    lines.append(json.dumps({"data": {"kind": "board", "outcome": "error", "body_base64": "AA=="}}) + "\n")
    # A poll may cross an archive rotation.
    extra = json.loads(lines[0])
    extra["data"].update(page=1, ts="2026-10-16T12:00:01Z")
    parts = [lines[:1], [json.dumps(extra) + "\n"] + lines[1:]]
    for i, part in enumerate(parts):
        compressed = subprocess.run(["zstd", "-q", "-c"], input="".join(part).encode(),
                                    check=True, capture_output=True).stdout
        (collector / f"{i}.jsonl.zst").write_bytes(compressed)
    (collector / "current.jsonl").write_text("not a completed archive")
    audit = Counter()
    observed, logs = read_observations([collector], audit)
    assert len(logs) == 2 and audit["failed_requests"] == 1
    assert len(observed) == 8
    assert observed[0].snapshot == observed[2].snapshot
    assert audit["pages_outside_window_or_without_start"] == 0
    metadata = prepare([collector], tmp_path / "hive")
    assert metadata["audit"]["labelled_departures"] == 2
    assert metadata["rows"] == 4


def test_corrupt_collector_archive_fails(tmp_path):
    archive = tmp_path / "bad.jsonl.zst"
    archive.write_bytes(b"not zstd")
    with pytest.raises(ValueError, match="cannot decompress"):
        read_observations([archive], Counter())


def test_chronological_split_keeps_departure_samples_together():
    rows = training_rows(match_departures(samples(), Counter()), Counter())
    data = []
    for day in range(6):
        for row_value in rows:
            value = dict(row_value, serviceDate=f"2026-10-{16 + day:02}", departureId=f"{day}-{row_value['departureId']}")
            for key in ("timestamp", "departureTime", "labelTimestamp"):
                value[key] += day * 86400
            data.append(value)
    split = chronological_split(pl.DataFrame(data), SCHEMA)
    ids = [set(part["departureId"]) for part in split]
    assert all(ids[i].isdisjoint(ids[j]) for i in range(3) for j in range(i))
    assert all(part.height > 0 for part in split)


def test_page_cutoff_and_partial_active_log_are_safe(tmp_path):
    archive = tmp_path / "raw"
    events = write_archive(archive)
    extra = dict(events[0], raw="far.body", page=1)
    # Page zero already crosses the lookahead; later full-board pages must not
    # become today's trains when they actually refer to tomorrow.
    (archive / "0.body").write_text(json.dumps(board(events[0]["ts"], [row("16:00")])))
    (archive / "far.body").write_text(json.dumps(board(events[0]["ts"], [row("14:30")])))
    (archive / "events.jsonl").write_text(json.dumps(events[0]) + "\n" + json.dumps(extra) + "\n" + '{"partial":')
    audit = Counter()
    result, _ = read_observations([archive], audit)
    assert result == []
    assert audit["unfinished_event_lines"] == 1
    assert audit["pages_outside_window_or_without_start"] == 1
