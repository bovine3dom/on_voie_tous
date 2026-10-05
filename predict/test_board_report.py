import csv

from predict.board_report import export


def test_report_preserves_ids_and_separates_missing_and_published_cases(tmp_path):
    audit, output = tmp_path / "stations.csv", tmp_path / "evaluation.csv"
    audit.write_text('station_id,station_name\n05123,"Station, name"\n4,Small\n')
    report = {"stations": [
        {"station_id": "05123", "status": "trained", "departures": 100,
         "split_departures": {"train": 70}, "model_last_date": "2026-10-05",
         "test": {"departures": 20, "accuracy": 0.75, "baseline_accuracy": 0.5,
                  "blank": {"departures": 5, "accuracy": 0.0, "baseline_accuracy": 0.2},
                  "published": {"departures": 15, "accuracy": 1.0, "baseline_accuracy": 0.6},
                  "changed": {"departures": 0}, "official_accuracy": 1.0}},
        {"station_id": "4", "status": "trained", "departures": 2, "model_departures": 2,
         "model_kind": "platform_prior", "backtest_status": "unavailable",
         "backtest_reason": "insufficient_chronological_periods"},
    ]}
    export(report, audit, output)
    assert b"\r" not in output.read_bytes()
    with output.open(newline="") as source:
        small, station = list(csv.DictReader(source))
    assert small["station_id"] == "4"
    assert small["all_accuracy_pct"] == ""
    assert small["model_departures"] == "2"
    assert small["backtest_status"] == "unavailable"
    assert small["model_kind"] == "platform_prior"
    assert station["station_id"] == "05123"
    assert station["station_name"] == "Station, name"
    assert station["blank_accuracy_pct"] == "0.0"
    assert station["published_accuracy_pct"] == "100.0"
    assert station["changed_departures"] == "0"
    assert station["changed_accuracy_pct"] == ""
    assert station["official_accuracy_pct"] == "100.0"
