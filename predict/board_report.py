"""Export a compact station backtest report."""
import argparse
import csv
import json
from pathlib import Path


def export(report, station_audit, output):
    with station_audit.open(newline="") as source:
        names = {row["station_id"]: row["station_name"] for row in csv.DictReader(source)}
    fields = ["station_id", "station_name", "status", "labelled_departures", "training_departures"]
    for group in ("all", "blank", "published", "changed"):
        fields.extend(f"{group}_{field}" for field in ("departures", "accuracy_pct", "baseline_accuracy_pct"))
    fields.extend(["official_accuracy_pct", "model_last_date"])
    with output.open("w", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=fields)
        writer.writeheader()
        for station in sorted(report["stations"], key=lambda row: int(row["station_id"])):
            row = {"station_id": station["station_id"], "station_name": names.get(station["station_id"], ""),
                   "status": station["status"], "labelled_departures": station["departures"],
                   "training_departures": station.get("split_departures", {}).get("train"),
                   "model_last_date": station.get("model_last_date")}
            test = station.get("test", {})
            for group in ("all", "blank", "published", "changed"):
                metrics = test if group == "all" else test.get(group, {})
                row[f"{group}_departures"] = metrics.get("departures")
                for field in ("accuracy", "baseline_accuracy"):
                    value = metrics.get(field)
                    row[f"{group}_{field}_pct"] = None if value is None else round(100 * value, 3)
            official = test.get("official_accuracy")
            row["official_accuracy_pct"] = None if official is None else round(100 * official, 3)
            writer.writerow(row)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("station_audit", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    export(json.loads(args.report.read_text()), args.station_audit, args.output)
