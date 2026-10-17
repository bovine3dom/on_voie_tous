import argparse
import json
from pathlib import Path

from predict.board_api import board_predictions
from predict.predict import get_model

from .features import SCHEMA, live_features
from .parse import instant, parse_board
from .prepare import ROOT


def predict_board(model, station, traffic, data, collected):
    observations, issues = parse_board(station, traffic, data, collected)
    observations = [row for row in observations if not row.excluded]
    rows = [live_features(row) for row in observations]
    identities = [{"rowIndex": row.row_index, "trainNumber": row.train,
                   "departureTime": row.departure.isoformat()} for row in observations]
    return {"predictions": board_predictions(model, rows, identities, SCHEMA) if rows else [],
            "issues": dict(issues)}


def main():
    parser = argparse.ArgumentParser(description="Predict platforms for an ADIF website JSON page.")
    parser.add_argument("board", type=Path)
    parser.add_argument("--station", required=True)
    parser.add_argument("--traffic", choices=("cercanias", "avldmd"), required=True)
    parser.add_argument("--at", required=True, help="original collection timestamp, including timezone")
    parser.add_argument("--models", type=Path, default=ROOT / "models")
    args = parser.parse_args()
    result = predict_board(get_model(args.station, args.models), args.station, args.traffic,
                           json.loads(args.board.read_bytes()), instant(args.at))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
