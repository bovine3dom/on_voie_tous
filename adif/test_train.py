import json
from pathlib import Path
import subprocess
import sys

from catboost import CatBoostClassifier

from predict.board_features import FEATURES, SCHEMA_VERSION
from rfi.test_train import data


def test_adif_entrypoint_reuses_the_shared_arrow_trainer(tmp_path):
    hive, models = tmp_path / "hive", tmp_path / "models"
    folder = hive / "station=51003"
    folder.mkdir(parents=True)
    data().write_ipc(folder / "part0.arrow")
    (hive / "dataset.json").write_text(json.dumps({
        "operator": "adif", "schema_version": SCHEMA_VERSION, "station_rows": {"51003": 240},
    }))
    subprocess.run([sys.executable, "-m", "adif.train", "--data", str(hive), "--models", str(models),
                    "--iterations=5", "--threads=1", "--min-trains=10", "--refit"],
                   cwd=Path(__file__).resolve().parents[1], check=True, capture_output=True, timeout=45)
    model = CatBoostClassifier()
    model.load_model(str(models / "51003.cbm"))
    assert list(model.feature_names_) == FEATURES
    report = json.loads((models / "report.json").read_text())
    assert report["stations"][0]["refitted"]
    assert report["stations"][0]["test"]["blank"]["departures"] == 10
