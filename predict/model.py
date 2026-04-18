# uv run python
import os
import glob
import polars as pl
import numpy as np
from catboost import Pool, CatBoostClassifier

SNCF_HIVE_DIR = "sncf-hive"
MODELS_DIR = "models"
TRAIN_MODELS = os.getenv("TRAIN_MODELS", "false").lower() == "true"

CAT_COLS = [
    "predictedPlatform",
    "predictedDestination",
    "predictedOrigin",
    "scheduledDestination",
    "scheduledOrigin",
    "trainLine",
    "trainMode",
    "trainNumber",
    "trainType",
    "trainStatus",
]


def get_station_folders(base_dir: str) -> list[str]:
    pattern = os.path.join(base_dir, "station=*")
    folders = glob.glob(pattern)
    return sorted(folders)


def extract_station_id(folder_path: str) -> str:
    return os.path.basename(folder_path).replace("station=", "")


def load_station_data(folder_path: str) -> pl.DataFrame:
    arrow_file = os.path.join(folder_path, "part0.arrow")
    return pl.read_ipc(arrow_file, memory_map=True)


def train_station_model(station_id: str, df: pl.DataFrame) -> CatBoostClassifier | None:
    unique_platforms = df["actualPlatform"].unique()
    if len(unique_platforms) < 2:
        print(
            f"  Skipping {station_id}: only {len(unique_platforms)} unique platform(s)"
        )
        return None

    cat_feature_names = [c for c in CAT_COLS if c in df.columns]
    cat_feature_indices = [df.columns.index(c) for c in cat_feature_names]

    full_pool = Pool(
        data=df.drop(["actualPlatform"]),
        label=df.select(["actualPlatform"]),
        cat_features=cat_feature_indices,
    )
    params = {
        "iterations": 100,
        "learning_rate": 0.1,
        "depth": 6,
        "loss_function": "MultiClass",
        "eval_metric": "Accuracy",
        "verbose": True,
        "random_seed": 1337,
    }
    model = CatBoostClassifier(**params)
    model.fit(full_pool)
    return model


def save_model(model: CatBoostClassifier, station_id: str):
    os.makedirs(MODELS_DIR, exist_ok=True)
    model_path = os.path.join(MODELS_DIR, f"{station_id}.cbm")
    model.save_model(model_path, format="cbm")
    print(f"Saved model to {model_path}")


def discover_stations():
    folders = get_station_folders(SNCF_HIVE_DIR)
    print(f"Found {len(folders)} station folders in {SNCF_HIVE_DIR}")

    stations = []
    for folder in folders:
        station_id = extract_station_id(folder)
        arrow_path = os.path.join(folder, "part0.arrow")
        stations.append({"station_id": station_id, "arrow_path": arrow_path})
        print(f"  - {station_id}")

    return stations


def train_all_models(max_to_train: int | None = None):
    stations = discover_stations()

    to_train = stations[:max_to_train] if max_to_train else stations
    trained = 0
    tried = 0

    for station in stations:
        if max_to_train and trained >= max_to_train:
            break
        station_id = station["station_id"]
        model_path = os.path.join(MODELS_DIR, f"{station_id}.cbm")

        if os.path.exists(model_path):
            print(f"Skipping {station_id} (model already exists)")
            continue

        print(f"\nTraining model for station {station_id}...")
        df = load_station_data(os.path.dirname(station["arrow_path"]))
        print(f"  Loaded {df.height} rows")
        tried += 1

        model = train_station_model(station_id, df)
        if model is None:
            continue
        save_model(model, station_id)
        trained += 1

        if max_to_train and trained >= max_to_train:
            print(f"\nReached max training limit ({max_to_train})")
            break

    print(f"\nTraining complete. Tried {tried} stations, trained {trained} models.")

    print(f"\nTraining complete. Trained {trained} models.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--max", type=int, default=None, help="Max models to train")
    args = parser.parse_args()

    if TRAIN_MODELS:
        train_all_models(max_to_train=args.max)
    else:
        discover_stations()
        print("\nSet TRAIN_MODELS=true to actually train models.")
