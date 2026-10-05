import json
from pathlib import Path

import numpy as np

from .board_features import FEATURES, SCHEMA_VERSION

MODEL_SUFFIXES = (".cbm", ".prior.json")


class PlatformPrior:
    """Use observed platform frequencies when trees cannot be fitted."""
    tree_count_ = 0

    def __init__(self, classes, probabilities, features=FEATURES):
        self.classes_ = np.asarray(classes, dtype=str)
        self.probabilities = np.asarray(probabilities, dtype=float)
        self.feature_names_ = list(features)

    def predict_proba(self, frame):
        return np.tile(self.probabilities, (frame.height, 1))

    def save_model(self, path):
        Path(path).write_text(json.dumps({
            "schema_version": SCHEMA_VERSION, "features": self.feature_names_,
            "classes": self.classes_.tolist(), "probabilities": self.probabilities.tolist(),
        }) + "\n")

    @classmethod
    def load_model(cls, path):
        saved = json.loads(Path(path).read_text())
        return cls(saved["classes"], saved["probabilities"], saved["features"])


def station_models(directory):
    return sorted({station for suffix in MODEL_SUFFIXES for path in Path(directory).glob("*" + suffix)
                   if (station := path.name.removesuffix(suffix)).isascii() and station.isdecimal()})
