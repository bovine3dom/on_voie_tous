from rfi.train import main

from .features import SCHEMA
from .prepare import ROOT

if __name__ == "__main__":
    main(root=ROOT, schema=SCHEMA)
