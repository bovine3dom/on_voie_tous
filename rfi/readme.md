# RFI platform predictions

The [userscript](../src/content.user.js) adds platform estimates to RFI departure boards.
Each station needs a trained model. The official platform, if available, is a model input.
The display uses `official | predicted`, or just predictions when the official platform is blank.
The percentages are model scores, not measured accuracy. Check station monitors and announcements.

Run the commands below from the repository root.

## Build models

Use complete `.zst` archives from the RFI data collector:

```bash
julia --project=rfi -e 'using Pkg; Pkg.instantiate()'
julia --project=rfi --threads=4 rfi/wrangler.jl /path/to/data/rfi-iechub
uv sync --project predict --frozen
uv run --project predict python -m rfi.train --workers=4 --threads=4 --refit
```

Training data goes to `rfi/hive/`; models and `report.json` go to `rfi/models/`.
Use `--stations 1728 2416` to train selected stations.
`--refit` trains on all data after evaluation.
Every selected station with labelled departures receives a predictor.
Single-platform or constant-input histories use `.prior.json` frequency predictors instead of CatBoost models.

Models predict the last stable platform reported near departure.
`report.json` compares the model with a historical-frequency baseline.
It has separate results for blank, published, and changed platforms.
Training still runs if a chronological evaluation is unavailable; no accuracy estimate is recorded.

## Run the server

```bash
uv run --project predict python -m predict.server
```

- `POST /predict?operator=rfi`: platform predictions.
- `GET /stations?operator=rfi`: stations with models.
- `/docs`: request and response fields.

The same server handles SNCF. Requests without an operator select SNCF.
Set `RFI_MODELS_DIR` to use another model directory.
Use `PREDICT_HOST` and `PREDICT_PORT` to change the address and port.
Restart the server after you replace models.

Predictions have no departure-time limit. Canceled trains and buses are excluded.
The script requests new predictions when train details change.
For another server, set `window.ON_VOIE_TOUS_SERVER` before the userscript starts.

## Select stations

[stations.txt](stations.txt) supplies the station IDs used to build training data.
To select stations from your own archives:

```bash
julia --project=rfi --threads=4 rfi/audit.jl /path/to/data/rfi-iechub
cp rfi/results/needs_predictions.txt rfi/stations.txt
```

By default, the audit checks platforms 30 minutes before departure.
It selects stations with at least 10% blank platforms and 100 departures.
`minimum_departures.json` sets exceptions for stations with less data.
Use `--lead=60 --min=100 --rate=0.20` to change the selection.
Results, station lists, and counts go to `rfi/results/`.
Archive summaries are cached in `rfi/.cache/`. Delete this directory to repeat the full scan.
Each archive decoder can use more than 256 MiB of memory. Use fewer threads on a small machine.

## Tests

```bash
julia --project=rfi rfi/test_audit.jl
julia --project=rfi rfi/test_wrangler.jl
uv run --project predict python -m pytest rfi/test_train.py rfi/test_server.py \
  predict/test_predict.py predict/test_startup.py
uv run --project predict --with playwright python -m pytest rfi/test_browser.py
```

Browser tests run headless. They use system `chromium`; set `CHROMIUM_PATH` to use another executable.
