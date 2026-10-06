# ADIF platform predictions

Julia prepares training data for CatBoost.
The shared server provides predictions.
The userscript adds estimates to departure boards on [Pantallas estaciones ADIF](https://pantallas-estaciones.vercel.app/).
Model scores are not measured accuracy. Check station monitors and announcements.

Run the commands below from the repository root.

## Build models

Use complete `.zst` archives from the ADIF data collector:

```bash
julia --project=rfi -e 'using Pkg; Pkg.instantiate()'
julia --project=rfi --threads=4 adif/prepare.jl /path/to/data/es-adif
uv sync --project predict --frozen
uv run --project predict python -m adif.train --workers=4 --threads=4 --refit
```

Training data goes to `adif/hive/`; models and `report.json` go to `adif/models/`.
Use `--stations 17000 71801` to train selected stations.
`--refit` trains on all data after evaluation.
Every selected station with labelled departures receives a predictor.
Single-platform or constant-input histories use `.prior.json` frequency predictors instead of CatBoost models.

Models use observations at all lead times to predict the last known platform. Rebuild training data and models to apply this change.
`report.json` compares the model with a historical-frequency baseline.
It has separate results for blank, published, and changed platforms.
Training still runs if a chronological evaluation is unavailable; no accuracy estimate is recorded.

The shared trainer also has [optional training-row compaction](../rfi/readme.md#optional-training-row-compaction).
It is disabled by default. Use a new model directory for each experiment.

## Select stations

`prepare.jl` selects stations before it writes training data.
By default, it checks platforms 30 minutes before departure.
It selects stations with at least 10% blank platforms and 100 departures.
`minimum_departures.json` sets exceptions for stations with less data. These override `--min`.
Use `--lead=60 --min=100 --rate=0.20` to change the selection.

Results, station lists, and counts go to `adif/results/`.
`insufficient_data.txt` lists unresolved stations, not confirmed skips.
Archive summaries are cached in `adif/.cache/`. Delete this directory to repeat the full scan.
Each archive decoder can use more than 256 MiB of memory. Use fewer threads on a small machine.

## Run the server

```bash
uv run --project predict python -m predict.server
```

- `POST /predict?operator=adif`: platform predictions.
- `GET /stations?operator=adif`: stations with models.
- `/docs`: request and response fields.

The same server handles SNCF and RFI. Requests without an operator select SNCF.
Set `ADIF_MODELS_DIR` to use another model directory.
Use `PREDICT_HOST` and `PREDICT_PORT` to change the address and port.
Restart the server after you replace models.

Use `station_settings.code` as the station ID and the planned technical number as `trainNumber`.
Train inputs require `trainId`, `trainNumber`, `scheduledTime`, `stopType`, and `destination`.
`scheduledTime` must include a UTC offset. Use `/docs` for the optional fields.
The current official platform is a model input.
Responses contain train identities and every platform score.
Predictions have no departure-time limit. Cancelled trains, buses, and arrivals are excluded.

## Browser use

Install [the userscript](../src/content.user.js), then reload the site.
Select a station and a departure board. Keep the platform column visible.
The script runs inside the embedded display and reads its board messages.
It sends departure data to the prediction server only for stations with models.
It does not change the source data, official platforms, or platform filters.
Estimates have an `Est.` label. Percentages are model scores, not measured accuracy.
Arrivals, buses, and cancelled trains do not receive estimates.
A changed board removes old estimates before it requests new ones.

## Tests

```bash
julia --project=rfi adif/test_prepare.jl
uv run --project predict python -m pytest adif/test_train.py adif/test_server.py
uv run --project predict --with playwright python -m pytest adif/test_browser.py rfi/test_browser.py
```
