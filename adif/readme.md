# ADIF platform audit and models

The Julia pipeline measures platform availability and reported platform changes.
It selects stations before it writes training data for Python.
It reuses the RFI departure summaries and audit rules.
The CatBoost trainer reuses the RFI trainer and the SNCF Arrow loader.

## Run

From the repository root:

```bash
julia --project=rfi -e 'using Pkg; Pkg.instantiate()'
julia --project=rfi --threads=16 adif/prepare.jl /path/to/datagrabber/data/es-adif
uv run --project predict python -m adif.train --workers=4 --threads=4 --iterations=100 --refit
```

Use `--limit=2 --output=/tmp/adif-pilot` for a small preprocessing run.
Only complete `.zst` archives are read. Source archives are not changed.
The decoder permits 256 MiB windows. Use fewer threads if memory is limited.
Compact archive summaries are cached in `adif/.cache/`.
Threshold changes can use these summaries without another decompression pass.

## Selection

The default target is 30 minutes before scheduled departure.
The closest snapshot before that target is used, up to 10 minutes earlier.
Counts at 15, 30, 60, and 120 minutes are also saved.
Repeated messages do not increase the departure count.

A station needs predictions if either condition has sufficient support:

- At least 10% of observed departure platforms are blank.
- At least 1% of early published platforms differ from the later stable report, with at least five such changes.

The default minimum is 100 distinct departures.
For the revision test, the denominator contains published platforms with stable later reports.
Either the full period or a supported month can meet a condition.
A station cannot be a confirmed skip without sufficient later reports to check revisions.

`adif/minimum_departures.json`, if present, sets station-specific minimums.
Use `--minimums=PATH` for another file. Overrides take priority over `--min`.
The options `--rate`, `--revision-rate`, `--min-revisions`, and `--lead` change the selection.

## Source and targets

SignalR `ReceiveMessage` arguments contain JSON text, sometimes with a second JSON encoding.
Empty messages and control replies are not empty station boards.
The station ID comes from `station_settings.code`, not the outer archive label.
Only origin and intermediate stops are departures. Destination stops are arrivals and are excluded.
Cancelled trains and buses are excluded.
Boards with missing timestamps or source data more than ten minutes old are excluded.

`departure_time` includes a date and UTC offset. Its offset is used directly.
Calendar features use `Europe/Madrid`.
The planned technical number identifies a departure because the feed's `id` can change when a train starts running.
Station, local service date, planned number, and scheduled timestamp form the departure key.
Leading zeroes and compound platform labels remain strings.

The target is the last reported `platform` near the delay-adjusted departure.
The accepted window is 15 minutes before departure through 10 minutes after departure.
The last two distinct source updates must agree.
Repeated copies of one source update cannot establish a stable target.
The target timestamp records when that update was first collected.

Training includes blank and published platforms.
Each input uses only its own snapshot's official platform.
`platform_in` and `platform_preview` are not used as departure targets.
The target is an online report, not proof of the physical platform used.

## Files and evaluation

`adif/results/` contains station and monthly CSV reports, three split lists, and `audit.json`.
`adif/hive/station=ID/part0.arrow` contains the selected training examples.
`adif/hive/dataset.json` records the input archives and selected stations.
Generated data, caches, models, and detailed reports are excluded from Git.

Each departure contributes at most one example per saved lead time, with a total weight of one.
All examples from a departure stay in one chronological training, validation, or test period.
Labels that cross a period boundary are excluded.
Validation log loss selects the tree count. Test data is not used for training.
`--refit` fits the final model on all data after the backtest.

The report separates blank, published, and revised platforms at the 30-minute target.
It also records a historical-frequency baseline and agreement with the early official platform.
Raw CatBoost scores are not calibrated probabilities of the physical platform.
Models need at least 50 training departures, three service dates, and multiple training platforms.

## API

The existing shared service also accepts `POST /predict?operator=adif`.
`GET /stations?operator=adif` lists available models.
The `/adif/predict` and `/adif/stations` routes remain aliases.
SNCF remains the default if `operator` is absent.
Set `ADIF_MODELS_DIR` to change the model directory.

Compact train inputs contain `trainId`, `trainNumber`, `scheduledTime`, `stopType`, and `destination`.
`scheduledTime` must include a UTC offset. `stopType` is `origin`, `intermediate`, or `destination`.
Optional fields are `carrier`, `category`, `trafficType`, `status`, `delayMinutes`, `platform`, and `cancelled`.
Use the planned technical number, company, sorted unique product names, and sorted unique destination codes from the feed.
Join multiple products or destination codes with `|`.

Station selection limits which stations have models, not which trains receive predictions.
At a station with a model, every non-cancelled train departure can receive predictions.
There is no lead-time cutoff. The lead time remains a model input.
The API returns every platform score, with train identity and local clock fields.
The official platform is an input, not a reason to skip a train.
No ADIF browser adapter is included in this work.

## Tests

```bash
julia --project=rfi adif/test_prepare.jl
uv run --project predict python -m pytest adif/test_train.py adif/test_server.py
```
