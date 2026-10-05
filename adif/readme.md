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

A station needs predictions if at least 10% of observed departure platforms are blank.
The default minimum is 100 distinct departures.
Either the full period or a supported month can meet these thresholds, as in RFI.
Stations with platforms consistently published are skipped.
Platform revisions are reported, but do not select a station for modelling.

`adif/minimum_departures.json` sets the minimum to five for 498 stations from the first audit.
These stations were unresolved and had 1–99 eligible departures at 30 minutes.
Other stations retain the default minimum. Use `--minimums=PATH` for another file.
Overrides take priority over `--min`.
The options `--min`, `--rate`, and `--lead` change the selection.

## Source and targets

SignalR `ReceiveMessage` arguments contain JSON text, sometimes with a second JSON encoding.
Empty messages and control replies are not empty station boards.
The station ID comes from `station_settings.code`, not the outer archive label.
The `PRE-ECM-`, `PRO-ECM-`, and `ECM-` subscription prefixes do not split station history.
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

Training retains the first snapshot in each 15-minute lead-time interval across the full observed board range.
There is no training lead-time cutoff. Delayed departures can have negative scheduled lead times.
The audit snapshots are also retained for consistent 30-minute evaluation.
Duplicate snapshots are removed. Each departure has a total training weight of one.
When three nonempty chronological periods exist, all examples from a departure stay in one period.
Labels that cross a period boundary are excluded.
Validation log loss selects the tree count. Test data is not used for training.
`--refit` fits the final model on all data after the backtest.
Without a usable backtest, the model fits all labelled data and records `backtest_status=unavailable`.

The report separates blank, published, and revised platforms at the 30-minute target.
These measurements do not validate other lead times or the client's merged platform display.
It also records a historical-frequency baseline and agreement with the early official platform.
Raw CatBoost scores are not calibrated probabilities of the physical platform.
There is no minimum number of training departures or service dates.
CatBoost fits histories with multiple platforms and variable inputs.
Single-platform histories or constant inputs use a weighted platform-frequency predictor saved as `.prior.json`.
The API serves both formats. A single observed platform receives a score of one, not a guarantee of future use.
Only stations without labelled departures cannot receive a predictor.

## Earlier results after the five-departure recheck: 5 October 2026

The run read 75 archives, with 661 stations in the feed.
Only four service dates had usable departure observations: 7–8 July and 4–5 October 2026.
July contributed 1,018,418 board messages before the subscription ID update.
All 78,300 archived August and September records were null SignalR completions, with no station boards.

| Split | Stations |
| --- | ---: |
| Needs predictions | 67 |
| Skip | 420 |
| Insufficient data | 174 |

The initial minimum of 100 selected only 17 stations and left 591 unresolved.
The 498 unresolved stations with 1–99 eligible departures were rechecked with a minimum of five.
Of these, 50 need predictions, 367 can be skipped, and 81 remain unresolved.
The other 93 unresolved stations had no eligible 30-minute observations.

`adif/stations.txt` records the selected IDs. Detailed counts remain in `adif/results/stations.csv`.
For example, missing platforms at 30 minutes were 96.8% at Madrid Chamartín and 91.8% at Barcelona Sants.
Stations with insufficient observations are not confirmed skips.

Julia produced 242,458 examples from 8,525 labelled departures at 61 selected stations.
Six selected stations had no stable targets. Twenty lacked three service dates.
Twenty-six had insufficient training data, and one had a single training platform.
Fourteen stations still produced models. The new candidates do not yet meet the training requirements.
A lower audit minimum does not supply more training history.
The models were refitted on all labelled data after evaluation.

Training used the available July dates. Validation used 4 October, and testing used 5 October.
At 30 minutes, the highest-score platform agreed with the later report as follows:

| Cases | Departures | CatBoost | Historical frequency |
| --- | ---: | ---: | ---: |
| All | 2,005 | 59.1% | 43.7% |
| Blank official platform | 1,433 | 51.8% | 41.1% |
| Published official platform | 572 | 77.4% | 50.2% |
| Published platform later changed | 16 | 18.8% | 18.8% |

Copying the early official platform agreed with 97.2% of later published targets.
CatBoost improved the blank-platform result, but was worse than copying published official platforms.
Only four observed dates support these results. Use the models as experiments, not guarantees.
Station monitors and announcements remain the authority.

`adif/evaluation.csv` records the per-station backtest. Regenerate it after training:

```bash
uv run --project predict python -m predict.board_report adif/models/report.json adif/results/stations.csv adif/evaluation.csv
cp adif/results/needs_predictions.txt adif/stations.txt
```

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
