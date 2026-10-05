# RFI station availability audit

This audit selects stations for later platform-prediction processing.
It does not train a model or measure whether platforms are predictable.

## Run

From the repository root:

```bash
julia --project=rfi -e 'using Pkg; Pkg.instantiate()'
julia --project=rfi --threads=16 rfi/audit.jl /path/to/datagrabber/data/rfi-iechub
julia --project=rfi rfi/test_audit.jl
```

Only complete `.zst` archives are read. The active `current.txt` file is not read.
The decoder permits 256 MiB windows.
Each worker streams its archives and keeps compact departure summaries.
Each active decoder can need 256 MiB of history memory, plus buffers and summaries.
Use fewer threads on a small machine.
The parser reads selected cells from escaped HTML without decoding embedded images.
The archive data is not changed.

## Output

Files are written to `rfi/results/`:

- `needs_predictions.txt`: one RFI station ID per line. Process these stations first.
- `skip.txt`: stations where platforms are usually available at the selected lead time.
- `insufficient_data.txt`: stations that must not be treated as confirmed skips.
- `stations.csv`: station names, split decisions, counts, and missing-platform percentages.
- `monthly.csv`: the same availability measurements for each service month.
- `audit.json`: input files, date coverage, thresholds, and run statistics.

Generated results and the local cache are excluded from Git.

## Build training data

Use the selected stations and complete archives:

```bash
julia --project=rfi --threads=16 rfi/wrangler.jl /path/to/datagrabber/data/rfi-iechub
julia --project=rfi rfi/test_wrangler.jl
```

Julia writes `rfi/hive/station=ID/part0.arrow` and `rfi/hive/dataset.json`.
Python can load one station at a time with the SNCF Arrow loader.
Archive summaries are cached separately from the availability audit.

Each departure supplies at most one blank-platform example at each saved lead time.
Examples share a departure ID and a total training weight of one.
The label is the last reported platform near the delay-adjusted departure.
The accepted window is 15 minutes before departure through 10 minutes after departure.
The final two distinct observations must agree.
Cancelled trains, buses, conflicting timestamps, and departures without stable labels are excluded.
A published early platform is never used as an input or a training example.
This label is not proof of the physical platform used.

## Train and evaluate

From the repository root, use the existing SNCF Python environment:

```bash
uv sync --project predict --frozen
uv run --project predict python -m rfi.train
uv run --project predict python -m pytest rfi/test_train.py
```

The trainer reuses SNCF station discovery, Arrow loading, and the `.cbm` model format.
It loads one station at a time. Models are saved in `rfi/models/`.
Use `--stations 1728 2416` for a small run.
Use `--workers=4 --threads=4` to train four stations at a time with four threads each.
The default minimum is 50 distinct training departures and three service dates.
Stations with only one training platform are skipped, as in the SNCF trainer.

Dates are split into chronological training, validation, and test periods.
All examples from a departure stay in one period.
Targets that cross the next period boundary are excluded.
Validation log loss selects the tree count. Test data is not used for training.
Unknown test platforms remain in the accuracy denominator.

`rfi/models/report.json` records accuracy, coverage, and a historical-frequency baseline.
The primary measurements use only blank-platform cases at the 30-minute lead time.
The confidence scores are raw CatBoost probabilities, not calibrated accuracy estimates.
Saved models use only the training period by default.
Use `--refit` to train the deployed model on all data after evaluation.
The report keeps the earlier backtest measurements separate from the deployed model's date and classes.

## MVP evaluation snapshot

The full run used 751 archives and the 219 selected stations.
Julia produced 310,302 examples from 101,823 labelled departures, in about 60 MiB of Arrow files.
Of the selected stations, 127 had usable examples and 85 produced models.
The other stations lacked stable targets, enough training data, or multiple training platforms.

`rfi/evaluation.csv` records the per-station backtest results.
The test period contained 11,298 eligible 30-minute cases across the trained models.
At an analysis cutoff of 0.8, coverage was 46.7% and agreement with later reports was 97.8%.
Without that cutoff, top-choice agreement was 70.5%, compared with 71.1% for the historical-frequency baseline.
These cutoff measurements do not describe the current grouped client display.
CatBoost did not improve the overall top-choice accuracy over that baseline.

| Station | Coverage at 0.8 | Agreement for accepted estimates |
| --- | ---: | ---: |
| Milano Centrale | 10.3% | 91.7% |
| Roma Termini | 10.8% | 95.9% |
| Napoli Centrale | 1.8% | 63.6% |
| Firenze Santa Maria Novella | 0% | No accepted estimates |
| Torino Porta Nuova | 0% | No accepted estimates |

These measurements apply to the earlier backtest models and observed online reports.
They are not guarantees for new trains or physical platforms.
The deployed models were refitted on all available labelled data after evaluation.
Model files and detailed reports remain local in `rfi/models/`.

## Serve predictions

Start the shared SNCF and RFI server from the repository root:

```bash
uv run --project predict python -m predict.server
uv run --project predict python -m pytest predict/test_predict.py rfi/test_server.py
```

Both operators use the same FastAPI app and `POST /predict` endpoint.
Use `/predict?operator=rfi` for RFI and `/predict?operator=sncf` for SNCF.
If `operator` is absent, the endpoint selects SNCF.
The `/rfi/predict` route remains an RFI alias.
RFI uses separate models and the shared model loader and response schema.
The `/rfi/stations` route lists available models.
Set `RFI_MODELS_DIR` to change the RFI model directory.
Restart the server after training to clear its model cache.
`PREDICT_HOST` and `PREDICT_PORT` retain their SNCF meanings.

RFI inputs contain a UTC collection timestamp, a station ID, and compact train fields.
Train fields include `trainId`, `trainNumber`, `clock`, `destination`, `carrier`, and `category`.
Optional fields are `delayMinutes`, `platform`, and `cancelled`.
Scheduled times are inferred in `Europe/Rome`, as in the Julia preprocessing code.
Responses include the train ID and clock. They do not depend on row order.

Published platforms, cancellations, buses, and unsupported lead times receive no prediction.
The supported range is 15–130 minutes before scheduled departure.
Every eligible train receives the full sorted platform distribution, with no server-side score cutoff.
Scores are not changed or rescaled. The client selects the displayed platforms.
A missing model or incompatible schema leaves the station board unchanged.

## Show estimates on RFI boards

Install `src/content.user.js` as a userscript, or build the existing browser extension.
The same script supports SNCF boards and RFI departure monitors.
Remove the old separate `rfi.user.js` userscript if you installed it.
The RFI adapter calls `/predict?operator=rfi`; the SNCF adapter calls `/predict`.

The default server URL is `https://compute.olie.science/on_voie_tous`.
It must run `predict.server` before RFI predictions are available.
The previous `rfi.server` command remains compatible.
Change `PREDICT_SERVER_URL` in the userscript for another server.
Alternatively, set `window.ON_VOIE_TOUS_SERVER` before the script starts.
This work does not deploy a public server.

The adapter requests only compact train fields.
It displays `Stima` beside blank official platform cells.
As in SNCF, scores above 0.05 enter the display selection.
Platforms with scores of at least 0.30 remain separate.
Consecutive numeric or single-letter labels with lower scores are merged, and their scores are added.
Compound labels such as `2EST` and `20B` remain separate.
The client displays at most two platforms or ranges with scores of at least 0.10.
It leaves official platforms unchanged and rejects stale responses.
Server failures leave the board unchanged.
Model scores are not calibrated probabilities of the physical platform.

Run the browser tests in headless Chromium:

```bash
uv run --project predict --with playwright python -m pytest rfi/test_browser.py
```

The tests use the system `chromium` executable.
Set `CHROMIUM_PATH` if it has another location.

## Results

The audit read 751 archives, with observations from 30 March to 5 October 2026.
There is no source data from May through September.

| Split | Stations |
| --- | ---: |
| Needs predictions | 219 |
| Skip | 1,809 |
| Insufficient data | 412 |

The 135 unresolved stations with 1–99 eligible departures were rechecked with a minimum of five.
Of these, 19 need predictions, 65 can be skipped, and 51 still have fewer than five departures.
Other station decisions did not change.

`rfi/stations.txt` is a versioned copy of the 219 selected IDs from this run.
Use it as an initial station filter for data processing.
After a new audit, update this copy if you want to change the filter:

```bash
cp rfi/results/needs_predictions.txt rfi/stations.txt
```

Of the selected stations, 85 had no populated platform in their 15-minute observations.
They might publish platforms later. This result does not establish whether training labels are available.
The full CSV reports retain these measurements for inspection.

## Default split

The default lead time is 30 minutes before scheduled departure.
The audit uses the closest observed snapshot before that target, up to 10 minutes earlier.
This is an observed availability estimate, not an exact announcement time.
A departure without a snapshot in that interval is not counted as a missing platform.

A station needs predictions if at least 10% of its platforms are missing.
The default minimum is 100 distinct departures. Station overrides can set a lower minimum.
Either the complete period or a supported service month can meet these thresholds.
This prevents the large April sample from hiding a different October publication rule.

A station is marked `skip` only when supported evidence stays below the missing-platform threshold.
It remains unresolved if a smaller month contains at least ten missing departures above that threshold.
Stations with insufficient coverage or substantial parsing failures also remain unresolved.

Counts at 15, 30, 60, and 120 minutes are saved.
Repeated snapshots of a departure do not increase its count.
Trains are grouped by station, local departure date, run ID, and scheduled time.
If the run ID is absent, train number, carrier, and destination form the fallback identity.
The station departure date is inferred from the nearest local clock time in `Europe/Rome`.
An overnight train's run ID can contain its origin date instead.
Ambiguous or nonexistent DST times are excluded.

Blank table rows, known cancellations, and bus services are excluded.
A missing platform cell is a parsing failure, not a blank platform.
A populated platform remains a string, including labels such as `2EST` and `20B`.
This audit does not verify the physical platform used by a train.

## Change the selection

Compact summaries are cached per archive in `rfi/.cache/`.
A second run does not need to decompress unchanged archives.
`rfi/minimum_departures.json` sets the minimum to five for the 135 rechecked stations.
It is loaded automatically. Overrides take priority over `--min`.
Use `--minimums=PATH` to load a different override file.

These options change the split without changing the cached observations:

```bash
julia --project=rfi --threads=16 rfi/audit.jl /path/to/data/rfi-iechub \
  --lead=60 --min=100 --rate=0.20 --output=rfi/results-60
```

`--lead` accepts 15, 30, 60, or 120.
`--min` sets the default minimum departure count. `--rate` must be greater than 0 and at most 1.
Use `--limit=1` for a one-archive test.
The cache checks source path, size, modification time, parser version, and Julia version.
Delete `rfi/.cache/` to force a full scan. Load only cache files created locally.

The time zone API is documented by [TimeZones](https://juliatime.github.io/TimeZones.jl/stable/).
The stream decoder options are documented by [CodecZstd](https://github.com/JuliaIO/CodecZstd.jl/blob/v0.8.7/src/decompression.jl).
