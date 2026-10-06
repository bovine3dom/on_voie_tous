# ADIF models

Install Julia and uv. Run these commands from the repository root.

## Prepare data and train

Use complete `.zst` archives from the ADIF data collector.
Each archive contains JSON-line records with `ts`, `station`, and `data` fields.
The `data` field contains a SignalR board message.

Replace `/path/to/data/es-adif` with your archive directory.

```bash
julia --project=rfi -e 'using Pkg; Pkg.instantiate()'
julia --project=rfi adif/prepare.jl /path/to/data/es-adif
uv sync --project predict --frozen
uv run --project predict python -m adif.train --workers=1 --threads=4 --refit
```

Preparation selects stations with at least 100 departures and at least 10% missing platforms at 30 minutes before departure.
`minimum_departures.json` overrides the minimum departure count for individual stations.
Use `--lead`, `--min`, and `--rate` to change the selection thresholds.

Training data goes to `adif/hive/`, and station selection results go to `adif/results/`.
Models and the evaluation report go to `adif/models/`.
Models predict the last reported platform. `--refit` trains on all available data after evaluation.

Use `--stations 17000 71801` to train selected stations, or `--help` to see all training options.
Preparation and training can require substantial memory. Start with one worker.
Use `--models PATH` to save models outside the directory used by a running server.

## Use the models

Follow [server setup](../predict/readme.md).
Set `ADIF_MODELS_DIR` if the models are not in `adif/models/`.
Use `operator=adif` for API requests.

For browser installation and supported boards, see [On Voie Tous](../readme.md#install-and-use).
