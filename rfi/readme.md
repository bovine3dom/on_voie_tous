# RFI models

Install Julia and uv. Run these commands from the repository root.

## Prepare data and train

Use complete `.zst` archives from the RFI data collector.
Each archive contains JSON-line records with `ts`, `station`, and `data` fields.
The `data` field contains the board HTML.

Replace `/path/to/data/rfi-iechub` with your archive directory.

```bash
julia --project=rfi -e 'using Pkg; Pkg.instantiate()'
julia --project=rfi rfi/wrangler.jl /path/to/data/rfi-iechub
uv sync --project predict --frozen
uv run --project predict python -m rfi.train --workers=1 --threads=4 --refit
```

[stations.txt](stations.txt) supplies the station IDs to process.
Training data goes to `rfi/hive/`. Models and the evaluation report go to `rfi/models/`.
Models predict the last reported platform. `--refit` trains on all available data after evaluation.

Use `--stations 1728 2416` to train selected stations, or `--help` to see all training options.
Preparation and training can require substantial memory. Start with one worker.
Use `--models PATH` to save models outside the directory used by a running server.

## Select stations from your archives

Run the audit before data preparation to replace the supplied station list:

```bash
julia --project=rfi rfi/audit.jl /path/to/data/rfi-iechub
cp rfi/results/needs_predictions.txt rfi/stations.txt
```

The audit selects stations with at least 100 departures and at least 10% missing platforms at 30 minutes before departure.
`minimum_departures.json` overrides the minimum departure count for individual stations.
Use `--lead`, `--min`, and `--rate` to change the selection thresholds.
Results go to `rfi/results/`.

## Use the models

Follow [server setup](../predict/readme.md).
Set `RFI_MODELS_DIR` if the models are not in `rfi/models/`.
Use `operator=rfi` for API requests.

For browser installation and supported boards, see [On Voie Tous](../readme.md#install-and-use).
