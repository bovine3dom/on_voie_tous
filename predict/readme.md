# Prediction server

Install uv and Python 3.11 or later. Run these commands from the repository root.

## Start the server

The server needs trained models for each station.
See [RFI training](../rfi/readme.md), [ADIF training](../adif/readme.md), or [SNCF training](#train-sncf-models).

```bash
uv sync --project predict --frozen
uv run --project predict python -m predict.server
```

The default address is `0.0.0.0:8000`.
Set `PREDICT_HOST` and `PREDICT_PORT` to change it.

| Model directory setting | Default |
| --- | --- |
| `SNCF_MODELS_DIR` | `predict/models/` |
| `RFI_MODELS_DIR` | `rfi/models/` |
| `ADIF_MODELS_DIR` | `adif/models/` |

Relative paths in these settings are relative to the working directory.
Restart the server after you replace models or change their directories.

## API and browser connection

- `GET /health`: server health.
- `GET /stations?operator=adif`: stations with ADIF models.
- `POST /predict?operator=adif`: ADIF platform predictions.
- `/docs`: request fields and interactive API documentation.

Use `operator=rfi` or `operator=sncf` for the other operators.
Requests without an operator select SNCF.

To connect the userscript to your server, change `PREDICT_SERVER_URL` in [src/content.user.js](../src/content.user.js).
Use an HTTPS URL that the browser can reach.

## Train SNCF models

The trainer reads `station=<ID>/part0.arrow` files from the data directory.
Each row needs `actualPlatform`, `timestamp`, `scheduledTime`, and `predictedTime`.
Times are Unix seconds. See [sncf_features.py](sncf_features.py) for the categorical input columns.

Replace `/path/to/sncf-hive` with your prepared data directory.

```bash
uv run --project predict python -m predict.model --data /path/to/sncf-hive --models predict/models
```

Use `--models PATH` to save models outside the directory used by a running server.
Set `SNCF_MODELS_DIR` to that directory when you start the server.
