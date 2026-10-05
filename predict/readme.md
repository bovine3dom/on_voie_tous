# SNCF models

Run these commands from `predict/`.

## Get archives

```bash
rsync -rlt --include="*.zst" --include='*/' --exclude='*' --info=progress2 the_server:/mnt/chungus/slowjects/datagrabber/data/sncf-gares-connexions/ .
```

## Train

Use `--data` to select the station data directory.

```bash
uv run model.py --data /mnt/sncf-hive --models models-v2
```

## Run the server

```bash
SNCF_MODELS_DIR=models-v2 uv run predict.py
```

Keep `models/` for fallback. Set `SNCF_MODELS_DIR=models` to use the old models.
Restart the server after you change the model directory.

See [rfi/](../rfi/readme.md) for RFI data and models.
