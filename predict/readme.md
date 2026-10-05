# SNCF models

Run these commands from `predict/`.

## Get archives

```bash
rsync -rlt --include="*.zst" --include='*/' --exclude='*' --info=progress2 the_server:/mnt/chungus/slowjects/datagrabber/data/sncf-gares-connexions/ .
```

## Train

Station data must be in `sncf-hive/`.

```bash
uv run model.py
```

## Run the server

```bash
uv run predict.py
```

See [rfi/](../rfi/readme.md) for RFI data and models.
