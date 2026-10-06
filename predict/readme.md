Run from `predict/`.

# grab data

```
rsync -rlt --include="*.zst" --include='*/' --exclude='*' --info=progress2 the_server:/path/to/sncf-gares-connexions/ .
```

# train

Training reads prepared `sncf-hive/station=ID/part0.arrow` files.

```
uv run model.py
```

# predict

```
SNCF_MODELS_DIR=models-v2 uv run predict.py
```

API docs: `http://localhost:8000/docs`. Restart after replacing models.
