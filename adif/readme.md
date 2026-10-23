# ADIF

The [userscript](../src/content.user.js) adds predictions to [Pantallas estaciones ADIF](https://pantallas-estaciones.vercel.app/) and to [ADIF station pages](https://www.adif.es/w/79400-barcelona-e.fran%C3%A7a).

Allow access to `www.adif.es` and `compute.olie.science` when you update the extension or userscript.
Website models use `adif/web/models/` (`ADIF_WEB_MODELS_DIR` to override).
The server provides `/adif-web/stations` and `/adif-web/predict`.

# train

Run from the repository root with complete collector archives.

```
julia --project=rfi -e 'using Pkg; Pkg.instantiate()'
julia --project=rfi adif/prepare.jl /path/to/data/es-adif
uv run --project predict python -m adif.train --refit
```

Models go to `adif/models/`. See [predict/](../predict/readme.md#predict) to run the server.

For adif.es boards, install `zstd` and use a new output directory:

```
uv run --project predict python -m adif.web.prepare /path/to/data/es-adif-web --output /path/to/web-hive
uv run --project predict python -m adif.web.train --data /path/to/web-hive --models /path/to/web-models --workers 15 --threads 2 --refit
```

Set `ADIF_WEB_MODELS_DIR` to the new models directory and restart the server.
