# ADIF

The [userscript](../src/content.user.js) adds predictions to [Pantallas estaciones ADIF](https://pantallas-estaciones.vercel.app/).
Select a departure board and keep the platform column visible.

It also adds experimental predictions to [ADIF station pages](https://www.adif.es/w/79400-barcelona-e.fran%C3%A7a).
`Est.` marks predictions. ADIF's platform stays unchanged. Scores are not calibrated probabilities.
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
