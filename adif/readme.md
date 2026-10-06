# ADIF

The [userscript](../src/content.user.js) adds predictions to [Pantallas estaciones ADIF](https://pantallas-estaciones.vercel.app/).
Select a departure board and keep the platform column visible.

# train

Run from the repository root with complete collector archives.

```
julia --project=rfi -e 'using Pkg; Pkg.instantiate()'
julia --project=rfi adif/prepare.jl /path/to/data/es-adif
uv run --project predict python -m adif.train --refit
```

Models go to `adif/models/`. See [predict/](../predict/readme.md#predict) to run the server.
