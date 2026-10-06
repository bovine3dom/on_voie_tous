# RFI

The [userscript](../src/content.user.js) adds predictions to [RFI departure boards](https://iechub.rfi.it/ArriviPartenze/ArrivalsDepartures/Monitor?placeId=1728&arrivals=False).

# train

Run from the repository root with complete collector archives. [stations.txt](stations.txt) selects the stations.

```
julia --project=rfi -e 'using Pkg; Pkg.instantiate()'
julia --project=rfi rfi/wrangler.jl /path/to/data/rfi-iechub
uv run --project predict python -m rfi.train --refit
```

Models go to `rfi/models/`. See [predict/](../predict/readme.md#predict) to run the server.
