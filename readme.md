# On Voie Tous

On Voie Tous adds platform estimates to SNCF, RFI, and ADIF departure boards.
Predictions require a trained model for the station.

## Install and use

Install the [Firefox extension](https://addons.mozilla.org/en-US/firefox/addon/on-voie-tous/),
or load [src/content.user.js](src/content.user.js) in a userscript manager such as Tampermonkey.
Reload the departure board after installation.

- **SNCF:** Open a [Gares & Connexions departure board](https://www.garesetconnexions.sncf/fr/gares-services/lyon-part-dieu/horaires).
- **RFI:** Open an [RFI departure board](https://iechub.rfi.it/ArriviPartenze/ArrivalsDepartures/Monitor?placeId=1728&arrivals=False).
- **ADIF:** Open [Pantallas estaciones ADIF](https://pantallas-estaciones.vercel.app/). Select a station and a departure board. Keep the platform column visible.

SNCF and RFI show `official | predicted`, or only predictions if the official platform is blank.
ADIF estimates have an `Est.` label beside the platform value.
Percentages are model scores, not measured accuracy. Always check station monitors and announcements.

The script sends station and train data to `https://compute.olie.science/on_voie_tous` for predictions.
You do not need to run a server to use this service.

![SNCF departure board with and without platform estimates](promo.png)

## Run your own server

See [server setup](predict/readme.md).
For data preparation and training, see [SNCF](predict/readme.md#train-sncf-models), [RFI](rfi/readme.md), or [ADIF](adif/readme.md).

## Development

Run these commands from the repository root.
The extension scripts require Bun and Firefox.

```bash
scripts/run.sh
scripts/build.sh
```

To run the tests, install uv, Julia, and Chromium:

```bash
uv sync --project predict --frozen
uv run --project predict --with playwright python -m pytest
julia --project=rfi -e 'using Pkg; Pkg.instantiate()'
julia --project=rfi rfi/test_audit.jl
julia --project=rfi rfi/test_wrangler.jl
julia --project=rfi adif/test_prepare.jl
```

Browser tests run headless. Set `CHROMIUM_PATH` if Chromium is not on your executable search path.
