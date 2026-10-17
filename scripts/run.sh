#!/bin/bash

bunx web-ext run -s src/ --start-url "${1:-https://www.garesetconnexions.sncf/fr/gares-services/nice/horaires}"
