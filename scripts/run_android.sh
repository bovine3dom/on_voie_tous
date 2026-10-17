#!/bin/bash

# Usage: ./run_android.sh <device_id> [url]
if [ -z "$1" ]; then
    echo "Usage: ./run_android.sh <device_id> [url]"
    exit 1
fi

bunx web-ext run -s src/ -t firefox-android --adb-device "$1" --firefox-apk org.mozilla.firefox ${2:+--start-url "$2"}
