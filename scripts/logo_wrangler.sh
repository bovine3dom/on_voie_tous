#!/bin/bash

# Convert $1 SVG to 32x32 PNG and 64x64 PNG
# Usage: logo_wrangler.sh <svg_file>

if [ $# -ne 1 ]; then
    echo "Usage: logo_wrangler.sh <svg_file>"
    exit 1
fi

svg_file=$1

inkscape -w 32 -h 32 $svg_file -o $(basename $svg_file .svg)_32.png 
inkscape -w 64 -h 64 $svg_file -o $(basename $svg_file .svg)_64.png 
inkscape -w 128 -h 128 $svg_file -o $(basename $svg_file .svg)_128.png 
