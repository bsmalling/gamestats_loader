#!/bin/zsh
python loader.py --verbose --reset
find ../gamestats_data -type f -name \*.csv -exec python loader.py --verbose --load '{}' \;
