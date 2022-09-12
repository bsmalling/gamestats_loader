#!/bin/zsh
python3 loader.py --verbose --reset
find ../gamestats_data -type f -name \*.csv -exec python3 loader.py --verbose --load '{}' \;
