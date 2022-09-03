#!/bin/zsh
python loader2.py --verbose --reset
find ../gamestats_data -type f -name \*.csv  -exec python loader2.py --verbose --load '{}' \;
