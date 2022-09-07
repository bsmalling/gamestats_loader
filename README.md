# gamestats_loader
Python script for loading game stats data into a MySql database from CSV files. The CSV files contain five sections:
1. MATCH OVERVIEW
2. MATCH PERFORMANCE
3. SIXTH PICK OVERVIEW (ignored!)
4. PLAYER ROUNDS DATA
5. ROUND EVENTS BREAKDOWN

The "MATCH OVERVIEW" section is loaded into the "matches" table. An integer "match_key" column is introduced which maps one-to-one with the string "match_id" column. The other tables only contain the integer "match_key" value (joining on string columns is generally a bad idea). You must join these other tables on the integer "matches.match_key" column.

The "MATCH PERFORMANCE" section is loaded into the "performance" table.

The "SIXTH PICK OVERVIEW" section is completely ignored!

The "PLAYER ROUNDS DATA" section is loaded into the "player_rounds" table.

The "ROUND EVENTS BREAKDOWN" column is loaded into the "round_events" table.

Steps to create the database:
1. Create a MySql database/schema called "gamestats".
2. Create a user for the "gamestats" database called "appuser". Allow all basic commands (SELECT,INSERT,UPDATE,DELETE...).
3. Run the "gamestats_schema.sql" script (as root) to create the database tables.
4. Run the commented script (as root) to GRANT DROP access to "appuser".
5. Create an environment variable called "GAMESTATS_APPUSER_PWD" that contains the "appuser" password.
6. Download the CSV files into the "../gamestats_data" directory (will be searched recursively so don't be shy with subdirectories).
7. Run the loader.zsh script to load all of the CSV data files from the "../gamestats_data" directory (recursively).
8. Query the data as is (from MySql) or use a Jupyter Notebook to analyze the data.

Try this in a Jupyter Notebook:

``` python
from sqlalchemy import create_engine
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import pymysql
import os

uri = "mysql+pymysql://appuser:%s@localhost/gamestats" % os.environ["GAMESTATS_APPUSER_PWD"]
engine = create_engine(uri)

matches_df = pd.read_sql('SELECT * FROM `matches`', con=engine)
performance_df = pd.read_sql('SELECT * FROM `performance`', con=engine)
by_count_rating=performance_df.groupby("player_name")["player_rating"].agg(["mean", "count"]).sort_values(by=["count","mean"], ascending=False).head(20)
by_mean_rating=by_count_rating.sort_values(by="mean", ascending=False)
rating_sorter=by_mean_rating.index

limit_data1=performance_df.loc[performance_df["player_name"].isin(rating_sorter)].copy()
limit_data1["player"] = pd.Categorical(
    limit_data1["player_name"], 
    categories=rating_sorter, 
    ordered=True
)
final_data1=limit_data1.sort_values(by="player")

final_data1.boxplot(column="player_rating", by="player", rot=90, figsize=[20,10])
```