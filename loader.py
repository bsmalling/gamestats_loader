#!/usr/bin/env python
import sqlalchemy as sa
import cryptography
from dateutil import parser as dtparser
import pymysql
import csv
import sys
import os

def format_column_values(values):
    return [ "NULL" if value == '' else "'" + value + "'" for value in values ]

def format_column_names(columns):
    return [ "`" + column + "`" for column in columns ]

def load_match_overview(uri, reader):
    columns = [
        #"match_key",
        "match_id",
        "match_time",
        "team1_name",
        "team2_name",
        "game_mode",
        "map_name",
        "winner_name",
        "team1score",
        "team2score",
        "atk_at_start",
        "team1_atk_wins",
        "team1_def_wins",
        "team2_atk_wins",
        "team2_def_wins",
        "team1_score_at_half",
        "team2_score_at_half"
    ]
    query_columns = ",".join(format_column_names(columns))

    engine = sa.create_engine(uri)
    row = next(reader)
    while len(row) > 2:
        values = row[1:]
        # Format match_time expression for MySql datetime
        values[1] = dtparser.parse(values[1]).strftime("%Y-%m-%d %H:%M:%S")
        values = ",".join(format_column_values(values))

        query = "INSERT INTO `matches` (%s) VALUES (%s)" % (query_columns, values)
        result = engine.execute(query)
        match_key = result.lastrowid
        row = next(reader)

    return match_key

def load_match_performance(uri, reader, match_key):
    columns = [
        "match_key",
        "team_name",
        "player_name",
        "player_rating",
        "atk_rating",
        "def_rating",
        "k_d",
        "entry",
        "trade_diff",
        "kost",
        "kpr",
        "srv",
        "hs",
        "atk_op",
        "def_op",
        "kills",
        "refrags",
        "headshots",
        "underdog_kills",
        "onevx",
        "multikill_rounds",
        "deaths",
        "traded_deaths",
        "traded_by_enemy",
        "opening_kills",
        "opening_deaths",
        "entry_kills",
        "entry_deaths",
        "planted_defuser",
        "disabled_defuser",
        "teamkills",
        "teamkilled",
        "ingame_points",
        "last_alive"
    ]
    query_columns = ",".join(format_column_names(columns))

    engine = sa.create_engine(uri)
    row = next(reader)
    while len(row) > 30:
        values = row[2:-1]
        # Fix kost value of "NaN"
        values[8] = "" if values[8] == "NaN" else values[8]
        # Fix hs percentage expression for MySql
        values[11] = str(float(values[11][:-1]) / 100.0)
        # Fix last_alive value of "undefined"
        values[32] = "" if values[32] == "undefined" else values[32]
        values = ",".join(format_column_values(values))

        query = "INSERT INTO `performance` (%s) VALUES (%d,%s)" % (query_columns, match_key, values)
        engine.execute(query)
        row = next(reader)

def load_match_player_rounds_data(uri, reader, match_key):
    columns = [
        "match_key",
        "team_name",
        "player_name",
        "round",
        "map",
        "site",
        "side",
        "result",
        "round_time",
        "victory_type",
        "operator",
        "time_spent_alive",
        "kills",
        "refrags",
        "headshots",
        "underdog_kills",
        "onevx",
        "death",
        "traded_death",
        "refragged_by",
        "traded_by_enemy",
        "opening_kill",
        "opening_death",
        "entry_kill",
        "entry_death",
        "planted_defuser",
        "disabled_defuser",
        "teamkills",
        "teamkilled",
        "ingame_points",
        "last_alive",
        "drones_deployed",
        "drone_survived_prep_phase",
        "drone_found_bomb",
        "total_droning_distance",
        "total_time_drone_piloting",
        "yellow_ping_leading_to_kill"
    ]
    query_columns = ",".join(format_column_names(columns))

    engine = sa.create_engine(uri)
    row = next(reader)
    while len(row) > 2:
        values = row[2:-1]
        # Fix time_spent_alive when set to "-"
        values[10] = "" if values[10] == "-" else values[10]
        # Fix ingame_points when set to "undefined"
        values[28] = "" if values[28] == "undefined" else values[28]
        # Fix boolean values
        if len(values) > 34:
            values[31] = "1" if values[31] == "true" else "0"
            values[32] = "1" if values[32] == "true" else "0"
            values[35] = "1" if values[35] == "true" else "0"
        # Empty values at the end of the table are not read from csv
        while len(values) < len(columns) - 1:
             values.append('')
        values = ",".join(format_column_values(values))

        query = "INSERT INTO `player_rounds` (%s) VALUES (%d,%s)" % (query_columns, match_key, values)
        engine.execute(query)
        row = next(reader)

def load_match_round_events_breakdown(uri, reader, match_key):
    columns = [
        "match_key",
        "round",
        "time_into_round",
        "vod_time",
        "team_name",
        "type",
        "actor_name",
        "victim_name",
        "blue_team_alive",
        "orange_team_alive",
        "kill_subtype"
    ]
    query_columns = ",".join(format_column_names(columns))

    engine = sa.create_engine(uri)
    row = next(reader)
    while len(row) > 2:
        values = row[2:]
        # Fix time_into_round value of "NaN"
        values[1] = "0" if values[1] == "NaN" else values[1]
        # Fix vod_time value of "aN:aN"
        values[2] = "00:00" if values[2] == "aN:aN" else values[2]
        values = ",".join(format_column_values(values))

        query = "INSERT INTO `round_events` (%s) VALUES (%d,%s)" % (query_columns, match_key, values)
        engine.execute(query)
        row = next(reader)

def do_load(uri, filename):
    fh = open(filename)
    try:
        reader = csv.reader(fh)
        row = next(reader)
        match_key = -1
        while True:
            if len(row) <= 1:
                row = next(reader)
                continue

            label = row[1]
            if label == "MATCH OVERVIEW":
                next(reader) # Skip header
                match_key = load_match_overview(uri, reader)
            elif label == "MATCH PERFORMANCE":
                next(reader) # Skip header
                load_match_performance(uri, reader, match_key)
            elif label == "PLAYER ROUNDS DATA":
                next(reader) # Skip header
                load_match_player_rounds_data(uri, reader, match_key)
            elif label == "ROUND EVENTS BREAKDOWN":
                next(reader) # Skip header
                load_match_round_events_breakdown(uri, reader, match_key)
                break

            row = next(reader)
    finally:
        fh.close()

def do_reset(uri):
    engine = sa.create_engine(uri)
    engine.execute("SET FOREIGN_KEY_CHECKS = 0")
    engine.execute("TRUNCATE TABLE `round_events`")
    engine.execute("TRUNCATE TABLE `player_rounds`")
    engine.execute("TRUNCATE TABLE `performance`")
    engine.execute("TRUNCATE TABLE `matches`")
    engine.execute("SET FOREIGN_KEY_CHECKS = 1")

def show_help():
    print("loader.py [command] [arguments...]")
    print()
    print("Commands:")
    print("  LOAD [filename]")
    print("  RESET")
    print("  HELP")

def main():
    uri = "mysql+pymysql://appuser:%s@localhost/gamestats" % os.environ["GAMESTATS_APPUSER_PWD"]

    command = sys.argv[1].lower()

    if command == "load":
        do_load(uri, sys.argv[2])
    elif command == "reset":
        do_reset(uri)
    elif command == "help":
        show_help()
    else:
        print("Invalid command.")
        show_help()

if __name__ == "__main__":
    main()