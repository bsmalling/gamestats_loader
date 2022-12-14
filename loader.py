#!/usr/bin/env python

"""gamestats loader

    Loads game statistics from a CSV file into the local gamestats MySQL database.
    The database table columns must match the columns in the CSV file (in order)!
"""

import os
import re
import sys
import csv
import getopt
import pymysql
import cryptography
import sqlalchemy as sa
from dateutil import parser as dtparser

VERBOSE = False

class MySQLTableLoader:
    """
    A class used to fill a MySQL table from corresponding CSV data.
    """

    def __init__(self, table_name, engine, verbose=False):
        self.__table_name = table_name
        self.__verbose = verbose
        self.__column_names = list()
        self.__column_info = dict()

        # Extract the current table schema from the database.
        results = engine.execute(f"DESCRIBE `{table_name}`")
        for col_name, col_type, _, _, _, extra in results:
            self.__column_names.append(col_name)
            col_type = re.search(r"(\w+)", col_type).group()
            extra = ("auto_increment" in extra)
            self.__column_info[col_name] = (col_type, extra)

    @property
    def table_name(self):
        """Returns the name of the table."""
        return self.__table_name

    def load(self, reader, engine, key=None):
        """Loads a data section from CSV reader into this database table. The key
        should contain the value of a previous auto-increment insert (if applicable).

        Returns:
            The new auto-increment value (if applicable), otherwise None.
        """

        if self.__verbose:
            print(f"Loading table {self.__table_name}...")

        # Assumes that any auto_increment column is always the first column.
        auto_inc = self.__column_info[self.__column_names[0]][1]

        row_count = 0
        match_key = None
        row = next(reader) # Skip header

        # This data is really shitty and unreliable.
        # Some rows contain meaningless values of 1,2,3,4...
        # We have to ignore these rows. Hence -10.
        while len(row) > len(self.__column_names) - 10:
            if auto_inc:
                # Load including match_id column (matches table).
                values = row[1:]
            else:
                # Load excluding match_id column (other tables).
                values = row[2:]

            # Insert previous auto-increment value into values.
            if key is not None:
                values.insert(0, str(key))

            table_columns = self._format__column_names()
            table_values = self._format_column_values(values)
            query = "INSERT INTO `%s` (%s) VALUES (%s)" % \
                (self.__table_name, table_columns, table_values)
            try:
                result = engine.execute(query)
                if auto_inc:
                    match_key = result.lastrowid
            except:
                print(query)
                raise
            row = next(reader)
            row_count += 1

        if self.__verbose:
            print(f"Loaded {row_count} rows.")
        return match_key

    def _format__column_names(self):
        sql_columns = []
        for col_name in self.__column_names:
            _, auto_inc = self.__column_info[col_name]
            # auto-increment columns are not inserted.
            if auto_inc:
                continue
            sql_columns.append("`" + col_name + "`")

        return ",".join(sql_columns)

    def _format_column_values(self, values):
        # Make sure each column has a value
        while len(values) < len(self.__column_names):
            values.append("")

        index = 0
        sql_values = []
        for col_name in self.__column_names:
            value = values[index]

            col_type, auto_inc = self.__column_info[col_name]
            # auto-increment columns are not inserted.
            if auto_inc:
                continue
            index += 1

            if col_type == "int":
                if value in ("", "-", "undefined"):
                    sql_values.append("NULL")
                else:
                    sql_values.append(value)
                continue

            if col_type == "bigint":
                if value in ("", "-", "undefined", "NaN"):
                    sql_values.append("NULL")
                else:
                    sql_values.append(value)
                continue

            if col_type == "float":
                if value in ("", "NaN"):
                    sql_values.append("NULL")
                elif value[-1] == "%":
                    sql_values.append(str(float(value[:-1]) / 100.0))
                else:
                    sql_values.append(value)
                continue

            if col_type == "varchar":
                if value in ("", "-"):
                    sql_values.append("NULL")
                else:
                    sql_values.append("'" + value + "'")
                continue

            if col_type == "datetime":
                mysql_date = dtparser.parse(value).strftime("%Y-%m-%d %H:%M:%S")
                sql_values.append("'" + mysql_date + "'")
                continue

            if col_type == "time":
                if value == "aN:aN":
                    sql_values.append("NULL")
                else:
                    sql_values.append("'" + value + "'")
                continue

            if col_type in ("tinyint", "boolean"):
                if value == "":
                    sql_values.append("NULL")
                else:
                    sql_values.append("1" if value == "true" else "0")
                continue

            raise NotImplementedError(f"Unknown column type: {col_type}.")

        return ",".join(sql_values)

# end class MySQLTableLoader

def do_load(engine, filename):
    """Loads all sections of data from the specified CSV file."""
    if VERBOSE:
        print(f"Loading {filename}...")

    matches = MySQLTableLoader("matches", engine, VERBOSE)
    performance = MySQLTableLoader("performance", engine, VERBOSE)
    player_rounds = MySQLTableLoader("player_rounds", engine, VERBOSE)
    round_events = MySQLTableLoader("round_events", engine, VERBOSE)

    with open(filename) as file:
        reader = csv.reader(file)
        row = next(reader)

        match_key = None
        while True:
            if len(row) <= 1:
                row = next(reader)
                continue

            label = row[1]
            if label == "MATCH OVERVIEW":
                next(reader) # Skip to header
                match_key = matches.load(reader, engine)
            elif label == "MATCH PERFORMANCE":
                next(reader) # Skip to header
                performance.load(reader, engine, match_key)
            elif label == "PLAYER ROUNDS DATA":
                next(reader) # Skip to header
                player_rounds.load(reader, engine, match_key)
            elif label == "ROUND EVENTS BREAKDOWN":
                next(reader) # Skip to header
                round_events.load(reader, engine, match_key)
                break

            row = next(reader)

def do_reset(engine):
    """Resets (or empties) all of the database tables."""
    if VERBOSE:
        print("Resetting database...")

    engine.execute("SET FOREIGN_KEY_CHECKS = 0")
    engine.execute("TRUNCATE TABLE `round_events`")
    engine.execute("TRUNCATE TABLE `player_rounds`")
    engine.execute("TRUNCATE TABLE `performance`")
    engine.execute("TRUNCATE TABLE `matches`")
    engine.execute("SET FOREIGN_KEY_CHECKS = 1")

    if VERBOSE:
        print("Database reset.")

def show_help():
    """Shows help message with arguments."""
    print("loader.py [arguments...]")
    print("Arguments:")
    print(str.ljust("  -l or --load [filename]", 30) + "Load [filename]")
    print(str.ljust("  -r or --reset", 30) + "Reset database")
    print(str.ljust("  -v or --verbose", 30) + "Verbose output")
    print(str.ljust("  -h or --help", 30) + "Show help")

def main():
    global VERBOSE

    try:
        # Ignoring any extraneous args from getopt. An error might be better?
        opts, _ = getopt.getopt(sys.argv[1:], "l:rvh", ["load=", "reset", "verbose", "help"])
    except getopt.GetoptError:
        show_help()
        sys.exit(2)

    reset = False
    filenames = []
    do_help = False
    for opt, arg in opts:
        if opt in ("-l", "--load"):
            filenames.append(arg)
        elif opt in ("-r", "--reset"):
            reset = True
        elif opt in ("-v", "--verbose"):
            VERBOSE = True
        elif opt in ("-h", "--help"):
            do_help = True

    if do_help or len(opts) == 0:
        show_help()
        sys.exit(0)

    # NOTE: This expects an environment variable called "GAMESTATS_APPUSER_PWD"
    # containing the MySQL database password. The user is expected to be "appuser".
    uri = "mysql+pymysql://appuser:%s@localhost/gamestats" % os.environ["GAMESTATS_APPUSER_PWD"]
    engine = sa.create_engine(uri)
    try:

        if reset:
            do_reset(engine)

        for filename in filenames:
            do_load(engine, filename)

    finally:
        engine.dispose()

if __name__ == "__main__":
    main()
