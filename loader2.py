#!/usr/bin/env python
import sqlalchemy as sa
import cryptography
from dateutil import parser as dtparser
import pymysql
import csv
import sys
import getopt
import os
import re

verbose = False

class MySQLTable:
    """
    A class used to fill a MySQL table from corresponding CSV data.
    """

    def __init__(self, table_name, engine):
        self._table_name = table_name
        results = engine.execute(f"DESCRIBE " + table_name)
        self._column_names = list()
        self._column_info = dict()
        for col_name, col_type, nullable, key, default, extra in results:
            self.column_names.append(col_name)
            col_type = re.search("(\w+)", col_type).group()
            extra = ("auto_increment" in extra)
            self._column_info[col_name] = (col_type, extra)

    @property
    def table_name(self):
        return self._table_name

    @property
    def column_names(self):
        return self._column_names

    @property
    def column_info(self):
        return self._column_info

    def load(self, reader, engine, key=None):
        if verbose:
            print(f"Loading table {self._table_name}...")

        auto_inc = self._column_info[self._column_names[0]][1]

        row_count = 0
        match_key = None
        row = next(reader)
        while len(row) > len(self._column_names) - 10:
            if auto_inc:
                values = row[1:]
            else:
                values = row[2:]

            if key != None:
                values.insert(0, str(key))

            table_columns = self._format_column_names()
            table_values = self._format_column_values(values) 
            query = "INSERT INTO `%s` (%s) VALUES (%s)" % (self._table_name, table_columns, table_values)
            #print(query)
            result = engine.execute(query)
            row_count += 1
            if auto_inc:
                match_key = result.lastrowid
            row = next(reader)

        if verbose:
            print(f"Loaded {row_count} rows.")
        return match_key

    def _format_column_names(self):
        sql_columns = []
        for col_name in self._column_names:
            _, auto_inc = self._column_info[col_name]
            if auto_inc:
                continue
            else:
                sql_columns.append("`" + col_name + "`")
        return ",".join(sql_columns)

    def _format_column_values(self, values):
        while len(values) < len(self._column_names):
            values.append('')

        index = 0
        sql_values = []
        for col_name in self._column_names:
            value = values[index]

            col_type, auto_inc = self._column_info[col_name]
            if auto_inc:
                continue
            index += 1

            if col_type == "int":
                if value == "" or value == "-" or value == "undefined":
                    sql_values.append("NULL")
                else:
                    sql_values.append(value)
                continue

            if col_type == "bigint":
                if value == "" or value == "-" or value == "undefined" or value == "NaN":
                    sql_values.append("NULL")
                else:
                    sql_values.append(value)
                continue

            if col_type == "float":
                if value == "" or value == "NaN":
                    sql_values.append("NULL")
                elif value[-1] == "%":
                    sql_values.append(str(float(value[:-1]) / 100.0))
                else:
                    sql_values.append(value)
                continue
               
            if col_type == "varchar":
                if (value == "-"):
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

            if col_type == "tinyint" or col_type == "boolean":
                if value == "":
                    sql_values.append("NULL")
                else:
                    sql_values.append("1" if value == "true" else "0")
                continue

            raise NotImplementedError(f"Unknown column type: {col_type}.")

        return ",".join(sql_values)

def do_load(engine, filename):
    matches = MySQLTable("matches", engine)
    performance = MySQLTable("performance", engine)
    player_rounds = MySQLTable("player_rounds", engine)
    round_events = MySQLTable("round_events", engine)

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
                match_key = matches.load(reader, engine)
            elif label == "MATCH PERFORMANCE":
                next(reader) # Skip header
                performance.load(reader, engine, match_key)
            elif label == "PLAYER ROUNDS DATA":
                next(reader) # Skip header
                player_rounds.load(reader, engine, match_key)
            elif label == "ROUND EVENTS BREAKDOWN":
                next(reader) # Skip header
                round_events.load(reader, engine, match_key)
                break

            row = next(reader)
    finally:
        fh.close()

def do_reset(engine):
    if verbose:
        print("Resetting database...")

    engine.execute("SET FOREIGN_KEY_CHECKS = 0")
    engine.execute("TRUNCATE TABLE `round_events`")
    engine.execute("TRUNCATE TABLE `player_rounds`")
    engine.execute("TRUNCATE TABLE `performance`")
    engine.execute("TRUNCATE TABLE `matches`")
    engine.execute("SET FOREIGN_KEY_CHECKS = 1")

    if verbose:
        print("Database reset.")

def show_help():
    print("loader2.py [arguments...]")
    print("Arguments:")
    print(str.ljust("  -l or --load [filename]", 30) + "Load [filename]")
    print(str.ljust("  -r or --reset", 30) + "Reset database")
    print(str.ljust("  -v or --verbose", 30) + "Verbose")
    print(str.ljust("  -h or --help", 30) + "Show help")

def main():
    global verbose

    try:
        opts, args = getopt.getopt(sys.argv[1:], "l:rvh", ["load=", "reset", "verbose", "help"])
    except getopt.GetoptError:
        show_help()
        sys.exit(2)

    reset = False
    filename = None
    for opt, arg in opts:
        if opt in ("-l", "--load"):
            filename = arg
        elif opt in ("-r", "--reset"):
            reset = True
        elif opt in ("-v", "--verbose"):
            verbose = True
        elif opt in ("-h", "--help"):
            show_help()
            sys.exit(0)

    uri = "mysql+pymysql://appuser:%s@localhost/gamestats" % os.environ["GAMESTATS_APPUSER_PWD"]
    engine = sa.create_engine(uri)

    if reset:
        do_reset(engine)

    if filename != None:
        if verbose:
            print(f"Loading {filename}...")
        do_load(engine, filename)

if __name__ == "__main__":
    main()