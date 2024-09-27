#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 27 21:38:09 2024

@author: alan
"""

import sqlite3

def readSqliteTable():
    try:
        sqliteConnection = sqlite3.connect('plant_data.db')
        cursor = sqliteConnection.cursor()

        sqlite_select_query = """SELECT * from plant_state_data"""
        cursor.execute(sqlite_select_query)
        records = cursor.fetchall()
        for row in records:
            print(row)
        cursor.close()

    except sqlite3.Error as error:
        print("Failed to read data from sqlite table", error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()
            
readSqliteTable()
