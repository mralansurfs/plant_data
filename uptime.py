#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 27 21:38:09 2024

@author: alan
"""

import sqlite3
import datetime
import pandas as pd


def querySqlite(queryString):
    queryResult = []
    try:
        sqliteConnection = sqlite3.connect('plant_data.db')
        queryResult = pd.read_sql_query(queryString, sqliteConnection)
        queryResult['timestamp'] = pd.to_datetime(queryResult['timestamp'])

    except sqlite3.Error as error:
        print("Failed to read data from sqlite table", error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()
    return(queryResult)
            
plant_state_data_queryResult = querySqlite("""SELECT timestamp,value,plant_line from plant_state_data""")

