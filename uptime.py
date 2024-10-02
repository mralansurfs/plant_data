#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 27 21:38:09 2024

@author: alan
"""

import sqlite3
import datetime
import pandas as pd
import numpy as np


def querySqlite(queryString):
    df = []
    try:
        sqliteConnection = sqlite3.connect('plant_data.db')
        df = pd.read_sql_query(queryString, sqliteConnection,
                                        parse_dates = ['timestamp'])
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        
    except sqlite3.Error as error:
        print("Failed to read data from sqlite table", error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()
    return(df)

def findStartEndTimes(df,plant_line):
    #For a plant_line fine all the start and end times of the it as defined as
    # changes too or from 4 in df["value_y"]
    df = df[df['plant_line'].str.match(plant_line)]
    # Sort on timestamp and Reset the index (optional)
    df = df.sort_values(by='timestamp')
    # Simplify the plant state to be 4 when on and 0 at all other states 
    df["value_x"] = df["value_x"].where(df['value_x'] == 4, other=0)
    # use diff to ignore all the observations where the plant state has not changed
    df["diff"] = df.groupby("plant_line")["value_x"].diff()
    df = df[df['diff'] != 0]    
     
    # Generate pair IDs 
    df['pair_id'] = (df['value_x'] == 4).cumsum()  # Increment  the pair id on each  4 
    ## replace 4 and 0 with something to use in the final dataframe
    df['start_end'] = df['value_x'].replace({4: 'start', 0: 'end'})
    ## long to wide by group and pair
    df['start_end'] = pd.Categorical(df['start_end'] , categories=['start', 'end'], ordered=True)
    df = df.pivot(index='pair_id', columns='start_end', values = ["timestamp","counter"])
    return(df)

def cleanCounter(df):
    # THe counter values have resets to zero in them. This function recomputes 
    # the counter so it has not resets to zero and is allways cumulative.
    # Take the difference in the counter values, zero any negitive values,
    # then cumulate sum the result
    df["diff"] = df.groupby("plant_line")["value"].diff()
    df['diff'] = df['diff'].where(df['diff'] >= 0, other=0)
    df["counter"] = df.groupby("plant_line")["diff"].cumsum()
    return(df)

def getMeanWeight(row,df_weights): 
    # Gets the mean weights from the df_weights dataframe for the time periods in row
    # Applied to each of the rows of df_CPL1 or df_CPL2 as a function
    df_inside = df_weights.loc[(df_weights['timestamp'] >= row["timestamp"]["start"]) & 
                (df_weights['timestamp'] <= row["timestamp"]["end"])]
    df_inside_selected = df_inside[df_inside["value"] > 0.5]
    #df_inside.plot.hist(column=["value"])
    weights_mean = (df_inside_selected["value"].mean(axis=0)) * 0.001
    return(weights_mean)
    
# Query the DB to get dataframes of each of the tables    
df_PSD = querySqlite("""SELECT timestamp,value,plant_line from plant_state_data""").sort_values(by='timestamp')
df_TCD = querySqlite("""SELECT timestamp,value,plant_line from totalised_counter_data""").sort_values(by='timestamp')
df_SBWD = querySqlite("""SELECT timestamp,value,plant_line from sample_box_weight_data""")

# Clean out the resets in the totalised_counter_data 
#i.e. make the counter continuallly cout up over the dataset not revert to zero at times
df_TCD = cleanCounter(df_TCD)
# df_TCD.query("plant_line == 'Cream Packing Line 1'").plot.line(y="counter",x="timestamp")
# df_TCD.query("plant_line == 'Cream Packing Line 2'").plot.line(y="counter",x="timestamp")

# merge the counter data on the timestamps
df = pd.merge_asof(df_PSD, df_TCD ,on='timestamp', by = 'plant_line')

df_CPL1 = findStartEndTimes(df,"Cream Packing Line 1") 
df_CPL2 = findStartEndTimes(df,"Cream Packing Line 2") 

df_CPL1["mean_weight"] = df_CPL1.apply(getMeanWeight, df_weights = df_SBWD[df_SBWD['plant_line'].str.match("Cream Packing Line 1")], axis = 1 )
df_CPL1["tones_produced"] = df_CPL1["mean_weight"] * (df_CPL1["counter"]["end"] - df_CPL1["counter"]["start"])

df_CPL2["mean_weight"] = df_CPL2.apply(getMeanWeight, df_weights = df_SBWD[df_SBWD['plant_line'].str.match("Cream Packing Line 2")], axis = 1 )
df_CPL2["tones_produced"] = (df_CPL2["mean_weight"] * (df_CPL2["counter"]["end"] - df_CPL2["counter"]["start"]))

df_CPL1.columns = ['_'.join(col) for col in df_CPL1.columns]
df_CPL2.columns = ['_'.join(col) for col in df_CPL2.columns]

# Renaming columns in place 
df_CPL1.rename(columns={ 'timestamp_start': 'start_time', 'timestamp_end': 'end_time'}, inplace=True)
df_CPL2.rename(columns={ 'timestamp_start': 'start_time', 'timestamp_end': 'end_time'}, inplace=True)

df_CPL1.reset_index().to_csv(path_or_buf="Cream_Packing_Line_1.csv",columns=[ "start_time","end_time","tones_produced_"])
df_CPL2.reset_index().to_csv(path_or_buf="Cream_Packing_Line_2.csv",columns=[ "start_time","end_time","tones_produced_"])