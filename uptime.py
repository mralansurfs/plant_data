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
    """ 
    Function to query the SQLite database and return the result as a DataFrame.
    Args:
        queryString (str): SQL query to execute.
    Returns:
        DataFrame containing the result of the query.
    """
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
    """ 
    Args:
        df (DataFrame): A merged dataframe of plant state and counter values (merged on timestamp)
        
    Returns:
        DataFrame with start and end timestamps for the plant lines and start and end counter.
        
    For a plant_line fine all the start and end times of the it as defined as
    changes too or from 4 in df["value_x"]
    """ 
    
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
    """ 
    Args:
        df (DataFrame): the counter dataframe 
        
    Returns:
        df (DataFrame): the counter dataframe with any resets accounted for.
    
    THe counter values have resets to a value less than the previous value in them. 
    This function recomputes the counter so it has no resets and is allways cumulative.
    Take the difference in the counter values, revert any negitive diff values to 
    the counter at that time, then cumulate sum the result
    """ 

    df["diff"] = df.groupby("plant_line")["value"].diff()
    df['diff'] = df['diff'].where(df['diff'] >= 0, other=df["value"])
    df["counter"] = df.groupby("plant_line")["diff"].cumsum()
    df["counter"] = df["counter"].fillna(0)
    return(df)

def getMeanWeight(row,df_weights): 
    """
    Args:
        df_weights (DataFrame): the sample weights dataframe (filtered to be only one plant_line) 
        row : the row of the plant start and end times dataframe this function is being applied to
        
    Returns:
        weights_mean
        
    ######################
    The weight limit for which to ignore values below is hardcoded as 0. 
    Removing this to a hardcode dataclass would be recomended
    ######################    
        
    # Gets the mean weights from the df_weights dataframe for the time periods in row
    # Applied to each of the rows of df_CPL1 or df_CPL2 as a function
    """
    df_inside = df_weights.loc[(df_weights['timestamp'] >= row["timestamp"]["start"]) & 
                (df_weights['timestamp'] <= row["timestamp"]["end"])]
    df_inside_selected = df_inside[df_inside["value"] > 0.0]
    #df_inside.plot.hist(column=["value"])
    weights_mean = (df_inside_selected["value"].mean(axis=0)) * 0.001
    return(weights_mean)

def getResults(df,plant_line,df_weights):
    """
    Args:
        df_weights (DataFrame): the sample weights dataframe (unfiltered)
        plant_line : the plant line e.g 'Cream Packing Line 1' being investigated
        df: the dataframe containing merged counter data and plant state on the timestamps
        
    Returns:
        No returns to inside this code. .csv is writen out
        
    do the timeperiod selection on the merged dataset of plant state and counter values (df), 
    carry over the counter start and end values for the time period. Apply the function 
    getMeanWeight to each timeperiod and use it to get the mean weight over that time period.
    do the counter subtraction and multiply by the mean weight. Write out to a csv.
    """
    df_CPL = findStartEndTimes(df,plant_line)  
    df_CPL["mean_weight"] = df_CPL.apply(getMeanWeight, df_weights = df_weights[df_weights['plant_line'].str.match(plant_line)], axis = 1 )
    df_CPL["tones_produced"] = df_CPL["mean_weight"] * (df_CPL["counter"]["end"] - df_CPL["counter"]["start"])
    df_CPL.columns = ['_'.join(col) for col in df_CPL.columns]
    
    # Renaming columns in place 
    df_CPL.rename(columns={ 'timestamp_start': 'start_time', 'timestamp_end': 'end_time'}, inplace=True)
    df_CPL.reset_index().to_csv(path_or_buf = plant_line + ".csv",columns=[ "start_time","end_time","tones_produced_"])
    
# Query the DB to get dataframes of each of the tables    
df_PSD = querySqlite("""SELECT timestamp,value,plant_line from plant_state_data""").sort_values(by='timestamp')
df_TCD = querySqlite("""SELECT timestamp,value,plant_line from totalised_counter_data""").sort_values(by='timestamp')
df_SBWD = querySqlite("""SELECT timestamp,value,plant_line from sample_box_weight_data""")

# Clean out the resets in the totalised_counter_data 
#i.e. make the counter continuallly cout up over the dataset not revert to zero at times
df_TCD = cleanCounter(df_TCD)
#df_TCD.query("plant_line == 'Cream Packing Line 1'").plot.line(y="counter",x="timestamp")
#df_TCD.query("plant_line == 'Cream Packing Line 2'").plot.line(y="counter",x="timestamp")

# merge the counter data on the timestamps
df = pd.merge_asof(df_PSD, df_TCD ,on='timestamp', by = 'plant_line')

# run this function to get the on timeperiods, the tonage produced and writeout to csv
getResults(df,'Cream Packing Line 1',df_SBWD)
getResults(df,'Cream Packing Line 2',df_SBWD)  