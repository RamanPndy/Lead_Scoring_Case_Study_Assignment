import pandas as pd
import sqlite3
from sqlite3 import Error

def create_database(db_full_path):
    """Creates an SQLite database if it doesn't exist."""    
    try:
        conn = sqlite3.connect(db_full_path)
        print(f"Database created successfully at {db_full_path}")
    except Error as e:
        print(f"Error creating database: {e}")

def connect_to_db(db_full_path):
    conn = sqlite3.connect(db_full_path)
    print("connecting to db from path: ", db_full_path)
    return conn

def concatenate_df(df, column, significant_categorical_list):
    new_df = df[~df[column].isin(significant_categorical_list)] # get rows for levels which are not present in significant_categorical_list
    new_df[column] = "others" # replace the value of these levels to others
    old_df = df[df[column].isin(significant_categorical_list)] # get rows for levels which are present in significant_categorical_list
    df = pd.concat([new_df, old_df])
    return df