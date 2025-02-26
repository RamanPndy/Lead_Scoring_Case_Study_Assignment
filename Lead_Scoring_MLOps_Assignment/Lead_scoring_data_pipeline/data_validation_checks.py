"""
Import necessary modules
############################################################################## 
"""

import sqlite3
import pandas as pd
import os
from constants import *
from schema import *

###############################################################################
# Define function to validate raw data's schema
############################################################################### 

def raw_data_schema_check():
    '''
    This function check if all the columns mentioned in schema.py are present in
    leadscoring.csv file or not.

   
    INPUTS
        DATA_DIRECTORY : path of the directory where 'leadscoring.csv' 
                        file is present
        raw_data_schema : schema of raw data in the form oa list/tuple as present 
                          in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Raw datas schema is in line with the schema present in schema.py' 
        else prints
        'Raw datas schema is NOT in line with the schema present in schema.py'

    
    SAMPLE USAGE
        raw_data_schema_check
    '''
    file_path = os.path.join(DATA_DIRECTORY, 'leadscoring.csv')

    if not os.path.exists(file_path):
        print(f"Error: File 'leadscoring.csv' not found in {DATA_DIRECTORY}")
        return

    df = pd.read_csv(file_path)
    file_columns = set(df.columns)
    schema_columns = set(raw_data_schema)

    if file_columns == schema_columns:
        print("Raw data schema is in line with the schema present in schema.py")
    else:
        print("Raw data schema is NOT in line with the schema present in schema.py")
        missing_columns = schema_columns - file_columns
        extra_columns = file_columns - schema_columns
        
        if missing_columns:
            print(f"Missing columns in data: {missing_columns}")
        if extra_columns:
            print(f"Extra columns in data: {extra_columns}")

###############################################################################
# Define function to validate model's input schema
############################################################################### 

def model_input_schema_check():
    '''
    This function check if all the columns mentioned in model_input_schema in 
    schema.py are present in table named in 'model_input' in db file.

   
    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be present
        model_input_schema : schema of models input data in the form oa list/tuple
                          present as in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Models input schema is in line with the schema present in schema.py'
        else prints
        'Models input schema is NOT in line with the schema present in schema.py'
    
    SAMPLE USAGE
        raw_data_schema_check
    '''
    db_file_path = os.path.join(DB_PATH, DB_FILE_NAME)

    if not os.path.exists(db_file_path):
        print(f"Error: Database file '{DB_FILE_NAME}' not found in {DB_PATH}")
        return

    conn = connect_to_db(db_file_path)
    if conn:
        cursor = conn.cursor()
        
        # Fetch column names from 'model_input' table
        cursor.execute("PRAGMA table_info(model_input);")
        table_columns = {row[1] for row in cursor.fetchall()} 

        schema_columns = set(model_input_schema)

        if table_columns == schema_columns:
            print("Model's input schema is in line with the schema present in schema.py")
        else:
            print("Model's input schema is NOT in line with the schema present in schema.py")
            missing_columns = schema_columns - table_columns
            extra_columns = table_columns - schema_columns

            if missing_columns:
                print(f"Missing columns in database: {missing_columns}")
            if extra_columns:
                print(f"Extra columns in database: {extra_columns}")
        
        conn.close()
    else:
        print("Unable to get DB connection")

def connect_to_db(db_full_path):
    conn = sqlite3.connect(db_full_path)
    print("connecting to db from path: ", db_full_path)
    return conn