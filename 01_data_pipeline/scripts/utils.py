##############################################################################
# Import necessary modules and files
##############################################################################


import pandas as pd
import os

from constants import *
from mapping.significant_categorical_level import *
from mapping.city_tier_mapping import *
from helper_funcs import create_database, connect_to_db, concatenate_df

###############################################################################
# Define the function to build database
###############################################################################
def build_dbs():
    '''
    This function checks if the db file with specified name is present 
    in the /Assignment/01_data_pipeline/scripts folder. If it is not present it creates 
    the db file with the given name at the given path. 


    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should exist  


    OUTPUT
    The function returns the following under the given conditions:
        1. If the file exists at the specified path
                prints 'DB Already Exists' and returns 'DB Exists'

        2. If the db file is not present at the specified loction
                prints 'Creating Database' and creates the sqlite db 
                file at the specified path with the specified name and 
                once the db file is created prints 'New DB Created' and 
                returns 'DB created'


    SAMPLE USAGE
        build_dbs()
    '''
    db_full_path = os.path.join(DB_PATH, DB_FILE_NAME)
    
    if os.path.exists(db_full_path):
        print("DB Already Exists")
        return
    
    print("Creating Database at path: ", db_full_path)    
    create_database(db_full_path)

###############################################################################
# Define function to load the csv file to the database
###############################################################################

def load_data_into_db():
    '''
    Thie function loads the data present in data directory into the db
    which was created previously.
    It also replaces any null values present in 'toal_leads_dropped' and
    'referred_lead' columns with 0.


    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be
        DATA_DIRECTORY : path of the directory where 'leadscoring.csv' 
                        file is present
        

    OUTPUT
        Saves the processed dataframe in the db in a table named 'loaded_data'.
        If the table with the same name already exsists then the function 
        replaces it.


    SAMPLE USAGE
        load_data_into_db()
    '''
    db_full_path = os.path.join(DB_PATH, DB_FILE_NAME)
    csv_file_path = os.path.join(DATA_DIRECTORY, "leadscoring.csv")
    try:
        # Load the CSV data
        print("reading data from path: ", csv_file_path)
        df = pd.read_csv(csv_file_path)
        
        # Replace null values in total_leads_droppped, referred_lead columns
        df["total_leads_droppped"].fillna(0, inplace=True)
        df["referred_lead"].fillna(0, inplace=True)
        
        # Connect to the database and save the data
        conn = connect_to_db(db_full_path)
        if conn:
            df.to_sql("loaded_data", conn, if_exists="replace", index=False)
            conn.close()
            
            print("Data successfully loaded into the database.")
        else:
            print("Unable to get DB connection")
    except Exception as e:
        print("Error in loading data into DB ", e)


###############################################################################
# Define function to map cities to their respective tiers
###############################################################################

    
def map_city_tier():
    '''
    This function maps all the cities to their respective tier as per the
    mappings provided in the city_tier_mapping.py file. If a
    particular city's tier isn't mapped(present) in the city_tier_mapping.py 
    file then the function maps that particular city to 3.0 which represents
    tier-3.


    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be
        city_tier_mapping : a dictionary that maps the cities to their tier

    
    OUTPUT
        Saves the processed dataframe in the db in a table named
        'city_tier_mapped'. If the table with the same name already 
        exsists then the function replaces it.

    
    SAMPLE USAGE
        map_city_tier()
    '''
    db_full_path = os.path.join(DB_PATH, DB_FILE_NAME)
    
    # Connect to the database and load data
    conn = connect_to_db(db_full_path)
    if conn:
        df = pd.read_sql_query("SELECT * FROM loaded_data", conn)
        
        # Map city to its respective tier
        df["city_tier"] = df["city_mapped"].map(city_tier_mapping).fillna(3.0)
        df=df.drop('city_mapped',axis=1)

        # calculate the frequency of individual category in first_platform_c column
        df_cat_freq = df['first_platform_c'].value_counts() 

        # index the dataframe that we created above 
        df_cat_freq = pd.DataFrame({'column':df_cat_freq.index, 'value':df_cat_freq.values})
        
        # calculate the cumulative percentage for each category. This is given by calculating cumulative sum and dividing by total sum for each category.
        df_cat_freq['perc'] = df_cat_freq['value'].cumsum()/df_cat_freq['value'].sum()

        # cumulative percetage is calculated for all the three columns - first_platform_c, first_utm_medium_c, first_utm_source_c
        for column in ['first_platform_c', 'first_utm_medium_c', 'first_utm_source_c']:
            df_cat_freq = df[column].value_counts()
            df_cat_freq = pd.DataFrame({'column':df_cat_freq.index, 'value':df_cat_freq.values})
            df_cat_freq['perc'] = df_cat_freq['value'].cumsum()/df_cat_freq['value'].sum()
            print(column,)
            print(list(df_cat_freq.loc[df_cat_freq['perc']<=0.9].column))
            print('\n')
        
        # Save the processed dataframe
        df.to_sql("city_tier_mapped", conn, if_exists="replace", index=False)
        conn.close()
        
        print("City tier mapping completed and saved to database.")
    else:
        print("Unable to get DB connection")

###############################################################################
# Define function to map insignificant categorial variables to "others"
###############################################################################
def map_categorical_vars():
    '''
    This function maps all the insignificant variables present in 'first_platform_c'
    'first_utm_medium_c' and 'first_utm_source_c'. The list of significant variables
    should be stored in a python file in the 'significant_categorical_level.py' 
    so that it can be imported as a variable in utils file.
    

    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be present
        list_platform : list of all the significant platform.
        list_medium : list of all the significat medium
        list_source : list of all rhe significant source

        **NOTE : list_platform, list_medium & list_source are all constants and
                 must be stored in 'significant_categorical_level.py'
                 file. The significant levels are calculated by taking top 90
                 percentils of all the levels. For more information refer
                 'data_cleaning.ipynb' notebook.
  

    OUTPUT
        Saves the processed dataframe in the db in a table named
        'categorical_variables_mapped'. If the table with the same name already 
        exsists then the function replaces it.

    
    SAMPLE USAGE
        map_categorical_vars()
    '''
    db_full_path = f"{DB_PATH}/{DB_FILE_NAME}"
    conn = connect_to_db(db_full_path)
    if conn:
        df = pd.read_sql("SELECT * FROM city_tier_mapped", conn)

        df = concatenate_df(df, "first_platform_c", list_platform)
        df = concatenate_df(df, "first_utm_medium_c", list_medium)
        df = concatenate_df(df, "first_utm_source_c", list_source)
        
        df['total_leads_droppped'] = df['total_leads_droppped'].fillna(0)
        df['referred_lead'] = df['referred_lead'].fillna(0)

        df = df.drop_duplicates()

        df.to_sql("categorical_variables_mapped", conn, if_exists="replace", index=False)
        conn.close()
    else:
        print("Unable to get DB connection")

##############################################################################
# Define function that maps interaction columns into 4 types of interactions
##############################################################################
def interactions_mapping():
    '''
    This function maps the interaction columns into 4 unique interaction columns
    These mappings are present in 'interaction_mapping.csv' file. 


    INPUTS
        DB_FILE_NAME: Name of the database file
        DB_PATH : path where the db file should be present
        INTERACTION_MAPPING : path to the csv file containing interaction's
                                   mappings
        INDEX_COLUMNS_TRAINING : list of columns to be used as index while pivoting and
                                 unpivoting during training
        INDEX_COLUMNS_INFERENCE: list of columns to be used as index while pivoting and
                                 unpivoting during inference
        NOT_FEATURES: Features which have less significance and needs to be dropped
                                 
        NOTE : Since while inference we will not have 'app_complete_flag' which is
        our label, we will have to exculde it from our features list. It is recommended 
        that you use an if loop and check if 'app_complete_flag' is present in 
        'categorical_variables_mapped' table and if it is present pass a list with 
        'app_complete_flag' column, or else pass a list without 'app_complete_flag'
        column.

    
    OUTPUT
        Saves the processed dataframe in the db in a table named 
        'interactions_mapped'. If the table with the same name already exsists then 
        the function replaces it.
        
        It also drops all the features that are not requried for training model and 
        writes it in a table named 'model_input'

    
    SAMPLE USAGE
        interactions_mapping()
    '''
    db_full_path = f"{DB_PATH}/{DB_FILE_NAME}"
    conn = connect_to_db(db_full_path)
    if conn:
        df = pd.read_sql("SELECT * FROM categorical_variables_mapped", conn)
        df = df.drop_duplicates()
        
        interaction_mapping = pd.read_csv(INTERACTION_MAPPING)
        df = df.melt(id_vars=INDEX_COLUMNS_TRAINING, var_name="interaction_type", value_name="interaction_value")
        # handle the nulls in the interaction value column
        df['interaction_value'] = df['interaction_value'].fillna(0)

        # map interaction type column with the mapping file to get interaction mapping
        df = df.merge(interaction_mapping, on="interaction_type", how="left")

        #dropping the interaction type column as it is not needed
        df = df.drop(['interaction_type'], axis=1)

        df = df.pivot_table(index=INDEX_COLUMNS_TRAINING, values="interaction_value", columns='interaction_mapping', aggfunc='sum').reset_index()
        
        df.to_sql("interactions_mapped", conn, if_exists="replace", index=False)
        
        if 'app_complete_flag' in df.columns:
            feature_columns = [col for col in df.columns if col not in NOT_FEATURES]
        else:
            feature_columns = [col for col in df.columns if col not in NOT_FEATURES and col != 'app_complete_flag']
        
        df[feature_columns].to_sql("model_input", conn, if_exists="replace", index=False)
        conn.close()
    else:
        print("Unable to get DB connection")
   