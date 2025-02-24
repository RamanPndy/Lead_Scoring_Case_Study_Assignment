"""
Import necessary modules
############################################################################## 
"""

import pandas as pd
import os
import sys
import sqlite3

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# You can create more variables according to your project. The following are the basic variables that have been provided to you
DB_PATH = '/Users/rpandey1/airflow/dags/Lead_scoring_data_pipeline/db'
DB_FILE_NAME = 'lead_scoring_data_cleaning.db'
UNIT_TEST_DB_FILE_NAME = ''
DATA_DIRECTORY = '/Users/rpandey1/airflow/dags/Lead_scoring_data_pipeline/data/data'
INTERACTION_MAPPING = '/Users/rpandey1/airflow/dags/Lead_scoring_data_pipeline/mapping/mapping/interaction_mapping.csv'
INDEX_COLUMNS_TRAINING = ['created_date', 'first_platform_c',
       'first_utm_medium_c', 'first_utm_source_c', 'total_leads_droppped', 'city_tier',
       'referred_lead', 'app_complete_flag']
INDEX_COLUMNS_INFERENCE = ['created_date', 'city_tier', 'first_platform_c',
       'first_utm_medium_c', 'first_utm_source_c', 'total_leads_droppped',
       'referred_lead', 'app_complete_flag']
NOT_FEATURES = ['created_date', 'assistance_interaction', 'career_interaction',
                'payment_interaction', 'social_interaction', 'syllabus_interaction']

raw_data_schema = ['created_date', 'city_mapped', 'first_platform_c',
           'first_utm_medium_c', 'first_utm_source_c', 'total_leads_droppped',
           'referred_lead', '1_on_1_industry_mentorship', 'call_us_button_clicked',
           'career_assistance', 'career_coach', 'career_impact', 'careers',
           'chat_clicked', 'companies', 'download_button_clicked',
           'download_syllabus', 'emi_partner_click', 'emi_plans_clicked',
           'fee_component_click', 'hiring_partners',
           'homepage_upgrad_support_number_clicked',
           'industry_projects_case_studies', 'live_chat_button_clicked',
           'payment_amount_toggle_mover', 'placement_support',
           'placement_support_banner_tab_clicked', 'program_structure',
           'programme_curriculum', 'programme_faculty',
           'request_callback_on_instant_customer_support_cta_clicked',
           'shorts_entry_click', 'social_referral_click',
           'specialisation_tab_clicked', 'specializations', 'specilization_click',
           'syllabus', 'syllabus_expand', 'syllabus_submodule_expand',
           'tab_career_assistance', 'tab_job_opportunities', 'tab_student_support',
           'view_programs_page', 'whatsapp_chat_click', 'app_complete_flag']


model_input_schema = ['total_leads_droppped', 'city_tier', 'referred_lead', 
                    'first_platform_c', 'first_utm_medium_c', 'first_utm_source_c', 
                    'app_complete_flag']

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

    try:
        conn = sqlite3.connect(db_file_path)
        cursor = conn.cursor()
        
        # Fetch column names from 'model_input' table
        cursor.execute("PRAGMA table_info(model_input);")
        table_columns = {row[1] for row in cursor.fetchall()}  # Extract column names
        
        conn.close()
    except sqlite3.Error as e:
        print(f"Error: Failed to connect to database. {e}")
        return

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

    
    
