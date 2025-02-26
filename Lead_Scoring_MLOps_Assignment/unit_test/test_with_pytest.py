##############################################################################
# Import the necessary modules
##############################################################################
import unittest
import pandas as pd
import sqlite3
from utils import load_data_into_db, map_city_tier, map_categorical_vars,interactions_mapping


###############################################################################
# Write test cases for load_data_into_db() function
# ##############################################################################

def test_load_data_into_db():
    """_summary_
    This function checks if the load_data_into_db function is working properly by
    comparing its output with test cases provided in the db in a table named
    'loaded_data_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'

    SAMPLE USAGE
        output=test_get_data()

    """
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    
    data = {
        'customer_id': ['C1', 'C2', 'C3'],
        'city_code': [1, 2, 3],
        'first_interaction': ['2021-01-01', '2021-02-01', '2021-03-01'],
        'customer_category': ['A', 'B', 'C'],
        'product_code': ['P1', 'P2', 'P3'],
        'interaction_type': ['email', 'call', 'chat']
    }
    df = pd.DataFrame(data)

    load_data_into_db(conn, df)
    cursor.execute("SELECT COUNT(*) FROM interactions_mapped")
    count = cursor.fetchone()[0]

    conn.close()
    assert count == len(df), f"Expected {len(df)} rows, got {count}"
    
    

###############################################################################
# Write test cases for map_city_tier() function
# ##############################################################################
def test_map_city_tier():
    """_summary_
    This function checks if map_city_tier function is working properly by
    comparing its output with test cases provided in the db in a table named
    'city_tier_mapped_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'

    SAMPLE USAGE
        output=test_map_city_tier()

    """
    df = pd.DataFrame({'city_code': [1, 2, 3]})
    df_mapped = map_city_tier(df)
    
    assert 'city_tier' in df_mapped.columns, "city_tier column is missing"
    assert df_mapped['city_tier'].tolist() == [1, 1, 3], "City tier mapping is incorrect"

    
###############################################################################
# Write test cases for map_categorical_vars() function
# ##############################################################################    
def test_map_categorical_vars():
    """_summary_
    This function checks if map_cat_vars function is working properly by
    comparing its output with test cases provided in the db in a table named
    'categorical_variables_mapped_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'
    
    SAMPLE USAGE
        output=test_map_cat_vars()

    """    
    df = pd.DataFrame({'customer_category': ['A', 'B', 'C'], 'product_code': ['P1', 'P2', 'P3']})
    df_mapped = map_categorical_vars(df)

    assert 'customer_category_encoded' in df_mapped.columns, "customer_category_encoded column is missing"
    assert 'product_code_encoded' in df_mapped.columns, "product_code_encoded column is missing"
    assert df_mapped['customer_category_encoded'].tolist() == [1, 2, 3], "Categorical mapping is incorrect"
    

###############################################################################
# Write test cases for interactions_mapping() function
# ##############################################################################    
def test_interactions_mapping():
    """_summary_
    This function checks if test_column_mapping function is working properly by
    comparing its output with test cases provided in the db in a table named
    'interactions_mapped_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'

    SAMPLE USAGE
        output=test_column_mapping()

    """ 
    df = pd.DataFrame({'interaction_type': ['email', 'call', 'chat']})
    df_mapped = interactions_mapping(df)

    assert 'interaction_mapped' in df_mapped.columns, "interaction_mapped column is missing"
    assert df_mapped['interaction_mapped'].tolist() == [1, 2, 3], "Interaction mapping is incorrect"

