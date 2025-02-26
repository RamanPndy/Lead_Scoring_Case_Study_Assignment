import sqlite3

def connect_to_db(db_full_path):
    conn = sqlite3.connect(db_full_path)
    print("connecting to db from path: ", db_full_path)
    return conn