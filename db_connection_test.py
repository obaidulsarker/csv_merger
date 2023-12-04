import pandas as pd
import numpy as np
import re  

# import necessary libraries
import os
import glob
import shutil

# Import data into DB
from sqlalchemy import create_engine, false
from sqlalchemy.sql import text as sa_text

# Database Connection
conn_file = open(os.getcwd() +"\db_connection.ini", mode="r")
conn_string = conn_file.read()
conn_file.close()

# Open database connection
db = create_engine(conn_string)

#Load data into staging
print("Loading data ..............................")
try:
        # Open Connection
        conn = db.connect()
        # # Load data into staging
        # df_2021a.to_sql("2021a", con=conn, if_exists='replace', index=False)
        # df_2021b.to_sql("2021b", con=conn, if_exists='replace', index=False)
        # df_2022a.to_sql("2022a", con=conn, if_exists='replace', index=False)
        # df_2022a.to_sql("2022b", con=conn, if_exists='replace', index=False)
        print("Connected !")
        conn.autocommit = True
        conn.close()

except Exception as e:
        print(e.__str__)
        conn.close()