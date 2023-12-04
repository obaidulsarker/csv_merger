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

source_pdf_directory = ""
target_pdf_directory = ""
csv_directory = ""

# Read files directory
# config_file = open(os.getcwd() +"\config.ini", mode="r")
# config_params = config_file.read()
# for line in config_params:
# 	print(line)
# 	param, value = line.split(":")
# 	if (param == "pdf_source_dir"):
# 		source_pdf_directory = value.strip()
# 	if (param == "pdf_target_dir"):
# 	    target_pdf_directory = value.strip()
	# if (param == "pdf_target_dir"):
	#     csv_directory = value.strip()

def validate_email(email):  
    if re.match(r"[^@]+@[^@]+\.[^@]+", email):  
        return True  
    return False  

cols = ["last_name","first_name","email_address","dt_date","year"]

# 2021
df_2021a = pd.DataFrame(columns=cols)
indx = 0

inv_data_2021a =[]

#2021_Immunitaetsbescheinigungen.csv
with open("D:\python-project\csv_merger\csv\\2021_Immunitaetsbescheinigungen.csv", encoding="utf8") as fp:
    Lines = fp.readlines()
    for line in Lines[1:]:
        words = line.split(";")
        if (len(words) < 4 or len(words) > 4):
            inv_data_2021a.append(line)
        else:
            dt_date=words[0].strip()
            last_name=words[1].strip()
            first_name=words[2].strip()
            email_address=words[3].strip()
            year = "2021"
            if (len(last_name)>0 and len(first_name)>0 and validate_email(email_address)== True):
                new_row = {'last_name': last_name, 'first_name': first_name, 'email_address':email_address, 'dt_date': dt_date, "year": year}
                df_2021a.loc[indx] = new_row
                indx = indx + 1
                #print("date=",dt_date,"last_name=",last_name,"first_name=",first_name,"email_address=",email_address)
            else:
                inv_data_2021a.append(line)
    
    print("Sample Data: 2021_Immunitaetsbescheinigungen.csv")
    print(df_2021a.head())


#2021_Unterstuetzer.csv
df_2021b = pd.DataFrame(columns=cols)
inv_data_2021b = []
indx = 0

with open("D:\python-project\csv_merger\csv\\2021_Unterstuetzer.csv", encoding="utf8") as fp:
    Lines = fp.readlines()
    for line in Lines[1:]:
        words = line.split(";")
        if (len(words) < 3 or len(words) > 3):
            inv_data_2021b.append(line)
        else:
            last_name=words[0].strip()
            first_name=words[1].strip()
            dt_date=words[2].strip()
            email_address=""
            year = "2021"
            if (len(last_name)>0 and len(first_name)>0 ):
                new_row = {'last_name': last_name, 'first_name': first_name, 'email_address':email_address, 'dt_date': dt_date, "year": year}
                df_2021b.loc[indx] = new_row
                indx = indx + 1
            else:
                inv_data_2021b.append(line)    

    print("Sample Data: 2021_Unterstuetzer.csv")
    print(df_2021b.head())
   
    
# 2022
df_2022a = pd.DataFrame(columns=cols)
indx = 0

inv_data_2022a =[]

#2022_Immunitaetsbescheinigungen.csv
with open("D:\python-project\csv_merger\csv\\2022_Immunitaetsbescheinigungen.csv", encoding="utf8") as fp:
    Lines = fp.readlines()
    for line in Lines[1:]:
        words = line.split(";")
        if (len(words) < 4 or len(words) > 4):
            inv_data_2022a.append(line)
        else:
            dt_date=words[0].strip()
            last_name=words[1].strip()
            first_name=words[2].strip()
            email_address=words[3].strip()
            year = "2022"
            if (len(last_name)>0 and len(first_name)>0 and validate_email(email_address)== True):
                new_row = {'last_name': last_name, 'first_name': first_name, 'email_address':email_address, 'dt_date': dt_date, "year": year}
                df_2022a.loc[indx] = new_row
                indx = indx + 1
                #print("date=",dt_date,"last_name=",last_name,"first_name=",first_name,"email_address=",email_address)
            else:
                inv_data_2022a.append(line)
    
    print("Sample Data: 2022_Immunitaetsbescheinigungen.csv")
    print(df_2022a.head())


#2022_Unterstuetzer.csv
df_2022b = pd.DataFrame(columns=cols)
inv_data_2022b = []
indx = 0

with open("D:\python-project\csv_merger\csv\\2022_Unterstuetzer.csv", encoding="utf8") as fp:
    Lines = fp.readlines()
    for line in Lines[1:]:
        words = line.split(";")
        if (len(words) < 3 or len(words) > 3):
            inv_data_2022b.append(line)
        else:
            last_name=words[0].strip()
            first_name=words[1].strip()
            dt_date=words[2].strip()
            email_address=""
            year = "2022"
            if (len(last_name)>0 and len(first_name)>0 ):
                new_row = {'last_name': last_name, 'first_name': first_name, 'email_address':email_address, 'dt_date': dt_date, "year": year}
                df_2022b.loc[indx] = new_row
                indx = indx + 1
            else:
                inv_data_2022b.append(line)    

    print("Sample Data: 2022_Unterstuetzer.csv")
    print(df_2022b.head())


#data_type={'last_name': create_engine.types.UnicodeText(), 'first_name': create_engine.types.UnicodeText(), 'email': create_engine.types.UnicodeText(), 'dt_date': create_engine.types.UnicodeText(), 'year': create_engine.types.UnicodeText()}

#Load data into staging
print("Loading data ..............................")
try:
        # Open Connection
        conn = db.connect()
    
        # Load data into staging
        df_2021a.to_sql(name = "2021a", con=conn, if_exists='replace', index=False)
        df_2021b.to_sql(name = "2021b", con=conn, if_exists='replace', index=False)
        df_2022a.to_sql(name = "2022a", con=conn, if_exists='replace', index=False)
        df_2022a.to_sql(name = "2022b", con=conn, if_exists='replace', index=False)

        conn.autocommit = True
        conn.close()

except Exception as e:
        print(e.__str__)
        conn.close()

    # print("printing invalid records")
    # for inv_line in inv_data_2021b:
    #     print(inv_line)