import pyodbc
import os
import requests
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler, LabelEncoder
import yaml
import sys

# Configure logging
logging.basicConfig(filename='../prepare_file.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def saving_to_sql (source_file):
    params = yaml.safe_load(open("params.yaml"))["store"]
    server = params["server"]
    username = params["username"]
    password = params["password"]
    driver = params["driver"]

    # Create connection string
    conn = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE=master;UID={username};PWD={password}', autocommit = True)

    # Create a cursor
    cursor = conn.cursor()

    db_name = 'TELCO_CHURN_DB';

    query = f"SELECT name FROM sys.databases WHERE name = '{db_name}'"
    cursor.execute(query)
    row = cursor.fetchone()
    if row:
        logging.info(f"Database '{db_name}' already exists.")
    else:
        logging.info(f"Database '{db_name}' does not exist. Creating now...")

        # Create the database
        cursor.execute(f"CREATE DATABASE {db_name}")
        conn.commit()
        logging.info(f"Database '{db_name}' created successfully.")

    conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{db_name}?driver=ODBC+Driver+17+for+SQL+Server"
    engine = create_engine(conn_str)
    conn = engine.connect()
    logging.info("Connected successfully!")

    df = pd.read_csv(source_file)
    df["tenure"] = df["tenure"].apply(lambda x: round(x, 5))
    df["MonthlyCharges"] = df["MonthlyCharges"].apply(lambda x: round(x, 5))
    df["TotalCharges"] = df["TotalCharges"].apply(lambda x: round(x, 5))
    df["TotalSpend"] = df["TotalSpend"].apply(lambda x: round(x, 5))
    table_name = 'telco_churn_table'
    query = f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'"

    cursor.execute(query)
    row = cursor.fetchone()

    if row:
        logging.info(f"Table '{table_name}' exists. Appending the data", source_file)
        df.to_sql(table_name, con=engine, if_exists="append", index=False)
        logging.info("Data written to sql server successfully")
    else:
        print(f"Table '{table_name}' does not exist. Creating Table")
        create_table_query = """
            CREATE TABLE telco_churn_table (
            gender int, SeniorCitizen int, Partner int, Dependents int, tenure float, PhoneService int, MultipleLines int, InternetService int,
            OnlineSecurity int, OnlineBackup int, DeviceProtection int, TechSupport int, StreamingTV int, StreamingMovies int, Contract int,
            PaperlessBilling int, PaymentMethod int, MonthlyCharges float, TotalCharges float, Churn int, TenureCategory int,
            TotalSpend float, HasInternet int, NumServices int
            )
           """
        cursor.execute(create_table_query)
        #conn.commit()
        logging.info ("Table Created successfully")
        df.to_sql(table_name, con=engine, if_exists="append", index=False)

    cursor.close()
    conn.close()

def main():
    saving_to_sql(sys.argv[1])
    saving_to_sql(sys.argv[2])

if __name__ == "__main__":
    main()
