import logging
# Configure logging
logging.basicConfig(filename='./logs/feature_store.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

import pandas as pd
import yaml
import hopsworks
import os
import pyodbc

from sqlalchemy import create_engine

def main():
    params = yaml.safe_load(open("params.yaml"))["feature_store"]
    logging.info("Storing data into feature store...")
    # connect to MS SQL Server and get data
    server = params["server"]
    username = params["username"]
    password = params["password"]
    driver = params["driver"]
    db_name = 'TELCO_CHURN_DB';
    #connect to MS SQL Server and get data into dataframe
    conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{db_name}?driver={driver}"
    engine = create_engine(conn_str)
    conn = engine.connect()
    logging.info("Connected successfully!")
    df = pd.read_sql("SELECT * FROM telco_churn_table", conn)
    df['sno'] = range(len(df))
    os.environ['HOPSWORKS_API_KEY'] = params["api_key"]

    project = hopsworks.login()
    fs = project.get_feature_store()

    # Replace with your feature group name
    feature_group_name = "customer_churn_features"

    # Create a feature group if it doesn't exist
    feature_group = fs.get_or_create_feature_group(
        name=feature_group_name,
        version=1,
        description="Customer churn features from SQL database",
        primary_key=["sno"]
    )

    # Insert the data into the feature group
    feature_group.insert(df)
    logging.info("Data inserted successfully!")

if __name__ == "__main__":
    main()
