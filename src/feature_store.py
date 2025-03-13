import pandas as pd
import yaml
import hopsworks
import os
import pyodbc

if __name__ == "__main__":
    params = yaml.safe_load(open("params.yaml"))["feature_store"]

    # connect to MS SQL Server and get data
    server = params["server"]
    username = params["username"]
    password = params["password"]
    driver = params["driver"]

    # Create connection string
    conn = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE=master;UID={username};PWD={password}', autocommit = True)

    df = pd.read_sql("SELECT * FROM telco_churn_table", conn)

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
        primary_key=["customerID"],  # Assuming 'id' is your primary key
    )

    # Insert the data into the feature group
    feature_group.insert(df)
