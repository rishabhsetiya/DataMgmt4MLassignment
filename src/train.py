import logging
import yaml
import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
import os
import pickle

# Configure logging
logging.basicConfig(filename='./logs/train.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    params = yaml.safe_load(open("params.yaml"))["feature_store"]
    logging.info("Storing data into feature store...")
    # connect to MS SQL Server and get data
    server = params["server"]
    username = params["username"]
    password = params["password"]
    driver = params["driver"]
    db_name = params["db_name"];
    params = yaml.safe_load(open("params.yaml"))["train"]
    #connect to MS SQL Server and get data into dataframe
    conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{db_name}?driver={driver}"
    engine = create_engine(conn_str)
    conn = engine.connect()
    logging.info("Connected successfully!")
    df = pd.read_sql("SELECT * FROM telco_churn_table", conn)

    #split data
    X = df.drop('Churn', axis=1)
    y = df['Churn']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    #train model and save it in a file
    clf = RandomForestClassifier()
    clf.fit(X_train, y_train)
    y_pred = clf.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    logging.info(f"Model trained with accuracy: {accuracy}")

    # Save the model as pkl file
    model_path = params["model_path"]
    model_path = model_path + "/model.pkl"
    os.makedirs(os.path.dirname(model_path), exist_ok=True)
    with open(model_path, 'wb') as file:
        pickle.dump(clf, file)

    logging.info(f"Model saved at {model_path}")

if __name__ == "__main__":
    main()
