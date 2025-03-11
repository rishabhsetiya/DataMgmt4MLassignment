import pandas as pd
import sqlite3
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score


def get_data_from_sql(db_path, query):
    """Connects to an SQL database and retrieves data into a pandas DataFrame.

    Args:
      db_path: The path to the SQL database file.
      query: The SQL query to execute.

    Returns:
      A pandas DataFrame containing the query results, or None if an error occurs.
    """
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        return None


if __name__ == "__main__":
    df = get_data_from_sql('example.db', 'SELECT * FROM customer_churn')

    #train a model using data to predict churn

    #split data
    X = df.drop('Churn', axis=1)
    y = df['Churn']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    #train model
    model = RandomForestClassifier()
    model.fit(X_train, y_train)

    #evaluate model
    y_pred = model.predict(X_test)

    print(f"Accuracy: {accuracy_score(y_test, y_pred)}")

    #save model
    model.save('model.h5')