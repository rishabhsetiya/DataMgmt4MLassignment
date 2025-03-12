import pandas as pd
import sqlite3
import hopsworks
import os


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
    conn = sqlite3.connect('example.db')
    cursor = conn.cursor()

    cursor.execute('''
      CREATE TABLE customer_churn (
        customerID INT PRIMARY KEY,
        gender TEXT,
        SeniorCitizen INT,
        Partner TEXT,
        Dependents TEXT,
        tenure INT,
        PhoneService TEXT,
        MultipleLines TEXT,
        InternetService TEXT,
        Churn TEXT
      )
    ''')

    sample_data = [
        (1, 'Female', 0, 'Yes', 'No', 1, 'No', 'No', 'DSL', 'No'),
        (2, 'Male', 0, 'No', 'No', 34, 'Yes', 'No', 'DSL', 'No'),
        (3, 'Female', 0, 'No', 'No', 2, 'Yes', 'No', 'Fiber optic', 'Yes')
        # ... add more rows here ...
    ]

    # Insert sample data into the table
    cursor.executemany("INSERT INTO customer_churn (customerID, gender, SeniorCitizen, Partner, Dependents, tenure, PhoneService, MultipleLines, InternetService, Churn) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", sample_data)
    conn.commit()
    conn.close()

    # Example usage:
    # Replace 'your_database.db' with the actual path to your database file.
    # Replace 'SELECT * FROM your_table' with your desired SQL query.
    db_file = 'example.db'  # @param {type:"string"}
    query = 'SELECT * FROM customer_churn'  # @param {type:"string"}
    df = get_data_from_sql(db_file, query)

    os.environ['HOPSWORKS_API_KEY'] = "CgYupUsvdGRc0duC.gyJlzE0UpNoXC8wPLZq0Se7ojMCS65TfLY3CrTOFEha6Pl2NItNFN5rgwFT4fi5v"

    project = hopsworks.login()
    fs = project.get_feature_store()

    # Replace with your feature group name
    feature_group_name = "customer_churn_features"

    # Create a feature group if it doesn't exist
    feature_group = fs.get_or_create_feature_group(
        name=feature_group_name,
        version=1,
        description="Customer churn features from SQLite database",
        primary_key=["customerID"],  # Assuming 'id' is your primary key
    )

    # Insert the data into the feature group
    feature_group.insert(df)
