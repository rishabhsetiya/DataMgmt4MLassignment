import pandas as pd
import great_expectations as gx
import logging
import json
import os
import sys
import yaml

# Configure logging
logging.basicConfig(filename='./logs/validate.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_csv(file_path):
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Loaded CSV file: {file_path}")
        return df
    except Exception as e:
        logging.error(f"Error loading CSV file {file_path}: {e}")
        raise

def main():
    params = yaml.safe_load(open("params.yaml"))["validate"]
    # Define file paths
    file_paths = [sys.argv[1], sys.argv[2]]

    # Load CSV files
    dfs = [load_csv(file_path) for file_path in file_paths]

    # combine the dataframes
    df = pd.concat(dfs)

    context = gx.get_context()

    data_source = context.data_sources.add_pandas("pandas")
    data_asset = data_source.add_dataframe_asset(name="pd dataframe asset")

    batch_definition = data_asset.add_batch_definition_whole_dataframe("batch definition")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})


    #Create Expectations.
    expectations = [
    gx.expectations.ExpectColumnValuesToBeInSet(column="gender", value_set=['Male', 'Female']),
    gx.expectations.ExpectColumnValuesToBeBetween(column="MonthlyCharges", min_value=0),
    gx.expectations.ExpectColumnValuesToBeInSet(column="Churn", value_set=['Yes', 'No']),
    gx.expectations.ExpectColumnValuesToBeBetween(column="tenure", min_value=0),
    gx.expectations.ExpectColumnValuesToBeInSet(column="Contract", value_set=['Month-to-month', 'One year', 'Two year'])
    ]
    
    results = []

    for expectation in expectations:
        validation_result = batch.validate(expectation)
        results.append(validation_result.to_json_dict()) 
    
    file_path = sys.argv[3]
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, 'w') as file:
        json.dump(results, file, indent=4)

if __name__ == "__main__":
    main()
