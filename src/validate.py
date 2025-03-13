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


    #Create an Expectation.
    #Expectations are a fundamental component of GX. They allow you to explicitly define the state to which your data should conform.
    #Run the following code to define an Expectation that the contents of the column passenger_count consist of values ranging from 2 to 6:
    expectation = gx.expectations.ExpectColumnValuesToBeInSet(
        column="gender", value_set=['Male', 'Female'],
    )

    # TotalCharges should be non-negative
    expectation1 = gx.expectations.ExpectColumnValuesToBeBetween(
    column="TotalCharges", min_value=0
    )

    expectation2 = gx.expectations.ExpectColumnValuesToBeOfType(
    column="MonthlyCharges", 
    type_list=["int", "float", "double", "decimal"]  # Allow only numeric types
)
    #Run and get the results!
    validation_result = batch.validate(expectation)
    validation_result1 = batch.validate(expectation1)
    validation_result2 = batch.validate(expectation2)

    results = {
        "validation_result": validation_result.to_json_dict(),
        "validation_result1": validation_result1.to_json_dict(),
        "validation_result2": validation_result2.to_json_dict(),
    }
    
    file_path = sys.argv[3]
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    #with open(file_path, 'w') as file:
    #    file.write(str(validation_result))

    with open(file_path, 'w') as file:
        json.dump(results, file, indent=4)

if __name__ == "__main__":
    main()
