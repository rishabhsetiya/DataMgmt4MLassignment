import pandas as pd
import great_expectations as gx
import logging
import json
import os
import sys
import yaml

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_csv(file_path):
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Loaded CSV file: {file_path}")
        return df
    except Exception as e:
        logger.error(f"Error loading CSV file {file_path}: {e}")
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

    #Run and get the results!
    validation_result = batch.validate(expectation)

    file_path = sys.argv[3]
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, 'w') as file:
        file.write(str(validation_result))


if __name__ == "__main__":
    main()
