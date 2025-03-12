import requests
import yaml
import csv
import logging
import sys
import os

# Configure logging
logging.basicConfig(filename='../api_to_csv.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def get_api_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        logging.info("API data fetched successfully.")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        raise


def json_to_csv(json_data, csv_file_path):
    # Ensure the directory exists
    os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)
    try:
        # Extract keys for header
        if isinstance(json_data, list) and len(json_data) > 0:
            header = json_data[0].keys()
        else:
            logging.error("JSON data is empty or not in expected format.")
            return

        # Write to CSV
        with open(csv_file_path, mode='w', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=header)
            writer.writeheader()
            for row in json_data:
                writer.writerow(row)
        logging.info("JSON data successfully written to CSV.")
    except Exception as e:
        logging.error(f"Error converting JSON to CSV: {e}")
        raise


def main():
    params = yaml.safe_load(open("params.yaml"))["ingest"]
    api_url = params['api_url']  # Replace with the actual API URL
    csv_file_path = sys.argv[1]

    try:
        # Fetch data from API
        json_data = get_api_data(api_url)

        # Convert JSON data to CSV
        json_to_csv(json_data, csv_file_path)

        logging.info("Data ingestion completed successfully.")
    except Exception as e:
        logging.error(f"Data ingestion failed: {e}")


if __name__ == "__main__":
    main()
