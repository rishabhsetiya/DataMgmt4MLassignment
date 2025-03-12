import requests
import logging
import os
import yaml
import sys

# Configure logging
logging.basicConfig(filename='download_csv.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def download_csv(url, local_file_path):
    try:
        response = requests.get(url)
        response.raise_for_status()

        # Ensure the directory exists
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

        with open(local_file_path, 'wb') as file:
            file.write(response.content)

        logging.info(f"CSV data successfully downloaded from {url} to {local_file_path}.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error downloading CSV data from {url}: {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise


def main():
    params = yaml.safe_load(open("params.yaml"))["ingest"]
    csv_url = params['csv_url']
    local_file_path = sys.argv[1]

    try:
        download_csv(csv_url, local_file_path)
    except Exception as e:
        logging.error(f"Failed to download CSV data: {e}")


if __name__ == "__main__":
    main()
