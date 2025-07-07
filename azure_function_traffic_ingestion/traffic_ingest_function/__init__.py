import logging
import azure.functions as func
import requests
import json
from datetime import datetime
from azure.storage.filedatalake import DataLakeServiceClient

# Set your TomTom API Key (move to environment variable or key vault in production)
TOMTOM_API_KEY = "Ksk3MaUCyi1hGpz9PUMpFo4vMxL8xkN0"  # 🔒 Replace with secure config if needed

# Your ADLS Gen2 config
STORAGE_ACCOUNT_NAME = "nyctrafficanalyticsadls"
FILE_SYSTEM_NAME = "trafficrawdata-bronze"
DIRECTORY_NAME = "trafficdata"

# Get connection string from local.settings.json for local dev or from App Settings in Azure
from os import getenv
STORAGE_CONNECTION_STRING = getenv("AzureWebJobsStorage")


def get_tomtom_traffic_data():
    url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
    params = {
        "point": "40.7580,-73.9855",  # Times Square coordinates
        "key": TOMTOM_API_KEY
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()


def upload_to_adls(data: dict):
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    file_name = f"traffic_{timestamp}.json"

    # Authenticate to ADLS
    service_client = DataLakeServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
    file_system_client = service_client.get_file_system_client(file_system=FILE_SYSTEM_NAME)
    
    # Create directory if not exists
    directory_client = file_system_client.get_directory_client(DIRECTORY_NAME)
    try:
        directory_client.create_directory()
    except Exception:
        pass  # Already exists

    # Create and upload file
    file_client = directory_client.create_file(file_name)
    file_content = json.dumps(data, indent=2)
    file_client.append_data(data=file_content, offset=0, length=len(file_content))
    file_client.flush_data(len(file_content))


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Traffic ingestion function triggered.")

    try:
        traffic_data = get_tomtom_traffic_data()
        upload_to_adls(traffic_data)
        return func.HttpResponse("✅ Traffic data successfully ingested to ADLS.", status_code=200)
    except Exception as e:
        logging.error(f"❌ Error: {str(e)}")
        return func.HttpResponse(f"❌ Failed: {str(e)}", status_code=500)
