import logging
import os
import requests
import datetime
from azure.storage.filedatalake import DataLakeServiceClient
import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('🔄 NYC Traffic ingestion triggered.')

    # ✅ Get environment variables
    api_key = os.environ.get("TOMTOM_API_KEY")
    connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")

    if not api_key or not connection_string:
        return func.HttpResponse("❌ API Key or Connection String missing from environment variables.", status_code=500)

    # ✅ Location: 10-mile radius around Brooklyn Pizza Shop
    lat, lon = 40.6782, -73.9442
    url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?point={lat},{lon}&unit=KMPH&key={api_key}"

    try:
        response = requests.get(url)
        data = response.json()
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"traffic_{timestamp}.json"

        # ✅ Upload to ADLS Gen2
        service_client = DataLakeServiceClient.from_connection_string(connection_string)
        file_system_client = service_client.get_file_system_client(file_system="trafficrawdata-bronze")
        directory_client = file_system_client.get_directory_client("ingested")

        file_client = directory_client.create_file(filename)
        file_client.append_data(data=str(data), offset=0, length=len(str(data)))
        file_client.flush_data(len(str(data)))

        logging.info(f"✅ Traffic data saved as {filename}")
        return func.HttpResponse(f"✅ Traffic data saved as {filename}", status_code=200)

    except Exception as e:
        logging.error(f"❌ Error: {str(e)}")
        return func.HttpResponse(f"❌ Error: {str(e)}", status_code=500)
