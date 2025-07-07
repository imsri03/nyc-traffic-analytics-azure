import logging
import requests
import os
from datetime import datetime
from azure.storage.filedatalake import DataLakeServiceClient
import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Triggered: traffic_ingest_function')

    # 1. Read environment variables
    api_key = os.getenv("TOMTOM_API_KEY")
    adls_account_name = os.getenv("ADLS_ACCOUNT_NAME")
    adls_account_key = os.getenv("ADLS_ACCOUNT_KEY")

    if not all([api_key, adls_account_name, adls_account_key]):
        return func.HttpResponse("❌ Missing one or more required environment variables.", status_code=500)

    # 2. Call TomTom Traffic API (NYC)
    api_url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?point=40.730610,-73.935242&key={api_key}"

    try:
        response = requests.get(api_url)
        response.raise_for_status()
        traffic_data = response.json()
    except Exception as e:
        return func.HttpResponse(f"❌ TomTom API error: {e}", status_code=500)

    # 3. Save traffic data to ADLS Gen2
    try:
        container_name = "trafficrawdata-bronze"
        file_path = f"traffic/nyc/{datetime.utcnow().strftime('%Y/%m/%d/%H%M%S')}.json"

        service_client = DataLakeServiceClient(
            account_url=f"https://{adls_account_name}.dfs.core.windows.net",
            credential=adls_account_key
        )

        file_system_client = service_client.get_file_system_client(container_name)
        file_client = file_system_client.create_file(file_path)

        file_contents = str(traffic_data)

        file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
        file_client.flush_data(len(file_contents))

        return func.HttpResponse(f"✅ Traffic data saved to: {file_path}", status_code=200)

    except Exception as e:
        return func.HttpResponse(f"❌ ADLS write error: {e}", status_code=500)
