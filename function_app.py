import azure.functions as func
import logging
import requests
import json
from datetime import datetime
from requests.exceptions import RequestException
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.keyvault.secrets import SecretClient

app = func.FunctionApp()

@app.timer_trigger(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False) 
def finance_ingestion_pipeline(myTimer: func.TimerRequest) -> None:
    logging.info('Capital Markets Ingestion Pipeline execution initiated.')

    # 1. Establish Zero-Trust Identity Connections
    try:
        credential = DefaultAzureCredential()
        vault_url = "https://kv-fincm-cr.vault.azure.net/"
        secret_client = SecretClient(vault_url=vault_url, credential=credential)
        
        # Securely retrieve the API key from Azure Key Vault
        mock_api_key = secret_client.get_secret("CoinGeckoApiKey").value
        logging.info("Successfully authenticated with Azure Key Vault via Managed Identity.")
    except Exception as e:
        logging.error(f"Security/Authentication Lifecycle Failure: Failed to retrieve secrets. Error: {str(e)}")
        return

    # 2. Extract Live Market Data with Network Resiliency
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=cad&include_24hr_vol=true"
    try:
        # Set a hard 10-second timeout to prevent hung compute instances
        response = requests.get(url, timeout=10)
        response.raise_for_status() 
        market_data = response.json()
        logging.info("Successfully extracted live assets from CoinGecko API endpoint.")
    except RequestException as req_err:
        logging.error(f"Ingestion API Transport Failure: Network request failed. Error: {str(req_err)}")
        return
    except json.JSONDecodeError:
        logging.error("Ingestion Data Corruption Failure: Received invalid or empty JSON response payload.")
        return

    # 3. Inject Processing Metadata
    market_data['ingestion_timestamp'] = datetime.utcnow().isoformat()
    market_data['pipeline_source'] = "AzureFunctions-MacV2"

    # 4. Stream Raw Asset Payload to the Data Lake Landing Zone
    try:
        blob_service_client = BlobServiceClient(account_url="https://stfincmcr.blob.core.windows.net", credential=credential)
        filename = f"ticks/{datetime.utcnow().strftime('%Y/%m/%d/%H')}/market_payload_{int(datetime.utcnow().timestamp())}.json"
        
        blob_client = blob_service_client.get_blob_client(container="market-tick-data", blob=filename)
        blob_client.upload_blob(json.dumps(market_data), overwrite=True)
        logging.info(f"Successfully streamed raw payload partition to landing zone: {filename}")
    except Exception as storage_err:
        logging.error(f"Data Lake Persistence Failure: Unable to stream blob to container. Error: {str(storage_err)}")
        return