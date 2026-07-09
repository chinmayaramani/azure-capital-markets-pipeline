import azure.functions as func
import logging
import requests
import json
from datetime import datetime
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.keyvault.secrets import SecretClient

app = func.FunctionApp()

@app.timer_trigger(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False) 
def finance_ingestion_pipeline(myTimer: func.TimerRequest) -> None:
    logging.info('Capital Markets Ingestion Pipeline started executing.')

    # 1. Establish Zero-Trust Identity Connections
    credential = DefaultAzureCredential()
    
    # 2. Connect to Key Vault securely using Managed Identity (No passwords in code!)
    vault_url = "https://kv-fincm-cr.vault.azure.net/"
    secret_client = SecretClient(vault_url=vault_url, credential=credential)
    
    # Fetch your mock API key from the vault safely
    mock_api_key = secret_client.get_secret("CoinGeckoApiKey").value
    logging.info(f"Successfully authenticated with Key Vault. Token verified.")

    # 3. Pull Live Market Data from CoinGecko (BTC & ETH against CAD)
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=cad&include_24hr_vol=true"
    response = requests.get(url)
    market_data = response.json()

    # Add processing timestamp metadata for risk tracking audit trails
    market_data['ingestion_timestamp'] = datetime.utcnow().isoformat()
    market_data['pipeline_source'] = "AzureFunctions-MacV2"

    # 4. Connect to Storage Lake and stream payload directly
    blob_service_client = BlobServiceClient(account_url="https://stfincmcr.blob.core.windows.net", credential=credential)
    
    # Create an audit-friendly filename structured by year/month/day/hour/timestamp
    filename = f"ticks/{datetime.utcnow().strftime('%Y/%m/%d/%H')}/market_payload_{int(datetime.utcnow().timestamp())}.json"
    
    blob_client = blob_service_client.get_blob_client(container="market-tick-data", blob=filename)
    blob_client.upload_blob(json.dumps(market_data), overwrite=True)

    logging.info(f"Successfully streamed raw asset payload to data lake: {filename}")