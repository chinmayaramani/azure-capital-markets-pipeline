# Azure Serverless Capital Markets Data Lake Ingestion Pipeline

An automated, end-to-end financial data engineering ingestion pipeline deployed on Microsoft Azure. The architecture streams high-fidelity digital asset market tickers (BTC/ETH to CAD conversions) into an object storage landing zone. It utilizes modern cloud design patterns, including zero-trust identity architectures and automated multi-tier storage lifecycle policies.

## Architectural Components

1. **Compute Layer:** Deployed an Azure Function using the Python 3.11 V2 programming model running on a Linux Flex Consumption tier. Compute execution is driven by an automated timer trigger running on a five-minute cron schedule (`0 */5 * * * *`).
2. **Identity & Security Framework:** Implemented a Zero-Trust security model using Azure Role-Based Access Control (RBAC). All hardcoded connection strings, master keys, and administrative secrets have been eliminated from the code base. 
3. **Directory Integration:** Provisioned a System-Assigned Managed Identity on the Azure Function compute resource. Mapped directory privileges explicitly to the granular scopes of `Key Vault Secrets User` and `Storage Blob Data Contributor`.
4. **Secret Management Vault:** Configured an isolated Azure Key Vault instance (`kv-fincm-cr`) to secure external ingestion tokens (`CoinGeckoApiKey`). Secrets are requested dynamically at execution runtime using the Azure Identity client library SDK.
5. **Data Lake Storage:** Established an Azure Blob Storage landing zone container (`market-tick-data`). Paysloads are streamed headlessly into chronological partitions matching an audit-friendly directory pattern: `ticks/%Y/%m/%d/%H/`.
6. **Data Lifecycle (FinOps) Policy:** Configured automated immutable retention parameters on the storage account. High-velocity ingestion blocks automatically transition from Hot to Cool storage after 72 hours, with automated purge rules executing after 7 days to eliminate unnecessary platform spend.

## Technical Specifications

* **Language & Framework:** Python 3.11, Azure Functions Python Worker V2
* **Cloud Infrastructure Services:** Microsoft Azure (Storage Account Data Lake Gen2, Key Vault, Azure Functions Engine)
* **Identity Infrastructure:** Azure Active Directory / Microsoft Entra ID, Managed Identities, Azure RBAC
* **Core Dependencies:** `azure-identity`, `azure-storage-blob`, `azure-keyvault-secrets`, `requests`