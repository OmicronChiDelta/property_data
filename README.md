# property_data
This is a pipeline built using various pieces of Microsoft/Azure infrastructure to reliably scrape house-price data from Rightmove. Its key components are:
- Azure Key Vault: Storage of sensitive information (e.g connection strings for Blob Storage)
- Azure Blob Storage: Storage of raw data (scraped), and curated information derived from it
- Azure Function App: Orchestration of the scraping each day, via Python code embedded in an Azure Function
- Microsoft Poweer BI: Visualisation curated data, and tracking of its evolution over time.
