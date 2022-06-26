# property_data
This is a pipeline built using various pieces of Microsoft/Azure infrastructure to reliably scrape house-price data from Rightmove. Its key components are:
- Azure Key Vault: Storage of sensitive information (e.g connection strings for Blob Storage)
- Azure Blob Storage: Storage of raw data (scraped), and curated information derived from it
- Azure Function App: Orchestration of the scraping each day, principally with the Python requests package called from an Azure Function
- Microsoft Poweer BI: Visualisation of curated data, and tracking of its evolution over time.

The following block diagram illustrates how these services communicate with each other. (TO DO)

Finally, here's an example of the scraped data and inferred statistics as found in the Power BI frontend - any subsequent modelling will also be delivered here.

<p align="center">
  <img src="https://github.com/OmicronChiDelta/property_data/blob/master/example_output.PNG?raw=true"/>
</p>
