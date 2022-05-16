from datetime import datetime
import logging
import sys
import os
import tempfile
import time
import re

import pandas as pd
import requests
from bs4 import BeautifulSoup
import azure.functions as func
from azure.storage.blob import BlobServiceClient

#Docs ask for a __init__.py to make a package - place functions here for now as a quick fix
#from helper_module import get_stats

path_dodge = "E:\\finance_tools"
if path_dodge in sys.path:
    sys.path.remove(path_dodge)

path_module = "C:\\Users\\Alex White\\Desktop\\rightmove\\shared_code"
if path_module not in sys.path:
    sys.path.append(path_module)


	
def make_price_frame(stew):
    
    """
    Extract unique property ID and listed price from rightmove HTML
    """
    
    hits = stew.find_all("a", {"class": "propertyCard-priceLink propertyCard-salePrice"})
    df = pd.DataFrame(columns=["idx", "prices"], data=[["EMPTY", "EMPTY"] for h in hits])

    for i, h in enumerate(hits):
        
        id_root = h.prettify().split('href="/properties/')[-1]
        lockon = "#/?channel=RES_BUY"
        
        #presence of lockon indicates a property record being linked to
        if lockon in id_root:
            df.at[i, "idx"]    = "property_"+id_root.split('#/?channel=RES_BUY')[0].strip()
            df.at[i, "prices"] = re.sub("\D", "", h.getText())
            
    return df



def df_to_container(df, filename, container):

    #connection string for blob loaded as environment variable by functions app - secure, and best practice
    conn_string = os.getenv("bconn")

    blob_service_client = BlobServiceClient.from_connection_string(conn_string)

    #volatile local save of data
    temp_folder = tempfile.TemporaryDirectory()
    path_df = os.path.join(temp_folder.name, filename)
    df.to_csv(path_df, index=False)

    #upload to container
    blob_client = blob_service_client.get_blob_client(container=container, blob=filename)
    with open(path_df, "rb") as data:
        blob_client.upload_blob(data)

    return True



def make_histogram(dataset, n_bins):

    p_max = dataset["prices"].max()
    p_min = dataset["prices"].min()
    bin_width = (p_max - p_min)/n_bins

    #compute uniform-bin histogram
    histo = pd.DataFrame(columns=["bin_start"])
    histo["bin_start"] = [p_min + i*bin_width for i in range(n_bins)]
    histo["bin_end"] = histo["bin_start"] + bin_width
    histo["bin_mid"] = 0.5*(histo["bin_start"] + histo["bin_end"])
    histo["freq"] = histo.apply(lambda x: len(dataset.loc[(dataset["prices"] >= x["bin_start"]) & (dataset["prices"] < x["bin_end"])]), axis=1)
    
    #due to strict ">", incidences of max must be added to the final bin
    histo.loc[histo["bin_end"] == p_max, "freq"] += len(dataset.loc[dataset["prices"] == p_max])
    
    histo["freq_dens"] = histo["freq"]/bin_width

    #generate presentable label
    histo["label"] = histo.apply(lambda x: f"{x['bin_start']/1000:.1f}k-\n{x['bin_end']/1000:.1f}k", axis=1)

    return histo
	
	

def get_data(mode="test"):
    """
    obtain data payload from rightmove
    """
    idx = 0
    pending = True
    price_master = []
    
    if mode == "live":

        while pending:

            page_index=str(idx*24)
    
            url = "https://www.rightmove.co.uk/property-for-sale/find.html?" +\
                  "locationIdentifier=REGION%5E1114" +\
                  "&maxBedrooms=2" +\
                  "&minBedrooms=2" +\
                  f"&index={page_index}" +\
                  "&propertyTypes=terraced" +\
                  "&secondaryDisplayPropertyType=terracedhouses" +\
                  "&includeSSTC=false" +\
                  "&mustHave=garden" +\
                  "&dontShow=newHome%2Cretirement%2CsharedOwnership" +\
                  "&furnishTypes=" +\
                  "&keywords="
    
            #GET request
            page = requests.get(url)            
            soup = BeautifulSoup(page.content, 'html.parser')
			
            prices = make_price_frame(soup)

            if "EMPTY" in prices["idx"].unique() or idx >= 10:
                pending = False
                prices = prices.loc[prices["idx"] != "EMPTY"].reset_index(drop=True)

                #check prices are purely numerical strings
                assert(prices["prices"].apply(lambda x: x.isdecimal()).all())

            price_master.extend([prices])
            idx += 1

            #Wait for 5 secs before triggering again to avoid stressing servers
            time.sleep(5)

    else:
        price_master = [pd.DataFrame(columns=["idx", "prices"], data=[["420", 69]])]
        pending = False

    return price_master



def main(mytimer: func.TimerRequest) -> None:
    stamp = datetime.now()
    stamp_str = stamp.strftime("%Y%m%d_%H%M%S")

    #Obtain rightmove price payload and summary stats
    prices = get_data(mode="live")
    
    df_prices = pd.concat(prices, ignore_index=True)
    df_prices["prices"] = df_prices["prices"].astype(int)
    df_prices.drop_duplicates(inplace=True)

    #daily stats
    df_stats = pd.DataFrame(columns = ["stat", "value"], data=[["n",    len(df_prices["prices"])],
                                                                ["q1",   df_prices["prices"].quantile(0.25)],
                                                                ["q2",   df_prices["prices"].quantile(0.50)],
                                                                ["q3",   df_prices["prices"].quantile(0.75)],
                                                                ["mean", df_prices["prices"].mean()]])

    #histogram
    df_histo = make_histogram(df_prices, 10)
                                                                                
    success_prices = df_to_container(df_prices, f"prices_{stamp_str}.csv", "prices")
    success_stats  = df_to_container(df_stats,  f"stats_{stamp_str}.csv",  "stats")
    success_histo  = df_to_container(df_histo,  f"distro_{stamp_str}.csv", "distro")

    logging.info(f"registered prices: {', '.join([str(p) for p in prices])}")
    logging.info(f"Python timer trigger function ran at {stamp_str}")
