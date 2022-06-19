from datetime import datetime, timedelta
import logging
import sys
import os
import tempfile
import time
import re
from io import StringIO
import json

import matplotlib.pyplot as plt
import pandas as pd
import requests
from bs4 import BeautifulSoup
import azure.functions as func
from azure.storage.blob import BlobServiceClient

from __init__ import make_histogram

#Docs ask for a __init__.py to make a package - place functions here for now as a quick fix
#from helper_module import get_stats

path_dodge = "E:\\finance_tools"
if path_dodge in sys.path:
    sys.path.remove(path_dodge)

path_module = "C:\\Users\\Alex White\\Desktop\\rightmove\\shared_code"
if path_module not in sys.path:
    sys.path.append(path_module)

if __name__ == "__main__":

    #securely obtain connection string
    with open("local.settings.json", "r") as f:
        local_settings = json.load(f)

    #connect to storage account
    conn_string = local_settings["Values"]["bconn"]
    blob_service_client = BlobServiceClient.from_connection_string(conn_string)
    
    #obtain available blobs in prices container
    container_client = blob_service_client.get_container_client("prices")
    blob_list = container_client.list_blobs()
    blob_content = {b.name:None for b in blob_list}

    #read the stream of data from each blob directly into dataframe
    for b in blob_content.keys():
        blob_download = container_client.download_blob(b)
        blob_content[b] = pd.read_csv(StringIO(blob_download.content_as_text()))

    #group and tidy
    price_data = pd.concat(blob_content, ignore_index=False).reset_index()
    price_data = price_data.drop("level_1", axis=1).rename({"level_0":"file"}, axis=1)
    price_data["date_gen"] = price_data["file"].apply(lambda x: pd.to_datetime(x.split("_")[1], format="%Y%m%d"))
    price_data["prices"] = price_data["prices"].astype(int) 

    #rolling statistics in a window of length "buffer"
    buffer = 7
    smooth_stats = {}
    max_date = price_data["date_gen"].max()

    for p in price_data.groupby("date_gen"):
        buff_data = price_data.loc[(price_data["date_gen"] <= p[0]) & 
                                   (price_data["date_gen"] > p[0] - timedelta(days=buffer))].reset_index(drop=True)

        buff_data.drop_duplicates(subset="idx", inplace=True)

        #power-bi plottable histogram bins
        if p[0] == max_date:
            histo = make_histogram(buff_data, n_bins=10)

        #statistics for window of time covered by buffer
        buff_stats = pd.DataFrame(
                     columns=["q1", "q2", "q3", "mean", "n", "sd"],
                     data=[[buff_data["prices"].quantile(0.25), 
                            buff_data["prices"].quantile(0.50), 
                            buff_data["prices"].quantile(0.75),
                            buff_data["prices"].mean(),
                            len(buff_data),
                            buff_data["prices"].std()]]
                    )
        
        smooth_stats[p[0]] = buff_stats

    stat_series = pd.concat(smooth_stats, ignore_index=False).reset_index()
    stat_series = stat_series.drop("level_1", axis=1).rename({"level_0":"date_gen"}, axis=1)
    
    #remove those with an incomplete buffer
    stat_series = stat_series.iloc[buffer-1:-1]

    #test visuals
    fig, ax = plt.subplots()
    ax.plot(stat_series["q1"].values, label="q1")
    ax.plot(stat_series["q2"].values, label="q2")
    ax.plot(stat_series["q3"].values, label="q3")
    ax.axhline(327500, ls="--", c="k", label="purchase")
    ax.legend()
    plt.show()

    fig, ax = plt.subplots()
    ax.bar(histo["label"], histo["freq_dens"])
    plt.show()