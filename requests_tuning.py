# -*- coding: utf-8 -*-
"""
Created on Mon May  9 21:49:23 2022

@author: Alex White
"""
import pandas as pd
import re
import requests
from bs4 import BeautifulSoup


#%%
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


#%%
idx = 0
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


#%%extract 
done_flag = False

#presence of "EMPTY" after frame filling indicates we're pulling blank data - done
test_frame = make_price_frame(soup)
if "EMPTY" in test_frame["idx"].unique():
    done_flag = True
    test_frame = test_frame.loc[test_frame["idx"] != "EMPTY"].reset_index(drop=True)
    
    #check no duplication of properties
    assert(len(test_frame) == len(test_frame["idx"].unique()))
    
    #check prices are purely numerical strings
    assert(test_frame["prices"].apply(lambda x: x.isdecimal()).all())
  
print(test_frame.sort_values(by=["idx"], ascending=True))
print(done_flag)