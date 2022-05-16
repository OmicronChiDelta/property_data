import requests
from bs4 import BeautifulSoup
import datetime
import numpy as np


#%%
def get_stats(data):
    """
    summary statistics
    """
    stats = {"mean":np.mean(data)
             ,"q1":np.percentile(data, 25)
             ,"q2":np.percentile(data, 50)
             ,"q3":np.percentile(data, 75)}
    return stats



#%%
def get_data(mode="test"):
    """
    obtain data payload from rightmove
    """
    idx = 0
    pending = True
    price_master = []
    
    while pending:
    
        page_index=str(idx*24)
    
        url = f"https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E1114&maxBedrooms=2&minBedrooms=2&index={page_index}&propertyTypes=terraced&secondaryDisplayPropertyType=terracedhouses&includeSSTC=false&mustHave=garden&dontShow=newHome%2Cretirement%2CsharedOwnership&furnishTypes=&keywords="
    

        if mode != "test":
            #request call
            page = requests.get(url)            
            soup = BeautifulSoup(page.content, 'html.parser')
        
            #extract prices
            hits = soup.find_all("div", {"class": "propertyCard-priceValue"})
            prices = [int(h.text.replace("£", "").replace(",", "").strip()) for h in hits if "£" in h.text]

            if len(prices) == 0:
                pending = False
            
            price_master.extend(prices)
            idx += 1

        else:
            price_master = [420, 69]
            pending = False

        return price_master


if __name__ == "__main__":
    print("testing...")

    print()
    print("...here are some stupid stats:")
    stupid_stats = get_stats([4, 2, 0, 6, 9])
    for s in stupid_stats.keys():
        print(f"{s}: {stupid_stats[s]}")

    print()
    print("...here's some real data")
    data = get_data("")
    print(data)

    print()
    print("...here are some real stats")
    stats = get_stats(data)
    for s in stats.keys():
        print(f"{s}: {stats[s]}")