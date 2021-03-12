#%%
from selectorlib import Extractor
import requests 
import json 
from time import sleep
from urllib.request import urlopen as uReq
from bs4 import BeautifulSoup as soup
import pandas as pd
#%%
# Create an Extractor by reading from the YAML file
e = Extractor.from_yaml_file('search_results.yml')

def scrape(url):  

    headers = {
        'dnt': '1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-user': '?1',
        'sec-fetch-dest': 'document',
        'referer': 'https://www.amazon.com/',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    }

    # Download the page using requests
    print("Downloading %s"%url)
    r = requests.get(url, headers=headers)
    # Simple check to check if page was blocked (Usually 503)
    if r.status_code > 500:
        if "To discuss automated access to Amazon data please contact" in r.text:
            print("Page %s was blocked by Amazon. Please try using better proxies\n"%url)
        else:
            print("Page %s must have been blocked by Amazon as the status code was %d"%(url,r.status_code))
        return None
    # Pass the HTML of the page and create 
    return e.extract(r.text)

#%%
# product_data = []

    
#%%
def load_jsonl(input_path) -> list:
    with open("search_results_urls.txt",'r') as urllist, open('search_results_output.jsonl','w') as outfile:
        for url in urllist.read().splitlines():
            data = scrape(url) 
            if data:
                for product in data['products']:
                    product['search_url'] = url
                    print("Saving Product: %s"%product['title'])
                    json.dump(product,outfile)
                    outfile.write("\n")
                    #sleep(1)
    """
    Read list of objects from a JSON lines file.
    """
    data = []
    with open(input_path, 'r', encoding='utf-8') as f:
        for line in f:
            data.append(json.loads(line.rstrip('\n|\r')))
    print('Loaded {} records from {}'.format(len(data), input_path))
    return data

# %%
def scrape_single_page(url):
	#grabs the page
	page = uReq(url)
	page_html = page.read()
	page.close()

	#html parsing
	page_soup = soup(page_html, "html.parser")

	#grabs all product containers
	containers = page_soup.findAll("div", {"class":"item-container"})

	pagedata = pd.DataFrame(columns = ["productName", "productPrice", "shippingCost"])
	#search starts at index 4, 0-3 are ads
	for i in range(4, len(containers)):

		container = containers[i]

		#grabs item title
		titleContainer = container.findAll("a", {"class":"item-title"})
		productName = titleContainer[0].text.strip()

		#grabs item price, might need try except
		priceContainer = container.findAll("li", {"class":"price-current"})
		productPrice = priceContainer[0].text.strip()
		#grabs only the item price from productPrice, discards the useless text
		productPriceList = productPrice.split()
		for x in range(len(productPriceList)):
			if "$" in productPriceList[x]:
				productPrice = productPriceList[x]
				break

		#grabs shipping price
		shippingContainer = container.findAll("li", {"class":"price-ship"})
		shippingCost = shippingContainer[0].text.strip()

		#print(productName)
		#print(productPrice)
		#print(shippingCost)

		pagedata.at[i,'productName'] = productName
		pagedata.at[i,'productPrice'] = productPrice
		pagedata.at[i,'shippingCost'] = shippingCost

	return pagedata
 
# %%
