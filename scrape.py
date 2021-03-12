#%%

import pandas as pd
import time
import re
import numpy as np
from scipy.stats import norm, zscore
from functions import scrape, load_jsonl, scrape_single_page

# %%
#Get Initial Data
neweggData = pd.DataFrame(columns = ["productName", "productPrice", "shippingCost"])
for i in range(1,13):
	url = "https://www.newegg.com/p/pl?cm_sp=CAT_Memory_1-_-VisNav-_-Desktop-Memory_2&N=100007611%204131%20601349177%20601275378%20601275376%20601275379&PageSize=96&page="
	url = url + str(i)
	pagedata = scrape_single_page(url)
	#time.sleep(1)
	neweggData = neweggData.append(pagedata, ignore_index=True)
neweggData['Source'] = 'Newegg'

latency = pd.read_csv('latencydata.csv')

amazonData = load_jsonl('search_results_output.jsonl')
amazonData = pd.DataFrame(amazonData)
amazonData = amazonData[['title','price']].rename(columns={"title": "productName", "price": "productPrice"})
amazonData['shippingCost'] = 0
amazonData['Source'] = 'Amazon'

# %%
#Data Cleaning
data = pd.concat([amazonData,neweggData])

data['Model'] = data.apply(lambda x: x['productName'].split()[-1], axis=1)
data['Company'] = data.apply(lambda x: x['productName'].split()[0], axis=1)

data["productPrice"] = pd.to_numeric(data["productPrice"]
                                     .str.replace(r',','')
                                     .str.extract('(\d*\.?\d+)')
                                     .fillna(0.0)[0]
                                     , errors='coerce')

data["shippingCost"] = pd.to_numeric(data["shippingCost"]
                                     .str.replace(r',','')
                                     .str.extract('(\d*\.?\d+)')
                                     .fillna(0.0)[0]
                                     , errors='coerce')

data['TotalCost'] = data['shippingCost']+data['productPrice'] 
data.loc[data['Source']=='Amazon', 'TotalCost'] *= 0.95 #5% discount on Amazon products

data['RGB'] = data['productName'].str.contains('RGB', regex = False)

removeStrings = '|'.join([r"\([^()]*\)", 'RGB', '- DDR4 -', 'DDR4 Unbuffered', 'DDR4 288-pin', 'DDR4 DRAM', 'DDR4 RAM'])

data['Name2'] = data['productName'].str.replace(removeStrings,'') #used for data pulls

data['GB'] = data['Name2'].str.extract('(.{,3})GB')[0]
data['GB'] = pd.to_numeric(data['GB'].str.replace(r'\D', ''),errors='coerce')
data['GB'] = pd.to_numeric(data['GB'].fillna(data['Name2'].str.extract('(.{,3})gb')[0]),errors='coerce')


data['PricePerGB'] = data['TotalCost']/data['GB']
data = data[data['PricePerGB']<30]

#Get speed
data['Speed'] = pd.to_numeric(data['Name2'].str.extract('SDRAM DDR4 (.{,4})')[0], errors='coerce')
data['Speed'] = pd.to_numeric(data['Speed'].fillna(data['Name2'].str.extract('DDR4 (.{,4})')[0]), errors='coerce')
data['Speed'] = pd.to_numeric(data['Speed'].fillna(data['Name2'].str.extract('DDR4-(.{,4})')[0]), errors='coerce')
data['Speed'] = pd.to_numeric(data['Speed'].fillna(data['Name2'].str.extract('(.{,4}) MHz')[0]), errors='coerce')
data['Speed'] = pd.to_numeric(data['Speed'].fillna(data['Name2'].str.extract('(.{,4})MHz')[0]), errors='coerce')
data['Speed'] = pd.to_numeric(data['Speed'].fillna(data['Name2'].str.extract('(.{,4})mhz')[0]), errors='coerce')

#Get timing
data['Timing'] = pd.to_numeric(data['Name2'].str.extract('CL(\d\d)')[0], errors='coerce')
data['Timing'] = pd.to_numeric(data['Timing'].fillna('1'+data['Name2'].str.extract('C1(\d)')[0]), errors='coerce')

data = data.drop(['Name2','shippingCost', 'productPrice'], axis=1)

data = data.dropna()
data = data[data['PricePerGB'] > 0]

finaldata = pd.merge(data, latency) #will drop blanks
finaldata = finaldata.drop_duplicates()

#finaldata['norm_speedScore'] = (1-norm.cdf(zscore(finaldata.Latency)))*10.0
#finaldata['norm_priceScore'] = (1-norm.cdf(zscore(finaldata.PricePerGB)))*10.0

finaldata['uniform_speedScore']=(1-(finaldata['Latency']-finaldata['Latency'].min())/(finaldata['Latency'].max()-finaldata['Latency'].min()))*10
finaldata['uniform_priceScore']=(1-(finaldata['PricePerGB']-finaldata['PricePerGB'].min())/(finaldata['PricePerGB'].max()-finaldata['PricePerGB'].min()))*10
#finaldata['Score'] = (0.6*finaldata['speedScore']+0.4*finaldata['priceScore'])

#finaldata = finaldata.drop(['invLat', 'invPrice'], axis=1)
#%%
finaldata.to_csv('output.csv',index=False)

# %%
