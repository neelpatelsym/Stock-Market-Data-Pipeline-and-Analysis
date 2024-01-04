import pandas as pd
import os
import json
import sys
import datetime
from yahoo_fin import stock_info as si
import yfinance as yf
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential

load_dotenv()

#storing the stocks json file location in system path 
sys.path.insert(0,'F:/Nasdaq-10-stock-market-data-project/Data Ingestion/')

#Opening the stock market json file
with open(r'nasdaq-10-stocks.json','r') as stocks_json:
    stock_list = json.load(stocks_json)
print(stock_list)

# Create an empty DataFrame to store the data
hist_stock_data = pd.DataFrame()

#Retrieving history of stocks from 1st Jan 2000
for stock in stock_list:
    hist_stock_data = pd.concat([hist_stock_data,si.get_data(f"{stock['ticker']}",start_date='01/01/2000')],ignore_index=False)

hist_stock_data.head()

# Cleaning retrieved stocks data
hist_stock_data = hist_stock_data.reset_index() #Resetting index as date is displayed as index for the data
hist_stock_data.rename(columns={'index':'Date'}) # Extracted date column and renamed it
# Creating a new column Stock to map stock name based on ticker column 
hist_stock_data['Stock'] = hist_stock_data['ticker'].map({stock['ticker']: stock['stock'] for stock in stock_list})


hist_stock_data.head()

# Converting data frame to csv format

hist_stock_data = hist_stock_data.to_csv(index=False)

# Getting Azure credentials to access ADLS Gen 2

client_id = os.environ.get('AZURE_CLIENT_ID')
tenant_id = os.environ.get('AZURE_TENANT_ID')
client_secret = os.environ.get('AZURE_CLIENT_SECRET')
adls_name = "nasdaqstockadls"
file_system = "nasdaq-stock-container"
directory = "nasdaq-stocks-history-load"
file = "stocks_history.csv"

def get_service_client_token_credential(account_name):
    account_url = f"https://{account_name}.dfs.core.windows.net"
    token_credential = ClientSecretCredential(client_id = client_id,
                                              client_secret = client_secret,
                                              tenant_id = tenant_id
                                              )

    service_client = DataLakeServiceClient(account_url, credential=token_credential)

    return service_client

datalake_client = get_service_client_token_credential(adls_name)

def upload_file_to_directory():
    try:
        file_system_client = datalake_client.get_file_system_client(file_system=file_system)
        directory_client = file_system_client.get_directory_client(directory)  
        file_client = directory_client.create_file(file)
        file_client.append_data(data=hist_stock_data, offset=0, length=len(hist_stock_data))
        file_client.flush_data(len(hist_stock_data))
    except Exception as e:
      print(e)

upload_file_to_directory()

