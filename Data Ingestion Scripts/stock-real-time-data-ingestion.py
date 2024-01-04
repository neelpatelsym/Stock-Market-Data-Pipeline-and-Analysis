import pandas as pd
import json
from yahoo_fin import stock_info as si
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
from azure.eventhub.exceptions import EventHubError
import asyncio 
import sys
import datetime
import access_key_vault

#storing the stocks json file location in system path 
sys.path.insert(0,'F:/Nasdaq-10-stock-market-data-project/Data Ingestion/')

#opening the stock market json file
with open(r'nasdaq-10-stocks.json','r') as stocks_json:
    stock_list = json.load(stocks_json)
print(stock_list)


#Setting desired time to start streaming data from api
start_time = datetime.time(9,30)
#Setting desired time to stop streaming data from api
end_time = datetime.time(16,0)
#Setting days of the week for streaming data from api
target_days = [1,2,3,4,5] #Monday to Friday




def get_stock_data():
    stocks_dict = []
    '''current_time = datetime.datetime.now().time()
    current_day = datetime.datetime.now().weekday()
    if current_time >= start_time and current_time <=end_time and current_day in target_days:'''
    for stock in stock_list:
        stocks_dict.append({'ticker':stock['ticker'],'price':si.get_live_price(f'{stock['ticker']}'),'timestamp':datetime.datetime.now()})
    return stocks_dict
    
datetime.datetime.now()

# Retrieving connection string and Event Hub Name from Azure Key Vault
connection_string = f"{access_key_vault.connection_str.value}"
eventhub_name = f"{access_key_vault.eventhub_name.value}" 

# print(connection_string)
# print(eventhub_name)

async def stock_data_feed():
    # Creating a producer client to send messages
    while True:
        await asyncio.sleep(30)
        producer = EventHubProducerClient.from_connection_string(conn_str = connection_string, eventhub_name = eventhub_name)
        async with producer:
            #Creating a batch
            event_data_batch = await producer.create_batch()

            #Add events to batch\
            event_data_batch.add(EventData(json.dumps(get_stock_data(),default=str)))
            

            #Send the batch of events to event hub
            await producer.send_batch(event_data_batch)
            
            print('Data sent successfully to Azure Event Hub')

# Looping stock_data_feed function

loop = asyncio.get_event_loop()
try:
    asyncio.ensure_future(stock_data_feed())
    loop.run_forever()
except KeyboardInterrupt:
    pass
finally:
    print("Terminating Loop")
    loop.close()
