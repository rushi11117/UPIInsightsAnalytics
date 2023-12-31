#### Imports Requered for Scraping or Access microsevice endpoint

# ! pip install requests
# ! pip install pandas
# ! pip install pymongo
# ! pip install random
# ! pip install collections
# ! pip install configparser


import requests
import pandas as pd
from pymongo import MongoClient
import random
import configparser

# help(requests)
class FetchTransactions:
    def getTransactionsFromGateway(self):  
        try:
            ResponseEntity = requests.get('http://localhost:8085/transactions-insights')
            ResponseEntity.raise_for_status()  # Raise an error for HTTP errors
        except requests.HTTPError as e:
            print(e)
            return pd.DataFrame()  # Return an empty DataFrame on error
        return pd.DataFrame(ResponseEntity.json())

class FetchLocations:
    def getAllLocationsCoordinates(self):
        config = configparser.ConfigParser()
        config.read(r'D:\Data Analysis\upi_data_analytics\databases\mongoDB\mongodb_config.ini')
        host = config['MongoDB']['host']
        port = config['MongoDB']['port']
        database = config['MongoDB']['database']
        
        client = MongoClient(host, int(port))
        db = client[database]
        collection = db['cordinates']  # Corrected the collection name
        return list(collection.find({}, {'_id': 0}))

class FetchTransactionsWithLocations:
    def __init__(self):
        self.transaction_gateway = FetchTransactions()
        self.location_fetcher = FetchLocations()

    def getAllTransactionsWithLocations(self):
        transactions_df = self.transaction_gateway.getTransactionsFromGateway()
        locations = self.location_fetcher.getAllLocationsCoordinates()
        print(len(locations))
        print(len(transactions_df))
        if not transactions_df.empty:
            no_of_transactions = len(transactions_df)
            transactions_df['TRANSACTION_CITY'] = [random.choice(locations)[str(i % 99)] for i in range(no_of_transactions)]
        return transactions_df  # Return the modified DataFrame


# transactions_with_locations = FetchLocations()
# result = transactions_with_locations.getAllLocationsCoordinates()
# print(result)