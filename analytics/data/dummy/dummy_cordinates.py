# pip install random
# pip install json
# pip install configparser
# pip install pymongo
# pip install  pandas
# pip install csv

import random
import json
import configparser 
from pymongo import MongoClient
import pandas as pd
import csv

class GenerateAndInsertCordinates:
    def getDB(self):
        config = configparser.ConfigParser()
        config.read(r'D:\Data Analysis\upi_data_analytics\databases\mongoDB\mongodb_config.ini')
        client = MongoClient(config['MongoDB']['host'],int(config['MongoDB']['port']))
        return client[config['MongoDB']['database']]
        
    def getNSPLocations(self):
        sets = {}
        with open('coordinates.csv', mode='r') as file:
            reader = csv.reader(file)
            for row in reader:
                set_name = row[0]
                coordinates = row[1:]
                sets[set_name] = coordinates
                
        for set_name, coordinates in sets.items():
            print(f"Set {set_name}:")
            for coord in coordinates:
                print(coord)



    def getNetworkLatancy(self, latitude, longitude):
        pass

    def transactionStatus(self,latitude,longitude):
        success_latancy, failure_latancy = self.getNetworkLatancy(latitude,longitude)
        return random.choice(["TRANSACTION_SUCCESS"]*success_latancy + ["TRANSCTION_PENDING"]*2 + ["TRANSCTION_FAILED"]*failure_latancy)

    
    def generate_coordinates(self):
        latitude = round(random.uniform(15.3523, 22.1002), 4)
        longitude = round(random.uniform(72.3629, 80.5412), 4)
        lat_direction = 'N' if latitude >= 0 else 'S'
        lon_direction = 'E' if longitude >= 0 else 'W'
        coordinates = {
            "latitude": f"{abs(latitude):.2f}{lat_direction}",
            "longitude": f"{abs(longitude):.2f}{lon_direction}",
            # "transction_status" : transactionStatus({abs(latitude):.4f},{abs(longitude):.4f})
        }
        return coordinates

    def InsertDataToMongoDB(self):
        collection = self.getDB()['cordinates']
        no_of_documents = collection.count_documents({})
        if  no_of_documents < 4000:
            for _ in range(4000 - no_of_documents):
                collection.insert_many([self.generate_coordinates()])

GenerateAndInsertCordinates().getNSPLocations()
