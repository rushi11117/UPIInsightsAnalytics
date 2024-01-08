# pip install random
# pip install json
# pip install configparser
# pip install pymongo
# pip install  pandas
# pip install csv
# pip install math

import random
import json
import configparser 
from pymongo import MongoClient
import pandas as pd
import csv
import math
import time

"""
    linear insertion 
    EXECUTION TIME for 2000 documents is 214.0633087158203
"""
class GenerateAndInsertCoordinates:


    """
    Distance Formula

    Dependancies:
    pip list || strstr math
    
    Args:
    Float : Latitude1
    Float : Latitude2
    Float : Logitude1
    FLoat : Longitude2

    Returns:
    Float : distance
    """
    def calculate_distance(self, lat1, lon1, lat2, lon2):
        # Haversine formula to calculate distance between two points on Earth
        R = 6371.0  # approximate radius of Earth in kilometers

        d_lat = math.radians(lat2 - lat1)
        d_lon = math.radians(lon2 - lon1)

        a = math.sin(d_lat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(d_lon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        distance = R * c
        return distance

    """
    Find Nearest NSPLocation And Minimum Distance From Client Request Location

    Dependancies:
    getNSPLocations()
    
    Args:
    Float : latitude
    Float : longitude

    Returns:
    Float : Minimum distance from closest NSPLocation
    Point : Closest NSPLocation
    """
    def get_nearest_NSPLocation_distance(self, latitude, longitude):
        NSPLocations = self.getNSPLocations()

        # Initial values for closest distance and NSPLocation
        closest_distance = float('inf')
        closest_NSPLocation = None

        for NSPLocation in NSPLocations:
            for coord in NSPLocation:
                lat, lon = map(float, coord.split(','))
                distance = self.calculate_distance(latitude, longitude, lat, lon)
                if distance < closest_distance:
                    closest_distance = distance
                    closest_NSPLocation = NSPLocation

        return closest_distance, closest_NSPLocation

    """
    Decide Transaction Status

    Dependancies:
    pip list || strstr pymongo.MongoClient && strstr configparser
    
    Args:
    None

    Returns:
    MongoDBDatabaseInstance : Instance 
    """
    
    def getDB(self):
        config = configparser.ConfigParser()
        config.read(r'D:\Data Analysis\upi_data_analytics\databases\mongoDB\mongodb_config.ini')
        client = MongoClient(config['MongoDB']['host'],int(config['MongoDB']['port']))
        return client[config['MongoDB']['database']]

    
    """
    Get lastest updated NSPLocations .csv

    Dependancies:
    pip list || strstr csv
    
    Args:
    None

    Returns:
    [Point] : NSPLocations
    """
    def getNSPLocations(self):
        NSPLocations = []
        with open('coordinates.csv', mode='r') as file:
            reader = csv.reader(file)
            for row in reader:
                set_name = row[0]
                coordinates = row[1:]
                NSPLocationSet = []
                for coord in coordinates:
                    NSPLocationSet.append(coord)
                NSPLocations.append(NSPLocationSet)
        return NSPLocations

    """
    Modulate Network Latency using NSPLocations and terrain

    Dependancies:
    get_nearest_NSPLocation_distance(Float, Float)
    
    Args:
    latitude(Float) longitude(Float)

    Returns:
    Success Latency(Interger), Failure Latency(Integer)
    """
    
    def getNetworkLatency(self, latitude, longitude):
        NSPLocation_distance = self.get_nearest_NSPLocation_distance(latitude, longitude)[0]
        band_strength_constant = 350
        failure_latency = round(NSPLocation_distance * 10)
        success_latency = band_strength_constant - failure_latency 
        return success_latency, failure_latency

    """
    Decide Transaction Status

    Dependancies:
    getNetworkLatancy(Float, Float)
    
    Args:
    Float : latitude ,Float : longitude

    Returns:
    enum(String): Transaction Status
    """
    def transactionStatus(self, latitude, longitude):
        success_latency, failure_latency = self.getNetworkLatency(latitude, longitude)
        return random.choice(["TRANSACTION_SUCCESS"] * success_latency + ["TRANSACTION_PENDING"] * 10 + ["TRANSACTION_FAILED"] * failure_latency)

    """
    Generate Coordinates Grid Bound Between longitude and latitude of shapio defined limits

    Dependancies:
    pip list || strstr randome
    transactionStatus(abs(FLoat), abs(Float))
    
    Args:
    Float[] : Two Direactional Bound Array [latitude ,latitude ,longitude ,longitude]

    Returns:
    JSON : coordinates
    """
    def generate_coordinates(self):
        latitude = round(random.uniform(15.3523, 22.1002), 4)
        longitude = round(random.uniform(72.3629, 80.5412), 4)
        lat_direction = 'N' if latitude >= 0 else 'S'
        lon_direction = 'E' if longitude >= 0 else 'W'
        transaction_status = self.transactionStatus(abs(latitude), abs(longitude))
        coordinates = {
            "latitude": f"{abs(latitude):.2f}{lat_direction}",
            "longitude": f"{abs(longitude):.2f}{lon_direction}",
            "transaction_status": transaction_status
        }
        return coordinates


    """
    Insert Operation To MongoDB(local)

    Dependancies:
    getDB()
    generate_coordinates()
    
    Args:
    Collection's Name From Mongo DB

    Returns:
    None

    Note:
    Spams Limit to 40K hits
    """
    def InsertDataToMongoDB(self, collection_name, number_of_documents):
        collection = self.getDB()[collection_name]
        no_of_documents = collection.count_documents({})
        # if no_of_documents < 40000:
        for _ in range(number_of_documents):
            collection.insert_many([self.generate_coordinates()])
start_time = time.time()
GenerateAndInsertCoordinates().InsertDataToMongoDB('coordinates1',2000)
print("EXECUTION TIME")
print(time.time() - start_time)