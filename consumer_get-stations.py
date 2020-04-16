# -*- coding: utf-8 -*-
"""
Created on Thu Mar 12 10:28:48 2020

@author: Djonga
"""

###Adaptation du projet issu du site openclassroom
#Question 1: Afficuer un message dans le topic empty-stations dès qu'une station devient vide (alors qu'elle n'était pas vide auparavant)
#Question 2: Afficuer un message dans le topic empty-stations dès qu'une station n'est plus vide (alors qu'elle était vide auparavant)

import json
from kafka import KafkaConsumer

stations = {}

#un consumer Kafka pour le topic "empty-stations". Ce consumer fait partie du groupe "velib-monitor-stations

consumer = KafkaConsumer("empty-stations", bootstrap_servers='localhost:9092', group_id="velib-monitor-stations")

for message in consumer:
    station = json.loads(message.value.decode())
    station_number = station["number"]
    contract = station["contract_name"]
    available_bike_stands = station["available_bike_stands"]

    if contract not in stations:
        stations[contract] = {}
    city_stations = stations[contract]
    if station_number not in city_stations:
        city_stations[station_number] = available_bike_stands

    count_diff = available_bike_stands - city_stations[station_number]
    
    #Question1
    if count_diff == 0 and city_stations[station_number]!=0:
        print("Les stations suivantes deviennent vides")
        city_stations[station_number] = available_bike_stands
        print("{}{} {} ({})".format(
            "+" if (count_diff == 0 and city_stations[station_number]!=0) else "",
            count_diff, station["address"], contract
        ))
        
     #Question2
    if count_diff != 0 and city_stations[station_number]==0:
        print("Les stations suivantes deviennent vides")
        city_stations[station_number] = available_bike_stands
        print("{}{} {} ({})".format(
            "+" if (count_diff > 0 and city_stations[station_number]==0) else "",
            count_diff, station["address"], contract
        ))   
        
#pour lancer le script  python consumer_get-stations.py        