# -*- coding: utf-8 -*-
"""
Created on Thu Mar 12 10:15:55 2020

@author: Djonga
"""

#Adaptation du projet issu du site openclassroom
#Le producer envoie les données sur les stations sur le topic 'empty-stations'
#Avant de lancer ce script il faut créer le topic dans kafka avec les commandes suivantes:
# on fera appel à l'aaplication toutes les 15 semaines
#$ ./bin/zookeeper-server-start.sh ./config/zookeeper.properties
#$ ./bin/kafka-server-start.sh ./config/server.properties
#$ ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic empty-stations

import json
import time
import urllib.request

from kafka import KafkaProducer

API_KEY = "fcd559923356642c0acb41f170f44105e9fa2192" 
url = "https://api.jcdecaux.com/vls/v1/stations?apiKey={}".format(API_KEY)

producer = KafkaProducer(bootstrap_servers="localhost:9092")

while True:
    response = urllib.request.urlopen(url)
    stations = json.loads(response.read().decode())
    for station in stations:
        producer.send("empty-stations", json.dumps(station).encode())
    print("{} Produced {} station records".format(time.time(), len(stations)))
    time.sleep(15)
    
#pour lancer le script  python producer_empty.py    