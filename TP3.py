#!/usr/bin/python
# -*- coding: utf-8 -*-

# TP SparkSQL

from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *

nomappli="TP3"
config=SparkConf().setAppName(nomappli)
sc=SparkContext(conf=config)
sqlContext=SQLContext(sc)
sc.setLogLevel("ERROR")


## création RDD sur le fichier transaction.csv
dfTran=sqlContext.read.csv("hdfs:/dataspark/DU/transaction_small.csv", header=True, mode="DROPMALFORMED")
print "Colonnes tables transactions"
print dfTran.columns

#donner un nom de table SQL à un DataFra
dfTran.registerTempTable("dfTran")

## Question 1 : Affichier les usager rotatif avec la requete SQL et l API SPARK SQL

resultat=sqlContext.sql("SELECT * FROM dfTran WHERE usager='Rotatif' limit 10")

print "Avec sqlContext.sql"
for nuplet in resultat.collect():
 print nuplet

result = dfTran.filter("usager='Rotatif'").select("*").limit(10).collect()

print "Avec API SPARK SQL"
print result

## Question 2 :  Repartiton des ugers
print "Repartiton des ugers"
result2 = dfTran.groupBy("usager").count().collect()
print result2

## Question 3 :  Montant moyen paiement par horodateur
print "Montant moyen paiement par horodateur"
result3 = dfTran.groupBy("numhorodateur").avg("montantcarte").collect()
print result3


