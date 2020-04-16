#!/usr/bin/python
# -*- coding: utf-8 -*-

# TP Pyspark sur la base stations meteo

from pyspark import SparkConf, SparkContext

# contexte d'exécution pour /usr/local/spark/bin/spark-submit TP_1.py
appName = "Q3_1"
conf = SparkConf().setAppName(appName)
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# ouvrir le fichier
myfile = sc.textFile("hdfs:/dataspark/DU/isd-history.txt")

## Question 1 : Repartition des stations par hémisphère

def hem(string):
 retour = ""
 if string == "+":
    retour = "Nord"

 if string == "-":
    retour = "Sud"
 return retour

#Extraction signe latitude
lat = myfile.map(lambda ligne: ligne[57])

#Creation couple (nord,1) ou (sud,1) 
paires = lat.map(lambda signe: (hem(signe),1) )

#filtre pas signe
paires_= paires.filter(lambda(signe,un): signe!="")

#Comptage par key
pairesSum = paires_.countByKey()

print "Repartition des stations par hémisphère : ",pairesSum


## Question 2 :  usaf et nom de la station avec la plus grande période de mesures

columns = myfile.map(lambda ligne: (int(ligne[82:86]),int(ligne[91:95]),ligne[1:6],ligne[13:29]))

#Verification bonne extraction des valeurs
print columns.take(10)

#filtre  NULL
paires_= columns.filter(lambda(begin,end,usaf,name): begin!="" and end!="")

#Creation couple (endbegin) 
paires = paires_.map(lambda (begin,end,usaf,name): (end-begin, usaf+name))

#Tris par ordre decroissant
pairesOrder = paires.sortByKey(ascending=False)

print "Stations avec plus grande période de mesures: ", pairesOrder.first()

## Question 3 : Pays ayant plus des stations

pays = myfile.map(lambda ligne: ligne[43:45])

#Verification bonne extraction des valeurs
print pays.take(10)

#filtre pas signe
paysok= pays.filter(lambda country: country !="")

#Creation couple (pays,1) 
paires = paysok.map(lambda country_: (country_,1))

#Somme par pays
pairesgr = paires.reduceByKey(lambda a,b: a+b)

#permutation key value
perkeyvalue = pairesgr.map(lambda (k,v): (v,k))

#Tris par ordre decroissant
pairesOrder = perkeyvalue.sortByKey(ascending=False)
 
print "Pays avec plus Stations : ", pairesOrder.first()

## Question 4 : Nombre des Pays ayant  des stations

print "Nombre des Pays ayant  des stations : ", pairesOrder.count()





