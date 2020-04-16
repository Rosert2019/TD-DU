#!/usr/bin/python
# -*- coding: utf-8 -*-

# TP Pyspark sur la base horodateurs residents parisiens

from pyspark import SparkConf, SparkContext

# contexte d'exécution pour /usr/local/spark/bin/spark-submit TP_2.py
appName = "Q4_1"
conf = SparkConf().setAppName(appName)
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# ouvrir le fichier
myfile = sc.textFile("hdfs:/dataspark/DU/horodateurs-transactions-de-paiement.csv")

## Question 1 : horodateur qui a rapporté le plus

#decoupage
splitfile = myfile.map(lambda ligne: ligne.split(";"))

#Extraction num, prix par heur et duree stationnement 
tuplehoro = splitfile.map(lambda ligne_: (ligne_[0],float(ligne_[4]),float(ligne_[5])))

#Entete du fichier
print "Entete du fichier"
print tuplehoro.take(10)

#Extraction paires
paires = tuplehoro.map(lambda (A, B, C) : (A, B*C))

#Somme par horodateur
pairesgr = paires.reduceByKey(lambda a,b: a+b)

#Tris par ordre decroissant
pairesOrder = pairesgr.sortByKey(ascending=False)
 
print "Horodateur qui a rapporté le plus : ", pairesOrder.first()

## Question 2 : prix moyen par heure de stationnement

#Extraction prix par heur  
paires = splitfile.map(lambda ligne_: (ligne_[0],float(ligne_[4])))

print "Prix moyen par heure de stationnement : ", paires.values().sum()/paires.count()

## Question 3 : Plus petite durée de stationnement

#Extraction  prix par heur et duree stationnement 
paires = splitfile.map(lambda ligne_: (float(ligne_[5]),float(ligne_[4])))

pairesOrder = paires.sortByKey(ascending=True)

print "Plus petite durée de stationnement et le montant payé : ", pairesOrder.first()

## Question 4 : Repartition des paiements par mois

#Extraction prix par heur  
paires = splitfile.map(lambda ligne_: ((ligne_[1])[5:7],float(ligne_[4])))

#Entete du fichier
print "Entete du fichier"
print paires.take(10)

#Somme par mois
pairesgr = paires.reduceByKey(lambda a,b: a+b)

print "Repartition des paiements par mois"
print pairesgr.collect()
