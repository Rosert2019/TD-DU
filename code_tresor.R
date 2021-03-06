# Module n�cessaire
library(rlang)
library(dismo)
library(raster)
library(sp)
library(XML)

#Sauvegarde adresses dans une liste
liste <-c('20 rue du d�partement,75018, Paris',
'147 quai du Pr�sident Roosevelt, 92130, Issy Les Moulineaux',
'143 Avenue de Versailles ,75016, Paris',
'158 Boulevard Haussmann, 75008, Paris',
'22 Rue des rosiers ,94230, Cachan')


#Lancemenr requetes par adresses de la liste


# On cr�e une variable vide, qui va contenir les requ�tes
requete <- NULL

for (i in 1:length(liste)){
requete[i] <- paste("http://maps.googleapis.com/maps/api/geocode/xml?address=",
                       liste[i],
                       "&key=AIzaSyCi16DBK_kzPibl73mEx9mESfERal3QM5k",
                       sep="")}

# Variable dans laquelle on va stocker les r�sultats
adresses <- NULL

# On traite chacune des requ�te en parcourant la liste

for(i in 1:length(requete)) {

  # Envoie la requ�te, re�oit la r�ponse, analyse sa structure
  reponse.xml <- readLines(requete[i])
  reponse <- xmlTreeParse(reponse.xml, useInternalNodes=TRUE)

 
  # on ne prends que la premi�re r�ponse 

  # On initialise comme NA
  latitude <- NA

  # Si valeur existe, remplace NA
  latitude <- xmlValue(reponse[["//result[1]//geometry/location/lat"]])

 
  longitude <- NA 
  longitude <- xmlValue(reponse[["//result[1]//geometry/location/lng"]]) 


  adresse.recue <- NA 
  adresse.recue <- xmlValue(reponse[["//result[1]//formatted_address"]]) 

  # On mets les r�sultats sous forme de dataframe
  latlon.etc <- cbind(adresses[i,], requete[i], latitude, longitude, adresse.recue)
  adresses.latlon <- rbind(adresses, latlon.etc)

  # Pause de 1 seconde
  Sys.sleep(1)

}




