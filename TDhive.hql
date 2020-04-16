--TD HIVEQL
--to run de script and result on the screen ‘hive –f /home/cloudera/dataspark/DU/TDhive.hql;‘
--to run de script and result on the file ‘hive –f /home/cloudera/dataspark/DU/TDhive.hql>result.txt;‘

--Creation de la table salarie
create table salarie ( CODE string, Prenom string, SEXE string, CP string, DP string, Ville string, DN string,SERVICE string, SALAIRE float) rows format delimited fields terminated by '\t',
stored as textfile;

--Description de la table salarie
describe salarie;

--chargement des donnees de la table salarie
load data local inpath ‘/home/cloudera/dataspark/DU/tabl1.txt’ overwrite into table salarie; 

--Les dix premiers salaries
select * from salarie limit 10;

--Creation de la chefs des services
create table chefs (service string, chef string) rows format delimited fields terminated by ';',
stored as textfile;

--Description de la table
describe chefs;

--chargement des donnee
load data local inpath ‘/home/cloudera/dataspark/DU/tabl2.csv’ overwrite into table chefs; 

--Les 5 premiers salaries
select * from chefs limit 5;

--Salaries habitant Paris
select * from salarie where ville like 'PARIS%';

--Nombres des salaries
select count(*) from salarie

--Masse salariale mensuelle pour les des salaries en ile de France
select sum (SALAIRE) where DP in (’75’,’91’,’77’,’78’,’94’,’95’,’92’,’93’);

--Nombres des salaries par services ordre croissant
select SERVICE, count (CODE) as effectif from salarie group by SERVICE order by effectif ASC;

--salaire moyen hors parisien
select avg(SALAIRE) from salarie where DP != ’75’;

--Les noms des chefs et les effectis de leurs equipes
select chefs.chef,count(salaire.CODE) from chefs,right join salaire on salaire.service = chef.service group by chefs.chef;

--salaire moyen par service
select service,avg(SALAIRE) from salarie group by service;

