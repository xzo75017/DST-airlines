DROP DATABASE LUFTHANSA;
CREATE DATABASE LUFTHANSA; 
CREATE SCHEMA Luft;


//Creation d'un stage s3211_data
CREATE STAGE s3212_data url = 's3://dst-airlines-lufthansa/data2_csv/'
credentials = (aws_key_id='**************',
                aws_secret_key='*****************');


// creation file format

create or replace file format csv_error
type = 'csv' compression = 'auto'
field_delimiter = ';' record_delimiter ='\n'
skip_header = 1 field_optionally_enclosed_by = 'NONE'
trim_space = false error_on_column_count_mismatch = false 
escape = 'NONE' escape_unenclosed_field = '\134' 
date_format = 'auto' timestamp_format = 'auto' null_if = ('\\N');

create or replace file format csv_coma_separated
type = 'csv' compression = 'auto'
field_delimiter = ';' record_delimiter ='\n'
skip_header = 1 field_optionally_enclosed_by = 'NONE'
trim_space = false error_on_column_count_mismatch = true
escape = 'NONE' escape_unenclosed_field = '\134'
date_format = 'auto' timestamp_format = 'auto' null_if = ('\\N');


//Création de la grande table flightInformation

CREATE OR REPLACE TABLE flightInformation ("FlightId" STRING, "DpAirportCode" String,"DpScheduledDate" DATE, "DpScheduledTime" TIME, "DpActualDate" DATE , "DpActualTime" TIME, "DpTerminalName" STRING, "DpTerminalGate" STRING,"DpStatusCode" STRING, "DpStatusDescription" text, "ArrAirportCode" STRING,"ArrScheduledDate" DATE, "ArrScheduledTime" TIME, "ArrActualDate" DATE, "ArrActualTime" TIME, "ArrTerminalName" STRING, "ArrTerminalGate" STRING, "ArrStatusCode" STRING, "ArrStatusDescription" TEXT, "AirlineID" STRING, "FlightNumber" STRING, "AircraftCode" STRING,"StatusCode" STRING, "StatusDescription" text);




//Create table Country

CREATE OR REPLACE TABLE Country ("CountryCode" STRING primary key , "Names" STRING);


//Creation de la table City

CREATE OR REPLACE TABLE City( "CityCode" STRING primary key,"CountryCode" STRING foreign key references Country("CountryCode"),"Names" STRING ,"UtcOffset" STRING ,"TimeZoneId" STRING);

//Creation de la table city Europe 

CREATE OR REPLACE TABLE City_Europe( "CityCode" STRING primary key,"CountryCode" STRING foreign key references Country("CountryCode"),"Names" STRING ,"UtcOffset" STRING ,"TimeZoneId" STRING);

//Création des tables intermédiaires OperatingCarrier, OperatingCarrierID et de la table résultante OperatingCarrier_distinct

CREATE OR REPLACE TABLE OperatingCarrier("AirlineID" STRING, "FlightNumber" STRING);

CREATE OR REPLACE TABLE OperatingCarrierid("OperatingCarrierID" number identity(1,1) primary key, "AirlineID" STRING, "FlightNumber" STRING);

CREATE TABLE OperatingCarrier_distinct(OperatingCarrierID number identity(1,1) primary key, "AirlineID" STRING, "FlightNumber" STRING);

//Creation des tables Statusdp, Statusarr, StatusState, la table résultante Status, Status_distinct comportant les valeurs uniques et Status_distinctid comportant la clé primaire; 


CREATE TABLE Statusdp("Code" STRING, "Description" text);
CREATE TABLE Statusarr("Code" STRING, "Description" text);
CREATE TABLE StatusState("Code" STRING, "Description" text);

CREATE OR REPLACE TABLE Status("StatusID" numeric identity(1,1) primary key, "StatusCode" STRING, "Description" text);
CREATE OR REPLACE TABLE Status_distinct("StatusCode" STRING primary key, "Description" Text);
CREATE OR REPLACE TABLE Status_distinctid(StatusID number identity(1,1) primary key, "Code" STRING, "Description" text);

//Create table intermédiaire Aircrafts12345 et de la table finale  Aircraft 
CREATE OR REPLACE TABLE Aircrafts12345("Unnamed: 0.1" numeric,"Unnamed: 0" numeric,"AircraftCode" STRING,"Names" STRING,"AirlineEquipCode" STRING);
CREATE TABLE Aircraft( "AircraftCode" STRING primary key,"Names" STRING,"AirlineEquipCode" STRING );

//Create table Airports 

CREATE OR REPLACE TABLE airports("AirportCode" STRING primary key,"CityCode" STRING foreign key references City("CityCode"),"CountryCode" STRING foreign key references Country("CountryCode"),"LocationType" STRING,"Names" STRING,"UtcOffset" STRING,"TimeZoneId" STRING,"Latitude" FLOAT,"Longitude" FLOAT);

//Création de la table airports_europe
CREATE OR REPLACE TABLE airports_Europe( "AirportCode" STRING primary key,"CityCode" STRING foreign key references City("CityCode"),"CountryCode" STRING foreign key references Country("CountryCode"),"LocationType" STRING,"Names" STRING,"UtcOffset" STRING,"TimeZoneId" STRING,"Latitude" FLOAT,"Longitude" FLOAT);

// TableFlightInformation


CREATE OR REPLACE TABLE fligtInformationid ("FlightId" STRING primary key , "DpAirportCode" STRING foreign key references Airports("AirportCode"),"DpScheduledDate" DATE, "DpScheduledTime" TIME, "DpActualDate" DATE , "DpActualTime" TIME, "DpTerminalName" STRING, "DpTerminalGate" STRING,"DpStatusCode" STRING ,"DpStatusDescription" text, "ArrAirportCode" STRING foreign key references Airports("AirportCode"),"ArrScheduledDate" DATE, "ArrScheduledTime" TIME, "ArrActualDate" DATE, "ArrActualTime" TIME, "ArrTerminalName" STRING, "ArrTerminalGate" STRING, "ArrStatusCode" STRING, "ArrStatusDescription" TEXT, "AirlineID" STRING, "FlightNumber" STRING, "AircraftCode" STRING,"StatusCode" STRING, "StatusDescription" text);


// Creation de la table finale Departure_withid1


CREATE OR REPLACE TABLE Departure_withid1 ("DepartureID" numeric  identity(1,1) primary key, "DpAirportCode" STRING foreign key references Airports("AirportCode"),"DpScheduledDate" STRING, "DpScheduledTime" TIME,"DpActualDate" DATE, "DpActualTime" STRING, "DpTerminalName" STRING, "DpTerminalGate" STRING, "DpStatusCode" STRING, "DpStatusDescription" text, "AirlineID" STRING,"FlightNumber" STRING,"AircraftCode" STRING foreign key references Aircraft("AircraftCode"));

//Creation de la table résultante Arrival_withid1  

CREATE OR REPLACE TABLE Arrival_withid1 ("ArrivalID" numeric  identity(1,1) primary key, "ArrAirportCode" STRING foreign key references Airports("AirportCode") ,"ArrScheduledDate" DATE,"ArrScheduledTime" TIME, "ArrActualDate" DATE,"ArrActualTime" TIME,"ArrTerminalName" STRING,"ArrTerminalGate" STRING,"ArrStatusCode" STRING,"ArrStatusDescription" text, "AirlineID" STRING,"FlightNumber" STRING, "AircraftCode" STRING foreign key references Aircraft("AircraftCode"));


//Insertion des données dans la grande table

COPY INTO flightInformation FROM @s3212_data/customer_flight_information_departures18.csv
file_format = csv_coma_separated;


//Insertion des données dans la table Country
COPY INTO Country FROM @s3212_data/countries2.csv
file_format = csv_error
ON_ERROR = "CONTINUE";



//Insertion des données dans la table city

COPY INTO City FROM @s3212_data/cities2.csv
file_format = csv_error
ON_ERROR = "CONTINUE";


//Insertion des données dans la table City_Europe

insert all into City_Europe("CityCode","CountryCode","Names","UtcOffset" ,"TimeZoneId")
select * from City WHERE "TimeZoneId" LIKE '%Europe%';

//Insertion des données dans la table OperatingCarrier, OperatingCarrierID et OperatingCarrier_distinct

INSERT INTO OperatingCarrier SELECT "AirlineID","FlightNumber" FROM flightInformation;

insert all 
    into OperatingCarrierid("AirlineID","FlightNumber")
select * from OperatingCarrier;


insert all 
    into OperatingCarrier_distinct( "AirlineID","FlightNumber")
select distinct "AirlineID", "FlightNumber" from OperatingCarrierid;

SELECT * FROM OperatingCarrier_distinct;


//Insertion des données dans les tables Statusdp, Statusarr, StatusState

INSERT INTO Statusdp SELECT "DpStatusCode","DpStatusDescription" FROM flightInformation;

INSERT INTO Statusarr SELECT "ArrStatusCode","ArrStatusDescription" FROM flightInformation;

INSERT INTO StatusState SELECT "StatusCode","StatusDescription" FROM flightInformation;

insert all 
    into Status("StatusCode","Description")
select * from STATUSDP;

insert all 
    into Status("StatusCode","Description")
select * from STATUSARR;

insert all 
    into Status("StatusCode","Description")
select * from StatusState;

insert all 
    into Status_distinct( "StatusCode","Description")
select distinct "StatusCode", "Description" from Status;

insert all 
    into Status_distinctid("Code","Description")
select "StatusCode", "Description" from STATUS_DISTINCT;


//Insertion des données dans Aircrafts12345 et dans la table final Aircraft;

COPY INTO Aircrafts12345 FROM @s3212_data/aircrafts.csv
file_format = csv_error
ON_ERROR = "CONTINUE";

insert all 
    into Aircraft("AircraftCode","Names","AirlineEquipCode")
select "AircraftCode","Names","AirlineEquipCode" from Aircrafts12345;


//Insertion des données dans la table airports et airports_europe

COPY INTO airports FROM @s3212_data/airports.csv
file_format = csv_error
ON_ERROR = "CONTINUE";

insert all into airports_Europe("AirportCode" ,"CityCode","CountryCode","LocationType","Names","UtcOffset","TimeZoneId" ,"Latitude" ,"Longitude")
select * from airports WHERE "TimeZoneId" LIKE '%Europe%';

//Insertion des donnée dans la table 

//Creation des tables intermediaire Departure_withid + insertion des donnée dans la table intermediaire, insertion des données dans fligtInformationid
CREATE OR REPLACE TABLE Departure_withid as select "DpAirportCode" ,"DpScheduledDate","DpScheduledTime", "DpActualDate","DpActualTime","DpTerminalName","DpTerminalGate","DpStatusCode","DpStatusDescription", "AirlineID","FlightNumber","AircraftCode" from flightInformation;

insert all into fligtInformationid ("FlightId","DpAirportCode","DpScheduledDate", "DpScheduledTime", "DpActualDate", "DpActualTime", "DpTerminalName" , "DpTerminalGate" ,"DpStatusCode" ,"DpStatusDescription", "ArrAirportCode" ,"ArrScheduledDate", "ArrScheduledTime" , "ArrActualDate", "ArrActualTime", "ArrTerminalName" , "ArrTerminalGate", "ArrStatusCode", "ArrStatusDescription", "AirlineID" , "FlightNumber", "AircraftCode","StatusCode", "StatusDescription")
select * from flightInformation;

//Insertion des donnée dans la table résultante Departure_withid1

insert all into Departure_withid1 ( "DpAirportCode" ,"DpScheduledDate","DpScheduledTime","DpActualDate", "DpActualTime","DpTerminalName","DpTerminalGate","DpStatusCode","DpStatusDescription", "AirlineID","FlightNumber","AircraftCode")
select * from Departure_withid;

// Ajout de la clé étrangere StatusID dans la table Departure_withid1;


ALTER TABLE DEPARTURE_WITHID1 ADD StatusID VARCHAR(10);

UPDATE DEPARTURE_WITHID1
SET StatusID = Status_distinctid.StatusID
FROM Status_distinctid
WHERE Departure_withid1."DpStatusCode" = Status_distinctid."Code"
AND Departure_withid1."DpStatusDescription" = Status_distinctid."Description";

// Ajout de la clé étrangère OperatingCarrierID dans la table Departure_withid1;

ALTER TABLE DEPARTURE_WITHID1 ADD OperatingCarrierID numeric;

UPDATE DEPARTURE_WITHID1
SET OperatingCarrierID = OperatingCarrier_distinct.OperatingCarrierID
FROM OperatingCarrier_distinct
WHERE DEPARTURE_WITHID1."AirlineID" = OperatingCarrier_distinct."AirlineID"
AND DEPARTURE_WITHID1."FlightNumber" = OperatingCarrier_distinct."FlightNumber";


// Suppression des autres colonnes pour garder la clé primères et les clé étrangères 

ALTER TABLE DEPARTURE_WITHID1 DROP COLUMN "DpStatusCode","DpStatusDescription","AirlineID","FlightNumber" ;


//Creation de la table intermédiaire Arrival  + insertion 

CREATE OR REPLACE TABLE Arrival_withid AS SELECT "ArrAirportCode" ,"ArrScheduledDate" ,"ArrScheduledTime", "ArrActualDate" ,"ArrActualTime" ,"ArrTerminalName" ,"ArrTerminalGate","ArrStatusCode" ,"ArrStatusDescription" ,"AirlineID","FlightNumber","AircraftCode"  from fligtInformationid;


//Insertion  dans la table résultante Arrival_withid1  
insert all into Arrival_withid1("ArrAirportCode" ,"ArrScheduledDate" ,"ArrScheduledTime","ArrActualDate","ArrActualTime","ArrTerminalName","ArrTerminalGate","ArrStatusCode","ArrStatusDescription","AirlineID", "FlightNumber","AircraftCode")
select * from Arrival_withid;


//Ajout de la clé étrangère StatusID dans la table Arrival_withid1

ALTER TABLE Arrival_withid1 ADD StatusID numeric;

UPDATE Arrival_withid1
SET StatusID = Status_distinctid.StatusID
FROM Status_distinctid
WHERE Arrival_withid1."ArrStatusCode" = Status_distinctid."Code"
AND Arrival_withid1."ArrStatusDescription" = Status_distinctid."Description";

//Ajout de la clé étrangère OperatingCarrierID dans la table Arrival_withid1

ALTER TABLE Arrival_withid1 ADD OperatingCarrierID numeric;

UPDATE Arrival_withid1
SET OperatingCarrierID = OperatingCarrier_distinct.OperatingCarrierID
FROM OperatingCarrier_distinct
WHERE Arrival_withid1."FlightNumber" = OperatingCarrier_distinct."FlightNumber"
AND Arrival_withid1."AirlineID" = OperatingCarrier_distinct."AirlineID";


// Suppression des autres colonnes pour garder la clé primères et les clé étrangères 

ALTER TABLE Arrival_withid1 DROP COLUMN "ArrStatusCode","ArrStatusDescription","AirlineID","FlightNumber" ;


// Creation du Schéma en étoile 



//Création des  dimentions DimAircraft,DimCity,DimCountry,DimOperatingCarrier,DimStatus et des tables de faits FactDeparture, FactArrival
CREATE TABLE DimAircraft( "AircraftCode" STRING primary key,"Names" STRING,"AirlineEquipCode" STRING );
CREATE OR REPLACE TABLE DimAirport("AirportCode" STRING primary key,"LocationType" STRING,"Names" STRING,"UtcOffset" STRING,"TimeZoneId" STRING,"Latitude" FLOAT,"Longitude" FLOAT);

CREATE TABLE DimCity("CityCode" STRING primary key,"Names" STRING ,"UtcOffset" STRING ,"TimeZoneId" STRING);
CREATE TABLE DimCountry("CountryCode" STRING primary key , "Names" STRING);

CREATE TABLE DimOperatingCarrier(OperatingCarrierID number identity(1,1) primary key, "AirlineID" STRING, "FlightNumber" STRING );;
CREATE TABLE DimStatus("StatusID" number identity(1,1) primary key, "Code" STRING, "Description" text);

CREATE OR REPLACE TABLE FactDeparture("DepartureID" number identity(1,1) primary key,"DpAirportCode" STRING foreign key references airports_Europe("AirportCode"),"DpScheduledDate" DATE,"DpScheduledTime" TIME, "DpActualDate" DATE, "DpActualTime" TIME, "StatusID" numeric foreign key references Status_distinctid("STATUSID"), OPERATINGCARRIERID numeric foreign key references  OperatingCarrier_distinct("OPERATINGCARRIERID"),"AircraftCode" String foreign key references aircraft("AircraftCode"), "countryCode" STRING foreign key references Country("CountryCode"),"CityCode" STRING foreign key references City_Europe("CityCode"));

CREATE OR REPLACE TABLE FactArrival("ArrivalID" number identity(1,1) primary key,"ArrAirportCode" STRING foreign key references airports_Europe("AirportCode"),"ArrScheduledDate" DATE,"ArrScheduledTime" TIME, "ArrActualDate" DATE, "ArrActualTime"TIME, "StatusID" numeric foreign key references Status_distinctid("STATUSID"), OPERATINGCARRIERID numeric foreign key references OperatingCarrier_distinct("OPERATINGCARRIERID"),"AircraftCode" String foreign key references aircraft("AircraftCode"),"countryCode" STRING foreign key references Country("CountryCode"), "CityCode" STRING foreign key references City_Europe("CityCode"));

// Insertions des données dans les tables de dimensions et dnas les tabe de fait


insert all into
DimAircraft ("AircraftCode","Names","AirlineEquipCode" )
select * from aircraft;


insert all into
DimCountry ("CountryCode", "Names" )
select * from Country;


insert all into
DimCity ("CityCode","Names","UtcOffset","TimeZoneId")
select "CityCode","Names","UtcOffset","TimeZoneId" from CITY_EUROPE;

insert all into
DimAirport ("AirportCode", "LocationType","Names","UtcOffset" ,"TimeZoneId","Latitude","Longitude")
select a."AirportCode", a."LocationType", a."Names", a."UtcOffset", a."TimeZoneId", a."Latitude", a."Longitude" FROM AIRPORTS_EUROPE a;

insert all into
DimOperatingCarrier (OperatingCarrierID, "AirlineID", "FlightNumber")
select * from OperatingCarrier_distinct;

insert all into
DimStatus ("StatusID", "Code", "Description")
select * from Status_distinctid;


insert all into
FactDeparture ("DepartureID" ,"DpAirportCode" ,"DpScheduledDate","DpScheduledTime", "DpActualDate", "DpActualTime", "StatusID", "OPERATINGCARRIERID" ,"AircraftCode", "countryCode", "CityCode")
select d."DepartureID", d."DpAirportCode" ,d."DpScheduledDate",d."DpScheduledTime",d."DpActualDate", d."DpActualTime",d."STATUSID",d."OPERATINGCARRIERID",d."AircraftCode", ai."CountryCode", ai."CityCode" from DEPARTURE_WITHID1 d  JOIN AIRPORTS_EUROPE ai ON ai."AirportCode" = d."DpAirportCode";


insert all into
FactArrival ("ArrivalID","ArrAirportCode","ArrScheduledDate","ArrScheduledTime", "ArrActualDate", "ArrActualTime", "StatusID" , "OPERATINGCARRIERID" ,"AircraftCode","countryCode", "CityCode")
select ar."ArrivalID", ar."ArrAirportCode" ,ar."ArrScheduledDate",ar."ArrScheduledTime",ar."ArrActualDate", ar."ArrActualTime",ar."STATUSID",ar."OPERATINGCARRIERID", ar."AircraftCode", ai."CountryCode", ai."CityCode" from Arrival_withid1 ar JOIN AIRPORTS_EUROPE ai ON ai."AirportCode" = ar."ArrAirportCode";


// Suppression des tables intermédiaires


DROP TABLE AIRCRAFTS12345;
DROP TABLE DEPARTURE_WITHID;
DROP TABLE FLIGHTINFORMATION;
DROP TABLE OPERATINGCARRIER;
DROP TABLE OPERATINGCARRIERID;
DROP TABLE STATUS;
DROP TABLE STATUSDP;
DROP TABLE STATUSARR;
DROP TABLE STATUSSTATE;
DROP TABLE STATUS_DISTINCT;


