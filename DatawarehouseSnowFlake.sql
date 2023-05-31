DROP DATABASE LUFTHANSA;
CREATE DATABASE LUFTHANSA; 
CREATE SCHEMA Luft;
USE WAREHOUSE LF;


//Creation d'un stage s3211_data
CREATE STAGE s3212_data url = 's3://dst-airlines-lufthansa/data2_csv/'
credentials = (aws_key_id='AKIA5HR3523CUD63XNV3',
                aws_secret_key='I0Wp6ejNDMGrehcUbDZup8rQD+nmzSVg/YSVvsg1');


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


CREATE OR REPLACE TABLE Statusdp("Code" STRING, "Description" text);
CREATE OR REPLACE TABLE Statusdpid("Code" STRING primary key, "Description" text);
CREATE OR REPLACE TABLE Statusarr("Code" STRING, "Description" text);
CREATE OR REPLACE TABLE Statusarrid("Code" STRING primary key, "Description" text);
CREATE OR REPLACE TABLE StatusState("Code" STRING, "Description" text);
CREATE OR REPLACE TABLE StatusStateid("Code" STRING primary key, "Description" text);

CREATE OR REPLACE TABLE Status("StatusCode" STRING primary key, "Description" text);
CREATE OR REPLACE TABLE Status_distinct("StatusCode" STRING primary key, "Description" Text);


//Create table intermédiaire Aircrafts12345 et de la table finale  Aircraft 
CREATE OR REPLACE TABLE Aircrafts12345("Unnamed: 0.1" numeric,"Unnamed: 0" numeric,"AircraftCode" STRING,"Names" STRING,"AirlineEquipCode" STRING);
CREATE TABLE Aircraft( "AircraftCode" STRING primary key,"Names" STRING,"AirlineEquipCode" STRING );

//Create table Airports 

CREATE OR REPLACE TABLE airports("AirportCode" STRING primary key,"CityCode" STRING foreign key references City("CityCode"),"CountryCode" STRING foreign key references Country("CountryCode"),"LocationType" STRING,"Names" STRING,"UtcOffset" STRING,"TimeZoneId" STRING,"Latitude" FLOAT,"Longitude" FLOAT);

//Création de la table airports_europe
CREATE OR REPLACE TABLE airports_Europe( "AirportCode" STRING primary key,"CityCode" STRING foreign key references City("CityCode"),"CountryCode" STRING foreign key references Country("CountryCode"),"LocationType" STRING,"Names" STRING,"UtcOffset" STRING,"TimeZoneId" STRING,"Latitude" FLOAT,"Longitude" FLOAT);

// TableFlightInformation


CREATE OR REPLACE TABLE fligtInformationid ("FlightId" STRING primary key , "DpAirportCode" STRING foreign key references Airports("AirportCode"),"DpScheduledDate" DATE, "DpScheduledTime" TIME, "DpActualDate" DATE , "DpActualTime" TIME, "DpTerminalName" STRING, "DpTerminalGate" STRING,"DpStatusCode" STRING ,"DpStatusDescription" text, "ArrAirportCode" STRING foreign key references Airports("AirportCode"),"ArrScheduledDate" DATE, "ArrScheduledTime" TIME, "ArrActualDate" DATE, "ArrActualTime" TIME, "ArrTerminalName" STRING, "ArrTerminalGate" STRING, "ArrStatusCode" STRING, "ArrStatusDescription" TEXT, "AirlineID" STRING, "FlightNumber" STRING, "AircraftCode" STRING foreign key references Aircraft("AircraftCode"),"StatusCode" STRING, "StatusDescription" text);


// Creation de la table finale Departure_withid1


CREATE OR REPLACE TABLE Departure_withid1 ("DepartureID" numeric  identity(1,1) primary key, "DpAirportCode" STRING foreign key references Airports("AirportCode"),"DpScheduledDate" STRING, "DpScheduledTime" TIME,"DpActualDate" DATE, "DpActualTime" STRING, "DpTerminalName" STRING, "DpTerminalGate" STRING, "DpStatusCode" STRING, "DpStatusDescription" text);

//Creation de la table résultante Arrival_withid1  

CREATE OR REPLACE TABLE Arrival_withid1 ("ArrivalID" numeric  identity(1,1) primary key, "ArrAirportCode" STRING foreign key references Airports("AirportCode") ,"ArrScheduledDate" DATE,"ArrScheduledTime" TIME, "ArrActualDate" DATE,"ArrActualTime" TIME,"ArrTerminalName" STRING,"ArrTerminalGate" STRING,"ArrStatusCode" STRING,"ArrStatusDescription" text);




//Insertion des données dans la grande table

COPY INTO flightInformation FROM @s3212_data/
pattern='.*customer_flight_information.*[.]csv'
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
    into Statusdpid("Code","Description")
select distinct * from STATUSDP;


insert all 
    into Statusarrid("Code","Description")
select distinct * from STATUSARR;

insert all 
into StatusStateid("Code","Description")
select distinct * from StatusState;



//DROP TABLE AIRCRAFT;
//DROP TABLE AIRCRAFTS12345;
//DROP TABLE AIRPORTS;
//DROP TABLE AIRPORTS_EUROPE;
//DROP TABLE ARRIVAL_WITHID;
//DROP TABLE ARRIVAL_WITHID1;
//DROP TABLE CITY;
//DROP TABLE CITY_EUROPE;
//DROP TABLE COUNTRY;
//DROP TABLE DEPARTURE_WITHID;
//DROP TABLE DEPARTURE_WITHID1;
//DROP TABLE FLIGHTINFORMATION;
//DROP TABLE FLIGTINFORMATIONID;
//DROP TABLE OPERATINGCARRIER;
//DROP TABLE OPERATINGCARRIERID;
//DROP TABLE OPERATINGCARRIER_DISTINCT;
//DROP TABLE STATUS;
//DROP TABLE STATUSDA;
//DROP TABLE STATUSDD;
//DROP TABLE STATUS_DISTINCT;
//DROP TABLE STATUS_DISTINCT1;



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
CREATE OR REPLACE TABLE Departure_withid as select "DpAirportCode" ,"DpScheduledDate","DpScheduledTime", "DpActualDate","DpActualTime","DpTerminalName","DpTerminalGate","DpStatusCode","DpStatusDescription" from flightInformation;

insert all into fligtInformationid ("FlightId","DpAirportCode","DpScheduledDate", "DpScheduledTime", "DpActualDate", "DpActualTime", "DpTerminalName" , "DpTerminalGate" ,"DpStatusCode" ,"DpStatusDescription", "ArrAirportCode" ,"ArrScheduledDate", "ArrScheduledTime" , "ArrActualDate", "ArrActualTime", "ArrTerminalName" , "ArrTerminalGate", "ArrStatusCode", "ArrStatusDescription", "AirlineID" , "FlightNumber", "AircraftCode","StatusCode", "StatusDescription")
select * from flightInformation;

//Insertion des donnée dans la table résultante Departure_withid1

insert all into Departure_withid1 ( "DpAirportCode" ,"DpScheduledDate","DpScheduledTime","DpActualDate", "DpActualTime","DpTerminalName","DpTerminalGate","DpStatusCode","DpStatusDescription")
select * from Departure_withid;




// Ajout de la clé étrangere StatusID dans la table Departure_withid1;

SELECT * FROM statusdpid;
ALTER TABLE Departure_withid1 ADD "Codedp" VARCHAR(10);

UPDATE Departure_withid1
SET "Codedp" = STATUS_DISTINCT."StatusCode"
FROM STATUS_DISTINCT
WHERE Departure_withid1."DpStatusCode" = STATUS_DISTINCT."StatusCode"
AND Departure_withid1."DpStatusDescription" = STATUS_DISTINCT."Description";



// ajour de la clé étrangère StatusID dans la table fligtInformation;
ALTER TABLE fligtInformationid ADD "CodeStatus" VARCHAR(10);

UPDATE fligtInformationid
SET "CodeStatus" = STATUS_DISTINCT."StatusCode"
FROM STATUS_DISTINCT
WHERE fligtInformationid."StatusCode" = STATUS_DISTINCT."StatusCode"
AND fligtInformationid."StatusDescription" = STATUS_DISTINCT."Description";

// Ajout de la clé étrangère OperatingCarrierID dans la table fligtInformation;

ALTER TABLE fligtInformationid ADD OperatingCarrierID numeric;

UPDATE fligtInformationid
SET OperatingCarrierID = OperatingCarrier_distinct.OperatingCarrierID
FROM OperatingCarrier_distinct
WHERE fligtInformationid."AirlineID" = OperatingCarrier_distinct."AirlineID"
AND fligtInformationid."FlightNumber" = OperatingCarrier_distinct."FlightNumber";



//Ajout de la clé étrangère DepartureID dans la table fligtInformation


ALTER TABLE fligtInformationid ADD DepartureID numeric;

UPDATE fligtInformationid
SET DepartureID = Departure_withid1."DepartureID"
FROM DEPARTURE_WITHID1
WHERE fligtInformationid."DpAirportCode" =Departure_withid1."DpAirportCode"
AND fligtInformationid."DpScheduledTime" = Departure_withid1."DpScheduledTime"
AND fligtInformationid."DpScheduledDate" = Departure_withid1."DpScheduledDate";

//Suppression des colonnes dans Departure

ALTER TABLE Departure_withid1 DROP COLUMN "DpTerminalName" ,"DpTerminalGate","DpStatusCode" ,"DpStatusDescription"  ;


//Creation de la table intermédiaire Arrival  + insertion 

CREATE OR REPLACE TABLE Arrival_withid AS SELECT "ArrAirportCode" ,"ArrScheduledDate" ,"ArrScheduledTime", "ArrActualDate" ,"ArrActualTime" ,"ArrTerminalName" ,"ArrTerminalGate","ArrStatusCode" ,"ArrStatusDescription"  from fligtInformationid;


//Insertion  dans la table résultante Arrival_withid1  
insert all into Arrival_withid1("ArrAirportCode" ,"ArrScheduledDate" ,"ArrScheduledTime","ArrActualDate","ArrActualTime","ArrTerminalName","ArrTerminalGate","ArrStatusCode","ArrStatusDescription")
select * from Arrival_withid;


//Ajout de la clé étrangère StatusID dans la table Arrival_withid1

ALTER TABLE arrival_withid1 ADD "CodeAr" VARCHAR(10);

UPDATE ARRIVAL_WITHID1
SET "CodeAr" = STATUS_DISTINCT."StatusCode"
FROM STATUS_DISTINCT
WHERE Arrival_withid1."ArrStatusCode" = STATUS_DISTINCT."StatusCode"
AND Arrival_withid1."ArrStatusDescription" = STATUS_DISTINCT."Description";



//Ajouter ArrStatus et AeeStatusDescription dans la  grande table

UPDATE fligtInformationid f
SET f."ArrStatusCode" = s."StatusCode", f."ArrStatusDescription" = s."Description"
FROM STATUS_DISTINCT s
WHERE f."CodeStatus" = s."StatusCode";

//Ajout de la colonne ArrivalID dans la grande table 

ALTER TABLE fligtInformationid ADD ArrivalID numeric;

UPDATE fligtInformationid
SET ArrivalID = Arrival_withid1."ArrivalID"
FROM ARRIVAL_WITHID1
WHERE fligtInformationid."ArrAirportCode" =Arrival_withid1."ArrAirportCode"
AND fligtInformationid."ArrScheduledTime" = Arrival_withid1."ArrScheduledTime"
AND fligtInformationid."ArrScheduledDate" = Arrival_withid1."ArrScheduledDate";


// Suppression des autres colonnes pour garder la clé primères et les clé étrangères 

ALTER TABLE fligtInformationid DROP COLUMN "DpAirportCode","DpScheduledDate", "DpScheduledTime", "DpActualDate", "DpActualTime", "DpTerminalName" , "DpTerminalGate" ,"DpStatusCode" ,"DpStatusDescription", "ArrAirportCode" ,"ArrScheduledDate", "ArrScheduledTime" , "ArrActualDate", "ArrActualTime", "ArrTerminalName" , "ArrTerminalGate", "ArrStatusCode", "ArrStatusDescription", "AirlineID" , "FlightNumber","StatusCode", "StatusDescription";

SELECT * FROM fligtinformationid;
// Suppression des autres colonnes pour garder la clé primères et les clé étrangères 

ALTER TABLE Arrival_withid1 DROP COLUMN "ArrTerminalName" ,"ArrTerminalGate","ArrStatusCode" ,"ArrStatusDescription"  ;


// Creation du Schéma en étoile 



//Création des  dimentions DimAircraft,DimCity,DimCountry,DimOperatingCarrier,DimStatus et des tables de faits FactDeparture, FactArrival


CREATE OR REPLACE TABLE DimOperatingCarrier(OperatingCarrierID number identity(1,1) primary key, "AirlineID" STRING, "FlightNumber" STRING );

insert all into
DimOperatingCarrier (OperatingCarrierID, "AirlineID", "FlightNumber")
select * from OperatingCarrier_distinct;

CREATE OR REPLACE TABLE DimStatus("Code" STRING primary key, "Description" text);

insert all into
DimStatus ("Code", "Description")
select * from STATUSDP;



// Insertions des données dans les tables de dimensions et dnas les tabe de fait

CREATE OR REPLACE TABLE DimAircraft( "AircraftCode" STRING primary key,"Names" STRING,"AirlineEquipCode" STRING );
insert all into
DimAircraft ("AircraftCode","Names","AirlineEquipCode" )
select * from aircraft;

CREATE OR REPLACE TABLE DimCity("CityCode" STRING primary key,"Names" STRING ,"UtcOffset" STRING ,"TimeZoneId" STRING);
CREATE OR REPLACE TABLE DimCountry("CountryCode" STRING primary key , "Names" STRING);

insert all into
DimCountry ("CountryCode", "Names" )
select * from Country;


insert all into
DimCity ("CityCode","Names","UtcOffset","TimeZoneId")
select "CityCode","Names","UtcOffset","TimeZoneId" from CITY_EUROPE;

CREATE OR REPLACE TABLE DimDepartureAirport("AirportCode" STRING PRIMARY KEY,"CityName" STRING foreign key references DimCity("CityCode"),"CountryName" STRING foreign key references Dimcountry("CountryCode"),"LocationType" STRING, "AirportNames" STRING,"UtcOffset" STRING,"TimeZoneId" STRING,"Latitude" FLOAT,"Longitude" FLOAT);

insert all into 
DimDepartureAirport("AirportCode","CityName" ,"CountryName" ,"LocationType" ,"AirportNames" ,"UtcOffset" ,"TimeZoneId" ,"Latitude" ,"Longitude")
select a."AirportCode", ci."Names", coun."Names", a."LocationType", a."Names",a."UtcOffset", a."TimeZoneId", a."Latitude", a."Longitude" FROM Airports a JOIN City ci ON a."CityCode" = ci."CityCode" JOIN Country coun ON a."CountryCode" = coun."CountryCode";


CREATE  OR REPLACE TABLE DimArrivalAirports ("AirportCode" STRING PRIMARY KEY,"CityName" STRING foreign key references DimCity("CityCode"),"CountryName" STRING foreign key references Dimcountry("CountryCode"),"LocationType" STRING, "AirportNames" STRING,"UtcOffset" STRING,"TimeZoneId" STRING,"Latitude" FLOAT,"Longitude" FLOAT);

insert all into 
DimArrivalAirports("AirportCode","CityName" ,"CountryName" ,"LocationType" ,"AirportNames" ,"UtcOffset" ,"TimeZoneId" ,"Latitude" ,"Longitude")
select a."AirportCode", ci."Names", coun."Names", a."LocationType", a."Names",a."UtcOffset", a."TimeZoneId", a."Latitude", a."Longitude" FROM Airports a JOIN City ci ON a."CityCode" = ci."CityCode" JOIN Country coun ON a."CountryCode" = coun."CountryCode";




CREATE OR REPLACE TABLE FactDeparture("DepartureID" number identity(1,1) primary key,"DpAirportCode" STRING foreign key references DimDepartureAirport("AirportCode"),"DpScheduledDate" DATE,"DpScheduledTime" TIME, "DpActualDate" DATE, "DpActualTime" TIME, "StatusCode" string foreign key references Statusdpid("Code"), OPERATINGCARRIERID numeric foreign key references  DimOperatingCarrier("OPERATINGCARRIERID"),"AircraftCode" String foreign key references Dimaircraft("AircraftCode"), "countryCode" STRING foreign key references DimCountry("CountryCode"),"CityCode" STRING foreign key references DimCity("CityCode"));

CREATE OR REPLACE TABLE FactArrival("ArrivalID" number identity(1,1) primary key,"ArrAirportCode" STRING foreign key references DimArrivalAirports("AirportCode"),"ArrScheduledDate" DATE,"ArrScheduledTime" TIME, "ArrActualDate" DATE, "ArrActualTime"TIME, "CodeAr" string foreign key references Statusarrid("Code"), OPERATINGCARRIERID numeric foreign key references DimOperatingCarrier("OPERATINGCARRIERID"),"AircraftCode" String foreign key references Dimaircraft("AircraftCode"),"countryCode" STRING foreign key references DimCountry("CountryCode"), "CityCode" STRING foreign key references DimCity("CityCode"));

insert all into
FactDeparture ("DepartureID" ,"DpAirportCode" ,"DpScheduledDate","DpScheduledTime", "DpActualDate", "DpActualTime", "StatusCode", "OPERATINGCARRIERID" ,"AircraftCode", "countryCode", "CityCode")
select d."DepartureID", d."DpAirportCode" ,d."DpScheduledDate",d."DpScheduledTime",d."DpActualDate", d."DpActualTime",d."Codedp",fi."OPERATINGCARRIERID",fi."AircraftCode", ai."CountryCode", ai."CityCode" from DEPARTURE_WITHID1 d  JOIN AIRPORTS_EUROPE ai ON ai."AirportCode" = d."DpAirportCode" JOIN fligtInformationid fi ON d."DepartureID" = fi."DEPARTUREID";


insert all into
FactArrival ("ArrivalID","ArrAirportCode","ArrScheduledDate","ArrScheduledTime", "ArrActualDate", "ArrActualTime", "CodeAr" , "OPERATINGCARRIERID" ,"AircraftCode","countryCode", "CityCode")
select ar."ArrivalID", ar."ArrAirportCode" ,ar."ArrScheduledDate",ar."ArrScheduledTime",ar."ArrActualDate", ar."ArrActualTime",ar."CodeAr",fi."OPERATINGCARRIERID", fi."AircraftCode", ai."CountryCode", ai."CityCode" from Arrival_withid1 ar JOIN AIRPORTS_EUROPE ai ON ai."AirportCode" = ar."ArrAirportCode" JOIN fligtinformationid fi ON ar."ArrivalID" = fi."ARRIVALID";


//2e schema en étoile
CREATE TABLE Dim2Aircraft( "AircraftCode" STRING primary key,"Names" STRING,"AirlineEquipCode" STRING );

insert all into
Dim2Aircraft ("AircraftCode","Names","AirlineEquipCode" )
select * from aircraft;



CREATE OR REPLACE TABLE Dim2OperatingCarrier("OperatingCarrierID" number identity(1,1) primary key, "AirlineID" STRING, "FlightNumber" STRING );

insert all into
Dim2OperatingCarrier ("OperatingCarrierID", "AirlineID", "FlightNumber")
select * from OperatingCarrier_distinct;

CREATE OR REPLACE TABLE Dim2Statusdp( "Code" STRING primary key , "Description" text);
insert all into
Dim2Statusdp ("Code", "Description")
select * from Statusdpid;


CREATE OR REPLACE TABLE Dim2Statusarr( "Code" STRING primary key, "Description" text);
insert all into
Dim2Statusarr ("Code", "Description")
select * from Statusarrid;


CREATE OR REPLACE TABLE Dim2StatusState( "Code" STRING primary key, "Description" text);
insert all into
Dim2StatusState ("Code", "Description")
select * from StatusStateid;


CREATE OR REPLACE TABLE Dim2City("CityCode" STRING primary key,"Names" STRING ,"UtcOffset" STRING ,"TimeZoneId" STRING);
CREATE OR REPLACE TABLE Dim2Country("CountryCode" STRING primary key , "Names" STRING);

insert all into
Dim2Country ("CountryCode", "Names" )
select * from Country;


insert all into
Dim2City ("CityCode","Names","UtcOffset","TimeZoneId")
select "CityCode","Names","UtcOffset","TimeZoneId" from CITY_EUROPE;

CREATE OR REPLACE TABLE Dim2DepartureAirport("AirportCode" STRING PRIMARY KEY,"CityName" STRING foreign key references Dim2City("CityCode"),"CountryName" STRING foreign key references Dim2country("CountryCode"),"LocationType" STRING, "AirportNames" STRING,"UtcOffset" STRING,"TimeZoneId" STRING,"Latitude" FLOAT,"Longitude" FLOAT);

insert all into 
Dim2DepartureAirport("AirportCode","CityName" ,"CountryName" ,"LocationType" ,"AirportNames" ,"UtcOffset" ,"TimeZoneId" ,"Latitude" ,"Longitude")
select a."AirportCode", ci."Names", coun."Names", a."LocationType", a."Names",a."UtcOffset", a."TimeZoneId", a."Latitude", a."Longitude" FROM Airports a JOIN City ci ON a."CityCode" = ci."CityCode" JOIN Country coun ON a."CountryCode" = coun."CountryCode";


CREATE  OR REPLACE TABLE Dim2ArrivalAirports ("AirportCode" STRING PRIMARY KEY,"CityName" STRING foreign key references Dim2City("CityCode"),"CountryName" STRING foreign key references Dim2country("CountryCode"),"LocationType" STRING, "AirportNames" STRING,"UtcOffset" STRING,"TimeZoneId" STRING,"Latitude" FLOAT,"Longitude" FLOAT);

insert all into 
Dim2ArrivalAirports("AirportCode","CityName" ,"CountryName" ,"LocationType" ,"AirportNames" ,"UtcOffset" ,"TimeZoneId" ,"Latitude" ,"Longitude")
select a."AirportCode", ci."Names", coun."Names", a."LocationType", a."Names",a."UtcOffset", a."TimeZoneId", a."Latitude", a."Longitude" FROM Airports a JOIN City ci ON a."CityCode" = ci."CityCode" JOIN Country coun ON a."CountryCode" = coun."CountryCode";


CREATE OR REPLACE TABLE FactFlight ("FlightId" STRING primary key,"DpScheduledDate" DATE,"DpScheduledTime" TIME,"DpActualDate" DATE,"DpActualTime" TIME, "DpAirportCode" STRING  FOREIGN KEY  REFERENCES Dim2DepartureAirport("AirportCode"), "ArrAirportCode" STRING  FOREIGN KEY  REFERENCES Dim2ArrivalAirports("AirportCode"),"ArrScheduledDate" DATE,"ArrScheduledTime" TIME,"ArrActualDate" DATE,"ArrActualTime" TIME,"CodeDp" STRING FOREIGN KEY  REFERENCES Dim2Statusdp("Code"), "CodeAr" STRING FOREIGN KEY  REFERENCES Dim2Statusarr("Code"),"CodeSt" STRING FOREIGN KEY  REFERENCES Dim2StatusState("Code"),"OperatingCarrierID" numeric  FOREIGN KEY  REFERENCES Dim2OperatingCarrier("OperatingCarrierID"),"AircraftCode" STRING  FOREIGN KEY  REFERENCES DimAirCraft("AircraftCode"));


 

insert all into 
FactFlight("FlightId","DpScheduledTime","DpActualDate","DpActualTime", "DpAirportCode" , "ArrAirportCode","ArrScheduledDate","ArrScheduledTime" ,"ArrActualDate","ArrActualTime","CodeDp", "CodeAr","CodeSt","OperatingCarrierID","AircraftCode")
select  fi."FlightId", dp."DpScheduledTime", dp."DpActualDate",dp."DpActualTime",dp."DpAirportCode", arr."ArrAirportCode", arr."ArrScheduledDate", arr."ArrScheduledTime", arr."ArrActualDate", arr."ArrActualTime", stdp."Code", star."Code",st."Code", fi."OPERATINGCARRIERID", fi."AircraftCode" FROM fligtinformationid fi JOIN departure_withid1 dp ON fi."DEPARTUREID" = dp."DepartureID" JOIN arrival_withid1 arr ON fi."ARRIVALID" = arr."ArrivalID" JOIN Dim2Statusdp stdp ON stdp."Code" = dp."Codedp" JOIN Dim2Statusarr star ON star."Code" =arr."CodeAr" JOIN Dim2StatusState st ON st."Code" = fi."CodeStatus"; 
