DROP DATABASE LUFTHANSA;
CREATE DATABASE LUFTHANSA; 
CREATE SCHEMA Luft;
USE WAREHOUSE LF;


//Creation d'un stage s3211_data
create or replace stage s3212_data url = 's3://dst-airlines-lufthansa/data2_csv/'
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

create or replace table flightInformation1 ( "DpAirportCode" String,"DpScheduledDate" DATE, "DpScheduledTime" TIME, "DpActualDate" DATE , "DpActualTime" TIME, "DpTerminalName" STRING, "DpTerminalGate" STRING,"DpStatusCode" STRING, "DpStatusDescription" text, "ArrAirportCode" STRING,"ArrScheduledDate" DATE, "ArrScheduledTime" TIME, "ArrActualDate" DATE, "ArrActualTime" TIME, "ArrTerminalName" STRING, "ArrTerminalGate" STRING, "ArrStatusCode" STRING, "ArrStatusDescription" TEXT, "AirlineID" STRING, "FlightNumber" STRING, "AircraftCode" STRING,"StatusCode" STRING, "StatusDescription" text);




//Create table Country

create or replace table Country ("CountryCode" STRING primary key , "Names" STRING);


//Creation de la table City

create or replace table City( "CityCode" STRING primary key,"CountryCode" STRING foreign key references Country("CountryCode"),"Names" STRING ,"UtcOffset" STRING ,"TimeZoneId" STRING);

//Creation de la table city Europe 

create or replace table City_Europe( "CityCode" STRING primary key,"CountryCode" STRING foreign key references Country("CountryCode"),"Names" STRING ,"UtcOffset" STRING ,"TimeZoneId" STRING);

//Création des tables intermédiaires OperatingCarrier, OperatingCarrierID et de la table résultante OperatingCarrier_distinct

create or replace table OperatingCarrier("AirlineID" STRING, "FlightNumber" STRING);

create or replace table OperatingCarrierid("OperatingCarrierID" number identity(1,1) primary key, "AirlineID" STRING, "FlightNumber" STRING);

create or replace table OperatingCarrier_distinct(OperatingCarrierID number identity(1,1) primary key, "AirlineID" STRING, "FlightNumber" STRING);

//Creation des tables Statusdp, Statusarr, StatusState, la table résultante Status, Status_distinct comportant les valeurs uniques et Status_distinctid comportant la clé primaire; 


create or replace table Statusdp("Code" STRING, "Description" text);
create or replace table Statusdpid("Code" STRING primary key, "Description" text);
create or replace table Statusarr("Code" STRING, "Description" text);
create or replace table Statusarrid("Code" STRING primary key, "Description" text);
create or replace table StatusState("Code" STRING, "Description" text);
create or replace table StatusStateid("Code" STRING primary key, "Description" text);

create or replace table Status("StatusCode" STRING primary key, "Description" text);
create or replace table Status_distinct("StatusCode" STRING primary key, "Description" Text);


//Create table intermédiaire Aircrafts12345 et de la table finale  Aircraft 
create or replace table Aircrafts12345("AircraftCode" STRING,"Names" STRING,"AirlineEquipCode" STRING);
create or replace table Aircraft( "AircraftCode" STRING primary key,"Names" STRING,"AirlineEquipCode" STRING );

//Create table Airports 

create or replace table airports("AirportCode" STRING primary key,"CityCode" STRING foreign key references City("CityCode"),"CountryCode" STRING foreign key references Country("CountryCode"),"LocationType" STRING,"Names" STRING,"UtcOffset" STRING,"TimeZoneId" STRING,"Latitude" FLOAT,"Longitude" FLOAT);

//Création de la table airports_europe
create or replace table airports_Europe( "AirportCode" STRING primary key,"CityCode" STRING foreign key references City("CityCode"),"CountryCode" STRING foreign key references Country("CountryCode"),"LocationType" STRING,"Names" STRING,"UtcOffset" STRING,"TimeZoneId" STRING,"Latitude" FLOAT,"Longitude" FLOAT);

// TableFlightInformation


create or replace table fligtInformationid ("FlightId" int primary key , "DpAirportCode" STRING foreign key references Airports("AirportCode"),"DpScheduledDate" DATE, "DpScheduledTime" TIME, "DpActualDate" DATE , "DpActualTime" TIME, "DpTerminalName" STRING, "DpTerminalGate" STRING,"DpStatusCode" STRING ,"DpStatusDescription" text, "ArrAirportCode" STRING foreign key references Airports("AirportCode"),"ArrScheduledDate" DATE, "ArrScheduledTime" TIME, "ArrActualDate" DATE, "ArrActualTime" TIME, "ArrTerminalName" STRING, "ArrTerminalGate" STRING, "ArrStatusCode" STRING, "ArrStatusDescription" TEXT, "AirlineID" STRING, "FlightNumber" STRING, "AircraftCode" STRING foreign key references Aircraft("AircraftCode"),"StatusCode" STRING, "StatusDescription" text);


// Creation de la table finale Departure_withid1


create or replace table Departure_withid1 ("DepartureID" numeric  identity(1,1) primary key, "DpAirportCode" STRING foreign key references Airports("AirportCode"),"DpScheduledDate" STRING, "DpScheduledTime" TIME,"DpActualDate" DATE, "DpActualTime" STRING, "DpTerminalName" STRING, "DpTerminalGate" STRING, "DpStatusCode" STRING, "DpStatusDescription" text);

//Creation de la table résultante Arrival_withid1  

create or replace table Arrival_withid1 ("ArrivalID" numeric  identity(1,1) primary key, "ArrAirportCode" STRING foreign key references Airports("AirportCode") ,"ArrScheduledDate" DATE,"ArrScheduledTime" TIME, "ArrActualDate" DATE,"ArrActualTime" TIME,"ArrTerminalName" STRING,"ArrTerminalGate" STRING,"ArrStatusCode" STRING,"ArrStatusDescription" text);

info loading_data;

COPY INTO table_name FROM 'data2_csv/customer_flight_information_departures19.csv' ON_ERROR = 'SKIP_FILE';


//Insertion des données dans la grande table

COPY INTO flightInformation1 FROM @s3212_data/
pattern='.*customer_flight_information.*[.]csv'
file_format = csv_coma_separated;

//Insertion des données dans la table Country
COPY INTO Country FROM @s3212_data/countries2.csv
file_format = csv_error
ON_ERROR = "CONTINUE";

//créer la vraie table flightInformation en enlevant les données de trains et de bus

create or replace table  flightInformation as select  "DpAirportCode","DpScheduledDate" , "DpScheduledTime" , "DpActualDate", "DpActualTime" , "DpTerminalName", "DpTerminalGate","DpStatusCode", "DpStatusDescription", "ArrAirportCode","ArrScheduledDate" , "ArrScheduledTime" , "ArrActualDate" DATE, "ArrActualTime" , "ArrTerminalName", "ArrTerminalGate", "ArrStatusCode", "ArrStatusDescription", "AirlineID" , "FlightNumber", "AircraftCode" ,"StatusCode" , "StatusDescription"  from flightinformation1 where "AircraftCode"!= 'TRN' and "AircraftCode"  != 'BUS';

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


//Insertion des données dans les tables Statusdp, Statusarr, StatusState

INSERT INTO Statusdp SELECT "DpStatusCode","DpStatusDescription" FROM flightInformation;

INSERT INTO Statusarr SELECT "ArrStatusCode","ArrStatusDescription" FROM flightInformation;

INSERT INTO StatusState SELECT "StatusCode","StatusDescription" FROM flightInformation;


insert all 
    into Status("StatusCode","Description")
select * from statusdp;

insert all 
    into Status("StatusCode","Description")
select * from statusarr;


insert all 
    into Status("StatusCode","Description")
select * from StatusState;

insert all 
    into Status_distinct( "StatusCode","Description")
select distinct "StatusCode", "Description" from Status;


insert all 
    into Statusdpid("Code","Description")
select distinct * from statusdp;


insert all 
    into Statusarrid("Code","Description")
select distinct * from statusarr;

insert all 
into StatusStateid("Code","Description")
select distinct * from StatusState;



//Insertion des données dans Aircrafts12345 et dans la table final Aircraft;

COPY INTO Aircrafts12345 FROM @s3212_data/aircrafts.csv
file_format = csv_error
ON_ERROR = "CONTINUE";

insert all 
    into Aircraft("AircraftCode","Names","AirlineEquipCode")
select "AircraftCode","Names","AirlineEquipCode" from Aircrafts12345;


//Insertion des données dans la table airports et airports_europe

copy into airports FROM @s3212_data/airports.csv
file_format = csv_error
on_error = "CONTINUE";

insert all into airports_Europe("AirportCode" ,"CityCode","CountryCode","LocationType","Names","UtcOffset","TimeZoneId" ,"Latitude" ,"Longitude")
select * from airports WHERE "TimeZoneId" LIKE '%Europe%' AND "LocationType" = 'Airport';

//Insertion des donnée dans la table 

//Creation des tables intermediaire Departure_withid + insertion des donnée dans la table intermediaire, insertion des données dans fligtInformationid
create or replace table Departure_withid as select "DpAirportCode" ,"DpScheduledDate","DpScheduledTime", "DpActualDate","DpActualTime","DpTerminalName","DpTerminalGate","DpStatusCode","DpStatusDescription" from flightInformation;

insert all into fligtInformationid ("DpAirportCode","DpScheduledDate", "DpScheduledTime", "DpActualDate", "DpActualTime", "DpTerminalName" , "DpTerminalGate" ,"DpStatusCode" ,"DpStatusDescription", "ArrAirportCode" ,"ArrScheduledDate", "ArrScheduledTime" , "ArrActualDate", "ArrActualTime", "ArrTerminalName" , "ArrTerminalGate", "ArrStatusCode", "ArrStatusDescription", "AirlineID" , "FlightNumber", "AircraftCode","StatusCode", "StatusDescription")
select * from flightInformation ;

//Insertion des donnée dans la table résultante Departure_withid1

insert all into Departure_withid1 ( "DpAirportCode" ,"DpScheduledDate","DpScheduledTime","DpActualDate", "DpActualTime","DpTerminalName","DpTerminalGate","DpStatusCode","DpStatusDescription")
select * from Departure_withid;




// Ajout de la clé étrangere StatusID dans la table Departure_withid1;

SELECT * FROM statusdpid;
ALTER TABLE Departure_withid1 ADD "Codedp" VARCHAR(10);

UPDATE Departure_withid1
SET "Codedp" = Status_distinct."StatusCode"
FROM Status_distinct
WHERE Departure_withid1."DpStatusCode" = Status_distinct."StatusCode"
AND Departure_withid1."DpStatusDescription" = Status_distinct."Description";



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


create or replace table DimOperatingCarrier(OperatingCarrierID number identity(1,1) primary key, "AirlineID" STRING, "FlightNumber" STRING );

insert all into
DimOperatingCarrier (OperatingCarrierID, "AirlineID", "FlightNumber")
select * from OperatingCarrier_distinct;

create or replace table DimStatus("Code" STRING primary key, "Description" text);

insert all into
DimStatus ("Code", "Description")
select * from STATUSDP;



// Insertions des données dans les tables de dimensions et dnas les tabe de fait

create or replace table DimAircraft( "AircraftCode" STRING primary key,"Names" STRING,"AirlineEquipCode" STRING );
insert all into
DimAircraft ("AircraftCode","Names","AirlineEquipCode" )
select * from aircraft;

create or replace table DimCity("CityCode" STRING primary key,"Names" STRING ,"UtcOffset" STRING ,"TimeZoneId" STRING);
create or replace table DimCountry("CountryCode" STRING primary key , "Names" STRING);

insert all into
DimCountry ("CountryCode", "Names" )
select * from Country;


insert all into
DimCity ("CityCode","Names","UtcOffset","TimeZoneId")
select "CityCode","Names","UtcOffset","TimeZoneId" from CITY_EUROPE;

create or replace table DimDepartureAirport("AirportCode" STRING PRIMARY KEY,"CityName" STRING foreign key references DimCity("CityCode"),"CountryName" STRING foreign key references Dimcountry("CountryCode"),"LocationType" STRING, "AirportNames" STRING,"UtcOffset" STRING,"TimeZoneId" STRING,"Latitude" FLOAT,"Longitude" FLOAT);

insert all into 
DimDepartureAirport("AirportCode","CityName" ,"CountryName" ,"LocationType" ,"AirportNames" ,"UtcOffset" ,"TimeZoneId" ,"Latitude" ,"Longitude")
select a."AirportCode", ci."Names", coun."Names", a."LocationType", a."Names",a."UtcOffset", a."TimeZoneId", a."Latitude", a."Longitude" FROM Airports a JOIN City ci ON a."CityCode" = ci."CityCode" JOIN Country coun ON a."CountryCode" = coun."CountryCode";


create or replace table DimArrivalAirports ("AirportCode" STRING PRIMARY KEY,"CityName" STRING foreign key references DimCity("CityCode"),"CountryName" STRING foreign key references Dimcountry("CountryCode"),"LocationType" STRING, "AirportNames" STRING,"UtcOffset" STRING,"TimeZoneId" STRING,"Latitude" FLOAT,"Longitude" FLOAT);

insert all into 
DimArrivalAirports("AirportCode","CityName" ,"CountryName" ,"LocationType" ,"AirportNames" ,"UtcOffset" ,"TimeZoneId" ,"Latitude" ,"Longitude")
select a."AirportCode", ci."Names", coun."Names", a."LocationType", a."Names",a."UtcOffset", a."TimeZoneId", a."Latitude", a."Longitude" FROM Airports a JOIN City ci ON a."CityCode" = ci."CityCode" JOIN Country coun ON a."CountryCode" = coun."CountryCode";




create or replace table FactDeparture("DepartureID" number identity(1,1) primary key,"DpAirportCode" STRING foreign key references DimDepartureAirport("AirportCode"),"DpScheduledDate" DATE,"DpScheduledTime" TIME, "DpActualDate" DATE, "DpActualTime" TIME, "StatusCode" string foreign key references Statusdpid("Code"), OPERATINGCARRIERID numeric foreign key references  DimOperatingCarrier("OPERATINGCARRIERID"),"AircraftCode" String foreign key references Dimaircraft("AircraftCode"), "countryCode" STRING foreign key references DimCountry("CountryCode"),"CityCode" STRING foreign key references DimCity("CityCode"));

create or replace table FactArrival("ArrivalID" number identity(1,1) primary key,"ArrAirportCode" STRING foreign key references DimArrivalAirports("AirportCode"),"ArrScheduledDate" DATE,"ArrScheduledTime" TIME, "ArrActualDate" DATE, "ArrActualTime"TIME, "CodeAr" string foreign key references Statusarrid("Code"), OPERATINGCARRIERID numeric foreign key references DimOperatingCarrier("OPERATINGCARRIERID"),"AircraftCode" String foreign key references Dimaircraft("AircraftCode"),"countryCode" STRING foreign key references DimCountry("CountryCode"), "CityCode" STRING foreign key references DimCity("CityCode"));

insert all into
FactDeparture ("DepartureID" ,"DpAirportCode" ,"DpScheduledDate","DpScheduledTime", "DpActualDate", "DpActualTime", "StatusCode", "OPERATINGCARRIERID" ,"AircraftCode", "countryCode", "CityCode")
select d."DepartureID", d."DpAirportCode" ,d."DpScheduledDate",d."DpScheduledTime",d."DpActualDate", d."DpActualTime",d."Codedp",fi."OPERATINGCARRIERID",fi."AircraftCode", ai."CountryCode", ai."CityCode" from DEPARTURE_WITHID1 d  JOIN AIRPORTS_EUROPE ai ON ai."AirportCode" = d."DpAirportCode" JOIN fligtInformationid fi ON d."DepartureID" = fi."DEPARTUREID";


insert all into
FactArrival ("ArrivalID","ArrAirportCode","ArrScheduledDate","ArrScheduledTime", "ArrActualDate", "ArrActualTime", "CodeAr" , "OPERATINGCARRIERID" ,"AircraftCode","countryCode", "CityCode")
select ar."ArrivalID", ar."ArrAirportCode" ,ar."ArrScheduledDate",ar."ArrScheduledTime",ar."ArrActualDate", ar."ArrActualTime",ar."CodeAr",fi."OPERATINGCARRIERID", fi."AircraftCode", ai."CountryCode", ai."CityCode" from Arrival_withid1 ar JOIN AIRPORTS_EUROPE ai ON ai."AirportCode" = ar."ArrAirportCode" JOIN fligtinformationid fi ON ar."ArrivalID" = fi."ARRIVALID";


//2e schema en étoile
create or replace table Dim2Aircraft( "AircraftCode" STRING primary key,"Names" STRING,"AirlineEquipCode" STRING );

insert all into
Dim2Aircraft ("AircraftCode","Names","AirlineEquipCode" )
select * from aircraft;



create or replace table Dim2OperatingCarrier("OperatingCarrierID" number identity(1,1) primary key, "AirlineID" STRING, "FlightNumber" STRING );

insert all into
Dim2OperatingCarrier ("OperatingCarrierID", "AirlineID", "FlightNumber")
select * from OperatingCarrier_distinct;

create or replace table Dim2Statusdp( "Code" STRING primary key , "Description" text);
insert all into
Dim2Statusdp ("Code", "Description")
select * from Statusdpid;


create or replace tableE Dim2Statusarr( "Code" STRING primary key, "Description" text);
insert all into
Dim2Statusarr ("Code", "Description")
select * from Statusarrid;


create or replace table Dim2StatusState( "Code" STRING primary key, "Description" text);
insert all into
Dim2StatusState ("Code", "Description")
select * from StatusStateid;


create or replace table Dim2City("CityCode" STRING primary key,"Names" STRING ,"UtcOffset" STRING ,"TimeZoneId" STRING);
create or replace table Dim2Country("CountryCode" STRING primary key , "Names" STRING);

insert all into
Dim2Country ("CountryCode", "Names" )
select * from Country;


insert all into
Dim2City ("CityCode","Names","UtcOffset","TimeZoneId")
select "CityCode","Names","UtcOffset","TimeZoneId" from city_europe;

create or replace table Dim2DepartureAirport("AirportCode" string primary key,"CityName" string foreign key references Dim2City("CityCode"),"CountryName" string foreign key references Dim2country("CountryCode"),"LocationType" string, "AirportNames" string,"UtcOffset" string,"TimeZoneId" string,"Latitude" float,"Longitude" float);

insert all into 
Dim2DepartureAirport("AirportCode","CityName" ,"CountryName" ,"LocationType" ,"AirportNames" ,"UtcOffset" ,"TimeZoneId" ,"Latitude" ,"Longitude")
select a."AirportCode", ci."Names", coun."Names", a."LocationType", a."Names",a."UtcOffset", a."TimeZoneId", a."Latitude", a."Longitude" from Airports a join City ci on a."CityCode" = ci."CityCode" join Country coun on a."CountryCode" = coun."CountryCode";


create or replace table Dim2ArrivalAirports ("AirportCode" string primarey key,"CityName" string foreign key references Dim2City("CityCode"),"CountryName" STRING foreign key references Dim2country("CountryCode"),"LocationType" STRING, "AirportNames" STRING,"UtcOffset" STRING,"TimeZoneId" STRING,"Latitude" float,"Longitude" float);

insert all into 
Dim2ArrivalAirports("AirportCode","CityName" ,"CountryName" ,"LocationType" ,"AirportNames" ,"UtcOffset" ,"TimeZoneId" ,"Latitude" ,"Longitude")
select a."AirportCode", ci."Names", coun."Names", a."LocationType", a."Names",a."UtcOffset", a."TimeZoneId", a."Latitude", a."Longitude" from Airports a join City ci on a."CityCode" = ci."CityCode" join Country coun on a."CountryCode" = coun."CountryCode";


create or replace table  FactFlight ("FlightId" string primary key,"DpScheduledDate" date,"DpScheduledTime" time,"DpActualDate" date,"DpActualTime" time, "DpAirportCode" string foreign key references  Dim2DepartureAirport("AirportCode"), "ArrAirportCode" string foreign key references Dim2ArrivalAirports("AirportCode"),"ArrScheduledDate" DATE,"ArrScheduledTime" TIME,"ArrActualDate" DATE,"ArrActualTime" TIME,"CodeDp" string foreign key references Dim2Statusdp("Code"), "CodeAr" string foreign key references Dim2Statusarr("Code"),"CodeSt" string foreign key references Dim2StatusState("Code"),"OperatingCarrierID" numeric  foreign key references Dim2OperatingCarrier("OperatingCarrierID"),"AircraftCode" string foreign key references  DimAirCraft("AircraftCode"));


 

insert all into 
FactFlight("FlightId","DpScheduledTime","DpActualDate","DpActualTime", "DpAirportCode" , "ArrAirportCode","ArrScheduledDate","ArrScheduledTime" ,"ArrActualDate","ArrActualTime","CodeDp", "CodeAr","CodeSt","OperatingCarrierID","AircraftCode")
select  fi."FlightId", dp."DpScheduledTime", dp."DpActualDate",dp."DpActualTime",dp."DpAirportCode", arr."ArrAirportCode", arr."ArrScheduledDate", arr."ArrScheduledTime", arr."ArrActualDate", arr."ArrActualTime", stdp."Code", star."Code",st."Code", fi."OPERATINGCARRIERID", fi."AircraftCode" FROM fligtinformationid fi JOIN departure_withid1 dp ON fi."DEPARTUREID" = dp."DepartureID" JOIN arrival_withid1 arr ON fi."ARRIVALID" = arr."ArrivalID" JOIN Dim2Statusdp stdp ON stdp."Code" = dp."Codedp" JOIN Dim2Statusarr star ON star."Code" =arr."CodeAr" JOIN Dim2StatusState st ON st."Code" = fi."CodeStatus"; 



