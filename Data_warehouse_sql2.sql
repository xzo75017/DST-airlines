CREATE SCHEMA init;
USE SCHEMA init;



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

CREATE OR REPLACE TABLE flightInformation ("FlightId" STRING, "DpAirportCode" String,"DpScheduledDate" DATE, "DpScheduledTime" TIME, "DpActualDate" DATE , "DpActualTime" TIME, "DpTerminalName" STRING, "DpTerminalGate" STRING,"DpStatusCode" STRING, "DpStatusDescription" text, "ArrAirportCode" STRING,"ArrScheduledDate" DATE, "ArrScheduledTime" TIME, "ArrActualDate" DATE, "ArrActualTime" TIME, "ArrTerminalName" STRING, "ArrTerminalGate" STRING, "ArrStatusCode" STRING, "ArrStatusDescription" TEXT, "AirlineID" STRING, "FlightNumber" STRING, "AircraftCode" STRING,"StatusCode" STRING, "StatusDescription" text);


COPY INTO flightInformation FROM @s3212_data/customer_flight_information_departures19.csv
file_format = csv_coma_separated;


//Create table Country

CREATE OR REPLACE TABLE Country ("CountryCode" STRING primary key , "Names" STRING);



COPY INTO Country FROM @s3212_data/countries2.csv
file_format = csv_error
ON_ERROR = "CONTINUE";



//Creation de la table City

CREATE OR REPLACE TABLE City( "CityCode" STRING primary key,"CountryCode" STRING foreign key references Country("CountryCode"),"Names" STRING ,"UtcOffset" STRING ,"TimeZoneId" STRING);


COPY INTO City FROM @s3212_data/cities2.csv
file_format = csv_error
ON_ERROR = "CONTINUE";



CREATE OR REPLACE TABLE City_Europe( "CityCode" STRING primary key,"CountryCode" STRING foreign key references Country("CountryCode"),"Names" STRING ,"UtcOffset" STRING ,"TimeZoneId" STRING);

insert all into City_Europe("CityCode","CountryCode","Names","UtcOffset" ,"TimeZoneId")
select * from City WHERE "TimeZoneId" LIKE '%Europe%';

//Création de la table OperatingCarrier_distinct

CREATE OR REPLACE TABLE OperatingCarrier("AirlineID" STRING, "FlightNumber" STRING);

CREATE OR REPLACE TABLE OperatingCarrierid("OperatingCarrierID" number identity(1,1) primary key, "AirlineID" STRING, "FlightNumber" STRING);

INSERT INTO OperatingCarrier SELECT "AirlineID","FlightNumber" FROM flightInformation;

insert all 
    into OperatingCarrierid("AirlineID","FlightNumber")
select * from OperatingCarrier;


CREATE TABLE OperatingCarrier_distinct(OperatingCarrierID number identity(1,1) primary key, "AirlineID" STRING, "FlightNumber" STRING);

insert all 
    into OperatingCarrier_distinct( "AirlineID","FlightNumber")
select distinct "AirlineID", "FlightNumber" from OperatingCarrierid;

SELECT * FROM OperatingCarrier_distinct;

//Creation de la table Status


CREATE TABLE Statusdd("Code" STRING, "Description" text);
CREATE TABLE Statusda("Code" STRING, "Description" text);

CREATE OR REPLACE TABLE Status("StatusID" numeric identity(1,1) primary key, "StatusCode" STRING, "Description" text);


INSERT INTO Statusdd SELECT "DpStatusCode","DpStatusDescription" FROM flightInformation;

INSERT INTO Statusda SELECT "ArrStatusCode","ArrStatusDescription" FROM flightInformation;

insert all 
    into Status("StatusCode","Description")
select * from STATUSDD;

insert all 
    into Status("StatusCode","Description")
select * from STATUSDA;


CREATE OR REPLACE TABLE Status_distinct("StatusCode" STRING primary key, "Description" Text);

insert all 
    into Status_distinct( "StatusCode","Description")
select distinct "StatusCode", "Description" from Status;

CREATE OR REPLACE TABLE Status_distinct1(StatusID number identity(1,1) primary key, "Code" STRING, "Description" text);

insert all 
    into Status_distinct1("Code","Description")
select "StatusCode", "Description" from STATUS_DISTINCT;




//Create table Aircraft 
CREATE OR REPLACE TABLE Aircrafts12345("Unnamed: 0.1" numeric,"Unnamed: 0" numeric,"AircraftCode" STRING,"Names" STRING,"AirlineEquipCode" STRING);

COPY INTO Aircrafts12345 FROM @s3212_data/aircrafts.csv
file_format = csv_error
ON_ERROR = "CONTINUE";

CREATE TABLE Aircraft( "AircraftCode" STRING primary key,"Names" STRING,"AirlineEquipCode" STRING );

insert all 
    into Aircraft("AircraftCode","Names","AirlineEquipCode")
select "AircraftCode","Names","AirlineEquipCode" from Aircrafts12345;


//Create table Airports 

CREATE OR REPLACE TABLE airports("AirportCode" STRING primary key,"CityCode" STRING foreign key references City("CityCode"),"CountryCode" STRING foreign key references Country("CountryCode"),"LocationType" STRING,"Names" STRING,"UtcOffset" STRING,"TimeZoneId" STRING,"Latitude" FLOAT,"Longitude" FLOAT);

COPY INTO airports FROM @s3212_data/airports.csv
file_format = csv_error
ON_ERROR = "CONTINUE";

CREATE OR REPLACE TABLE airports_Europe( "AirportCode" STRING primary key,"CityCode" STRING foreign key references City("CityCode"),"CountryCode" STRING foreign key references Country("CountryCode"),"LocationType" STRING,"Names" STRING,"UtcOffset" STRING,"TimeZoneId" STRING,"Latitude" FLOAT,"Longitude" FLOAT);


insert all into airports_Europe("AirportCode" ,"CityCode","CountryCode","LocationType","Names","UtcOffset","TimeZoneId" ,"Latitude" ,"Longitude")
select * from airports WHERE "TimeZoneId" LIKE '%Europe%';

// TableFlightInformation


CREATE OR REPLACE TABLE fligtInformationid ("FlightId" STRING primary key , "DpAirportCode" STRING foreign key references Airports("AirportCode"),"DpScheduledDate" DATE, "DpScheduledTime" TIME, "DpActualDate" DATE , "DpActualTime" TIME, "DpTerminalName" STRING, "DpTerminalGate" STRING,"DpStatusCode" STRING ,"DpStatusDescription" text, "ArrAirportCode" STRING foreign key references Airports("AirportCode"),"ArrScheduledDate" DATE, "ArrScheduledTime" TIME, "ArrActualDate" DATE, "ArrActualTime" TIME, "ArrTerminalName" STRING, "ArrTerminalGate" STRING, "ArrStatusCode" STRING, "ArrStatusDescription" TEXT, "AirlineID" STRING, "FlightNumber" STRING, "AircraftCode" STRING,"StatusCode" STRING, "StatusDescription" text);


insert all into fligtInformationid ("FlightId","DpAirportCode","DpScheduledDate", "DpScheduledTime", "DpActualDate", "DpActualTime", "DpTerminalName" , "DpTerminalGate" ,"DpStatusCode" ,"DpStatusDescription", "ArrAirportCode" ,"ArrScheduledDate", "ArrScheduledTime" , "ArrActualDate", "ArrActualTime", "ArrTerminalName" , "ArrTerminalGate", "ArrStatusCode", "ArrStatusDescription", "AirlineID" , "FlightNumber", "AircraftCode","StatusCode", "StatusDescription")
select * from flightInformation;



//Creation table départure
CREATE OR REPLACE TABLE Departure_withid as select "DpAirportCode" ,"DpScheduledDate","DpScheduledTime", "DpActualDate","DpActualTime","DpTerminalName","DpTerminalGate","DpStatusCode","DpStatusDescription", "AirlineID","FlightNumber","AircraftCode" from flightInformation;


CREATE OR REPLACE TABLE Departure_withid1 ("DepartureID" numeric  identity(1,1) primary key, "DpAirportCode" STRING foreign key references Airports("AirportCode"),"DpScheduledDate" STRING, "DpScheduledTime" TIME,"DpActualDate" DATE, "DpActualTime" STRING, "DpTerminalName" STRING, "DpTerminalGate" STRING, "DpStatusCode" STRING, "DpStatusDescription" text, "AirlineID" STRING,"FlightNumber" STRING,"AircraftCode" STRING foreign key references Aircraft("AircraftCode"));



insert all into Departure_withid1 ( "DpAirportCode" ,"DpScheduledDate","DpScheduledTime","DpActualDate", "DpActualTime","DpTerminalName","DpTerminalGate","DpStatusCode","DpStatusDescription", "AirlineID","FlightNumber","AircraftCode")
select * from Departure_withid;






ALTER TABLE DEPARTURE_WITHID1 ADD StatusID VARCHAR(10);

UPDATE DEPARTURE_WITHID1
SET StatusID = Status_distinct1.StatusID
FROM STATUS_DISTINCT1
WHERE Departure_withid1."DpStatusCode" = Status_distinct1."Code"
AND Departure_withid1."DpStatusDescription" = Status_distinct1."Description";


ALTER TABLE DEPARTURE_WITHID1 ADD OperatingCarrierID numeric;

UPDATE DEPARTURE_WITHID1
SET OperatingCarrierID = OperatingCarrier_distinct.OperatingCarrierID
FROM OperatingCarrier_distinct
WHERE DEPARTURE_WITHID1."AirlineID" = OperatingCarrier_distinct."AirlineID"
AND DEPARTURE_WITHID1."FlightNumber" = OperatingCarrier_distinct."FlightNumber";




ALTER TABLE DEPARTURE_WITHID1 DROP COLUMN "DpStatusCode","DpStatusDescription","AirlineID","FlightNumber" ;


//Creation table Arrival 

CREATE OR REPLACE TABLE Arrival_withid AS SELECT "ArrAirportCode" ,"ArrScheduledDate" ,"ArrScheduledTime", "ArrActualDate" ,"ArrActualTime" ,"ArrTerminalName" ,"ArrTerminalGate","ArrStatusCode" ,"ArrStatusDescription" ,"AirlineID","FlightNumber","AircraftCode"  from fligtInformationid;



CREATE OR REPLACE TABLE Arrival_withid1 ("ArrivalID" numeric  identity(1,1) primary key, "ArrAirportCode" STRING foreign key references Airports("AirportCode") ,"ArrScheduledDate" DATE,"ArrScheduledTime" TIME, "ArrActualDate" DATE,"ArrActualTime" TIME,"ArrTerminalName" STRING,"ArrTerminalGate" STRING,"ArrStatusCode" STRING,"ArrStatusDescription" text, "AirlineID" STRING,"FlightNumber" STRING, "AircraftCode" STRING foreign key references Aircraft("AircraftCode"));


insert all into Arrival_withid1("ArrAirportCode" ,"ArrScheduledDate" ,"ArrScheduledTime","ArrActualDate","ArrActualTime","ArrTerminalName","ArrTerminalGate","ArrStatusCode","ArrStatusDescription","AirlineID", "FlightNumber","AircraftCode")
select * from Arrival_withid;




ALTER TABLE Arrival_withid1 ADD StatusID numeric;

UPDATE Arrival_withid1
SET StatusID = Status_distinct1.StatusID
FROM STATUS_DISTINCT1
WHERE Arrival_withid1."ArrStatusCode" = Status_distinct1."Code"
AND Arrival_withid1."ArrStatusDescription" = Status_distinct1."Description";



ALTER TABLE Arrival_withid1 ADD OperatingCarrierID numeric;

UPDATE Arrival_withid1
SET OperatingCarrierID = OperatingCarrier_distinct.OperatingCarrierID
FROM OperatingCarrier_distinct
WHERE Arrival_withid1."FlightNumber" = OperatingCarrier_distinct."FlightNumber"
AND Arrival_withid1."AirlineID" = OperatingCarrier_distinct."AirlineID";


ALTER TABLE Arrival_withid1 DROP COLUMN "ArrStatusCode","ArrStatusDescription","AirlineID","FlightNumber" ;



// Creation schéma en étoile 



CREATE TABLE DimAircraft( "AircraftCode" STRING primary key,"Names" STRING,"AirlineEquipCode" STRING );
CREATE OR REPLACE TABLE DimAirport("AirportCode" STRING primary key,"LocationType" STRING,"Names" STRING,"UtcOffset" STRING,"TimeZoneId" STRING,"Latitude" FLOAT,"Longitude" FLOAT);

CREATE TABLE DimCity("CityCode" STRING primary key,"Names" STRING ,"UtcOffset" STRING ,"TimeZoneId" STRING);
CREATE TABLE DimCountry("CountryCode" STRING primary key , "Names" STRING);
CREATE TABLE DimDate("Date" DATE primary key );
CREATE TABLE DimTime("Time" TIME primary key );
CREATE TABLE DimOperatingCarrier(OperatingCarrierID number identity(1,1) primary key, "AirlineID" STRING, "FlightNumber" STRING );;
CREATE TABLE DimStatus("StatusID" number identity(1,1) primary key, "Code" STRING, "Description" text);

CREATE OR REPLACE TABLE FactDeparture("DepartureID" number identity(1,1) primary key,"DpAirportCode" STRING foreign key references airports_Europe("AirportCode"),"DpScheduledDate" DATE,"DpScheduledTime" TIME, "DpActualDate" DATE, "DpActualTime" TIME, "StatusID" numeric foreign key references Status_distinct1("STATUSID"), OPERATINGCARRIERID numeric foreign key references  OperatingCarrier_distinct("OPERATINGCARRIERID"),"AircraftCode" String foreign key references aircraft("AircraftCode"), "countryCode" STRING foreign key references Country("CountryCode"),"CityCode" STRING foreign key references City_Europe("CityCode"));

CREATE OR REPLACE TABLE FactArrival("ArrivalID" number identity(1,1) primary key,"ArrAirportCode" STRING foreign key references airports_Europe("AirportCode"),"ArrScheduledDate" DATE,"ArrScheduledTime" TIME, "ArrActualDate" DATE, "ArrActualTime"TIME, "StatusID" numeric foreign key references Status("StatusID"), OPERATINGCARRIERID numeric foreign key references OperatingCarrier_distinct("OPERATINGCARRIERID"),"AircraftCode" String foreign key references aircraft("AircraftCode"),"countryCode" STRING foreign key references Country("CountryCode"), "CityCode" STRING foreign key references City_Europe("CityCode"));




insert all into
DimAircraft ("AircraftCode","Names","AirlineEquipCode" )
select * from aircraft;


insert all into
DimCountry ("CountryCode", "Names" )
select * from Country;



insert all into
DimAirport ("AirportCode", "LocationType","Names","UtcOffset" ,"TimeZoneId","Latitude","Longitude")
select a."AirportCode", a."LocationType", a."Names", a."UtcOffset", a."TimeZoneId", a."Latitude", a."Longitude" FROM AIRPORTS_EUROPE a;

insert all into
DimOperatingCarrier (OperatingCarrierID, "AirlineID", "FlightNumber")
select * from OperatingCarrier_distinct;

insert all into
DimStatus ("StatusID", "Code", "Description")
select * from Status_distinct1;


insert all into
FactDeparture ("DepartureID" ,"DpAirportCode" ,"DpScheduledDate","DpScheduledTime", "DpActualDate", "DpActualTime", "StatusID", "OPERATINGCARRIERID" ,"AircraftCode", "countryCode", "CityCode")
select d."DepartureID", d."DpAirportCode" ,d."DpScheduledDate",d."DpScheduledTime",d."DpActualDate", d."DpActualTime",d."STATUSID",d."OPERATINGCARRIERID",d."AircraftCode", ai."CountryCode", ai."CityCode" from DEPARTURE_WITHID1 d  JOIN AIRPORTS_EUROPE ai ON ai."AirportCode" = d."DpAirportCode";


insert all into
FactArrival ("ArrivalID","ArrAirportCode","ArrScheduledDate","ArrScheduledTime", "ArrActualDate", "ArrActualTime", "StatusID" , "OPERATINGCARRIERID" ,"AircraftCode","countryCode", "CityCode")
