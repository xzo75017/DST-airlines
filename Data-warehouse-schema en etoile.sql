create database lufthansa;
use database lufthansa;


create schema private;
use schema private;


---stage
CREATE STAGE s3_data url = '****************'
credentials = (aws_key_id='************',
                aws_secret_key='***************');
list @s3_data;

---use the quering warehouse
use warehouse query;
alter warehouse query resume;

---creating table
create or replace table countries(
    CountryCode     string,
    CountryName     string
);

create or replace table cities(
    CityCode         string,
    CountryCode      string,
    CityName         string,
    UtcOffset        string,
    TimeZoneId       string
);

create or replace table airports (
    AirportCode     string,
    CityCode        string,
    CountryCode     string,
    LocationType    string,
    AirportName     string,
    UtcOffset       string,
    TimeZoneId      string,
    Latitude        float,
    Longitude       float,
    Continent       string
);

create or replace table aircrafts (
    AircraftCode     string,
    AircraftName     string,
    AirlineEquipCode string
);


create or replace table airlines (
    AirlineId        string,
    AirlineName      string
);


create or replace table flight_information (
    FlightId              string,    
    DpAirportCode         string,
    DpScheduledDate       date,
    DpScheduledTime       time,
    DpActualDate          date,
    DpActualTime          time,
    DpTerminalName        string,
    DpTerminalGate        string,
    DpStatusCode          string,
    DpStatusDescription   string,
    ArrAirportCode        string,
    ArrScheduledDate      date,
    ArrScheduledTime      time,
    ArrActualDate         date,
    ArrActualTime         time,
    ArrTerminalName       string,
    ArrTerminalGate       string,
    ArrStatusCode         string,
    ArrStatusDescription  string,
    AirlineID             string,
    FlightNumber          int,
    AircraftCode          string,
    StatusCode            string,
    StatusDescription     string
    
);

select * from flight_information;
---create file format
create file format csv_coma_separated
type = 'csv' compression = 'auto'
field_delimiter = ';' record_delimiter ='\n'
skip_header = 1 field_optionally_enclosed_by = 'NONE'
trim_space = false error_on_column_count_mismatch = true
escape = 'NONE' escape_unenclosed_field = '\134'
date_format = 'auto' timestamp_format = 'auto' null_if = ('\\N');


---use the loading warehouse
use warehouse load;
alter warehouse load resume;

---inserting data
copy into countries
from @s3_data/countries.csv
file_format = csv_coma_separated;


copy into cities
from @s3_data/cities.csv
file_format = csv_coma_separated;


copy into airports
from @s3_data/airports.csv
file_format = csv_coma_separated;


copy into airlines
from @s3_data/airlines.csv
file_format = csv_coma_separated;


copy into aircrafts
from @s3_data/aircrafts.csv
file_format = csv_coma_separated;

copy into flight_information
from @s3_data/
pattern='.*/customer_flight_information_departures.*\.csv$'
file_format=csv_coma_separated;

---cration de nouvelles table

create schema public;
use schema public;

create table countries(
    CountryCode     string    primary key,
    CountryName     string
);

create table cities(
    CityCode         string   primary key,
    CountryCode      string   foreign key references countries(CountryCode),
    CityName         string,
    UtcOffset        string,
    TimeZoneId       string
);

create or replace table airports (
    AirportCode     string    primary key,
    CityCode        string    foreign key references cities(CityCode),
    CountryCode     string    foreign key references countries(CountryCode),
    LocationType    string,
    AirportName     string,
    UtcOffset       string,
    TimeZoneId      string,
    Latitude        float,
    Longitude       float,
    Continent       string
);

create table aircrafts (
    AircraftCode     string    primary key,
    AircraftName     string,
    AirlineEquipCode string
);


create table airlines (
    AirlineId        string    primary key,
    AirlineName      string
);




create or replace table flights (
    FlightId           string   primary key,    
    DpAirportCode      string   foreign key references airports(AirportCode),
    ArrAirportCode     string   foreign key references airports(AirportCode),
    AirlineID          string   foreign key references airlines(AirlineId),
    FlightNumber       int,
    AircraftCode       string   foreign key references aircrafts(AircraftCode),
    StatusCode         string
);

select count(*) from flights;

/*drop table terminals;
create table terminals (
    TerminalId         int identity(1,1) primary key,
    FlightId           string    foreign key references flights(FlightId),
    DpTerminalName     string,
    DpTerminalGate     string,
    ArrTerminalName    string,
    ArrTerminalGate    string
);
*/

create table status (
    StatusCode        string   primary key,
    StatusDescription string
);


create or replace table departures (
    DepartureId           int identity(1,1) primary key,
    FlightId              string  foreign key references flights(FlightId),
    DpAirportCode         string  foreign key references airports(AirportCode),
    DpScheduledDate       date,
    DpScheduledTime       time,
    DpActualDate          date,
    DpActualTime          time,
    DpTerminalName        string,
    DpTerminalGate        string,
    DpStatusCode          string  foreign key references status(StatusCode)
);


create or replace table arrivals (
    ArrivalId             int identity(1,1) primary key,
    FlightId              string   foreign key references flights(FlightId),
    ArrAirportCode        string   foreign key references airports(AirportCode),
    ArrScheduledDate      date,
    ArrScheduledTime      time,
    ArrActualDate         date,
    ArrActualTime         time,
    ArrTerminalName       string,
    ArrTerminalGate       string,
    ArrStatusCode         string   foreign key references status(StatusCode)
);

select count(*) from;
---peuplement des tables
insert into countries
select *
from private.countries;

insert into cities
select *
from private.cities;

insert into airports
select *
from private.airports;

insert into aircrafts
select *
from private.aircrafts;

insert into airlines
select *
from private.airlines;


insert into  flights (FlightId, DpAirportCode, ArrAirportCode, AirlineID, FlightNumber, AircraftCode, StatusCode)
select       FlightId, DpAirportCode, ArrAirportCode, AirlineID, FlightNumber, AircraftCode, StatusCode
from         private.flight_information;

SELECT * FROM flights; 

insert into departures (FlightId, DpAirportCode, DpScheduledDate, DpScheduledTime, DpActualDate, DpActualTime, DpTerminalName, DpTerminalGate, DpStatusCode)
select      FlightId, DpAirportCode, DpScheduledDate, DpScheduledTime, DpActualDate, DpActualTime, DpTerminalName, DpTerminalGate, DpStatusCode
from        private.flight_information;


insert into arrivals (FlightId, ArrAirportCode, ArrScheduledDate, ArrScheduledTime, ArrActualDate, ArrActualTime, ArrTerminalName, ArrTerminalGate, ArrStatusCode)
select      FlightId, ArrAirportCode, ArrScheduledDate, ArrScheduledTime, ArrActualDate, ArrActualTime, ArrTerminalName, ArrTerminalGate, ArrStatusCode
from        private.flight_information;



insert into     status (StatusCode, StatusDescription)
select distinct DpStatusCode, DpStatusDescription 
from            private.flight_information
union
select distinct ArrStatusCode, ArrStatusDescription 
from            private.flight_information
union
select distinct StatusCode, StatusDescription 
from            private.flight_information;

select citycode, count(airportcode)
from airports
group by citycode
order by count(airportcode) desc;

select * from terminals;


---shema en etoile


Create schema STAR_SCHEMA;
Use schema star_schema;

Create or replace table Departure_Airports (
    DpAirportCode string Primary key,
    AirportName string,
    Latitude float,
    Longitude float,
    CityName string,
    UtcOffset string,
    TimeZoneId string,
    CountryName string
);

Insert Into Departure_Airports (DpAirportCode, AirportName, Latitude, Longitude, CityName, UtcOffset, TimeZoneId, CountryName)
select A.AIRPORTCODE, A.AIRPORTNAME, A.LATITUDE, A.LONGITUDE, C.CITYNAME, A.UTCOFFSET, A.TIMEZONEID, CN.COUNTRYNAME
From public.airports A
LEFT Join public.cities C on A.CITYCODE = C.CITYCODE
LEFT Join public.countries CN on A.COUNTRYCODE = CN.COUNTRYCODE;

select * from departure_Airports;

select * from Departure_Airports;

Create or replace table Arrival_Airports (
    ArrAirportCode string Primary key,
    AirportName string,
    Latitude float,
    Longitude float,
    CityName string,
    UtcOffset string,
    TimeZoneId string,
    CountryName string
);


Insert Into Arrival_Airports (ArrAirportCode, AirportName, Latitude, Longitude, CityName, UtcOffset, TimeZoneId, CountryName)
select A.AIRPORTCODE, A.airportname, A.LATITUDE, A.LONGITUDE, C.CITYNAME, A.UTCOFFSET, A.TIMEZONEID, CN.COUNTRYNAME
From public.airports A
--left Join public.airports A on Ar.ARRAIRPORTCODE = A.AIRPORTCODE
left Join public.cities C on A.CITYCODE = C.CITYCODE
left Join public.countries CN on A.COUNTRYCODE = CN.COUNTRYCODE;


select distinct * From Arrival_Airports;


Create or replace table Departure_Status (
    StatusCode string Primary Key,
    StatusDescription string
);

insert into     Departure_Status (StatusCode, StatusDescription)
select distinct s.StatusCode, s.StatusDescription
from            public.status s
join            public.departures d on s.StatusCode = d.DpStatusCode;

Create or replace table Arrival_Status (
    StatusCode string Primary key,
    StatusDescription string
);

insert into     Arrival_Status (StatusCode, StatusDescription)
select distinct s.StatusCode, s.StatusDescription
from            public.status s
join            public.arrivals arr on s.StatusCode = arr.ArrStatusCode;

Create or replace table Airlines (
    AirlineId string Primary key,
    AirlineName string
);

Insert into Airlines (AirlineId, AirlineName)
select AirlineId, AirlineName
From public.airlines;

Create or replace table Flight_Status (
    StatusCode string Primary key,
    StatusDescription string
);

insert into     Flight_Status (StatusCode, StatusDescription)
select distinct s.StatusCode, s.StatusDescription
from            public.status s
join            public.flights f on s.StatusCode = f.StatusCode;

Create or replace table Aircrafts (
    AircraftCode string Primary key,
    AicraftName string,
    AirlineEquipcode string
);

Insert all into AIRCRAFTS select * from public.aircrafts;


create or replace table Flights (
    FlightID string Primary key,
    DpAirportCode string foreign key references Departure_Airports (DpAirportCode),
    DpScheduledDate Date,
    DpScheduledTime time,
    DpActualDate Date,
    DpActualTime time,
    DpStatusCode string foreign key references Departure_Status (StatusCode),
    AirlineId string foreign key references Airlines (AirlineId),
    AircfraftCode string foreign key references Aircrafts (AircraftCode),
    ArrAirportCode string foreign key references Arrival_Airports (ArrAirportCode),
    ArrScheduledDate Date,
    ArrScheduledTime time,
    ArrActualDate Date,
    ArrActualTime time,
    ArrStatusCode string foreign key references Arrival_Status (StatusCode),
    FlightStatus string foreign key references Flight_Status (StatusCode)
);

Insert into Flights (FLIGHTID, DpAirportCode, DpScheduledDate, DpScheduledTime, DpActualDate, DpActualTime, DpStatusCode, AirlineId, AircfraftCode, ArrAirportCode, ArrScheduledDate, ArrScheduledTime, ArrActualDate, ArrActualTime, ArrStatusCode, FlightStatus)
select DISTINCT F.FLIGHTID, DPAIRPORTCODE , DPSCHEDULEDDATE, DPSCHEDULEDTIME, DPACTUALDATE, DPACTUALTIME, DPSTATUSCODE, AL.AIRLINEID, F.AIRCRAFTCODE , ARRAIRPORTCODE, ARRSCHEDULEDDATE, ARRSCHEDULEDTIME, ARRACTUALDATE, ARRACTUALTIME, ARRSTATUSCODE, f.STATUSCODE
From public.flights F 
NATURAL JOIN public.departures 
LEFT JOIN public.airlines AL on AL.airlineid = f.airlineid
NATURAL JOIN public.arrivals
LEFT JOIN public.status S on S.statuscode = f.StatusCode;
 
select * from Flights; 
