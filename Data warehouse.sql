create database lufthansa;
use database lufthansa;


create schema private;
use schema private;


---stage
CREATE STAGE s3_data url = 's3://dstairline/data'
credentials = (aws_key_id='AKIAV5AVCMDX7HN6MYOT',
                aws_secret_key='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');
list @s3_data;

---use the quering warehouse
use warehouse query;
alter warehouse query resume;

---creating table
create table countries(
    CountryCode     string,
    CountryName     string
);

create table cities(
    CityCode         string,
    CountryCode      string,
    CityName         string,
    UtcOffset        string,
    TimeZoneId       string
);

create table airports (
    AirportCode     string,
    CityCode        string,
    CountryCode     string,
    LocationType    string,
    AirportName     string,
    UtcOffset       string,
    TimeZoneId      string,
    Latitude        float,
    Longitude       float
);

create table aircrafts (
    AircraftCode     string,
    AircraftName     string,
    AirlineEquipCode string
);


create table airlines (
    AirlineId        string,
    AirlineName      string
);


create table flight_information (
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



drop file format classic_csv;
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

/*copy into flight_information_departures
from @s3_data/flight_information_departures.csv
file_format = csv_coma_separated;


copy into flight_information_arrivals
from @s3_data/flight_information_arrivals.csv
file_format = csv_coma_separated; */




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

create table airports (
    AirportCode     string    primary key,
    CityCode        string    foreign key references cities(CityCode),
    CountryCode     string    foreign key references countries(CountryCode),
    LocationType    string,
    AirportName     string,
    UtcOffset       string,
    TimeZoneId      string,
    Latitude        float,
    Longitude       float
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




create table flights (
    FlightId           string   primary key,    
    DpAirportCode      string   foreign key references airports(AirportCode),
    ArrAirportCode     string   foreign key references airports(AirportCode),
    AirlineID          string   foreign key references airlines(AirlineId),
    FlightNumber       int,
    AircraftCode       string   foreign key references aircrafts(AircraftCode)
);

create table terminals (
    TerminalId         int identity(1,1) primary key,
    FlightId           string    foreign key references flights(FlightId),
    DpTerminalName     string,
    DpTerminalGate     string,
    ArrTerminalName    string,
    ArrTerminalGate    string
);


create table status (
    StatusCode        string   primary key,
    StatusDescription string
);


create table departures (
    DepartureId           int identity(1,1) primary key,
    FlightId              string  foreign key references flights(FlightId),
    DpAirportCode         string  foreign key references airports(AirportCode),
    DpScheduledDate       date,
    DpScheduledTime       time,
    DpActualDate          date,
    DpActualTime          time,
    DpStatusCode          string  foreign key references status(StatusCode)
);


create table arrivals (
    ArrivalId             int identity(1,1) primary key,
    FlightId              string   foreign key references flights(FlightId),
    ArrAirportCode        string   foreign key references airports(AirportCode),
    ArrScheduledDate      date,
    ArrScheduledTime      time,
    ArrActualDate         date,
    ArrActualTime         time,
    ArrStatusCode         string   foreign key references status(StatusCode)
);



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


insert into  flights (FlightId, DpAirportCode, ArrAirportCode, AirlineID, FlightNumber, AircraftCode)
select       FlightId, DpAirportCode, ArrAirportCode, AirlineID, FlightNumber, AircraftCode
from         private.flight_information;


insert into departures (FlightId, DpAirportCode, DpScheduledDate, DpScheduledTime, DpActualDate, DpActualTime, DpStatusCode)
select      FlightId, DpAirportCode, DpScheduledDate, DpScheduledTime, DpActualDate, DpActualTime, DpStatusCode
from        private.flight_information;


insert into arrivals (FlightId, ArrAirportCode, ArrScheduledDate, ArrScheduledTime, ArrActualDate, ArrActualTime, ArrStatusCode)
select      FlightId, ArrAirportCode, ArrScheduledDate, ArrScheduledTime, ArrActualDate, ArrActualTime, ArrStatusCode
from        private.flight_information;


insert into terminals (FlightId, DpTerminalName, DpTerminalGate, ArrTerminalName, ArrTerminalGate)
select      FlightId, DpTerminalName, DpTerminalGate, ArrTerminalName, ArrTerminalGate
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


select distinct d.dpairportcode, t.dpterminalgate
from departures as d
join terminals as t on d.flightid = t.flightid
where d.dpairportcode = 'CDG';



---shema en etoile
