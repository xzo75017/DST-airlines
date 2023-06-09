drop database lufthansa;
create database lufthansa;
use database lufthansa;


create schema private;
use schema private;


---stage
CREATE STAGE s3_data url = 's3://dstairline/data'
credentials = (aws_key_id='AKIAV5AVCMDX7HN6MYOT',
                aws_secret_key='MS2/X+kHeKi/1QC6T1JHzuyRfkkcmE3MMmMI9sca');


---use the quering warehouse
use warehouse query;


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
    Longitude       float
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


create or replace table flight_info (  
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

create or replace table flight_information ( 
    FlightID              int identity(1,1),
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


copy into flight_info
from @s3_data/
pattern='.*/customer_flight_information_departures.*\.csv$'
file_format=csv_coma_separated;


insert into  flight_information (DpAirportCode, DpScheduledDate,  DpScheduledTime, DpActualDate,DpActualTime, DpTerminalName, DpTerminalGate, DpStatusCode, DpStatusDescription, ArrAirportCode, ArrScheduledDate, ArrScheduledTime, ArrActualDate, ArrActualTime, ArrTerminalName, ArrTerminalGate, ArrStatusCode, ArrStatusDescription, AirlineID, FlightNumber, AircraftCode, StatusCode, StatusDescription)
select *
from flight_info
where AircraftCode not in ('BUS', 'TRN') 
      and ArrAirportCode in (select airportcode
                             from airports);


---use the quering warehouse
use warehouse query;



---cration de nouvelles table

use schema public;

create or replace table countries(
    CountryCode     string    primary key,
    CountryName     string
);

create or replace table cities(
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
    Longitude       float
);

create or replace table aircrafts (
    AircraftCode     string    primary key,
    AircraftName     string,
    AirlineEquipCode string
);


create or replace table airlines (
    AirlineId        string    primary key,
    AirlineName      string
);




create or replace table flights (
    FlightId           int      identity(1,1) primary key,    
    DpAirportCode      string   foreign key references airports(AirportCode),
    ArrAirportCode     string   foreign key references airports(AirportCode),
    AirlineID          string   foreign key references airlines(AirlineId),
    FlightNumber       int,
    AircraftCode       string   foreign key references aircrafts(AircraftCode),
    StatusCode         string
);



create or replace table status (
    StatusCode        string   primary key,
    StatusDescription string
);


create or replace table departures (
    DepartureId           int     identity(1,1) primary key,
    FlightId              int     foreign key references flights(FlightId),
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
    ArrivalId             int     identity(1,1) primary key,
    FlightId              int     foreign key references flights(FlightId),
    ArrAirportCode        string  foreign key references airports(AirportCode),
    ArrScheduledDate      date,
    ArrScheduledTime      time,
    ArrActualDate         date,
    ArrActualTime         time,
    ArrTerminalName       string,
    ArrTerminalGate       string,
    ArrStatusCode         string   foreign key references status(StatusCode)
);


---use the loading warehouse
use warehouse load;



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


insert into departures (FlightId, DpAirportCode, DpScheduledDate, DpScheduledTime, DpActualDate, DpActualTime, DpTerminalName, DpTerminalGate, DpStatusCode)
select      FlightId, DpAirportCode, DpScheduledDate, DpScheduledTime, DpActualDate, DpActualTime, DpTerminalName, DpTerminalGate, DpStatusCode
from        private.flight_information;


insert into arrivals (FlightId, ArrAirportCode, ArrScheduledDate, ArrScheduledTime, ArrActualDate, ArrActualTime, ArrTerminalName, ArrTerminalGate, ArrStatusCode)
select      FlightId, ArrAirportCode, ArrScheduledDate, ArrScheduledTime, ArrActualDate, ArrActualTime, ArrTerminalName, ArrTerminalGate, ArrStatusCode
from        private.flight_information;




insert into     status (StatusCode, StatusDescription)
select distinct DpStatusCode, DpStatusDescription 
from            private.flight_information
where           DpStatusCode is not null and DpStatusDescription is not null
union
select distinct ArrStatusCode, ArrStatusDescription 
from            private.flight_information
where           ArrStatusCode is not null and ArrStatusDescription is not null
union
select distinct StatusCode, StatusDescription 
from            private.flight_information
where           StatusCode is not null and StatusDescription is not null;



---shema en etoile

create schema star_schema;
use schema star_schema;


create or replace table departures (
    DepartureId       int  primary key,
    AirportCode       string,
    AirportName       string,
    Latitude          float,
    Longitude         float,
    CityName          string,
    UtcOffset         string,
    TimeZoneId        string,
    CountryName       string,
    TerminalName      string,
    TerminalGate      string);


create or replace table arrivals (
    ArrivalId         int  primary key,
    AirportCode       string,
    AirportName       string,
    Latitude          float,
    Longitude         float,
    CityName          string,
    UtcOffset         string,
    TimeZoneId        string,
    CountryName       string,
    TerminalName      string,
    TerminalGate      string);


create or replace table departure_status (
    StatusCode         string primary key,
    StatusDescription  string
);


create or replace table arrival_status (
    StatusCode         string primary key,
    StatusDescription  string
);

create or replace table flight_status (
    StatusCode         string primary key,
    StatusDescription  string
);

create or replace table airlines (
    AirlineId        string    primary key,
    AirlineName      string
);

create or replace table aircrafts (
    AircraftCode     string    primary key,
    AircraftName     string
);

create or replace table flights (
    FlightId                string      primary key,
    DepartureId             int         foreign key references departures(DepartureId),
    ArrivalId               int         foreign key references arrivals(ArrivalId),
    DepartureStatusCode     string      foreign key references departure_status(StatusCode),
    ArrivalStatusCode       string      foreign key references arrival_status(StatusCode),
    FlightStatusCode        string      foreign key references flight_status(StatusCode),
    DpScheduledDate         date,
    DpScheduledTime         time,
    DpActualDate            date,
    DpActualTime            time,
    ArrScheduledDate        date,
    ArrScheduledTime        time,
    ArrActualDate           date,
    ArrActualTime           time,
    AirlineId               string      foreign key references Airlines(AirlineId),
    AircraftCode            string      foreign key references Aircrafts(AircraftCode)
);


---use the loading warehouse
use warehouse load;
select * from departures
order by departureid asc;

---

insert into departures (DepartureId, AirportCode, AirportName, Latitude, Longitude, CityName, UtcOffset, TimeZoneId, CountryName, TerminalName, TerminalGate)
select      d.departureid, d.dpairportcode, a.AirportName, a.Latitude, a.Longitude, c.CityName, a.UtcOffset, a.TimeZoneId, co.CountryName, d.DpTerminalName, d.DpTerminalGate
from        public.departures d
left join        public.airports a   on d.DpAirportCode = a.AirportCode
left join        public.cities c     on a.CityCode = c.CityCode
left join        public.countries co on a.CountryCode = co.CountryCode;



insert into Arrivals (ArrivalId, AirportCode, AirportName, Latitude, Longitude, CityName, UtcOffset, TimeZoneId, CountryName, TerminalName, TerminalGate)
select      arr.arrivalid, arr.arrairportcode, a.AirportName, a.Latitude, a.Longitude, c.CityName, a.UtcOffset, a.TimeZoneId, co.CountryName, arr.ArrTerminalName, arr.ArrTerminalGate
from        public.arrivals arr
left join        public.airports a   on arr.arrairportcode = a.airportcode
left join        public.cities c     on a.CityCode = c.CityCode
left join        public.countries co on a.CountryCode = co.CountryCode;



select count (distinct airportcode)
from arrivals;

select count (distinct airportcode)
from departures;


insert into     departure_status (StatusCode, StatusDescription)
select distinct s.StatusCode, s.StatusDescription
from            public.status s
join            public.departures d on s.StatusCode = d.DpStatusCode;



insert into     arrival_status (StatusCode, StatusDescription)
select distinct s.StatusCode, s.StatusDescription
from            public.status s
join            public.arrivals arr on s.StatusCode = arr.ArrStatusCode;


insert into     flight_status (StatusCode, StatusDescription)
select distinct s.StatusCode, s.StatusDescription
from            public.status s
join            public.flights f on s.StatusCode = f.StatusCode;


insert into     airlines (AirlineId, AirlineName)
select          AirlineId, AirlineName
from            public.airlines ;


insert into     aircrafts (AircraftCode, AircraftName)
select          AircraftCode, AircraftName
from            public.aircrafts;


insert into     flights (FlightId, DepartureId, ArrivalId, DepartureStatusCode, ArrivalStatusCode, FlightStatusCode, DpScheduledDate, DpScheduledTime, DpActualDate, DpActualTime, ArrScheduledDate, ArrScheduledTime, ArrActualDate, ArrActualTime, AirlineId, AircraftCode)
select          f.FlightId, d.DepartureId, arr.ArrivalId, d.DpStatusCode, arr.ArrStatusCode, f.statuscode,              d.DpScheduledDate, d.DpScheduledTime, d.DpActualDate, d.DpActualTime, arr.ArrScheduledDate, arr.ArrScheduledTime, arr.ArrActualDate, arr.ArrActualTime, f.AirlineId, f.AircraftCode
from            public.flights f
left join       public.departures d on f.FlightId = d.FlightId
left join       public.arrivals arr on f.FlightId = arr.FlightId;
