#!/usr/bin/env python
# coding: utf-8

# In[2]:


# importing packages
import pandas as pd
import requests
from time import sleep
import datetime
import itertools
from tqdm import tqdm




#
def get_country (Access_Token) :
    
    """This function takes as parameter the API access token provided \
by lufthansa "LH OpenAPI testing" and returns a DataFrame of the country. """
    
    # 
    authentification = {}
    authentification["Authorization"] = "Bearer " + Access_Token

    #
    
    df_list = []
    offset_list = list(range(0,300,100))


    for i in offset_list:
        response = requests.get(("https://api.lufthansa.com/v1/mds-references/countries/?lang=AN&limit=100&offset={offset}").format(offset = i), headers = authentification).json()
        df = pd.DataFrame(response['CountryResource']['Countries']['Country'])
        df_list.append(df)
        Countries = pd.concat(df_list)
        Countries["Names"] = Countries["Names"].apply(lambda x : x['Name'][ '$'])
        Countries = Countries.drop_duplicates().reset_index(drop = True)
        #Countries.insert(0, "CountryId" , Countries.index+1)
    
    return Countries


#
def get_cities(Access_Token)  :
    
    #
    authentification = {}
    authentification["Authorization"] = "Bearer " + Access_Token
    
    df_list = []
    offset_list = list(range(0,10700,100))
    #
    for i in offset_list:
        sleep(0.09)
        response = requests.get(("https://api.lufthansa.com/v1/mds-references/cities/?lang=AN&limit=100&offset={offset}").format(offset = i), headers = authentification).json()
        df_list.append(pd.DataFrame(response['CityResource']['Cities']['City']))
        Cities = pd.concat(df_list).reset_index(drop = True)
        Cities["Names"] = Cities["Names"].apply(lambda x : x['Name'][ '$'])
        Cities["Airports"] = Cities["Airports"].apply(lambda x: x['AirportCode'] if isinstance(x, dict) else None)
        
    Cities = Cities.explode("Airports").drop_duplicates().reset_index(drop=True)
    Cities.insert(0, "CountryId" , Cities.index+1)
    return Cities


#
def get_airports (Access_Token)  :
    
    #
    authentification = {}
    authentification["Authorization"] = "Bearer " + Access_Token
    
    #
    df_list = []
    offset_list = list(range(0,1500,100))
    offset_list[offset_list.index(100)]= 170
    offset_list[offset_list.index(1000)]= 1005
    
    for i in offset_list:
        sleep(0.1)
        reponse = requests.get(("https://api.lufthansa.com/v1/mds-references/airports/?lang=AN&limit=100&offset={offset}&LHoperated=1").format(offset = i), headers = authentification).json()
        df_list.append(pd.DataFrame(reponse['AirportResource']['Airports']['Airport']))

    df = pd.concat(df_list)  
    df = df.reset_index(drop=True) 

    for i in range(len(df)):
        airport_name = df.loc[i, "Names"].get("Name", {}).get("$",)
        df.loc[i, "Names"] = airport_name


        latitude = df.loc[i, "Position"]["Coordinate"].get("Latitude")
        df.loc[i, "Latitude"] = latitude

        longitude = df.loc[i, "Position"]["Coordinate"].get("Longitude")
        df.loc[i, "Longitude"] = longitude



    Airports = df.drop("Position", axis= 1)
    
    return Airports



def get_aircrafts(Access_Token) :
    
    #
    authentification = {}
    authentification["Authorization"] = "Bearer " + Access_Token
    
    #
    df_list = []
    offset_list = list(range(0,400,100))  

    for i in offset_list:
        reponse = requests.get(("https://api.lufthansa.com/v1/mds-references/aircraft/?limit=100&offset={offset}&LHoperated=1").format(offset = i), headers = authentification).json()
        df_list.append(pd.DataFrame(reponse['AircraftResource']['AircraftSummaries']['AircraftSummary']))

    df = pd.concat(df_list)  
    df = df.reset_index(drop=True)

    for i in range(len(df)):
            aircraft_name = df.loc[i, "Names"].get("Name", {}).get("$")
            df.loc[i, "Names"] = aircraft_name

    Aircrafts = df
    return Aircrafts


def get_flight_schedules(Access_Token, origins, destinations, start_date, num_days):

    authentification = {}
    authentification["Authorization"] = "Bearer " + Access_Token


    dates = [start_date + datetime.timedelta(days=x) for x in range(num_days)]
    dates = [d.strftime('%Y-%m-%d') for d in dates]
    
    df_list = []
    counter = 0
    
    for origin, destination, date in tqdm(itertools.product(origins, destinations, dates)):
        if origin == destination:
            continue

        try:
            response = requests.get(
                ("https://api.lufthansa.com/v1/operations/schedules/{origin}/{destination}/{date}"
                 "?directFlights=0&limit=100&offset=0"
                ).format(origin = origin, destination = destination, date = date),
                headers=authentification).json()

            for element in response['ScheduleResource']['Schedule']:
                for flight in element.get('Flight', []):
                    departure = flight.get('Departure', {})
                    arrival = flight.get('Arrival', {})
                    carrier = flight.get('MarketingCarrier', {})
                    equipment = flight.get('Equipment', {})
                    details = flight.get('Details', {})

                    df = pd.DataFrame({
                        'DepAirportCode': departure.get('AirportCode'),
                        'DepScheduledTimeLocal': departure.get('ScheduledTimeLocal', {}).get('DateTime'),
                        'ArrAirportCode': arrival.get('AirportCode'),
                        'ArrScheduledTimeLocal': arrival.get('ScheduledTimeLocal', {}).get('DateTime'),
                        'Terminal': arrival.get('Terminal', {}).get('Name'),
                        'AirlineID': carrier.get('AirlineID'),
                        'FlightNumber': carrier.get('FlightNumber'),
                        'AircraftCode': equipment.get('AircraftCode'),
                        'DaysOfOperation' : details.get('DaysOfOperation',),
                        'Effective' : details.get('DatePeriod',).get('Effective'),
                        'Expiration' : details.get('DatePeriod',).get('Expiration')                  
                    }, index=[0])

                    df_list.append( df[df['AirlineID'] == "LH"].reset_index(drop=True))

            counter += 1

            # Pause de 0,2 seconde toutes les 5 requetes
            if counter % 5 == 0:
                time.sleep(0.2)

                

            # Pause de 50 nimutes toutes les 1000 requetes
            #if counter % 1000 == 0:
                #time.sleep(3000)
                #FlightSchedules.to_csv(f"/content/drive/MyDrive/europe_europe{counter}.csv", index=False)

        except:
            continue

    FlightSchedules = pd.concat(df_list).drop_duplicates().reset_index(drop=True)
    FlightSchedules.insert(0, "FlightScheduleID" , FlightSchedules.index+1)
    return FlightSchedules
