#!/usr/bin/env python
# coding: utf-8

# In[2]:


# importing packages
import pandas as pd
import requests
from time import sleep



#
def get_country (Access_Token) :
    
    """This function takes as parameter the API access token provided by lufthansa "LH OpenAPI testing" and returns a DataFrame of the country. """
    
    # 
    authentification = {}
    authentification["Authorization"] = "Bearer " + Access_Token

    #
    country_endpoints = [] 
    response_list = []
    CountryCode = []
    CountryName = []
    
    
    #   
    response = requests.get("https://api.lufthansa.com/v1/mds-references/countries/?lang=AN&limit=100&offset=0", headers = authentification).json()
    for i in range(len(response['CountryResource']['Meta']['Link'])-1):  
        country_endpoints.append(response['CountryResource']['Meta']['Link'][i]['@Href'])

    #
    for endpoint in country_endpoints: 
        response_list.append(requests.get(endpoint , headers = authentification).json()['CountryResource']['Countries']['Country'])

    for response in response_list:
        for i in response:
            CountryCode.append(i['CountryCode'])
            CountryName.append(i['Names']['Name']['$'])
    
    #
    Countries = pd.DataFrame({
        "CountryCode" : CountryCode,
        "CountryName" : CountryName
    })

    return Countries



#
def get_cities(Access_Token)  :
    
    #
    authentification = {}
    authentification["Authorization"] = "Bearer " + Access_Token

    #
    df_list = []
    offset_list = list(range(0,10700,100))

    #
    for i in offset_list:
        sleep(0.1)
        response = requests.get(("https://api.lufthansa.com/v1/mds-references/cities/?lang=AN&limit=100&offset={offset}").format(offset = i), headers = authentification).json()
        pd.DataFrame(response['CityResource']['Cities']['City'])
        df_list.append(pd.DataFrame(response['CityResource']['Cities']['City']))

    #
    for df in df_list :
        for i in range(len(df)):
            city_name = df.loc[i, "Names"]["Name"]["$"]
            df.loc[i, "Names"] = city_name
        #    
        for i in range(len(df)):
            if isinstance(df.loc[i, "Airports"], dict) == True:       
                airport_code = df.loc[i, "Airports"]["AirportCode"]
                df.loc[i, "Airports"] = airport_code

    Cities = pd.concat(df_list) 
    Cities = Cities.explode("Airports")
    Cities = Cities.reset_index(drop=True)
    
    return Cities



#
def get_airports (Access_Token)  :
    
    #
    authentification = {}
    authentification["Authorization"] = "Bearer " + Access_Token
    
    #
    df_list = []
    offset_list = list(range(0,1500,100))
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
