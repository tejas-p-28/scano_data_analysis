import requests
import pandas as pd
from geopy.geocoders import GoogleV3
from pandas import json_normalize
import os
from dotenv import load_dotenv
global api_key
load_dotenv()
api_key = os.getenv('api_key')

# fetching data from the api hosted
def fetch_data(url, auth_token):
    headers = {
          'Authorization': f'Bearer {auth_token}',
          'Content-Type': 'application/json'
    }
    try:
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data
    except Exception as e:
        print(f"Error fetching data from API: {e}")
        return None

def preprocess_data(data):
    # print(data)
    df = pd.DataFrame(data)
    # print(df.head())
    df['city'] = df['address'].apply(get_city_from_address)
    df['city'].fillna(df['address'], inplace=True)
    # print(df.isna().sum())
    # print(df.head())
    df['state'] = df['city'].apply(get_state_from_city)
    df.drop(columns=['address','location'], inplace=True)
    month_wise = df.explode('monthWiseCounts').reset_index(drop=True)
    month_wise_data = pd.concat([month_wise.drop(['monthWiseCounts'], axis=1), month_wise['monthWiseCounts'].apply(pd.Series)], axis=1)
    df = pd.DataFrame(month_wise_data)
    print(df.columns)
    print(df.head())

# get city with respect to address 
def get_city_from_address(address):
    # api_key = 'AIzaSyAjT_SMBAWzPJgU3rskSl8kXGE1QE0hGY8'
    geolocator = GoogleV3(api_key=api_key)
    location = geolocator.geocode(address)
    if location:
        address_components = location.raw.get('address_components', [])
        for component in address_components:
            if 'locality' in component['types']:
                return component['long_name']
        return location.address
    else:
        return None    

# get state with respect to city
def get_state_from_city(city):
    geolocator = GoogleV3(api_key=api_key)
    location = geolocator.geocode(city)
    if location:
        address_components = location.raw.get('address_components', [])
        for component in address_components:
            if 'administrative_area_level_1' in component['types']:
                return component['long_name']
        return location.address
    return None
if __name__ == '__main__':

    url = 'http://192.168.57.148:3100/v1/analytics/get-month-wise-analytics?startDate=31/08/2024&endDate=30/12/2024'
    auth_token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6IiIsImlkIjoiNjU5MDA4ODljMGIxMzhlYTY0MGJjZDE1IiwiaWF0IjoxNzM1OTc5ODI5fQ.JnYxf5H02A8ggWvnM3oviThhRvt0u1B9D4YTvytNg2A'
    data = fetch_data(url, auth_token)
    if data :

        preprocessed_data = preprocess_data(data)
        if preprocessed_data:
            print(preprocessed_data)
        else:
            print('preprocessed data not avialable')
    else:
        print('data not received')

