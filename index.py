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
def fetch_data_df(url, auth_token):
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

#fetch disease data
def fetch_data_disease(url, auth_token):
    headers = {
        'Authorization': f'Bearer {auth_token}',
        'Content-Type' : 'Application/json'
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        disease=response.json()
        return disease
    except Exception as e:
        print(f'Error fetching data from API : {e}')
        return None

#fetch city data
def fetch_data_city(url, auth_token):
    headers = {
        'Authorization': f'Bearer {auth_token}',
        'Content-Type' : 'Application/json'
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        city=response.json()
        return city
    except Exception as e:
        print(f'Error fetching data from API : {e}')
        return None


#fethc month wise disease
# def fetch_data_month(url, auth_token):
#     headers = {
#         'Authorization': f'Bearer {auth_token}',
#         'Content-Type' : 'Application/json'
#     }
#     try:
#         response = requests.get(url, headers=headers)
#         response.raise_for_status()
#         month=response.json()
#         return month
#     except Exception as e:
#         print(f'Error fetching data from API : {e}')
#         return None

# def preprocess_data(data):
#     # print(data)
#     total_counts_disease(data)

#df preprocessing to calc total counts of disease or 
def total_counts_disease(data):
    df = pd.DataFrame(data)
    print(df.shape)
    print(df.head())
    df['city'] = df['address'].apply(get_city_from_address)
    df['city'].fillna(df['address'], inplace=True)
    # print(df.isna().sum())
    # print(df.head())
    df['state'] = df['city'].apply(get_state_from_city)
    df.drop(columns=['address','location'], inplace=True)
    month_wise = df.explode('monthWiseCounts').reset_index(drop=True)
    month_wise_data = pd.concat([month_wise.drop(['monthWiseCounts'], axis=1), month_wise['monthWiseCounts'].apply(pd.Series)], axis=1)
    df = pd.DataFrame(month_wise_data)
    disease_wise = df.explode('diseaseWiseMonthCount').reset_index(drop=True)
    disease_wise_data = pd.concat([disease_wise.drop(['diseaseWiseMonthCount'], axis=1), month_wise['monthWiseCounts'].apply(pd.Series)], axis=1)
    df = pd.DataFrame(disease_wise_data)
    df.columns = rename_duplicate_month(df.columns)
    diseaseWiseCount = df['monthWisecount'].rename('diseaseWiseCount')
    diseaseWiseCount = pd.DataFrame(diseaseWiseCount)
    df = pd.concat([df, diseaseWiseCount], axis=1)
    df = df.drop(columns=["monthWisecount"])

    diseaseWiseMonth = df['monthWisemonth'].rename('diseaseWiseMonth')
    diseaseWiseMonths = pd.DataFrame(diseaseWiseMonth)
    df = pd.concat([df, diseaseWiseMonths], axis=1)
    df = df.drop(columns=["monthWisemonth"])
    df = df[['city', 'state', 'month', 'count', 'diseaseWiseCount', 'diseaseWiseMonth']]
    df['diseaseWiseCount'].fillna(0, inplace=True)
    df['diseaseWiseMonth'].fillna(0, inplace=True)
    df['state'] = df['state'].apply(check_null_state)
    df.drop_duplicates(inplace= True)
    df.to_csv('data.csv', index = False)

#disease wise counts
def disease_wise_counts(disease):
    disease = pd.DataFrame(disease)
    print(disease.shape)
    disease = disease[['location', 'address','diseaseWiseMonthCount']]
    disease['city'] = disease['address'].apply(get_city_from_address)
    disease['city'].fillna(disease['address'], inplace = True)
    disease.drop(columns=['address'], inplace=True)
    disease['state'] = disease['city'].apply(get_state_from_city)
    disease['state'] = disease['state'].apply(check_null_state)
    # month_list = []
    # disease_list = []
    # counts = []
    for item in disease['diseaseWiseMonthCount']:
        for sub_item in item:
            month_list = sub_item.get('month')
            diseaseWiseCount = sub_item.get('diseaseWiseCount', {})
            diseaseWiseCount.keys()
            diseaseWiseCount.values()
    df_keys = pd.DataFrame({'month': month_list, 'disease_list': diseaseWiseCount.keys(), 'disease_counts': diseaseWiseCount.values()})
    df_keys.reset_index(inplace=True, drop=True)
    disease.reset_index(inplace=True, drop=True)
    disease = pd.concat([disease, df_keys], axis=1)  
    disease.drop(columns = ['diseaseWiseMonthCount','location'],inplace=True)
    disease = disease.iloc[:, ~disease.columns.duplicated()]


    disease.to_csv('disease_data.csv', index = False)

#city wise disease
def city_wise_disease(city):
    city = pd.DataFrame(city)
    city['city'] = city['address'].apply(get_city_from_address)
    city['city'].fillna(city['address'], inplace=True)
    city['state'] = city['city'].apply(get_state_from_city)
    city.drop(columns = ['address','location'], inplace=True)
    city = city[['diseaseWiseMonthCount','city','state']]
    city['diseaseKeys'] = city['diseaseWiseMonthCount'].apply(extract_disease_keys)
    city.dropna(inplace=True)

    extracted_values = []
    for index, row in city.iterrows():
        try:
            for value in row['diseaseKeys']:
                extracted_values.append({
                    'city': row['city'],
                    'state': row['state'],
                    'diseaseKey': value
                })
        except (TypeError, AttributeError):
            pass
    extracted_df = pd.DataFrame(extracted_values)
    city = pd.merge(city, extracted_df, on=['city', 'state'], how='left')
    city.drop(columns = ['diseaseKeys'], inplace = True)
    city = city[['city', 'state', 'diseaseKey']]
    city.dropna(inplace=True)
    city.drop_duplicates(inplace=True)
    city.to_csv('city_disease.csv', index = False)

# #month wise disease
# def month_wise_disease(month):
#     month = pd.DataFrame(month)
#     month = month[['diseaseWiseMonthCount','address','location']]
#     month['city'] = month['address'].apply(get_city_from_address)
#     month['city'].fillna(month['address'], inplace=True)
#     month['state'] = month['city'].apply(get_state_from_city)
#     month = month[['diseaseWiseMonthCount','city','state']]
#     month_wise = month.explode('diseaseWiseMonthCount').reset_index(drop=True)
#     month_wise_data = pd.concat([ month_wise['diseaseWiseMonthCount'].apply(pd.Series)], axis=1)
#     month = pd.DataFrame(month_wise_data)
#     month['diseaseKeys'] = month['diseaseWiseCount'].apply(extract_disease_keys)
#     extracted_values = []
#     for index, row in month.iterrows():
#         try:
#             for value in row['diseaseWiseCount']:
#                 extracted_values.append({
#                     'city': row['city'],
#                     'state': row['state'],
#                     'diseaseKey': value
#                 })
#         except (TypeError, AttributeError):
#             pass
#     extracted_df = pd.DataFrame(extracted_values)
#     month_data = pd.merge(month, extracted_df, on=['city', 'state'], how='left')
#     month_data.drop(columns = ['diseaseKeys'], inplace = True)
#     month = pd.DataFrame(month_data)
#     month = month[['city', 'state', 'month','diseaseKey']]
#     month.drop_duplicates(inplace=True)
#     month.dropna(inplace=True)
#     month.to_csv('month_disease.csv', index = False)
#disease key extract
def extract_disease_keys(diseaseWiseMonthCount):
    keys = set()
    for entry in diseaseWiseMonthCount:
        if 'diseaseWiseCount' in entry:
            keys.update(entry['diseaseWiseCount'].keys())
    return list(keys) if keys else None



# get city with respect to address 
def get_city_from_address(address):
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

#rename of duplicate columns found
def rename_duplicate_month(column):
    counts = {}
    new_column = []
    for col in column:
        if col in counts:
            counts[col] += 1
            new_column.append(f"monthWise{col}")
        else:
            counts[col] = 0
            new_column.append(col)
    return new_column

#check if theirs null value in the state column
def check_null_state(state):
  if state == None:
    return 'Unknown'
  else:
    return state

if __name__ == '__main__':

    url = 'http://192.168.57.148:3100/v1/analytics/get-month-wise-analytics?startDate=01/01/2024&endDate=30/7/2024'
    auth_token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6IiIsImlkIjoiNjU5MDA4ODljMGIxMzhlYTY0MGJjZDE1IiwiaWF0IjoxNzM1OTc5ODI5fQ.JnYxf5H02A8ggWvnM3oviThhRvt0u1B9D4YTvytNg2A'
    data = fetch_data_df(url, auth_token)
    disease = fetch_data_disease(url,auth_token)
    city = fetch_data_city(url,auth_token)
    # month = fetch_data_month(url,auth_token)
    if data and city and disease:
        total_counts = total_counts_disease(data)
        disease_data = disease_wise_counts(disease)
        city_data = city_wise_disease(city)
        # month_data = month_wise_disease(month)
        # if preprocessed_data:
        #     print(preprocessed_data)
        # else:
        #     print('preprocessed data not available')
    else:
        print('data not available')