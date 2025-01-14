import requests
import pandas as pd
api_url = "http://182.168.1.210:3100/v1/analytics/get-month-wise-analytics?startDate=01/06/2024&endDate=30/06/2024"
api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6IiIsImlkIjoiNjU5MDA4ODljMGIxMzhlYTY0MGJjZDE1IiwiaWF0IjoxNzM2NTA5NDM0fQ.rUVa4DQEPoznXph72mI4jDAnWVTS3pFmkbsrdibBbjM"

headers = {
    "Authorization": f"Bearer {api_key}"
}

response = requests.get(api_url, headers=headers)
print(response.json())
response = pd.DataFrame(response)
response.to_csv('test.csv')
