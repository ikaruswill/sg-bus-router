import requests
import json
from keys import api_key


url = 'http://datamall2.mytransport.sg/ltaodataservice/BusRoutes'
headers = {
    'AccountKey': api_key,
    'Accept': 'application/json'
}

json_str = requests.get(url=url, headers=headers).json()
data_chunk = json.loads(json_str)['value']
