import requests
from time import sleep

import pandas as pd
from sqlalchemy import create_engine

from keys import api_key

LOG_INTERVAL = 1000

db_conn = create_engine('sqlite:///sg-bus-routes.db')

url = 'http://datamall2.mytransport.sg/ltaodataservice/BusRoutes'
params = {
    '$skip': 0
}
headers = {
    'AccountKey': api_key,
    'Accept': 'application/json'
}

SKIP_INTERVAL = 50
DB_TABLE = 'bus_routes'

while True:
    data_chunk = requests.get(url=url, headers=headers, params=params).json()['value']
    if len(data_chunk) == 0:
        break
    df = pd.DataFrame(data_chunk)
    df.to_sql(name=DB_TABLE, con=db_conn, if_exists='append')
    params['$skip'] += SKIP_INTERVAL
    if params['$skip'] % LOG_INTERVAL == 0:
        print('Downloaded {}'.format(params['$skip']))
    # Avoid triggering rate limits
    # sleep(1)
