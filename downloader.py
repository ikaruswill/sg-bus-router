import requests
from time import sleep

import pandas as pd
from sqlalchemy import create_engine

from keys import api_key

# Variables
API_PATH = 'BusStops'
DB_TABLE = 'bus_stops'

# Options
DB_PATH = 'sqlite:///sg-bus-router.db'
LOG_INTERVAL = 1000

# Constants
API_URL_FORMAT = 'http://datamall2.mytransport.sg/ltaodataservice/{}'
API_SKIP_INTERVAL = 50
API_JSON_KEY = 'value'

def main():
    db_conn = create_engine(DB_PATH)
    url = API_URL_FORMAT.format(API_PATH)
    params = {
        '$skip': 0
    }
    headers = {
        'AccountKey': api_key,
        'Accept': 'application/json'
    }

    while True:
        data_chunk = requests.get(
            url=url, headers=headers, params=params).json()[API_JSON_KEY]
        if len(data_chunk) == 0:
            break
        df = pd.DataFrame(data_chunk)
        df.to_sql(name=DB_TABLE, con=db_conn, if_exists='append')

        # Log progress
        params['$skip'] += API_SKIP_INTERVAL
        if params['$skip'] % LOG_INTERVAL == 0:
            print('Downloaded {}'.format(params['$skip']))

        # # Avoid triggering rate limits
        # sleep(1)

if __name__ == '__main__':
    main()
