from collections import defaultdict
import pickle
from pprint import pprint

import pandas as pd
from sqlalchemy import create_engine

print('Loading tables')
db_conn = create_engine('sqlite:///sg-bus-router.db')
rt = pd.read_sql_table(table_name='bus_routes', con=db_conn)
bs = pd.read_sql_table(table_name='bus_stops', con=db_conn)

print('Setting Bus Stop index')
bs.set_index('BusStopCode', inplace=True)

print('Generating Bus Routes index dict')
rt_idx_dict = rt.to_dict(orient='index')
rt_bus_stop_dict = defaultdict(dict)

print('Populating Bus Routes stops dict')
for bus_stop_code, _ in bs.iterrows():
    rt_bus_stop_dict[bus_stop_code] = rt[(rt.BusStopCode == bus_stop_code)].to_dict(orient='index')

print('Generating Bus Stops dict')
bs_bus_stop_dict = bs.to_dict(orient='index')

print('Pickling dicts')
with open('rt_idx.pkl', 'wb') as f:
    pickle.dump(rt_idx_dict, f)

with open('rt_bs.pkl', 'wb') as f:
    pickle.dump(rt_bus_stop_dict, f)

with open('bs.pkl', 'wb') as f:
    pickle.dump(bs_bus_stop_dict, f)
