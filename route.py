from collections import deque

import pandas as pd
from sqlalchemy import create_engine

start = '19051'
end = '18129'
MAX_TRANSFER = 2

db_conn = create_engine('sqlite:///sg-bus-routes.db')
df = pd.read_sql_table(table_name='bus_routes', con=db_conn)

solution_routes = []
traversal_queue = deque()

# Initialization step
current_bus_stop = start
initial_services = df[(df.BusStopCode == start)]
transfer_cache = set(initial_services.ServiceNo)
for service in initial_services:
    service_no = service.ServiceNo
    solution_route = [
        {
            'service': service_no,
            'start': current_bus_stop,
            'end': None,
            'route': df[(df.ServiceNo == service_no) & (df.Direction == service.Direction)]
        }
    ]
    traversal_queue.append(solution_route)
