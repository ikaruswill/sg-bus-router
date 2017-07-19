import math
import heapq
from functools import total_ordering

import pandas as pd
from sqlalchemy import create_engine

db_conn = create_engine('sqlite:///sg-bus-routes.db')
df = pd.read_sql_table(table_name='bus_routes', con=db_conn)

def dijkstra(df, start, end):
    # Initialization step
    visited_codes = set()
    origin = (start, 0, ())
    traversal_queue = []
    distance_cache = {start: 0}
    heapq.heappush(traversal_queue, origin)

    # Dijkstra iterations
    while len(traversal_queue):
        node = heapq.heappop(traversal_queue)
        code, distance, path = node
        stop = df[(df.BusStopCode == code)]

        if code == end:
            return node

        print('Current:', node)

        next_service_stops = []
        # Discover next stop of each service
        for row in stop.itertuples():
            # Use iloc[0] as df returns series as it does not know the
            # number of rows returned
            next_service_stops.append(df[
                (df.ServiceNo == row.ServiceNo) & \
                (df.Direction == row.Direction) & \
                (df.StopSequence == row.StopSequence + 1)].iloc[0])

        for next_service_stop in next_service_stops:
            service = next_service_stop.ServiceNo
            stop_distance = stop[(stop.ServiceNo == service)].iloc[0].Distance
            next_code = next_service_stop.BusStopCode
            next_stop_distance = next_service_stop.Distance - stop_distance
            new_distance = next_stop_distance + distance

            print('Next:', next_code)

            # Not in traversal queue yet
            if not next_code in distance_cache:
                print('Not in queue')
                next_node = (next_code, new_distance, (node[0], service, node[2]))
                heapq.heappush(traversal_queue, next_node)
                distance_cache[next_code] = new_distance
            # Already in traversal queue
            else:
                print('Already in queue')
                if new_distance < distance_cache[next_code]:
                    # Get existing node
                    popped = []
                    while len(traversal_queue):
                        next_node = heapq.heappop(traversal_queue)
                        if next_node[0] == next_code:
                            break
                        popped.append(next_node)

                    # Return popped to queue
                    for popped_node in popped:
                        heapq.heappush(traversal_queue, popped_node)

                    # Replace node
                    next_node = (next_code, new_distance, (node[0], service, node[2]))
                    heapq.heappush(traversal_queue, next_node)
                    distance_cache[next_code] = new_distance
        visited_codes.add(code)
    return False

start = '19051'
end = '18129'

print(dijkstra(df, start, end))
