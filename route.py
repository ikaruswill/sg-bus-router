import math
import heapq
from functools import total_ordering

import pandas as pd
from sqlalchemy import create_engine

db_conn = create_engine('sqlite:///sg-bus-routes.db')
df = pd.read_sql_table(table_name='bus_routes', con=db_conn)

def calculate_cost(curr_cost, curr_svc, curr_svc_stop, next_svc_stop):
    cost = next_svc_stop.Distance - curr_svc_stop.Distance + curr_cost
    if next_svc_stop.ServiceNo != curr_svc:
        cost += 5
    #     print('TRANSFER!')
    #     print(curr_svc, next_svc_stop.ServiceNo, cost)
    # else:
    #     print('NO TRANSFER')
    #     print(curr_svc, next_svc_stop.ServiceNo, cost)
    return cost

def dijkstra(df, start, end):
    routes = []
    # Try all origin bus services
    for origin_row in df[(df.BusStopCode == start)].itertuples():
        # Initialization step
        origin = (start, 0, (None, origin_row.ServiceNo, None))
        visited_codes = set()
        traversal_queue = []
        cost_cache = {start: 0}
        heapq.heappush(traversal_queue, origin)

        # Dijkstra iterations
        while len(traversal_queue):
            node = heapq.heappop(traversal_queue)
            current_code, current_cost, _ = node
            stop = df[(df.BusStopCode == current_code)]

            if current_code == end:
                break

            print('Current:', node)

            next_service_stops = []
            # Discover next stop of each service
            for row in stop.itertuples():
                # Use iloc[0] as df returns series as it does not know the
                # number of rows returned
                query = df[
                    (df.ServiceNo == row.ServiceNo) & \
                    (df.Direction == row.Direction) & \
                    (df.StopSequence == row.StopSequence + 1)]

                if len(query):
                    next_service_stops.append(query.iloc[0])

            for next_service_stop in next_service_stops:
                service = next_service_stop.ServiceNo
                service_stop = stop[(stop.ServiceNo == service)].iloc[0]
                next_code = next_service_stop.BusStopCode
                new_cost = calculate_cost(current_cost, node[2][1], service_stop, next_service_stop)

                if new_cost < 0:
                    print('NEGATIVE EDGE WEIGHT!')
                    print(service_stop)
                    print(next_service_stop)
                    exit()

                print('Next:', next_code)

                # Not in traversal queue yet
                if not next_code in cost_cache:
                    next_node = (next_code, new_cost, (service, node[0], node[2]))
                    heapq.heappush(traversal_queue, next_node)
                    cost_cache[next_code] = new_cost
                # Already in traversal queue
                else:
                    if new_cost < cost_cache[next_code]:
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
                        next_node = (next_code, new_cost, (service, node[0], node[2]))
                        heapq.heappush(traversal_queue, next_node)
                        cost_cache[next_code] = new_cost
            visited_codes.add(current_code)

        routes.append(node)
    # Return best route
    min_dist = math.inf
    min_route = None
    for route in routes:
        if route[1] < min_dist:
            min_dist = route[1]
            min_route = route

    return min_route

start = '59039'
end = '54589'

print(dijkstra(df, start, end))
