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
        # Distance in km equivalent to the time & effort a transfer requires
        cost += 5
    #     print('TRANSFER!')
    #     print(curr_svc, next_svc_stop.ServiceNo, cost)
    # else:
    #     print('NO TRANSFER')
    #     print(curr_svc, next_svc_stop.ServiceNo, cost)
    return cost

# TODO: Use deletion marking and object maps to speed up this O(n) process
def replace_node(code, new_node, queue):
    # Get existing node
    popped = []
    while len(queue):
        node = heapq.heappop(queue)
        if node[0] == code:
            break
        popped.append(node)

    # Return popped to queue
    for popped_node in popped:
        heapq.heappush(queue, popped_node)

    # Add new node
    heapq.heappush(queue, new_node)

def discover_next_service_stops(current_stop_services):
    next_service_stops = []
    # Discover next stop of each service
    for row in current_stop_services.itertuples():
        # Use iloc[0] as df returns series as it does not know the
        # number of rows returned
        query = df[
            (df.ServiceNo == row.ServiceNo) & \
            (df.Direction == row.Direction) & \
            (df.StopSequence == row.StopSequence + 1)]

        if len(query):
            next_service_stops.append(query.iloc[0])
    return next_service_stops

def dijkstra(df, start, end):
    routes = []
    # Try all origin bus services
    for origin_row in df[(df.BusStopCode == start)].itertuples():
        # Initialization step
        # A node is (
        #     BusStopCode,
        #     cost_from_origin,
        #     route=(bus_service_from_prev_node,
        #            prev_bus_stop_code,
        #            prev_route))
        origin = (start, 0, (None, origin_row.ServiceNo, None))
        traversal_queue = []
        cost_cache = {start: 0}
        heapq.heappush(traversal_queue, origin)

    # Dijkstra iterations
    while len(traversal_queue):
        current_node = heapq.heappop(traversal_queue)
        current_code, current_cost, _ = current_node
        stop = df[(df.BusStopCode == current_code)]

        if current_code == end:
            break

        print('Current:', current_node)

        next_service_stops = discover_next_service_stops(stop)

        for next_service_stop in next_service_stops:
            service = next_service_stop.ServiceNo
            service_stop = stop[(stop.ServiceNo == service) & (stop.Direction == next_service_stop.Direction)].iloc[0]
            next_code = next_service_stop.BusStopCode
            new_cost = calculate_cost(current_cost, current_node[2][1], service_stop, next_service_stop)

            if new_cost < 0:
                print('NEGATIVE EDGE WEIGHT!')
                print(service_stop)
                print(next_service_stop)
                exit()

            print('Next:', next_code)
            # TODO: Use visited_codes to check for lowest cost path and use
            # Not in traversal queue yet
            if not next_code in cost_cache:
                next_node = (next_code, new_cost, (service, current_node[0], current_node[2]))
                heapq.heappush(traversal_queue, next_node)
                cost_cache[next_code] = new_cost
            # Already in traversal queue
            else:
                if new_cost < cost_cache[next_code]:
                    # Replace node
                    next_node = (next_code, new_cost, (service, current_node[0], current_node[2]))
                    cost_cache[next_code] = new_cost
                    replace_node(next_code, next_node, traversal_queue)
                # elif new_cost == cost_cache[next_code]:
                #     # Add node into queue
                #     next_node = (next_code, new_cost, (service, current_node[0], current_node[2]))
                #     heapq.heappush(traversal_queue, next_node)

            # visited_codes.add(current_code)

        routes.append(current_node)
    # Return best route
    min_dist = math.inf
    min_route = None
    for route in routes:
        if route[1] < min_dist:
            min_dist = route[1]
            min_route = route

    return min_route

start = '19059'
end = '18129'

print(dijkstra(df, start, end))
