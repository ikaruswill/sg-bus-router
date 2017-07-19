import math
import heapq
from functools import total_ordering

import pandas as pd
from sqlalchemy import create_engine


start = '19051'
end = '18129'

db_conn = create_engine('sqlite:///sg-bus-routes.db')
df = pd.read_sql_table(table_name='bus_routes', con=db_conn)

# def discover_edges(node, visited, unvisited):
#     bus_route = df[(df.ServiceNo == node.ServiceNo) & (df.Direction == node.Direction) & (df.StopSequence > df.StopSequence)]
#     for point in bus_route:
#
#         bus_services = df[df.BusStopCode == point.BusStopCode]
#         for

@total_ordering
class Node:
    def __lt__(self, node):
        if self.distance < node.distance:
            return True
    def __init__(self, bus_stop_code, distance=math.inf):
        self.bus_stop_code = bus_stop_code
        self.distance = distance

def dijkstra(df, start):
    # Initialization step
    visited = set()
    unvisited = heapq([Node(start, 0)])
    unvisited_cache = set(unvisited)
    while True:
        if not len(unvisited):
            break
        current_node = unvisited.popleft()
        edges = df[(df.BusStopCode == start)]
        for edge in edges:
            next_bus_stop = df[(df.ServiceNo == edge.ServiceNo) & \
                               (df.Direction == edge.Direction) & \
                               (df.StopSequence == edge.StopSequence + 1)]
            if not next_bus_stop.BusStopCode in unvisited_cache:
                current_bus_stop = df[df.BusStopCode == current_node.bus_stop_code]
                next_node = Node(
                    next_bus_stop.BusStopCode,
                    next_bus_stop.Distance - current_bus_stop.Distance)
                unvisited.append(next_node)
                unvisited_cache.add(next_bus_stop)





for service in initial_services:
    discover_edges(service)

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





# Traversal needs:
# Route
# StopSequence
# Traversal Queue
# Service Cache
#
#

def generate_new_solution_routes(service, traversal_queue):


def traverse(route, stop_sequence, world, traversal_queue, service_cache):
    bus_stop = route[route.StopSequence == stop_sequence].BusStopCode
    services_available = world[world.BusStopCode == bus_stop]
    for service in services_available:
        service_no = service.ServiceNo
        if service_no not in service_cache:
            generate_new_solution_routes(service, traversal_queue)


for service in initial_services:
    service_no = service.ServiceNo
    next_bus_stop = df[(df.serviceNo == service_no) & (df.Direction == service.Direction) & (df.StopSequence == service.StopSequence + 1)]



def depth_first_search(traversal_queue):


def depth_first_search(start, service_no):
    transfer_cache = set([service_no])
    direction = int(df[(df.BusStopCode == start) & (df.ServiceNo == service_no)].Direction)
    route = df[(df.ServiceNo == service_no) & (df.Direction == direction)]
    for stop in route.itertuples():
        buses_available = df[df.BusStopCode == stop.BusStopCode]
        for bus in buses_available:
            if bus.ServiceNo not in transfer_cache:




for pending_route in routes_available.itertuples():
    if not exists_in_cache(pending_route.ServiceNo):
        route = deque()
