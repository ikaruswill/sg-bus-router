from argparse import ArgumentParser
from collections import defaultdict
from functools import total_ordering
import heapq
from math import radians, cos, sin, asin, sqrt, inf
from pprint import pprint

import pandas as pd
from sqlalchemy import create_engine

# Features
# STASHED: Use Python logging module
# TODO: Allow multiple destination nodes
# TODO: Allow multiple source nodes
# TODO: Allow for multiple route suggestions without re-running algorithm
# TODO: Add custom exceptions instead of exit()

# Optimizations
# TODO: Save bus-stops table with BusStopCode as index to remove set_index() step
# TODO: Create new table from bus-routes grouped by BusStopCode to speed up node discovery

db_conn = create_engine('sqlite:///sg-bus-router.db')
rt = pd.read_sql_table(table_name='bus_routes', con=db_conn)
bs = pd.read_sql_table(table_name='bus_stops', con=db_conn)

bs.set_index('BusStopCode', inplace=True)

TRANSFER_PENALTY = 5

@total_ordering
class Node:
    goal_stop = None
    heuristics = {}

    def __init__(self, bus_stop_code, best_cost=inf, best_dist=inf, last_transfer_index=-1):
        self.bus_stop_code = bus_stop_code
        self.bus_stop = bs.loc[self.bus_stop_code]
        self.h_dist = self.calculate_heuristic()
        self.best_dist = best_dist
        self.best_cost = best_cost
        self.best_metric = self.best_cost + self.h_dist
        self.best_route = []
        self.last_transfer_index = last_transfer_index
        self.services = rt[(rt.BusStopCode == self.bus_stop_code)]

    def haversine(self, lon1, lat1, lon2, lat2):
        """
        Calculate the great circle distance between two points
        on the earth (specified in decimal degrees)
        """
        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        # haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        km = 6367 * c
        return km

    def calculate_heuristic(self):
        if self.bus_stop_code in Node.heuristics:
            return Node.heuristics[self.bus_stop_code]
        else:
            heuristic = self.haversine(
                self.bus_stop.Longitude, self.bus_stop.Latitude,
                self.goal_stop.Longitude, self.goal_stop.Latitude)
            Node.heuristics[self.bus_stop_code] = heuristic
        return heuristic

    def __lt__(self, other):
        return self.best_metric < other.best_metric

    def __hash__(self):
        return hash('{} {}'.format(self.bus_stop_code, self.service.ServiceNo))

    def __repr__(self):
        return '{}: {:>6.1f} | {:>6.1f} | {:>6.1f}km'.format(
            self.bus_stop_code, self.best_metric,
            self.h_dist, self.best_dist)


class Edge:
    def __init__(self, source, services, dest, distance):
        self.source = source
        self.dest = dest
        self.services = services
        self.distance = distance
        self.has_transferred = False
        self.cost = self.calculate_cost()
        self.update_dest_distance_cost_route()

    def calculate_cost(self):
        cost = self.distance
        if self.source.best_route:
            # If current edge services are disjoint with the best route,
            # a transfer has occurred
            if not self.services.intersection(
                self.source.best_route[self.source.last_transfer_index].services):
                # Distance in km equivalent to the time & effort a transfer requires
                cost += TRANSFER_PENALTY
                self.has_transferred = True

        if cost < 0:
            print('ERROR: Negative edge cost')
            self.cost = cost
            exit(repr(self))

        return cost

    def update_dest_distance_cost_route(self):
        new_dist = self.source.best_dist + self.distance
        # stops_per_km = (len(self.source.best_route) + 1)/new_dist
        # new_cost = self.source.best_cost + self.cost + stops_per_km + self.dest.h_dist
        new_cost = self.source.best_cost + self.cost
        new_metric = new_cost + self.dest.h_dist
        if new_metric < self.dest.best_metric:
            self.dest.best_cost = new_cost
            self.dest.best_dist = new_dist
            self.dest.best_metric = new_metric
            self.dest.best_route = self.source.best_route + [self]
            if self.has_transferred:
                self.dest.last_transfer_index = len(self.dest.best_route) - 1
            else:
                self.dest.last_transfer_index = self.source.last_transfer_index

    def __repr__(self):
        return '< {} --> {} >: {:>4.1f} | {:>4.1f}km | {}'.format(
            self.source.bus_stop_code, self.dest.bus_stop_code, self.cost,
            self.distance, self.services)


def discover_next_stops(node):
    next_stops = defaultdict(lambda: defaultdict(set))
    # Discover next stop of each service
    for idx, current_service_stop in node.services.iterrows():
        try:
            next_service_stop = rt.loc[idx + 1]
        except KeyError:
            print('INFO: Reached end of dataframe')
            continue
        if next_service_stop.StopSequence == current_service_stop.StopSequence + 1:
            next_stops[next_service_stop.BusStopCode]['services'].add(next_service_stop.ServiceNo)
            next_stops[next_service_stop.BusStopCode]['distance'] = next_service_stop.Distance - current_service_stop.Distance
    return next_stops

def postprocess_route(route):
    # Forward intersect
    reference_services = route[0].services
    for edge in route:
        if edge.has_transferred:
            reference_services = edge.services
            continue
        edge.services = edge.services.intersection(reference_services)

    # Reverse intersect
    route = route[::-1]
    reference_services = route[0].services
    for edge in route:
        if reference_services is None:
            reference_services = edge.services
            continue
        edge.services = edge.services.intersection(reference_services)
        if edge.has_transferred:
            reference_services = None

def dijkstra(origin_code, goal_code):
    traversal_queue = []
    nodes = {}
    optimal_nodes = set()

    # Initialize goal stop
    Node.goal_stop = bs.loc[str(goal_code)]

    # Initialize origin node
    origin = Node(origin_code, 0, 0, 0)
    traversal_queue.append(origin)

    # Dijkstra iterations
    while traversal_queue:
        current_node = heapq.heappop(traversal_queue)
        nodes[current_node.bus_stop_code] = current_node
        optimal_nodes.add(current_node.bus_stop_code)

        print(current_node)

        next_service_stops = discover_next_stops(current_node)

        # Create next nodes
        for next_bus_stop_code, next_bus_stop_info in next_service_stops.items():
            # Already optimal, ignore
            if next_bus_stop_code in optimal_nodes:
                continue

            # Check if node already exists
            if next_bus_stop_code in nodes:
                next_node = nodes[next_bus_stop_code]
            else:
                next_node = Node(next_bus_stop_code)
                nodes[next_bus_stop_code] = next_node
                traversal_queue.append(next_node)

            # print('++', next_node)

            # Create edge and relax
            edge = Edge(current_node, next_bus_stop_info['services'],
                        next_node, next_bus_stop_info['distance'])

            # print(' -', edge)

        # FIXME: Final stop will take the first bus service encountered that reaches destination
        # Store optimal route found for bus stop (Service agnostic)
        if current_node.bus_stop_code == goal_code:
            break

        # TODO: Could do with a little optimization, currently O(hn)
        # h = len(heapq), n = len(rt)
        # Maintain heap property in event node best cost has changed
        heapq.heapify(traversal_queue)


    postprocess_route(current_node.best_route)
    return current_node

def main():
    global TRANSFER_PENALTY

    # No transfers      : 19051 -> 18111
    # Single transfer   : 19051 -> 18129
    # Equally optimal   : 59039 -> 54589
    # Loops             : 11389 -> 11381
    # LS                : 19051 -> 03381
    # Tim               : 18129 -> 10199
    # Skipped stops     : 59119 -> 63091

    DEBUG_ORIGIN = '59039'
    DEBUG_GOAL = '54589'

    # Argument handling
    parser = ArgumentParser(
        description='Finds the shortest bus route between a source and a '
        'destination bus stop.')
    parser.add_argument(
        '-t', '--transfer-penalty', default=TRANSFER_PENALTY,
        help="distance in km equivalent to the time & effort a transfer requires")
    parser.add_argument('-o', '--origin', help="origin bus stop code",
                        default=DEBUG_ORIGIN)
    parser.add_argument(
        '-g', '--goal', help="destination bus stop code", default=DEBUG_GOAL)

    args = parser.parse_args()
    TRANSFER_PENALTY = float(args.transfer_penalty)
    origin = args.origin
    goal = args.goal

    # Fallback prompts
    if not origin:
        origin = input('Source bus stop code: ')
    if not goal:
        goal = input('Destination bus-stop codes: ')

    # Run algorithm
    solution = dijkstra(origin, goal)
    print('Solution')
    pprint(solution.best_route)

if __name__ == '__main__':
    main()
