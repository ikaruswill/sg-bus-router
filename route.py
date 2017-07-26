from argparse import ArgumentParser
from functools import total_ordering
import heapq
import math
from pprint import pprint

import pandas as pd
from sqlalchemy import create_engine

# TODO: Allow multiple solutions per destination. Currently takes the first optimal service stop as solution and ignores equally good routes
# TODO: Allow multiple source nodes
# TODO: Heuristic function using GPS distance to prune strayed subtrees
# TODO: Allow for multiple route suggestions without re-running algorithm
# TODO: Add custom exceptions instead of exit()

db_conn = create_engine('sqlite:///sg-bus-router.db')
df = pd.read_sql_table(table_name='bus_routes', con=db_conn)

TRANSFER_PENALTY = 1

@total_ordering
class Node:
    def __init__(self, bus_stop_code, service, best_cost=math.inf,
                 best_dist=math.inf, best_route=[]):
        self.bus_stop_code = bus_stop_code
        self.best_dist = best_dist
        self.best_cost = best_cost
        self.best_route = best_route
        self.service = service # Service stop
        self.services = df[(df.BusStopCode == self.bus_stop_code)]

    def __lt__(self, other):
        return self.best_cost < other.best_cost

    def __hash__(self):
        return hash('{} {}'.format(self.bus_stop_code, self.service))

    def __repr__(self):
        return '{} ({}): {:.1f} | {:.1f}km'.format(
            self.bus_stop_code, self.service.ServiceNo, self.best_cost,
            self.best_dist)


class Edge:
    def __init__(self, source, service, dest):
        self.source = source
        self.service = service
        self.dest = dest
        self.distance = self.calculate_dist()
        self.cost = self.calculate_cost()
        self.update_dest_distance_cost_route()

    def calculate_dist(self):
        prev_service_stop = self.source.services[
            (self.source.services.ServiceNo == self.service.ServiceNo) & \
            (self.source.services.Direction == self.service.Direction) & \
            (self.source.services.StopSequence == self.service.StopSequence - 1)].iloc[0]
        return self.service.Distance - prev_service_stop.Distance

    def calculate_cost(self):
        cost = self.distance
        if self.service.ServiceNo != self.source.service.ServiceNo:
            # Distance in km equivalent to the time & effort a transfer requires
            cost += TRANSFER_PENALTY

        if cost < 0:
            print('NEGATIVE EDGE')
            self.cost = cost
            exit(repr(self))

        return cost

    def update_dest_distance_cost_route(self):
        new_dist = self.source.best_dist + self.distance
        stops_per_km = new_dist/(len(self.source.best_route) + 1)
        new_cost = self.source.best_cost + self.cost + stops_per_km
        if new_cost < self.dest.best_cost:
            self.dest.best_cost = new_cost
            self.dest.best_dist = new_dist
            self.dest.service = self.service
            self.dest.best_route = self.source.best_route + [self]

    def __repr__(self):
        return '< {} ({}) --> {} ({}): {:.1f} | {:.1f}km >'.format(
            self.source.bus_stop_code, self.source.service.ServiceNo,
            self.dest.bus_stop_code, self.dest.service.ServiceNo, self.cost,
            self.distance)


def discover_next_service_stops(node):
    next_service_stops = []
    # Discover next stop of each service
    for row in node.services.itertuples():
        # Use iloc[0] as df returns series as it does not know the
        # number of rows returned
        query = df[
            (df.ServiceNo == row.ServiceNo) & \
            (df.Direction == row.Direction) & \
            (df.StopSequence == row.StopSequence + 1)]

        if len(query):
            next_service_stops.append(query.iloc[0])
    return next_service_stops

def dijkstra(source, dests):
    traversal_queue = []
    nodes = {}
    optimal_nodes = set()
    dests = set(dests)
    soln_nodes = []

    origin_services = df[(df.BusStopCode == source)]
    for origin_service in origin_services.itertuples():
        origin = Node(source, origin_service, 0, 0)
        traversal_queue.append(origin)

    # Dijkstra iterations
    while len(dests):
        current_node = heapq.heappop(traversal_queue)
        nodes[(current_node.bus_stop_code, current_node.service.ServiceNo)] = current_node
        optimal_nodes.add(current_node.bus_stop_code)

        print(current_node)

        # Store optimal route found for bus stop (Service agnostic)
        if current_node.bus_stop_code in dests:
            dests.remove(current_node.bus_stop_code)
            soln_nodes.append(current_node)

        next_service_stops = discover_next_service_stops(current_node)

        # Create next nodes
        for next_service_stop in next_service_stops:
            next_bus_stop_code = next_service_stop.BusStopCode
            next_service_no = next_service_stop.ServiceNo
            # Already optimal, ignore
            if next_bus_stop_code in optimal_nodes:
                continue

            # Check if node already exists
            if (next_bus_stop_code, next_service_no) in nodes:
                next_node = nodes[(next_bus_stop_code, next_service_no)]
            else:
                next_node = Node(next_bus_stop_code, next_service_stop)
                nodes[(next_bus_stop_code, next_service_no)] = next_node
                traversal_queue.append(next_node)

            print('++', next_node)

            # Create edge and relax
            edge = Edge(current_node, next_service_stop, next_node)

            print(' -', edge)

        # TODO: Could do with a little optimization, currently O(hn)
        # h = len(heapq), n = len(df)
        # Maintain heap property in event node best cost has changed
        heapq.heapify(traversal_queue)

    return soln_nodes

def main():
    global TRANSFER_PENALTY

    # No transfers      : 19051 -> 18111
    # Single transfer   : 19051 -> 18129
    # Goal              : 19051 -> 03381
    # Equally optimal   : 59039 -> 54589
    # Loops             : 11389 -> 11381

    DEBUG_SOURCE = '19051'
    DEBUG_DEST = ['18129']

    # Argument handling
    parser = ArgumentParser(
        description='Finds the shortest bus route between a source and multiple '
        'destination bus stops.')
    parser.add_argument(
        '-t', '--transfer-penalty', default=TRANSFER_PENALTY,
        help="distance in km equivalent to the time & effort a transfer requires")
    parser.add_argument('-s', '--source', help="source bus stop code",
                        default=DEBUG_SOURCE)
    parser.add_argument(
        '-d', '--dests', nargs='*',
        help="space-delimited acceptable destination bus stop codes",
        default=DEBUG_DEST)

    args = parser.parse_args()
    TRANSFER_PENALTY = args.transfer_penalty
    source = args.source
    dests = args.dests

    # Fallback prompts
    if not source:
        source = input('Source bus stop code: ')
    if not dests:
        dests = input('Space-delimited destination bus-stop codes: ')
        dests = [dest.strip() for dest in dests.split(',')]

    # Run algorithm
    solutions = dijkstra(source, dests)
    for solution in solutions:
        print(solution)
        pprint(solution.best_route)

if __name__ == '__main__':
    main()
