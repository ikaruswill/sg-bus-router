from argparse import ArgumentParser
from collections import defaultdict
from functools import total_ordering
import heapq
import logging
import math
import pickle
from pprint import pprint

import geo
import postprocess

# Features
# TODO: Filter out buses that are outside of current availability
# TODO: Add custom exceptions instead of exit()

# Parameters
TRANSFER_PENALTY = 5
NEARBY_STOPS_RADIUS = 0.3

# Constants
FROM_ORIGIN_KEY = 'DistanceFromOrigin'
TO_GOAL_KEY = 'DistanceToGoal'

# Load data
with open('rt_idx.pkl',
          'rb') as a, open('rt_bs.pkl',
                           'rb') as b, open('bs.pkl',
                                            'rb') as c:
    rt_idx = pickle.load(a)
    rt_bs = pickle.load(b)
    bs = pickle.load(c)

@total_ordering
class Node:

    def __init__(self, bus_stop_code, best_cost=math.inf, best_dist=math.inf,
                 last_transfer_index=-1):
        self.bus_stop_code = bus_stop_code
        self.bus_stop = bs[self.bus_stop_code]
        self.h_dist = bs[self.bus_stop_code][TO_GOAL_KEY]
        self.best_dist = best_dist
        self.best_cost = best_cost
        self.best_metric = self.best_cost + self.h_dist
        self.best_route = []
        self.last_transfer_index = last_transfer_index
        self.services = rt_bs[self.bus_stop_code]

    def __lt__(self, other):
        return self.best_metric < other.best_metric

    def __hash__(self):
        return hash('{} {}'.format(self.bus_stop_code,
                                   self.service['ServiceNo']))

    def __repr__(self):
        return '{}: {:>6.1f} | {:>6.1f} | {:>6.1f}km'.format(
            self.bus_stop_code, self.best_metric,
            self.h_dist, self.best_dist)


class Edge:
    def __init__(self, source, services, dest, distance):
        self.source = source
        self.dest = dest
        self.has_transferred = False
        self.distance = distance
        self.services = services
        self._services = self.get_best_route_common_services(services) # For routing algorithm
        self.cost = self.calculate_cost()
        self.update_dest_distance_cost_route()

    def get_best_route_common_services(self, services):
        if not self.source.best_route:
            return services
        common_services = services & self.source.best_route[-1]._services
        if not common_services:
            self.has_transferred = True
            return services
        return common_services

    def calculate_cost(self):
        cost = self.distance
        # if self.distance:
        #     cost += 1/self.distance
        if self.has_transferred:
            cost += TRANSFER_PENALTY

        if cost < 0:
            print('ERROR: Negative edge cost')
            self.cost = cost
            exit(repr(self))

        return cost

    def update_dest_distance_cost_route(self):
        new_dist = self.source.best_dist + self.distance
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
        return '< {} --> {} > {:>1s} {:>4.1f} | {:>4.1f}km | {}'.format(
            self.source.bus_stop_code, self.dest.bus_stop_code,
            '*' if self.has_transferred else '', self.cost,
            self.distance, self.services)


def precalculate_distances(lat, lon, dest_key):
    for bus_stop_code, bus_stop_data in bs.items():
        stop_lon, stop_lat = bus_stop_data[
            'Longitude'], bus_stop_data['Latitude']
        bs[bus_stop_code][dest_key] = geo.distance(
            lat, lon, stop_lat, stop_lon)

def discover_next_stops(node):
    next_stops = defaultdict(lambda: defaultdict(set))
    # Discover next stop of each service
    for idx, current_service_stop in node.services.items():
        try:
            next_service_stop = rt_idx[idx + 1]
        except KeyError:
            print('INFO: Reached end of dataframe')
            continue
        if next_service_stop[
            'StopSequence'] == current_service_stop['StopSequence'] + 1:
            next_service_stop_code = next_service_stop['BusStopCode']
            next_stops[next_service_stop_code]['services'].add(
                next_service_stop['ServiceNo'])
            next_stops[
                next_service_stop_code]['distance'] = next_service_stop[
                    'Distance'] - current_service_stop['Distance']
    return next_stops

def find_nearby_stops(bs_dict, dist_key, radius):
    nearby_stops = []
    for bus_stop_code, bus_stop_data in bs_dict.items():
        if bus_stop_data[dist_key] <= radius:
            nearby_stops.append(bus_stop_code)
    return nearby_stops

def dijkstra(origin_codes, goal_codes):
    traversal_queue = []
    nodes = {}
    optimal_nodes = set()

    # Initialize origin nodes
    for origin_code in origin_codes:
        origin = Node(
            origin_code, bs[origin_code].get(FROM_ORIGIN_KEY, 0),
            bs[origin_code].get(FROM_ORIGIN_KEY, 0), 0)
        traversal_queue.append(origin)

    # Dijkstra iterations
    while traversal_queue:
        current_node = heapq.heappop(traversal_queue)
        nodes[current_node.bus_stop_code] = current_node
        optimal_nodes.add(current_node.bus_stop_code)

        logging.info(current_node)

        next_service_stops = discover_next_stops(current_node)

        # Create next nodes
        for (next_bus_stop_code,
             next_bus_stop_info) in next_service_stops.items():
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

            logging.debug(' ++{}'.format(next_node))

            # Create edge and relax
            edge = Edge(current_node, next_bus_stop_info['services'],
                        next_node, next_bus_stop_info['distance'])

            logging.debug(' -{}'.format(edge))

        # Store optimal route found for bus stop (Service agnostic)
        if current_node.bus_stop_code in goal_codes:
            break

        # TODO: Could do with a little optimization, currently O(hn)
        # h = len(heapq), n = len(rt)
        # Maintain heap property in event node best cost has changed
        heapq.heapify(traversal_queue)


    # postprocess.latest_transfer(current_node.best_route)
    # postprocess.earliest_transfer(current_node.best_route)
    postprocess.permissive_route(current_node.best_route)
    return current_node

def main():
    global TRANSFER_PENALTY
    global NEARBY_STOPS_RADIUS

    # No transfers      : 19051 -> 18111
    # Single transfer   : 19051 -> 18129
    # Equally optimal   : 59039 -> 54589
    # Loops             : 11389 -> 11381
    # LS                : 19051 -> 03381
    # Tim               : 18129 -> 10199
    # Skipped stops     : 59119 -> 63091
    # Optimality        : 07319 -> 57111 : 66,67 -> 980
    DEBUG_ORIGIN_STOPS = ['19051']
    DEBUG_GOAL_STOP = '03381'

    # DEBUG_ORIGIN_COORDS = (1.297686, 103.786218)
    # DEBUG_GOAL_COORDS = (1.384380, 103.771181)

    DEBUG_ORIGIN_COORDS = (1.297686, 103.786218)
    DEBUG_GOAL_COORDS = (1.394015, 103.900321)

    # Argument handling
    parser = ArgumentParser(
        description='Finds the shortest bus route')
    parser.add_argument(
        '-v', action='count', dest='verbosity', default=0,
        help="set verbosity level")
    parser.add_argument(
        '-t', '--transfer-penalty', default=TRANSFER_PENALTY, type=float,
        help="distance in km equivalent to the time & effort a transfer requires")
    subparsers = parser.add_subparsers(help='mode', dest='mode')
    subparsers.required = True

    # Route with GPS Coordinates
    coordparser = subparsers.add_parser(
        'coords', help='find the shortest bus route between GPS coordinates')
    coordparser.add_argument(
        '-o', '--origin', default=DEBUG_ORIGIN_COORDS, type=float, nargs=2,
        metavar=('LAT', 'LON'),
        help="origin lattitude and longitude")
    coordparser.add_argument(
        '-g', '--goal', default=DEBUG_GOAL_COORDS, type=float, nargs=2,
        metavar=('LAT', 'LON'),
        help="goal lattitude and longitude")
    coordparser.add_argument(
        '-r', '--radius', default=NEARBY_STOPS_RADIUS, type=float,
        help='radius from both origin and goal to search for admissible bus stops')

    # Route with Bus Stop Codes
    codeparser = subparsers.add_parser(
        'codes', help='find the shortest bus route between bus stop codes')
    codeparser.add_argument(
        '-o', '--origin', default=DEBUG_ORIGIN_STOPS, nargs='+', metavar='ORIGIN',
        help="origin bus stop codes")
    codeparser.add_argument(
        '-g', '--goal', default=DEBUG_GOAL_STOP, help="destination bus stop code")

    args = parser.parse_args()

    # Set logging level
    if args.verbosity == 0:
        LOG_LEVEL = logging.WARNING
    if args.verbosity == 1:
        LOG_LEVEL = logging.INFO
    elif args.verbosity >= 2:
        LOG_LEVEL = logging.DEBUG
    logging.basicConfig(level=LOG_LEVEL, datefmt='%H:%M:%S',
                        format='%(asctime)s %(message)s')

    TRANSFER_PENALTY = args.transfer_penalty
    origin = args.origin
    goal = args.goal

    if args.mode == 'coords':
        NEARBY_STOPS_RADIUS = args.radius
        origin_lat, origin_lon = origin
        goal_lat, goal_lon = goal
        precalculate_distances(origin_lat, origin_lon, dest_key=FROM_ORIGIN_KEY)
        precalculate_distances(goal_lat, goal_lon, dest_key=TO_GOAL_KEY)
        origin_codes = find_nearby_stops(bs, FROM_ORIGIN_KEY,
                                         NEARBY_STOPS_RADIUS)
        goal_codes = find_nearby_stops(bs, TO_GOAL_KEY, NEARBY_STOPS_RADIUS)
        goal_codes = set(goal_codes)
    elif args.mode == 'codes':
        origin_codes = origin
        precalculate_distances(
            bs[goal]['Latitude'], bs[goal]['Longitude'], dest_key=TO_GOAL_KEY)
        goal_codes = set([goal])

    # Run algorithm
    solution = dijkstra(origin_codes, goal_codes)
    print('Solution')
    pprint(solution.best_route)
    print('{} | {} stops'.format(solution, len(solution.best_route)))

if __name__ == '__main__':
    main()
