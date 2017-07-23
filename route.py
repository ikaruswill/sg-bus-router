import math
import heapq
from functools import total_ordering

import pandas as pd
from sqlalchemy import create_engine

db_conn = create_engine('sqlite:///sg-bus-routes.db')
df = pd.read_sql_table(table_name='bus_routes', con=db_conn)

@total_ordering
class Node:
    def __init__(self, bus_stop_code, best_cost=math.inf, best_route=[]):
        global df
        self.bus_stop_code = bus_stop_code
        self.best_cost = best_cost
        self.best_route = best_route
        self.services = df[(df.BusStopCode == self.bus_stop_code)]

    def __lt__(self, other):
        return self.best_cost < other.best_cost

    def __hash__(self):
        return hash(self.code)

    def __repr__(self):
        return '{}: {}'.format(self.bus_stop_code, self.best_cost)


class Edge:
    def __init__(self, source, dest, service):
        self.source = source
        self.dest = dest
        self.service = service # Service taken at source
        self.cost = self.calculate_cost()
        self.update_dest_cost_route()

    def calculate_cost(self):
        next_service_stop = self.dest.services[
            (self.dest.services.ServiceNo == self.service.ServiceNo) & \
            (self.dest.services.StopSequence == self.service.StopSequence + 1)].iloc[0]
        cost = next_service_stop.Distance - self.service.Distance
        if self.source.best_route:
            prev_optim_edge = self.source.best_route[-1]
            if self.service.ServiceNo != prev_optim_edge.service.ServiceNo:
                # Distance in km equivalent to the time & effort a transfer requires
                cost += 5
        return cost

    def update_dest_cost_route(self):
        new_cost = self.source.best_cost + self.cost
        if new_cost < self.dest.best_cost:
            self.dest.best_cost = new_cost
            self.dest.best_route = self.source.best_route + [self]

    def __repr__(self):
        return '({} -- {} -- {})'.format(
            self.source.bus_stop_code, self.service.ServiceNo,
            self.dest.bus_stop_code)


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

def dijkstra(start, end):
    global df
    traversal_queue = []
    nodes = {}
    optimal_nodes = set()

    origin = Node(start, 0)
    heapq.heappush(traversal_queue, origin)

    # Dijkstra iterations
    while len(traversal_queue):
        current_node = heapq.heappop(traversal_queue)
        nodes[current_node.bus_stop_code] = current_node
        optimal_nodes.add(current_node.bus_stop_code)

        if current_node.bus_stop_code == end:
            print('BEST ROUTE:', current_node.best_route)
            return current_node

        next_service_stops = discover_next_service_stops(current_node)

        # Create next nodes
        for next_service_stop in next_service_stops:
            next_bus_stop_code = next_service_stop.BusStopCode
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

            print(next_node)

            # Create edge and relax
            current_service_stop = current_node.services[
                current_node.services.ServiceNo == next_service_stop.ServiceNo].iloc[0]
            edge = Edge(current_node, next_node, current_service_stop)

            print(edge)

            # Maintain heap property in event node best cost has changed
            heapq.heapify(traversal_queue)

def main():
    start = '19051'
    end = '18129'
    # 18111 no transfers
    # 18129 single transfer

    print(dijkstra(start, end))

if __name__ == '__main__':
    main()
