def latest_transfer(route):
    # Forward intersect
    reference_services = route[0].services
    for edge in route:
        if edge.has_transferred:
            reference_services = edge.services
            continue
        edge.services &= reference_services

    # Reverse intersect
    reference_services = route[-1].services
    for edge in reversed(route):
        if reference_services is None:
            reference_services = edge.services
            continue
        edge.services &= reference_services
        if edge.has_transferred:
            reference_services = None

def earliest_transfer(route):
    all_route_services = []
    # Forward intersect and make a copy
    reference_services = route[0].services
    for edge in route:
        if edge.has_transferred:
            reference_services = edge.services
            all_route_services.append(edge.services)
        else:
            all_route_services.append(edge.services & reference_services)

    # Reverse intersect disregarding predefined transfer points
    # NOTE: Discards alternative services if earliest transfer stop lies on
    #       a 'narrow' point
    reference_services = all_route_services[-1]
    for edge, forward_intersected_services in zip(reversed(route),
                                                  reversed(all_route_services)):
        allowed_services = edge.services & reference_services
        if not allowed_services:
            reference_services = forward_intersected_services
            allowed_services = edge.services & reference_services
        edge.services = allowed_services

def permissive_route(route):
    # Find legs in route
    route_legs = []
    start = 0
    for i, edge in enumerate(route):
        if edge.has_transferred:
            route_legs.append((start, i - 1))
            start = i
    route_legs.append((start, i))

    all_route_services = []
    # Forward intersect and make a copy
    reference_services = route[0].services
    for edge in route:
        if edge.has_transferred:
            reference_services = edge.services
            all_route_services.append(edge.services)
        else:
            all_route_services.append(edge.services & reference_services)

    # Reverse intersect
    reference_services = all_route_services[-1]
    for i in reversed(range(len(all_route_services))):
        if reference_services is None:
            reference_services = all_route_services[i]
        all_route_services[i] &= reference_services
        if route[i].has_transferred:
            reference_services = None

    # Permissive forward intersect
    next_legs = iter(route_legs[1:])
    for start, end, in route_legs:
        try:
            next_start_services = all_route_services[next(next_legs)[0]]
        # On final leg of route, use goal reference services
        except StopIteration:
            next_start_services = all_route_services[-1]
        for i in range(start, end + 1):
            reference_services = all_route_services[i] | next_start_services
            route[i].services &= reference_services

    # Permissive reverse intersect
    # Removes service 'spikes' in a route that has services from the next leg
    # in the middle of the route
    for start, end in route_legs:
        most_restrictive_services = route[end].services
        for i in range(end, start - 1, -1):
            if len(route[i].services) >= len(most_restrictive_services):
                route[i].services &= most_restrictive_services
            else:
                most_restrictive_services = route[i].services
