from math import radians, cos, sin, asin, sqrt

# Earth equatorial radius in km based on WGS-84 geoid
EARTH_RADIUS = 6378.137

def _equirectangular(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    x = (lon2 - lon1) * cos(0.5 * (lat2 + lat1))
    y = lat2 - lat1
    d = EARTH_RADIUS * sqrt(x * x + y * y)
    return d

def _haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * asin(sqrt(a))
    km = EARTH_RADIUS * c
    return km

def distance(lat1, lon1, lat2, lon2):
    return _haversine(lat1, lon1, lat2, lon2)
