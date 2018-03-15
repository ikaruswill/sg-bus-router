import unittest

import geo

class KnownVincentyDistanceTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Coordinates based on furthest distance in Singapore, to elicit highest error
        # Distance calculated with Vincenty: http://www.5thandpenn.com/GeoMaps/GMapsExamples/distanceComplete2.html
        cls.point1_lat, cls.point1_lon = 1.355830, 104.028351
        cls.point2_lat, cls.point2_lon = 1.267683, 103.611312
        cls.distance = 47.424879

    def test_haversine_vs_vincenty(self):
        # Acceptable margin of error: 20m
        self.assertAlmostEqual(
            geo._haversine(
                self.point1_lat, self.point1_lon,
                self.point2_lat, self.point2_lon), self.distance, delta=0.02)

    def test_equirectangular_vs_vincenty(self):
        # Acceptable margin of error: 20m
        self.assertAlmostEqual(
            geo._equirectangular(
                self.point1_lat, self.point1_lon,
                self.point2_lat, self.point2_lon), self.distance, delta=0.02)

if __name__ == '__main__':
    unittest.main()
