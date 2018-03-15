import unittest

import route

class NearbyStopsTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_dist = 200
        distances = [0, 0.1, cls.test_dist, cls.test_dist - 0.1,
                     cls.test_dist + 0.1]
        keys = ['{:05d}'.format(i) for i in range(len(distances))]
        cls.dist_key = 'Distance'
        cls.bs = {key: {cls.dist_key: dist} for key, dist in zip(keys,
                                                                 distances)}

    def test_within_dist(self):
        nearby_stops = route.find_nearby_stops(self.bs, self.dist_key,
                                               self.test_dist)

        for bus_stop_code, bus_stop_dict in self.bs.items():
            # True Positive subtest: Correct bus stops included
            with self.subTest(bus_stop_code=bus_stop_code):
                if bus_stop_dict[self.dist_key] <= self.test_dist:
                    self.assertIn(
                        bus_stop_code, nearby_stops,
                        msg='False negative.\nTest: <= {}  Excluded: {}'.format(
                            self.test_dist, bus_stop_dict[self.dist_key]))
            # True Negative subtest: Wrong bus stops excluded
            with self.subTest(bus_stop_code=bus_stop_code):
                if bus_stop_dict[self.dist_key] > self.test_dist:
                    self.assertNotIn(
                        bus_stop_code, nearby_stops,
                        msg='False positive.\nTest: <= {}  Included: {}'.format(
                            self.test_dist, bus_stop_dict[self.dist_key]))


if __name__ == '__main__':
    unittest.main()
