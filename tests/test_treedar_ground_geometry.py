"""Exact LAS XY geometry, without optional parser/geospatial dependencies."""
from itertools import permutations
import unittest

from processing.treedar_admission import _ground_noncollinear


class GroundGeometryTests(unittest.TestCase):
    def test_noncollinearity_is_independent_of_point_order_and_chunk_size(self):
        # At a 1nm LAS scale these cross products fell below the old fixed
        # floating-point threshold, even though the integer geometry is valid.
        points = [(0, 0), (1, 0), (0, 100), (10, 20)]
        for ordered in permutations(points):
            for chunk_size in (1, 2, 3, 4):
                with self.subTest(order=ordered, chunk_size=chunk_size):
                    basis, found = [], False
                    for start in range(0, len(ordered), chunk_size):
                        if not found:
                            found = _ground_noncollinear(basis, ordered[start:start + chunk_size])
                    self.assertTrue(found)

    def test_collinear_geometry_stays_rejected_in_every_order(self):
        points = [(-7, -11), (-2, -1), (3, 9), (8, 19)]
        for ordered in permutations(points):
            with self.subTest(order=ordered):
                basis = []
                for position in ordered:
                    self.assertFalse(_ground_noncollinear(basis, [position]))
                self.assertLessEqual(len(basis), 2)

    def test_duplicate_points_do_not_consume_the_second_basis_slot(self):
        basis = []
        self.assertFalse(_ground_noncollinear(basis, [(5, 8)] * 10))
        self.assertEqual(basis, [(5, 8)])
        self.assertFalse(_ground_noncollinear(basis, [(6, 9)]))
        self.assertTrue(_ground_noncollinear(basis, [(6, 10)]))

    def test_extreme_las_integer_coordinates_do_not_overflow(self):
        minimum, maximum = -(2**31), 2**31 - 1
        basis = []
        self.assertFalse(_ground_noncollinear(basis, [(minimum, minimum), (maximum, maximum)]))
        self.assertTrue(_ground_noncollinear(basis, [(maximum, minimum)]))

    def test_coordinates_are_promoted_before_arithmetic(self):
        class FixedWidthScalar:
            def __init__(self, value):
                self.value = value

            def __int__(self):
                return self.value

            def __sub__(self, other):
                raise AssertionError('fixed-width arithmetic could overflow')

        points = [(FixedWidthScalar(x), FixedWidthScalar(y))
                  for x, y in ((-(2**31), -(2**31)), (2**31 - 1, 2**31 - 1), (2**31 - 1, -(2**31)))]
        basis = []
        self.assertTrue(_ground_noncollinear(basis, iter(points)))
        self.assertTrue(all(type(value) is int for point in basis for value in point))

    def test_translations_and_positive_axis_rescaling_preserve_classification(self):
        for points, expected in (([(0, 0), (1, 1), (2, 2)], False),
                                 ([(0, 0), (1, 1), (2, 3)], True)):
            for xscale, yscale in ((1, 1), (1, 10**9), (10**9, 1)):
                for xoffset, yoffset in ((0, 0), (2**60, -(2**60))):
                    with self.subTest(points=points, scale=(xscale, yscale), offset=(xoffset, yoffset)):
                        transformed = [(x*xscale + xoffset, y*yscale + yoffset) for x, y in points]
                        self.assertEqual(_ground_noncollinear([], transformed), expected)

    def test_empty_chunks_preserve_the_accumulated_basis(self):
        basis = []
        self.assertFalse(_ground_noncollinear(basis, []))
        self.assertFalse(_ground_noncollinear(basis, [(0, 0)]))
        self.assertFalse(_ground_noncollinear(basis, []))
        self.assertFalse(_ground_noncollinear(basis, [(1, 0)]))
        self.assertFalse(_ground_noncollinear(basis, []))
        self.assertTrue(_ground_noncollinear(basis, [(0, 1)]))

    def test_input_point_collection_is_not_modified(self):
        points = [[0, 0], [1, 0], [0, 1]]
        self.assertTrue(_ground_noncollinear([], points))
        self.assertEqual(points, [[0, 0], [1, 0], [0, 1]])


if __name__ == '__main__':
    unittest.main()
