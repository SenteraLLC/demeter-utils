import pytest
from sure import expect

from demeter_utils.query.weather._weather import find_duplicate_points
from demeter_utils.tests.data import COORDINATE_LIST_WITH_DUPLICATES


class TestFieldTrialsUtils:
    def test_find_duplicate_points(
        self,
    ):
        dup_points = find_duplicate_points(
            coordinate_list=COORDINATE_LIST_WITH_DUPLICATES
        )
        len(dup_points).should.be.equal_to(2)
        set(dup_points).should.be.equal_to(
            set(["POINT (-90.636626 44.690766)", "POINT (-90.63662 44.690766)"])
        )

    # TODO: _join_coordinates_to_unique_cell_ids() can be tested by mocking the get_cell_id() function for each point
