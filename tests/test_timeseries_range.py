import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase

# TODO: Aggregation and groupby tests are not (yet) implemented in this test case.
class TestTimeSeriesRange(ValkeyTimeSeriesTestCaseBase):
    def setup_data(self):
        # Setup some time series data
        self.client.execute_command('TS.CREATE', 'ts1')
        self.client.execute_command('TS.ADD', 'ts1', 1000, 10.1)
        self.client.execute_command('TS.ADD', 'ts1', 2000, 20.2)
        self.client.execute_command('TS.ADD', 'ts1', 3000, 30.3)
        self.client.execute_command('TS.ADD', 'ts1', 4000, 40.4)
        self.client.execute_command('TS.ADD', 'ts1', 5000, 50.5)

    def test_basic_range(self):
        """Test basic TS.RANGE with start and end timestamps"""

        self.setup_data()

        result = self.client.execute_command('TS.RANGE', 'ts1', 2000, 4000)
        assert result == [[2000, b'20.2'], [3000, b'30.3'], [4000, b'40.4']]

    def test_full_range(self):
        """Test TS.RANGE with '-' and '+'"""

        self.setup_data()

        result = self.client.execute_command('TS.RANGE', 'ts1', '-', '+')
        assert len(result) == 5
        assert result[0] == [1000, b'10.1']
        assert result[-1] == [5000, b'50.5']

    def test_range_with_count(self):
        """Test TS.RANGE with COUNT option"""

        self.setup_data()

        # Forward count
        result = self.client.execute_command('TS.RANGE', 'ts1', '-', '+', 'COUNT', 2)
        assert result == [[1000, b'10.1'], [2000, b'20.2']]

    def test_range_filter_by_ts(self):
        """Test TS.RANGE with FILTER_BY_TS"""

        self.setup_data()

        result = self.client.execute_command('TS.RANGE', 'ts1', '-', '+', 'FILTER_BY_TS', 1000, 3000, 5000)
        assert result == [[1000, b'10.1'], [3000, b'30.3'], [5000, b'50.5']]

    def test_range_filter_by_value(self):
        """Test TS.RANGE with FILTER_BY_VALUE"""

        self.setup_data()

        result = self.client.execute_command('TS.RANGE', 'ts1', '-', '+', 'FILTER_BY_VALUE', 20, 40)
        assert result == [[2000, b'20.2'], [3000, b'30.3']]
        # Note: 40.4 is excluded because the range is min <= value < max

        # Test inclusive range (using slightly adjusted values)
        result = self.client.execute_command('TS.RANGE', 'ts1', '-', '+', 'FILTER_BY_VALUE', 20.2, 40.4)
        assert result == [[2000, b'20.2'], [3000, b'30.3'], [4000, b'40.4']]

    def test_range_filter_by_ts_and_value(self):
        """Test TS.RANGE combining FILTER_BY_TS and FILTER_BY_VALUE"""

        self.setup_data()

        result = self.client.execute_command('TS.RANGE', 'ts1', '-', '+',
                                             'FILTER_BY_TS', 2000, 4000, 5000,
                                             'FILTER_BY_VALUE', 35, 60)
        assert result == [[4000, b'40.4'], [5000, b'50.5']]

    def test_range_aggregation(self):
        """Test TS.RANGE with basic aggregation"""

        self.setup_data()

        # Average over 2000ms buckets
        result = self.client.execute_command('TS.RANGE', 'ts1', 1000, 5000, 'AGGREGATION', 'AVG', 2000)
        assert len(result) == 3

        assert result[0][0] == 0
        assert float(result[0][1]) == pytest.approx(10.1)
        # Bucket 2 (3000-4999): avg(30.3, 40.4) = 35.35
        assert result[1][0] == 2000
        assert float(result[1][1]) == pytest.approx(25.25)
        # Bucket 3 (5000-6999): avg(50.5) = 50.5
        assert result[2][0] == 4000
        assert float(result[2][1]) == pytest.approx(45.45)

    def test_range_aggregation_options(self):
        """Test TS.RANGE aggregation with ALIGN, BUCKETTIMESTAMP, EMPTY"""

        self.setup_data()

        # Align to 0, bucket timestamp mid, report empty
        result = self.client.execute_command('TS.RANGE', 'ts1', 500, 5000,
                                             'ALIGN', 0,
                                             'AGGREGATION', 'SUM', 2000,
                                             'BUCKETTIMESTAMP', 'MID')
        assert len(result) == 3
        # Bucket 0 (0-1999): sum(10.1) = 10.1, mid timestamp = 1000
        assert result[0] == [1000, b'10.1']
        # Bucket 1 (2000-3999): sum(20.2, 30.3) = 50.5, mid timestamp = 3000
        assert result[1] == [3000, b'50.5']
        # Bucket 2 (4000-5999): sum(40.4, 50.5) = 90.9, mid timestamp = 5000
        assert result[2] == [5000, b'90.9']

    def test_aggregation_empty_buckets(self):
        """Test TS.RANGE aggregation with ALIGN, BUCKETTIMESTAMP, EMPTY"""

        self.setup_data()

        # Align to 0, bucket timestamp mid, report empty
        result = self.client.execute_command('TS.RANGE', 'ts1', 500, 6500,
                                             'ALIGN', 0,
                                             'AGGREGATION', 'SUM', 2000,
                                             'BUCKETTIMESTAMP', 'MID',
                                             'EMPTY')
        assert len(result) == 4
        # Bucket 0 (0-1999): sum(10.1) = 10.1, mid timestamp = 1000
        assert result[0] == [1000, 10.1]
        # Bucket 1 (2000-3999): sum(20.2, 30.3) = 50.5, mid timestamp = 3000
        assert result[1] == [3000, 50.5]
        # Bucket 2 (4000-5999): sum(40.4, 50.5) = 90.9, mid timestamp = 5000
        assert result[2] == [5000, 90.9]
        # Bucket 3 (6000-7999): empty, mid-timestamp = 7000 (reported due to EMPTY)
        assert result[3] == [7000, None] # Empty buckets return None

    def test_range_aggregation_with_filters(self):
        """Test TS.RANGE combining aggregation and filters"""

        self.client.execute_command('TS.CREATE', 'ts1')
        for i in range(0, 1000, 10):
            self.client.execute_command('TS.ADD', 'ts1', (i + 1) * 1000, (i + 1) * 10)

        info = self.ts_info('ts1')
        print(info)

        samples = self.client.execute_command('TS.RANGE', 'ts1', '-', '+', 'FILTER_BY_VALUE', 20, 50)
        print(samples)

        # Filter values > 30, then aggregate SUM over 3000ms buckets
        result = self.client.execute_command('TS.RANGE', 'ts1', '-', '+',
                                             'FILTER_BY_VALUE', 20, 50,
                                             'ALIGN', 0,
                                             'AGGREGATION', 'SUM', 3000)
        assert len(result) == 2

        assert result[0][0] == 3000
        assert float(result[0][1]) == pytest.approx(121.2)
        # Bucket 3 (6000-...) No values

    def test_range_empty_series(self):
        """Test TS.RANGE on an existing but empty series"""

        self.setup_data()

        self.client.execute_command('TS.CREATE', 'ts_empty')
        result = self.client.execute_command('TS.RANGE', 'ts_empty', '-', '+')
        assert result == []

        # With aggregation and EMPTY
        result = self.client.execute_command('TS.RANGE', 'ts_empty', 0, 1000, 'AGGREGATION', 'SUM', 500, 'EMPTY')
        assert result == []

    def test_range_non_existent_series(self):
        """Test TS.RANGE on a non-existent key"""

        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command('TS.RANGE', 'ts_nonexistent', '-', '+')

        assert "key does not exist" in str(excinfo.value).lower()


    def test_range_edge_cases(self):
        """Test TS.RANGE with edge case timestamps"""

        self.setup_data()

        # Exact start/end match
        result = self.client.execute_command('TS.RANGE', 'ts1', 2000, 2000)
        assert result == [[2000, b'20.2']]

        # Range before first sample
        result = self.client.execute_command('TS.RANGE', 'ts1', 0, 500)
        assert result == []

        # Range after last sample
        result = self.client.execute_command('TS.RANGE', 'ts1', 6000, 7000)
        assert result == []

        # Range partially overlapping
        result = self.client.execute_command('TS.RANGE', 'ts1', 4500, 5500)
        assert result == [[5000, b'50.5']]

    def test_range_error_handling(self):
        """Test error conditions for TS.RANGE"""
        # Wrong number of arguments
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command('TS.RANGE', 'ts1')
        assert "wrong number of arguments" in str(excinfo.value).lower()
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command('TS.RANGE', 'ts1', '-')
        assert "wrong number of arguments" in str(excinfo.value).lower()

        # Invalid start/end timestamp
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command('TS.RANGE', 'ts1', 'invalid', '+')
        assert "invalid start timestamp" in str(excinfo.value).lower()
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command('TS.RANGE', 'ts1', '-', 'invalid')
        assert "invalid end timestamp" in str(excinfo.value).lower()

        # Invalid filter values
        with pytest.raises(ResponseError) as excinfo:
            self.client.execute_command('TS.RANGE', 'ts1', '-', '+', 'FILTER_BY_VALUE', 'a', 'b')
        assert "cannot parse value filter" in str(excinfo.value).lower()