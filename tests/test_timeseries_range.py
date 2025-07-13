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

        self.client.execute_command('TS.ADD', 'ts1', 8000, 80.)
        self.client.execute_command('TS.ADD', 'ts1', 9000, 90)
        self.client.execute_command('TS.ADD', 'ts1', 10000, 100)
        self.client.execute_command('TS.ADD', 'ts1', 11000, 110)
        self.client.execute_command('TS.ADD', 'ts1', 12000, 120)

        # Align to 0, bucket timestamp mid, report empty
        result = self.client.execute_command('TS.RANGE', 'ts1', "-", "+",
                                             'ALIGN', 0,
                                             'AGGREGATION', 'SUM', 2000,
                                             'BUCKETTIMESTAMP', 'START',
                                             'EMPTY')
        print(result)

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

    def setup_aggregation_data(self):
        """Setup predictable test data for aggregation tests"""
        self.client.execute_command('TS.CREATE', 'agg_test')

        # Add known values: [1, 2, 3, 4, 5, 6] at timestamps 1000, 2000, 3000, 4000, 5000, 6000
        values = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
        for i, value in enumerate(values):
            self.client.execute_command('TS.ADD', 'agg_test', (i + 1) * 1000, value)

    def test_avg_aggregation(self):
        """Test AVG aggregation"""
        self.setup_aggregation_data()

        # Single bucket containing all values [1,2,3,4,5,6] -> avg = 3.5
        result = self.client.execute_command('TS.RANGE', 'agg_test', '-', '+',
                                             'AGGREGATION', 'AVG', 7000)
        assert len(result) == 1
        assert float(result[0][1]) == pytest.approx(3.5)

        # Two buckets: [1,2,3] and [4,5,6] -> avg = 2.0 and 5.0
        result = self.client.execute_command('TS.RANGE', 'agg_test', '-', '+',
                                             'AGGREGATION', 'AVG', 3000, 'ALIGN', 0)
        assert len(result) == 3
        assert float(result[0][1]) == pytest.approx(1.5)  # (1+2)/2
        assert float(result[1][1]) == pytest.approx(4.0)  # (3+4+5)/3
        assert float(result[2][1]) == pytest.approx(6.0)  # (3+4+5)/3


    def test_sum_aggregation(self):
        """Test SUM aggregation"""
        self.setup_aggregation_data()

        # Single bucket: sum of [1,2,3,4,5,6] = 21
        result = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                             'AGGREGATION', 'SUM', 7000)
        assert len(result) == 1
        assert float(result[0][1]) == pytest.approx(21.0)

        result = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                             'AGGREGATION', 'SUM', 3000, 'ALIGN', 0)

        assert len(result) == 3
        assert float(result[0][1]) == pytest.approx(3.0)
        assert float(result[1][1]) == pytest.approx(12.0)
        assert float(result[2][1]) == pytest.approx(6.0)

    def test_min_aggregation(self):
        """Test MIN aggregation"""
        self.setup_aggregation_data()

        # Single bucket: min of [1,2,3,4,5,6] = 1
        result = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                             'AGGREGATION', 'MIN', 7000)
        assert len(result) == 1
        assert float(result[0][1]) == pytest.approx(1.0)

        # Two buckets: [1,2,3] and [4,5,6] -> min = 1 and 4
        result = self.client.execute_command('TS.RANGE', 'agg_test', 1000, 7000,
                                             'AGGREGATION', 'MIN', 3000, 'ALIGN', 'start')
        assert len(result) == 2
        assert float(result[0][1]) == pytest.approx(1.0)
        assert float(result[1][1]) == pytest.approx(4.0)

    def test_max_aggregation(self):
        """Test MAX aggregation"""
        self.setup_aggregation_data()

        # Single bucket: max of [1,2,3,4,5,6] = 6
        result = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                             'AGGREGATION', 'MAX', 7000)
        assert len(result) == 1
        assert float(result[0][1]) == pytest.approx(6.0)

        # Two buckets: [1,2,3] and [4,5,6] -> max = 3 and 6
        result = self.client.execute_command('TS.RANGE', 'agg_test', 1000, 7000,
                                             'AGGREGATION', 'MAX', 3000, 'ALIGN', '-')
        assert len(result) == 2
        assert float(result[0][1]) == pytest.approx(3.0)
        assert float(result[1][1]) == pytest.approx(6.0)

    def test_count_aggregation(self):
        """Test COUNT aggregation"""
        self.setup_aggregation_data()

        # Single bucket: count of [1,2,3,4,5,6] = 6
        result = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                             'AGGREGATION', 'COUNT', 7000)
        assert len(result) == 1
        assert float(result[0][1]) == pytest.approx(6.0)

        # Two buckets: [1,2,3] and [4,5,6] -> count = 3 and 3
        result = self.client.execute_command('TS.RANGE', 'agg_test', 1000, 7000,
                                             'AGGREGATION', 'COUNT', 3000, 'ALIGN', 'start')
        assert len(result) == 2
        assert float(result[0][1]) == pytest.approx(3.0)
        assert float(result[1][1]) == pytest.approx(3.0)

    def test_first_aggregation(self):
        """Test FIRST aggregation"""
        self.setup_aggregation_data()

        # Single bucket: first of [1,2,3,4,5,6] = 1
        result = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                             'AGGREGATION', 'FIRST', 7000)
        assert len(result) == 1
        assert float(result[0][1]) == pytest.approx(1.0)

        # Two buckets: [1,2,3] and [4,5,6] -> first = 1 and 4
        result = self.client.execute_command('TS.RANGE', 'agg_test', 1000, 7000,
                                             'AGGREGATION', 'FIRST', 3000, 'ALIGN', 'start')
        assert len(result) == 2
        assert float(result[0][1]) == pytest.approx(1.0)
        assert float(result[1][1]) == pytest.approx(4.0)

    def test_last_aggregation(self):
        """Test LAST aggregation"""
        self.setup_aggregation_data()

        # Single bucket: last of [1,2,3,4,5,6] = 6
        result = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                             'AGGREGATION', 'LAST', 7000)
        assert len(result) == 1
        assert float(result[0][1]) == pytest.approx(6.0)

        # Two buckets: [1,2,3] and [4,5,6] -> last = 3 and 6
        result = self.client.execute_command('TS.RANGE', 'agg_test', 1000, 7000,
                                             'AGGREGATION', 'LAST', 3000, 'ALIGN', '-')
        assert len(result) == 2
        assert float(result[0][1]) == pytest.approx(3.0)
        assert float(result[1][1]) == pytest.approx(6.0)

    def test_range_aggregation(self):
        """Test RANGE aggregation (max - min)"""
        self.setup_aggregation_data()

        # Single bucket: range of [1,2,3,4,5,6] = 6 - 1 = 5
        result = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                             'AGGREGATION', 'RANGE', 7000)
        assert len(result) == 1
        assert float(result[0][1]) == pytest.approx(5.0)

        # Two buckets: [1,2,3] and [4,5,6] -> range = 2 and 2
        result = self.client.execute_command('TS.RANGE', 'agg_test', 1000, 7000,
                                             'AGGREGATION', 'RANGE', 3000, 'ALIGN', 'start')
        assert len(result) == 2
        assert float(result[0][1]) == pytest.approx(2.0)  # 3-1
        assert float(result[1][1]) == pytest.approx(2.0)  # 6-4

    def test_std_p_aggregation(self):
        """Test STD.P aggregation (population standard deviation)"""
        self.setup_aggregation_data()

        # Single bucket: std.p of [1,2,3,4,5,6]
        # Population std dev = sqrt(sum((x-mean)^2)/N)
        # mean = 3.5, variance = ((1-3.5)^2 + (2-3.5)^2 + ... + (6-3.5)^2) / 6
        # = (6.25 + 2.25 + 0.25 + 0.25 + 2.25 + 6.25) / 6 = 17.5 / 6 ≈ 2.917
        # std.p = sqrt(2.917) ≈ 1.708
        result = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                             'AGGREGATION', 'STD.P', 7000)
        assert len(result) == 1
        assert float(result[0][1]) == pytest.approx(1.708, abs=0.01)

    def test_std_s_aggregation(self):
        """Test STD.S aggregation (sample standard deviation)"""
        self.setup_aggregation_data()

        # Single bucket: std.s of [1,2,3,4,5,6]
        # Sample std dev = sqrt(sum((x-mean)^2)/(N-1))
        # Using the same variance calculation as above but divided by (N-1)
        # std.s = sqrt(17.5 / 5) = sqrt(3.5) ≈ 1.871
        result = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                             'AGGREGATION', 'STD.S', 7000)
        assert len(result) == 1
        assert float(result[0][1]) == pytest.approx(1.871, abs=0.01)

    def test_var_p_aggregation(self):
        """Test VAR.P aggregation (population variance)"""
        self.setup_aggregation_data()

        # Single bucket: var.p of [1,2,3,4,5,6]
        # Population variance = sum((x-mean)^2)/N = 17.5 / 6 ≈ 2.917
        result = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                             'AGGREGATION', 'VAR.P', 7000)
        assert len(result) == 1
        assert float(result[0][1]) == pytest.approx(2.917, abs=0.01)

    def test_var_s_aggregation(self):
        """Test VAR.S aggregation (sample variance)"""
        self.setup_aggregation_data()

        # Single bucket: var.s of [1,2,3,4,5,6]
        # Sample variance = sum((x-mean)^2)/(N-1) = 17.5 / 5 = 3.5
        result = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                             'AGGREGATION', 'VAR.S', 7000)
        assert len(result) == 1
        assert float(result[0][1]) == pytest.approx(3.5, abs=0.01)

    def test_aggregation_with_empty_buckets(self):
        """Test aggregation behavior with empty buckets"""
        self.setup_aggregation_data()

        # Create gaps in data by using larger bucket size
        result = self.client.execute_command('TS.RANGE', 'agg_test', 0, 10000,
                                             'AGGREGATION', 'SUM', 2000, 'ALIGN', 0, 'EMPTY')

        # Should have buckets: [1,2], [3,4], [5,6], [empty], [empty]
        assert len(result) >= 3
        assert float(result[0][1]) == pytest.approx(3.0)   # 1+2
        assert float(result[1][1]) == pytest.approx(7.0)   # 3+4
        assert float(result[2][1]) == pytest.approx(11.0)  # 5+6

    def test_aggregation_single_value_bucket(self):
        """Test aggregation with buckets containing single values"""
        self.setup_aggregation_data()

        # Use a small bucket size so each contains only one value
        result = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                             'AGGREGATION', 'AVG', 1000, 'ALIGN', 0)

        # Each bucket should contain one value, so AVG should equal the value
        assert len(result) == 6
        for i, bucket in enumerate(result):
            assert float(bucket[1]) == pytest.approx(float(i + 1))

    def test_aggregation_with_bucket_timestamps(self):
        """Test aggregation with different BUCKETTIMESTAMP options"""
        self.setup_aggregation_data()

        # Test with START bucket timestamp
        result_start = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                                   'AGGREGATION', 'SUM', 3000, 'ALIGN', 0,
                                                   'BUCKETTIMESTAMP', 'START')

        # Test with MID bucket timestamp
        result_mid = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                                 'AGGREGATION', 'SUM', 3000, 'ALIGN', 0,
                                                 'BUCKETTIMESTAMP', 'MID')

        # Test with END bucket timestamp
        result_end = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                                 'AGGREGATION', 'SUM', 3000, 'ALIGN', 0,
                                                 'BUCKETTIMESTAMP', 'END')

        # Values should be the same, timestamps should differ
        assert len(result_start) == len(result_mid) == len(result_end) == 2

        # Check that timestamps are different but values are the same
        for i in range(len(result_start)):
            assert float(result_start[i][1]) == float(result_mid[i][1]) == float(result_end[i][1])
            # Timestamps should be different (exact values depend on bucket alignment)
            assert result_start[i][0] != result_mid[i][0] or result_mid[i][0] != result_end[i][0]

    @pytest.mark.parametrize("agg_type,expected_single_bucket", [
        ('AVG', 3.5),
        ('SUM', 21.0),
        ('MIN', 1.0),
        ('MAX', 6.0),
        ('COUNT', 6.0),
        ('FIRST', 1.0),
        ('LAST', 6.0),
        ('RANGE', 5.0),
        ('STD.P', 1.708),
        ('STD.S', 1.871),
        ('VAR.P', 2.917),
        ('VAR.S', 3.5),
    ])
    def test_all_aggregation_types_parametrized(self, agg_type, expected_single_bucket):
        """Parametrized test for all aggregation types with a single bucket"""
        self.setup_aggregation_data()

        result = self.client.execute_command('TS.RANGE', 'agg_test', 0, 7000,
                                             'AGGREGATION', agg_type, 7000)

        assert len(result) == 1
        if agg_type in ['STD.P', 'STD.S', 'VAR.P', 'VAR.S']:
            assert float(result[0][1]) == pytest.approx(expected_single_bucket, abs=0.01)
        else:
            assert float(result[0][1]) == pytest.approx(expected_single_bucket)


    def test_range_edge_cases(self):
        """Test TS.RANGE with edge case timestamps"""

        self.setup_data()

        # Exact start/end match
        result = self.client.execute_command('TS.RANGE', 'ts1', 2000, 2000)
        assert result == [[2000, b'20.2']]

        # Range before first sample
        result = self.client.execute_command('TS.RANGE', 'ts1', 0, 500)
        assert result == []

        # Range after the last sample
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