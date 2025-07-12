import pytest
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase

class TestTsSingle(ValkeyTimeSeriesTestCaseBase):

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
                                             'AGGREGATION', 'SUM', 2000)
        assert len(result) == 2

        assert result[0][0] == 3000
        assert float(result[0][1]) == pytest.approx(121.2)
        # Bucket 3 (6000-...) No values

