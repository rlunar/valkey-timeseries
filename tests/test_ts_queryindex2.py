import pytest
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestTsQueryIndex(ValkeyTimeSeriesTestCaseBase):

    def setup_test_data(self, client):
        """Create a set of time series with different label combinations for testing"""
        # Create test series with various labels
        client.execute_command('TS.CREATE', 'ts1', 'LABELS', 'n', '1')
        client.execute_command('TS.CREATE', 'ts2', 'LABELS', 'n', '1', 'i', 'a')
        client.execute_command('TS.CREATE', 'ts3', 'LABELS', 'n', '1', 'i', 'b')
        client.execute_command('TS.CREATE', 'ts4', 'LABELS', 'n', '1', 'i', '\n')
        client.execute_command('TS.CREATE', 'ts5', 'LABELS', 'n', '2')
        client.execute_command('TS.CREATE', 'ts6', 'LABELS', 'n', '2.5')
        client.execute_command('TS.CREATE', 'ts7', 'LABELS', 'i', 'c')
        client.execute_command('TS.CREATE', 'ts8', 'LABELS', 'complex', 'val1&val2')

    def test_basic_equal_matching(self):
        """Test simple equality matching of TS.QUERYINDEX"""
        self.setup_test_data(self.client)

        # Basic filter matching all 'n=1' series
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n="1"'))
        assert result == [b'ts1', b'ts2', b'ts3', b'ts4']

        # Match specific label combination
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n="1"', 'i=a'))
        assert result == [b'ts2']

        # Non-existent value
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n=nonexistent'))
        assert result == []

    def test_empty_label_filtering(self):
        """Test filtering for series with or without specific labels"""
        self.setup_test_data(self.client)

        # Find series without 'i' label
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'i='))
        assert result == [b'ts1', b'ts5', b'ts6']

        # Find series with 'i' label
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'i="~.+"'))
        assert result == [b'ts2', b'ts3', b'ts4', b'ts7']

    def test_not_equal_matching(self):
        """Test negation matching with TS.QUERYINDEX"""
        self.setup_test_data(self.client)

        # Not equal to n=1
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n!=1'))
        assert result == [b'ts5', b'ts6']

        # Combine equality and negation
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n=1', 'i!=a'))
        assert result == [b'ts1', b'ts3', b'ts4']

        # Negation of empty value (finds all with the label set)
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'i!='))
        assert result == [b'ts2', b'ts3', b'ts4', b'ts7']

    def test_regex_matching(self):
        """Test regex matching capabilities of TS.QUERYINDEX"""
        self.setup_test_data(self.client)

        # Match with regex pattern
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n=~"^1$"'))
        assert result == [b'ts1', b'ts2', b'ts3', b'ts4']

        # Match with OR pattern
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n=~"1|2"'))
        assert result == [b'ts1', b'ts2', b'ts3', b'ts4', b'ts5']

        # Match all with .* pattern
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n=~".*"'))
        assert len(result) == 6  # All series with label 'n'

        # Match non-empty values with .+
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'i=~".+"'))
        assert result == [b'ts2', b'ts3', b'ts4', b'ts7']

    def test_regex_not_matching(self):
        """Test regex negation matching of TS.QUERYINDEX"""
        self.setup_test_data(self.client)

        # Not matching regex
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n!~"^1$"'))
        assert result == [b'ts5', b'ts6', b'ts7', b'ts8']

        # Not matching OR pattern
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n=~"1|2"'))
        assert result == [b'ts6', b'ts7', b'ts8']

        # Not matching anything (should return empty set)
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n=~".*"'))
        assert result == [b'ts7', b'ts8']  # Only series without 'n' label

    def test_complex_combinations(self):
        """Test more complex query combinations"""
        self.setup_test_data(self.client)

        # Combination of equals, not equals and regex
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n=1', 'i!=a', 'i="~.*"'))
        assert result == [b'ts3', b'ts4']

        # Using multiple mutually exclusive conditions
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n=1', 'n=2'))
        assert result == []

        # Complex regex pattern
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n=~"^[12].*$"'))
        assert result == [b'ts1', b'ts2', b'ts3', b'ts4', b'ts5', b'ts6']

    def test_special_characters(self):
        """Test handling of special characters in labels and values"""
        self.setup_test_data(self.client)

        # Match newline character
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'i=\n'))
        assert result == [b'ts4']

        # Match with regex for special character
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'i=~"\\n"'))
        assert result == [b'ts4']

        # Match ampersand in value
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'complex="val1&val2"'))
        assert result == [b'ts8']

    def test_error_cases(self):
        """Test error conditions with TS.QUERYINDEX"""
        self.setup_test_data(self.client)

        # Empty query should return error
        self.verify_error_response(self.client, 'TS.QUERYINDEX',
                                   "wrong number of arguments for 'TS.QUERYINDEX' command")

        # Invalid filter format
        self.verify_error_response(self.client, 'TS.QUERYINDEX invalid_filter',
                                   "Invalid filter: invalid_filter")

        # Invalid regex pattern
        self.verify_error_response(self.client, 'TS.QUERYINDEX n=~[abc',
                                   "Invalid pattern: [abc")

    def test_empty_result_cases(self):
        """Test cases that should return empty results"""
        self.setup_test_data(self.client)

        # Non-existent label value
        result = self.client.execute_command('TS.QUERYINDEX', 'n=nonexistent')
        assert result == []

        # Impossible combination
        result = self.client.execute_command('TS.QUERYINDEX', 'n=1', 'n=2')
        assert result == []

        # Combination of regex patterns that can't be satisfied
        result = self.client.execute_command('TS.QUERYINDEX', 'i=~"a.*"', 'i=~"b.*"')
        assert result == []

    def test_query_with_multiple_filters(self):
        """Test multiple filter queries in a single command"""
        self.setup_test_data(self.client)

        # Using multiple independent filters
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n=1', 'i=a'))
        assert result == [b'ts2']

        # Multiple filters with regex
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n=~"^[12]$"', 'i=~"[ab]"'))
        assert result == [b'ts2', b'ts3']

        # Multiple filters with negation
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n=1', 'i!=', 'i!=a'))
        assert result == [b'ts3', b'ts4']

    def test_match_all_patterns(self):
        """Test special patterns that match all or none"""
        self.setup_test_data(self.client)

        # Match all series with a wildcard
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n=~".*"'))
        assert len(result) == 6  # All series with n label

        # Match all and filter with another condition
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n=~".*"', 'i=a'))
        assert result == [b'ts2']

        # Using .+ to match non-empty values only
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'i=~".+"'))
        assert result == [b'ts2', b'ts3', b'ts4', b'ts7']

        # Not matching anything
        result = sorted(self.client.execute_command('TS.QUERYINDEX', 'n!~".*"', 'i!~".*"'))
        assert result == []  # No series can satisfy this