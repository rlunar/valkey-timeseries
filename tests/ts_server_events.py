import time

import pytest
from valkeytestframework.util.waiters import *
from valkeytestframework.conftest import resource_port_tracker
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase


class TestServerEvents(ValkeyTimeSeriesTestCaseBase):
    """Test cases for server event handling in the TimeSeries module."""

    def setup_data(self):
        self.start_ts = 1577836800  # 2020-01-01 00:00:00

    def create_ts(self, key, timestamp=None, value=1.0):
        """Helper to create a time series with a sample data point."""
        
        self.setup_data()
        
        if timestamp is None:
            timestamp = self.start_ts

        self.client.execute_command("TS.CREATE", key, "LABELS", "key", key, "sensor", "temp")
        self.client.execute_command("TS.ADD", key, timestamp, value)
        return key

    def test_loaded_event(self):
        """Test that a series is properly indexed when loaded."""

        self.setup_data()

        key = "ts:loaded"
        self.client.execute_command("TS.CREATE", key, "LABELS", "__name__", "sensor", "sensor", "temp")

        # Save and reload to trigger load events
        self.client.save()
        self.client.execute_command("DEBUG", "RELOAD")

        # Verify the key exists and is queryable
        assert self.client.exists(key)

        # Query the key to ensure it was loaded and indexed correctly
        keys = self.client.execute_command("TS.QUERYINDEX", "key={}".format(key))
        assert len(keys) == 1
        assert keys[0] == key

        keys = self.client.execute_command("TS.QUERYINDEX", 'sensor="temp"')
        assert len(keys) == 1
        assert keys[0] == key

    def test_delete_event(self):
        """Test that a deleted series is properly removed from the index."""

        self.setup_data()

        key = "ts:delete"
        self.create_ts(key)

        # Delete the key
        self.client.delete(key)

        # The key should be removed
        assert not self.client.exists(key)

        keys = self.client.execute_command("TS.QUERYINDEX", "key={}".format(key))
        assert len(keys) == 0

        keys = self.client.execute_command("TS.QUERYINDEX", 'sensor="temp"')
        assert len(keys) == 0

        # Create the key again - should succeed without index interference
        self.create_ts(key, self.start_ts + 1, 2.0)
        result = self.client.execute_command("TS.RANGE", key, 0, "+")
        assert len(result) == 1
        assert result[0][0] == self.start_ts + 1

    def test_rename_event(self):
        """Test that series is reindexed after being renamed."""

        self.setup_data()

        old_key = "ts:oldname"
        new_key = "ts:newname"
        self.create_ts(old_key)

        # Rename the key
        self.client.rename(old_key, new_key)

        # Old key should be gone, new key should exist
        assert not self.client.exists(old_key)
        
        # The old key should not be queryable
        keys = self.client.execute_command("TS.QUERYINDEX", "key={}".format(old_key))
        assert len(keys) == 0
        keys = self.client.execute_command("TS.QUERYINDEX", 'sensor="temp"')
        assert len(keys) == 0
        
        assert self.client.exists(new_key)

        # The new key should be queryable
        result = self.client.execute_command("TS.RANGE", new_key, 0, "+")
        assert len(result) == 1
        assert result[0][0] == self.start_ts
        
        # Verify the key is indexed correctly
        keys = self.client.execute_command("TS.QUERYINDEX", "key={}".format(new_key))
        assert len(keys) == 1
        assert keys[0] == new_key
        
        keys = self.client.execute_command("TS.QUERYINDEX", 'sensor="temp"')
        assert len(keys) == 1
        assert keys[0] == new_key

    def test_restore_event(self):
        """Test that a restored series is properly reindexed."""

        self.setup_data()

        key = "ts:restore"
        self.create_ts(key)

        # Dump the key
        dumped = self.client.dump(key)
        self.client.delete(key)

        # Restore the key
        self.client.restore(key, 0, dumped)

        # Query the key to ensure it was restored and indexed correctly
        keys = self.client.execute_command("TS.QUERYINDEX", "key={}".format(key))
        assert len(keys) == 1
        assert keys[0] == key

        keys = self.client.execute_command("TS.QUERYINDEX", 'sensor="temp"')
        assert len(keys) == 1


        # The key should be queryable
        result = self.client.execute_command("TS.RANGE", key, 0, "+")
        assert len(result) == 1
        assert result[0][0] == self.start_ts

    def test_move_key_between_dbs(self):
        """Test that a key moved between databases is correctly reindexed."""

        self.setup_data()

        key = "ts:move"
        self.create_ts(key)

        # Move key to db 1
        self.client.move(key, 1)

        # Key should not exist in db 0
        assert not self.client.exists(key)

        # should not be able to find it in db 1 by index
        key = self.client.execute_command("TS.QUERYINDEX", "key={}".format(key))
        assert len(key) == 0

        key = self.client.execute_command("TS.QUERYINDEX", "sensor=sensor")
        assert len(key) == 0


        # Key should exist in db 1 and be queryable
        self.client.select(1)
        assert self.client.exists(key)
        result = self.client.execute_command("TS.RANGE", key, 0, "+")
        assert len(result) == 1
        assert result[0][0] == self.start_ts

        # Verify the key is indexed correctly in db 1
        keys = self.client.execute_command("TS.QUERYINDEX", "key={}".format(key))
        assert len(keys) == 1
        assert keys[0] == key

        keys = self.client.execute_command("TS.QUERYINDEX", "sensor=sensor")
        assert len(keys) == 1
        assert keys[0] == key

        # Return to db 0 for other tests
        self.client.select(0)

    def test_flushdb_event(self):
        """Test that the index is cleared when the database is flushed."""

        self.setup_data()

        # Create multiple keys
        keys = ["ts:flush1", "ts:flush2", "ts:flush3"]
        for i, key in enumerate(keys):
            self.create_ts(key, self.start_ts + i, float(i))

        # All keys should exist
        for key in keys:
            assert self.client.exists(key)

        keys = self.client.execute_command("TS.QUERYINDEX", 'key=~"ts:flush*"')
        assert len(keys) == 3

        # Flush the database
        self.client.flushdb()

        # No keys should exist
        for key in keys:
            assert not self.client.exists(key)

        # the index should be empty
        keys = self.client.execute_command("TS.QUERYINDEX", 'key=~"ts:flush*"')
        assert len(keys) == 0

        keys = self.client.execute_command("TS.QUERYINDEX", 'sensor="temp"')
        assert len(keys) == 0

        # Create a new key - should work without index interference
        new_key = "ts:new"
        self.create_ts(new_key)
        result = self.client.execute_command("TS.RANGE", new_key, 0, "+")
        assert len(result) == 1

    def test_expire_event(self):
        """Test that an expired series is removed from the index."""

        self.setup_data()

        key = "ts:expire"
        self.create_ts(key)

        # Set the key to expire in 1 second
        self.client.expire(key, 1)

        # Wait for the key to expire
        time.sleep(2)

        # The key should be gone
        assert not self.client.exists(key)
        
        # The key should not be queryable
        keys = self.client.execute_command("TS.QUERYINDEX", "key={}".format(key))
        assert len(keys) == 0
        keys = self.client.execute_command("TS.QUERYINDEX", 'sensor="temp"')
        assert len(keys) == 0

        # Create the key again - should succeed without index interference
        self.create_ts(key, self.start_ts + 1, 2.0)
        result = self.client.execute_command("TS.RANGE", key, 0, "+")
        assert len(result) == 1
        assert result[0][0] == self.start_ts + 1

    def test_concurrent_events(self):
        """Test handling of concurrent server events."""

        self.setup_data()

        # Create multiple keys
        keys = ["ts:concurrent1", "ts:concurrent2", "ts:concurrent3"]
        for i, key in enumerate(keys):
            self.create_ts(key, self.start_ts + i, float(i))

        # Perform multiple operations in quick succession
        # 1. Rename the first key
        self.client.rename(keys[0], "ts:renamed")

        # 2. Move the second key to db 1
        self.client.move(keys[1], 1)

        # 3. Delete the third key
        self.client.delete(keys[2])

        # Verify the state after all operations
        assert not self.client.exists(keys[0])
        assert self.client.exists("ts:renamed")

        assert not self.client.exists(keys[1])
        self.client.select(1)
        assert self.client.exists(keys[1])
        self.client.select(0)

        assert not self.client.exists(keys[2])

        # All remaining keys should be queryable
        result = self.client.execute_command("TS.RANGE", "ts:renamed", 0, "+")
        assert len(result) == 1
        assert result[0][0] == self.start_ts

        self.client.select(1)
        result = self.client.execute_command("TS.RANGE", keys[1], 0, "+")
        assert len(result) == 1
        assert result[0][0] == self.start_ts + 1
        self.client.select(0)