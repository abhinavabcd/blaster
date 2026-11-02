import unittest
from collections import deque
from unittest.mock import Mock, patch

import blaster
from blaster import connection_pool, tools
from blaster.cloud import analytics


class TestAnalytics(unittest.TestCase):
	def setUp(self):
		self.client = Mock()
		self.client.insert_rows_json.return_value = []
		self.tasks = []
		analytics._table_pending_data_to_push.clear()
		analytics._user_already_tracked.clear()
		patches = [
			patch.object(analytics, "GCLOUD_CREDENTIALS", True),
			patch.object(analytics, "BQ_USER_EVENT_TABLE", "events"),
			patch.object(analytics, "BQ_USER_PROPERTIES_TABLE", "properties"),
			patch.object(analytics, "LOG_ERROR"),
			patch.object(analytics, "LOG_DEBUG"),
			patch.object(analytics, "_flush_table", side_effect=self.tasks.append),
			patch.dict(connection_pool._conn_pools, {"blaster_analytics_bq_client": deque()}),
			patch.dict(connection_pool._pool_item_generators, {
				"blaster_analytics_bq_client": lambda: self.client
			}),
		]
		# Save the real worker before replacing its scheduling wrapper.
		self.flush = analytics._flush_table._original
		for patcher in patches:
			patcher.start()
			self.addCleanup(patcher.stop)
		self.addCleanup(analytics._table_pending_data_to_push.clear)
		self.addCleanup(analytics._user_already_tracked.clear)

	def drain(self):
		while self.tasks:
			self.flush(self.tasks.pop(0))

	def test_burst_coalesces_and_caps_requests(self):
		rows = [{"n": n} for n in range(1201)]
		for row in rows:
			analytics.TRACK_EVENT("events", row)
		self.assertEqual(self.tasks, ["events"])
		self.drain()
		batches = [call.args[1] for call in self.client.insert_rows_json.call_args_list]
		self.assertEqual([len(batch) for batch in batches], [500, 500, 201])
		self.assertEqual([row for batch in batches for row in batch], rows)
		self.assertEqual(analytics._table_pending_data_to_push, {})

	def test_tables_are_isolated(self):
		analytics.TRACK_EVENT("events", {"n": 1})
		analytics.TRACK_EVENT("other", [{"n": 2}])
		self.assertEqual(self.tasks, ["events", "other"])
		self.drain()
		self.assertEqual(self.client.insert_rows_json.call_count, 2)

	def test_submission_during_insert_gets_its_own_batch(self):
		def insert(table, rows):
			if rows == [{"n": 1}]:
				analytics.TRACK_EVENT(table, {"n": 2})
			return []
		self.client.insert_rows_json.side_effect = insert
		analytics.TRACK_EVENT("events", {"n": 1})
		self.drain()
		self.assertEqual(
			[call.args[1] for call in self.client.insert_rows_json.call_args_list],
			[[{"n": 1}], [{"n": 2}]]
		)

	def test_empty_input_does_not_schedule_or_acquire_client(self):
		analytics.TRACK_EVENT("events", [])
		self.assertEqual(analytics.bq_insert_rows("events", []), [])
		self.assertEqual(self.tasks, [])
		self.client.insert_rows_json.assert_not_called()

	def test_sync_dict_is_wrapped_in_a_list(self):
		analytics.bq_insert_rows("events", {"n": 1})
		self.client.insert_rows_json.assert_called_once_with("events", [{"n": 1}])

	def test_error_indices_refer_to_original_rows(self):
		error = {"index": 0, "errors": [{"reason": "invalid"}]}
		self.client.insert_rows_json.side_effect = [[], [error]]
		result = analytics.bq_insert_rows("events", [{"n": n} for n in range(501)])
		self.assertEqual(result, [{**error, "index": 500}])
		self.assertEqual(error["index"], 0)

	def test_failed_insert_does_not_strand_future_submissions(self):
		self.client.insert_rows_json.side_effect = RuntimeError("offline")
		analytics.TRACK_EVENT("events", {"n": 1})
		with self.assertRaisesRegex(RuntimeError, "offline"):
			self.drain()
		self.assertEqual(analytics._table_pending_data_to_push, {})
		self.client.insert_rows_json.side_effect = None
		analytics.TRACK_EVENT("events", {"n": 2})
		self.drain()
		self.client.insert_rows_json.assert_called_with("events", [{"n": 2}])

	def test_failed_scheduling_cleans_pending_batch(self):
		analytics._flush_table.side_effect = RuntimeError("runner unavailable")
		with self.assertRaisesRegex(RuntimeError, "runner unavailable"):
			analytics.TRACK_EVENT("events", {"n": 1})
		self.assertEqual(analytics._table_pending_data_to_push, {})

	def test_row_errors_are_logged(self):
		self.client.insert_rows_json.return_value = [{"index": 0, "errors": []}]
		analytics.TRACK_EVENT("events", {"n": 1})
		self.drain()
		analytics.LOG_ERROR.assert_called_once()

	def test_properties_preserve_falsy_values_and_epoch(self):
		for value in (0, False, "", None):
			analytics.TRACK_USER_PROPERTY("u", "p", value, timestamp=0)
			self.client.insert_rows_json.assert_called_with("properties", [{
				"user_id": "u", "property": "p",
				"value": None if value is None else str(value), "timestamp": 0
			}])

	def test_events_preserve_falsy_values_and_default_parameter(self):
		analytics.TRACK_USER_EVENT("u", "e", {"zero": 0, "false": False, "null": None}, timestamp=0)
		analytics.TRACK_USER_EVENT("u", "empty", timestamp=0)
		self.drain()
		rows = self.client.insert_rows_json.call_args.args[1]
		self.assertEqual([row["value"] for row in rows], ["0", "False", None, ""])
		self.assertEqual(rows[-1]["param"], "")
		self.assertTrue(all(row["timestamp"] == 0 for row in rows))

	def test_unconfigured_logging_and_disabled_tables(self):
		with patch.object(analytics, "GCLOUD_CREDENTIALS", None):
			for is_dev in (True, False):
				with patch.object(analytics, "IS_DEV", is_dev):
					analytics.bq_insert_rows("events", {"n": 1})
		analytics.LOG_DEBUG.assert_called_once()
		analytics.LOG_ERROR.assert_called_once()
		with patch.object(analytics, "BQ_USER_EVENT_TABLE", None), \
				patch.object(analytics, "BQ_USER_PROPERTIES_TABLE", None):
			analytics.TRACK_USER_EVENT("u", "e")
			analytics.TRACK_USER_PROPERTY("u", "p", 1)
		self.assertEqual(self.tasks, [])
		self.client.insert_rows_json.assert_not_called()

	def test_variant_zero_is_tracked_once(self):
		with patch.object(analytics, "FarmHash64", return_value=2):
			self.assertEqual(analytics.TRACK_USER_EXPERIMENT("u", "exp"), 0)
			self.assertEqual(analytics.TRACK_USER_EXPERIMENT("u", "exp"), 0)
		self.client.insert_rows_json.assert_called_once()

	def test_experiment_cache_does_not_conflate_concatenated_ids(self):
		with patch.object(analytics, "FarmHash64", return_value=1):
			analytics.TRACK_USER_EXPERIMENT("bc", "a")
			analytics.TRACK_USER_EXPERIMENT("c", "ab")
		self.assertEqual(self.client.insert_rows_json.call_count, 2)

	def test_legacy_assignments_remain_stable(self):
		for key in (0, 1, analytics.INT64_MAX // 2, analytics.INT64_MAX, 2**64 - 1):
			for rollout in (0, 25, 100):
				with patch.object(analytics, "FarmHash64", return_value=key):
					actual = analytics.TRACK_USER_EXPERIMENT("u", "e", rollout=rollout, num_variants=3)
					expected = key % 3 if key / analytics.INT64_MAX < rollout / 100 else 0
					self.assertEqual(actual, expected)

	def test_real_background_runner_drains_on_shutdown(self):
		runner = tools.PartitionedTasksRunner()
		with patch.object(tools, "partitioned_tasks_runner", runner), \
				patch.object(analytics, "_flush_table", tools.background_task(self.flush)):
			try:
				for n in range(1000):
					analytics.TRACK_EVENT("events", {"n": n})
			finally:
				runner.stop()
		batches = [call.args[1] for call in self.client.insert_rows_json.call_args_list]
		self.assertEqual([row["n"] for batch in batches for row in batch], list(range(1000)))
		self.assertEqual(len(batches), 2)
		self.assertEqual(analytics._table_pending_data_to_push, {})


if __name__ == "__main__":
	unittest.main()
