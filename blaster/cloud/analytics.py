"""Best-effort BigQuery analytics using the application's gevent task runner."""

from farmhash import FarmHash64
from ..config import BQ_USER_PROPERTIES_TABLE, \
	BQ_USER_EVENT_TABLE, IS_DEV, GCLOUD_CREDENTIALS
from ..connection_pool import use_connection_pool, \
	register_pool_item_generator, get_gcloud_bigquery
from ..logging import LOG_DEBUG, LOG_ERROR
from ..tools import background_task, cur_ms, LRUCache

DEFAULT_EMPTY_PARAMS = {"": ""}
MAX_BATCH_ROWS = 500

# Only rows waiting for a worker live here. Workers detach batches before I/O.
_table_pending_data_to_push = {}
register_pool_item_generator("blaster_analytics_bq_client", get_gcloud_bigquery)


@use_connection_pool(bq_client="blaster_analytics_bq_client")
def _bq_insert_batches(table_id, rows, bq_client=None):
	errors = []
	for offset in range(0, len(rows), MAX_BATCH_ROWS):
		batch_errors = bq_client.insert_rows_json(
			table_id, rows[offset:offset + MAX_BATCH_ROWS]
		)
		# BigQuery reports row indices relative to each request.
		for error in batch_errors or ():
			errors.append({**error, "index": error["index"] + offset})
	return errors


def bq_insert_rows(table_id, rows: dict | list[dict]):
	"""Insert synchronously; return BigQuery errors with input row indices."""
	if(isinstance(rows, dict)):
		rows = [rows]
	if(not rows):
		return []
	if(not GCLOUD_CREDENTIALS):
		log = LOG_DEBUG if IS_DEV else LOG_ERROR
		log(
			"bq_insert_rows", desc="BigQuery not configured for tracking events",
			table_id=table_id, rows=str(rows)
		)
		return
	return _bq_insert_batches(table_id, rows)


def TRACK_EVENT(table_id, rows: dict | list[dict]):
	"""Queue rows, coalescing submissions by table until a worker starts.

	Uses the application's background runner, including its shutdown drain.
	Delivery is best effort; failed inserts are logged, not retried here.
	"""
	rows = [rows] if isinstance(rows, dict) else rows
	if(not rows):
		return
	pending = _table_pending_data_to_push.get(table_id)
	if(pending is not None):
		pending.extend(rows)
		return
	# No yielding between looking up and publishing the batch in gevent.
	_table_pending_data_to_push[table_id] = list(rows)
	try:
		_flush_table(table_id)
	except Exception:
		_table_pending_data_to_push.pop(table_id, None)
		raise


@background_task
def _flush_table(table_id):
	rows = _table_pending_data_to_push.pop(table_id)
	errors = bq_insert_rows(table_id, rows)
	if(errors):
		LOG_ERROR(
			"bq_insert_rows", desc=f"errors: {errors}",
			table_id=table_id, rows=str(rows)
		)


def _timestamp_seconds(timestamp):
	return (cur_ms() if timestamp is None else timestamp) // 1000


def _string_value(value):
	return None if value is None else str(value)


def TRACK_USER_PROPERTY(user_id, property_id, value, timestamp=None):
	"""Synchronously record a property; timestamp is in milliseconds."""
	if(not BQ_USER_PROPERTIES_TABLE):
		return
	errors = bq_insert_rows(
		BQ_USER_PROPERTIES_TABLE,
		{
			"user_id": user_id,
			"property": property_id,
			"value": _string_value(value),
			"timestamp": _timestamp_seconds(timestamp)
		}
	)
	if(errors):
		LOG_ERROR(
			"bq_insert_rows", desc=f"errors: {errors}",
			table_id=BQ_USER_PROPERTIES_TABLE
		)


def TRACK_USER_EVENT(user_id, event_id, params=None, timestamp=None):
	"""Queue one row per event parameter; timestamp is in milliseconds."""
	if(not BQ_USER_EVENT_TABLE):
		return
	params = params or DEFAULT_EMPTY_PARAMS
	timestamp = _timestamp_seconds(timestamp)
	rows = [
		{
			"user_id": user_id,
			"event": event_id,
			"param": str(k),
			"value": _string_value(v),
			"timestamp": timestamp
		} for k, v in params.items()
	]
	TRACK_EVENT(BQ_USER_EVENT_TABLE, rows)


_user_already_tracked = LRUCache(10000)
INT64_MAX = 9223372036854775807


def TRACK_USER_EXPERIMENT(user_id, experiment_id, rollout=100, num_variants=2):
	# Preserve the legacy hash and rollout scale to keep existing assignments.
	key = FarmHash64(f"{experiment_id}{user_id}")
	if(key / INT64_MAX < rollout / 100):
		variant = key % num_variants
		cache_key = (user_id, experiment_id)
		if(_user_already_tracked.get(cache_key) is None):
			TRACK_USER_PROPERTY(user_id, experiment_id, variant)
			_user_already_tracked[cache_key] = variant
		return variant
	return 0
