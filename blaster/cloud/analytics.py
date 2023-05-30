import metrohash
from ..config import BQ_ANALYTICS_CLIENT_POOL_NAME, BQ_USER_PROPERTIES_TABLE,\
	BQ_USER_EVENT_TABLE
from ..connection_pool import use_connection_pool
from ..tools import background_task, cur_ms, LRUCache
from collections import deque

DEFAULT_EMPTY_PARAMS = {"": ""}

_table_pending_data_to_push = {}


@use_connection_pool(bq_client=BQ_ANALYTICS_CLIENT_POOL_NAME)
def bq_push(table_id, bq_client=None):
	l = len(q := _table_pending_data_to_push[table_id])
	if(l > 0):
		return bq_client.insert_rows_json(table_id, [q.popleft() for _ in range(l)])


def bq_insert_rows(table_id, rows):
	if((_pending_to_push := _table_pending_data_to_push.get(table_id)) == None):
		_pending_to_push = _table_pending_data_to_push[table_id] = deque()
	_pending_to_push.extend(rows)
	return bq_push(table_id)  # deferred call


@background_task
def TRACK_EVENT(table_id, rows, ns=None):
	if(ns):
		table_id += f"_{ns}"
	# collect all rows to push into a bucket to batch	
	bq_insert_rows(table_id, rows if isinstance(rows, list) else [rows])


# property_id cannot contain ":"
# synchronously added to queue
# each ns has it's own ttl/partition retention policy
'''
CREATE TABLE `PROJECT.<<BQ_USER_PROPERTIES_TABLE>>`
(
	user_id STRING(100) NOT NULL,
	property STRING(100) NOT NULL,
	value STRING,
	timestamp TIMESTAMP NOT NULL
)
PARTITION BY DATE(timestamp)
'''


def TRACK_USER_PROPERTY(user_id, property_id, value, timestamp=None, ns=None):
	if(not BQ_USER_PROPERTIES_TABLE):
		return
	bq_insert_rows(
		BQ_USER_PROPERTIES_TABLE,
		{
			"user_id": user_id,
			"property": property_id,
			"value": value and str(value),
			"timestamp": (timestamp or cur_ms()) // 1000
		},
		ns=ns
	)


'''
CREATE TABLE `sukhiba-e4413.user_analytics.user_events`
(
	user_id STRING(100) NOT NULL,
	event STRING(100) NOT NULL,
	param STRING(100) NOT NULL,
	value STRING(100),
	timestamp TIMESTAMP NOT NULL
)
PARTITION BY DATE(timestamp);
OPTIONS(
	require_partition_filter=true
);
'''


# defered added to queue
def TRACK_USER_EVENT(user_id, event_id, params=None, timestamp=None, ns=None):
	if(not BQ_USER_EVENT_TABLE):
		return
	params = params or DEFAULT_EMPTY_PARAMS
	timestamp = (timestamp or cur_ms()) // 1000
	rows = [
		{
			"user_id": user_id,
			"event": event_id,
			"param": str(k) if k else None,
			"value": str(v) if v else None,
			"timestamp": timestamp
		} for k, v in params.items()
	]
	TRACK_EVENT(BQ_USER_EVENT_TABLE, rows, ns=ns)


# Experimentation
_user_already_tracked = LRUCache(10000)  # to reduce number of LOGS
# Logs event
INT64_MAX = 9223372036854775807


def TRACK_USER_EXPERIMENT(user_id, experiment_id, rollout=100, num_variants=2):
	# consistent hash 0-INT64_MAX
	user_exp_str = f"{experiment_id}{user_id}"
	key = metrohash.hash64_int(user_exp_str.encode())
	d = key / INT64_MAX
	# which variant ?
	# in each variant only partial rollout , check if we qualify
	if(d < rollout / 100):
		# not in experiment, always base not tracked
		variant = key % num_variants
		if(not _user_already_tracked.get(user_exp_str)):
			_user_already_tracked[user_exp_str] = variant  # TODO: change this
			TRACK_USER_PROPERTY(user_id, experiment_id, variant, ns="exp")
		return variant
	return 0
