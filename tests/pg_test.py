import blaster  # gevent monkey-patch must happen first

import unittest
import time

from blaster.pg_orm import (
	Model, Attribute, INDEX,
	ASCENDING, DESCENDING,
	DatabaseNode, initialize_postgres,
)
from blaster.tools import get_random_id

# ── DB connection ─────────────────────────────────────────────────────────────

# ── Models ────────────────────────────────────────────────────────────────────

class User(Model):
	_table_name_ = "pg_test_users"

	id    = Attribute(str, column=True)
	name  = Attribute(str, column=True)
	age   = Attribute(int, default=0, column=True)
	score = Attribute(float, default=0.0, column=True)
	meta  = Attribute(dict)
	tags  = Attribute(list)

	INDEX((id, ASCENDING), {"unique": True})
	INDEX(name, {"unique": False})
	INDEX((age, DESCENDING), {"unique": False})


class UserAddress(Model):
	_table_name_ = "pg_test_user_addresses"

	id      = Attribute(str, column=True)
	user_id = Attribute(str, column=True)
	city    = Attribute(str, column=True)
	street  = Attribute(str, column=True)

	INDEX((id, ASCENDING), {"unique": True})
	INDEX(user_id, {"unique": False})


class UserAddress(Model):
	_table_name_ = "pg_test_user_addresses"

	id      = Attribute(str, column=True)
	user_id = Attribute(str, column=True)
	city    = Attribute(str, column=True)
	street  = Attribute(str, column=True)

	INDEX((id, ASCENDING), {"unique": True})
	INDEX(user_id, {"unique": False})


class Post(Model):
	_table_name_ = "pg_test_posts"

	user_id = Attribute(str, column=True)
	title   = Attribute(str, column=True)
	content = Attribute(str, column=True)

	INDEX(user_id, {"unique": False})


class DefaultIdModel(Model):
	"""A Mongo-compatible model that relies on its declared _id index."""
	_table_name_ = "pg_test_default_ids"

	_id = Attribute(str)
	name = Attribute(str, column=True)


class MongoStyleModel(Model):
	"""
	A model written against mongo_orm: names its table `_collection_name_`, and
	declares several indexes in a single INDEX() call. Its only declared index
	is non-unique, so the primary key falls back to the declared `_id`.
	"""
	_collection_name_ = "pg_test_mongo_style"

	_id = Attribute(str)
	session_id = Attribute(str)
	user_id = Attribute(str)
	created_at = Attribute(int)
	data = Attribute(dict)

	INDEX(
		(session_id, user_id, {"unique": False}),
		((created_at, -1), {"unique": False}),
	)



initialize_postgres(
	dict(
		host="localhost",
		port=5499,
		user="postgres",
		password="postgres",
		db_name="postgres"
	)
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def uid():
	return get_random_id()


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestSetup(unittest.TestCase):
	@classmethod
	def setUpClass(cls):
		User.create_table()

	@classmethod
	def tearDownClass(cls):
		with User._db_node_.use_conn() as conn:
			with conn.cursor() as cur:
				cur.execute("DROP TABLE IF EXISTS pg_test_users")
			conn.commit()


class TestBasicCRUD(TestSetup):
	def test_insert_and_get(self):
		_id = uid()
		u = User(id=_id, name="Alice", age=30)
		u.commit()

		fetched = User.get(id=_id)
		self.assertIsNotNone(fetched)
		self.assertEqual(fetched.name, "Alice")
		self.assertEqual(fetched.age, 30)
		self.assertEqual(User.get(_id, use_cache=False).id, _id)

	def test_get_missing_returns_none(self):
		self.assertIsNone(User.get(id="does-not-exist"))

	def test_delete(self):
		u = User(id=uid(), name="ToDelete")
		u.commit()
		u.delete()
		self.assertIsNone(User.get(id=u.id))

	def test_query_equality(self):
		_id = uid()
		User(id=_id, name="QueryMe", age=55).commit()
		results = list(User.query({"name": "QueryMe"}))
		self.assertTrue(any(r.id == _id for r in results))

	def test_query_operator_gt(self):
		_id1, _id2 = uid(), uid()
		User(id=_id1, name="Low", age=10).commit()
		User(id=_id2, name="High", age=90).commit()
		results = list(User.query({"age": {"$gt": 80}}))
		ids = [r.id for r in results]
		self.assertIn(_id2, ids)
		self.assertNotIn(_id1, ids)

	def test_query_in_operator(self):
		_id1, _id2, _id3 = uid(), uid(), uid()
		User(id=_id1, name="A1", age=1).commit()
		User(id=_id2, name="A2", age=2).commit()
		User(id=_id3, name="A3", age=3).commit()
		results = list(User.query({"id": {"$in": [_id1, _id3]}}))
		ids = {r.id for r in results}
		self.assertEqual(ids, {_id1, _id3})

	def test_query_limit_offset(self):
		prefix = uid()
		for i in range(5):
			User(id=f"{prefix}_{i}", name=f"Page_{i}", age=i).commit()
		results = list(User.query({"name": {"$gte": "Page_"}}, sort=[("age", ASCENDING)], limit=3, offset=1))
		# with limit/offset we just check count is bounded
		self.assertLessEqual(len(results), 3)

	def test_dont_update_empty_fields(self):
		u = User(id=uid(), name="EmptyList")
		u.commit()
		self.assertEqual(u._row_["__"].get("meta"), None)


class TestSetAttribute(TestSetup):
	def test_field_update_tracked(self):
		u = User(id=uid(), name="Before", age=10)
		u.commit()
		u.name = "After"
		u.commit()

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.name, "After")
		# age unchanged
		self.assertEqual(fetched.age, 10)

	def test_multiple_field_updates(self):
		u = User(id=uid(), name="Multi", age=1, score=1.0)
		u.commit()
		u.name = "MultiUpdated"
		u.age = 99
		u.score = 3.14
		u.commit()

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.name, "MultiUpdated")
		self.assertEqual(fetched.age, 99)
		self.assertAlmostEqual(fetched.score, 3.14, places=2)


class TestDictTracking(TestSetup):
	def test_top_level_dict_set(self):
		u = User(id=uid(), meta={"city": "NYC"})
		u.commit()
		u.meta["city"] = "LA"
		u.commit()

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.meta["city"], "LA")

	def test_nested_dict_path_patch(self):
		u = User(id=uid(), meta={"addr": {"city": "NYC", "zip": "10001"}})
		u.commit()
		u.meta["addr"]["city"] = "SF"
		u.commit()

		fetched = User.get(id=u.id)
		# Only city changed; zip must be intact
		self.assertEqual(fetched.meta["addr"]["city"], "SF")
		self.assertEqual(fetched.meta["addr"]["zip"], "10001")

	def test_dict_key_unset(self):
		u = User(id=uid(), meta={"keep": "yes", "drop": "no"})
		u.commit()
		del u.meta["drop"]
		u.commit()

		fetched = User.get(id=u.id)
		self.assertIn("keep", fetched.meta)
		self.assertNotIn("drop", fetched.meta)

	def test_parent_path_supersedes_child(self):
		"""Setting a parent dict should supersede any pending child updates."""
		u = User(id=uid(), meta={"a": {"b": {"c": "old"}}})
		u.commit()
		u.meta["a"]["b"]["c"] = "intermediate"   # records path (meta, a, b, c)
		u.meta["a"]["b"] = {"d": "final"}         # supersedes (meta, a, b, c)
		u.commit()

		fetched = User.get(id=u.id)
		self.assertNotIn("c", fetched.meta["a"]["b"])
		self.assertEqual(fetched.meta["a"]["b"]["d"], "final")

	def test_replace_whole_dict(self):
		u = User(id=uid(), meta={"old": 1})
		u.commit()
		u.meta = {"new": 2}
		u.commit()

		fetched = User.get(id=u.id)
		self.assertNotIn("old", fetched.meta)
		self.assertEqual(fetched.meta["new"], 2)


class TestListTracking(TestSetup):
	def test_list_append(self):
		u = User(id=uid(), tags=[])
		u.commit()
		u.tags.append("a")
		u.tags.append("b")
		u.commit()

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.tags, ["a", "b"])

	def test_list_insert(self):
		u = User(id=uid(), tags=["b", "c"])
		u.commit()
		u.tags.insert(0, "a")
		u.commit()

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.tags, ["a", "b", "c"])

	def test_list_remove(self):
		u = User(id=uid(), tags=["a", "b", "c"])
		u.commit()
		u.tags.remove("b")
		u.commit()

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.tags, ["a", "c"])

	def test_list_pop(self):
		u = User(id=uid(), tags=["x", "y", "z"])
		u.commit()
		popped = u.tags.pop()
		self.assertEqual(popped, "z")
		u.commit()

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.tags, ["x", "y"])

	def test_list_multiple_ops_before_commit(self):
		u = User(id=uid(), tags=[])
		u.commit()
		u.tags.append(1)
		u.tags.append(2)
		u.tags.append(3)
		u.tags.remove(2)
		u.commit()

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.tags, [1, 3])

	def test_list_on_new_object(self):
		_id = uid()
		u = User(id=_id)
		u.tags.append("first")
		u.commit()

		fetched = User.get(id=_id)
		self.assertEqual(fetched.tags, ["first"])


class TestOptimisticLocking(TestSetup):
	def test_concurrent_update_retries_and_succeeds(self):
		"""Stale copy retries after lock conflict and succeeds (last-write-wins)."""
		u = User(id=uid(), name="Locked", age=1)
		u.commit()

		copy1 = User.get(id=u.id)
		copy2 = User.get(id=u.id)

		copy1.name = "Winner"
		copy1.commit()

		# copy2 is stale but the retry should fetch the fresh _ and succeed
		copy2.name = "Loser"
		copy2.commit()  # should not raise

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.name, "Loser")

	def test_successful_sequential_updates(self):
		u = User(id=uid(), name="Seq", age=0)
		u.commit()
		for i in range(1, 4):
			u.age = i
			u.commit()

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.age, 3)


class TestExplicitUpdate(TestSetup):
	def test_set_operator(self):
		u = User(id=uid(), name="Before", age=10)
		u.commit()
		u.update({"$set": {"name": "After", "age": 99}})

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.name, "After")
		self.assertEqual(fetched.age, 99)

	def test_unset_operator(self):
		u = User(id=uid(), meta={"keep": 1, "drop": 2})
		u.commit()
		u.update({"$unset": {"meta.drop": 1}})

		fetched = User.get(id=u.id)
		self.assertIn("keep", fetched.meta)
		self.assertNotIn("drop", fetched.meta)

	def test_inc_operator(self):
		u = User(id=uid(), score=10.0)
		u.commit()
		u.update({"$inc": {"score": 5}})

		# Local state refreshed automatically after $inc
		self.assertAlmostEqual(u.score, 15.0, places=1)

		fetched = User.get(id=u.id)
		self.assertAlmostEqual(fetched.score, 15.0, places=1)

	def test_inc_from_zero(self):
		"""$inc on a field with no existing value defaults to 0."""
		_id = uid()
		User(id=_id, name="IncZero").commit()
		u = User.get(id=_id)
		u.update({"$inc": {"age": 7}})

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.age, 7)

	def test_set_and_inc_combined(self):
		u = User(id=uid(), name="Old", score=5.0)
		u.commit()
		u.update({"$set": {"name": "New"}, "$inc": {"score": 10}})

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.name, "New")
		self.assertAlmostEqual(fetched.score, 15.0, places=1)

	def test_nested_set(self):
		u = User(id=uid(), meta={"a": 1, "b": 2})
		u.commit()
		u.update({"$set": {"meta.b": 99}})

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.meta["a"], 1)
		self.assertEqual(fetched.meta["b"], 99)

	def test_extra_conditions_match(self):
		"""Update succeeds when extra condition matches."""
		u = User(id=uid(), name="CondOK", age=10)
		u.commit()
		u.update({"$set": {"name": "Updated"}}, conditions={"age": "10"})

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.name, "Updated")

	def test_extra_conditions_no_match_raises(self):
		"""Update raises when extra condition doesn't match (treated as lock conflict)."""
		u = User(id=uid(), name="CondFail", age=10)
		u.commit()
		self.assertFalse(
			u.update({"$set": {"name": "ShouldNotUpdate"}}, conditions={"age": "999"})
		)

	def test_no_op_when_empty(self):
		"""_update with empty updates dict does nothing."""
		u = User(id=uid(), name="NoOp")
		u.commit()
		original_ts = u._
		u.update({})
		self.assertEqual(u._, original_ts)

	def test_push_to_existing_array(self):
		"""$push appends an element to an existing JSONB array."""
		u = User(id=uid(), tags=["a", "b"])
		u.commit()
		u.update({"$push": {"tags": "c"}})

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.tags, ["a", "b", "c"])

	def test_push_to_missing_array_defaults_to_empty(self):
		"""$push on a field that doesn't exist yet creates a one-element array."""
		u = User(id=uid(), name="PushNew")
		u.commit()
		u.update({"$push": {"tags": "first"}})

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.tags, ["first"])

	def test_push_multiple_times(self):
		"""Multiple $push calls accumulate elements in order."""
		u = User(id=uid(), tags=[])
		u.commit()
		u.update({"$push": {"tags": 1}})
		u.update({"$push": {"tags": 2}})
		u.update({"$push": {"tags": 3}})

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.tags, [1, 2, 3])

	def test_push_dict_element(self):
		"""$push works with dict values (complex JSONB elements)."""
		u = User(id=uid(), meta={"events": []})
		u.commit()
		u.update({"$push": {"meta.events": {"type": "login", "ts": 123}}})

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.meta["events"], [{"type": "login", "ts": 123}])

	def test_push_combined_with_set(self):
		"""$push and $set in the same call both take effect."""
		u = User(id=uid(), name="Before", tags=["x"])
		u.commit()
		u.update({"$set": {"name": "After"}, "$push": {"tags": "y"}})

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.name, "After")
		self.assertEqual(fetched.tags, ["x", "y"])

	def test_push_local_state_refreshed(self):
		"""Local object state is refreshed after $push."""
		u = User(id=uid(), tags=["a"])
		u.commit()
		u.update({"$push": {"tags": "b"}})

		self.assertEqual(u.tags, ["a", "b"])


class TestPkFromIndex(TestSetup):
	def test_pk_derived_from_unique_index(self):
		self.assertEqual(User._pk_attrs_, ["id"])

	def test_indexes_populated(self):
		index_names = {spec["name"] for spec in User._indexes_}
		self.assertIn("pg_test_users_id_asc", index_names)
		self.assertIn("pg_test_users_name_asc", index_names)
		self.assertIn("pg_test_users_age_desc", index_names)


class TestDefaultIdIndex(unittest.TestCase):
	@classmethod
	def setUpClass(cls):
		DefaultIdModel.create_table()

	@classmethod
	def tearDownClass(cls):
		with DefaultIdModel._db_node_.use_conn() as conn:
			with conn.cursor() as cur:
				cur.execute("DROP TABLE IF EXISTS pg_test_default_ids")
			conn.commit()

	def test_declared_id_gets_unique_index_and_supports_positional_get(self):
		self.assertEqual(DefaultIdModel._pk_attrs_, ["_id"])
		self.assertEqual(len(DefaultIdModel._indexes_), 1)
		self.assertTrue(DefaultIdModel._indexes_[0]["unique"])

		item = DefaultIdModel(_id=uid(), name="Mongo-compatible")
		item.commit()
		self.assertEqual(DefaultIdModel.get(item._id).name, "Mongo-compatible")


class TestCallbackSetup(unittest.TestCase):
	@classmethod
	def setUpClass(cls):
		User.create_table()
		UserAddress.create_table()

	@classmethod
	def tearDownClass(cls):
		with User._db_node_.use_conn() as conn:
			with conn.cursor() as cur:
				cur.execute("DROP TABLE IF EXISTS pg_test_user_addresses")
				cur.execute("DROP TABLE IF EXISTS pg_test_users")
			conn.commit()


class TestUpdateCallback(TestCallbackSetup):
	def test_callback_success(self):
		"""Callback succeeds: both the address update and the user update persist."""
		user = User(id=uid(), name="Alice", age=30).commit()
		addr = UserAddress(id=uid(), user_id=user.id, city="NYC", street="5th Ave").commit()

		def on_addr_updated(address, result):
			user.update({"$set": {"name": "AliceUpdated"}})

		addr.update({"$set": {"city": "LA"}}, callback=on_addr_updated)

		self.assertEqual(UserAddress.get(id=addr.id).city, "LA")
		self.assertEqual(User.get(id=user.id).name, "AliceUpdated")

	def test_callback_failure_rolls_back_address(self):
		"""Callback raises: address update is rolled back, user is untouched."""
		user = User(id=uid(), name="Bob", age=25).commit()
		addr = UserAddress(id=uid(), user_id=user.id, city="Boston", street="Main St").commit()

		def failing_callback(address, result):
			if(not user.update({"$set": {"name": "BobUpdated"}}, conditions={"age": "26"})):
				raise Exception("something went wrong")

		with self.assertRaises(Exception):
			addr.update({"$set": {"city": "Chicago"}}, callback=failing_callback)

		# address rolled back — city must still be "Boston"
		self.assertEqual(UserAddress.get(id=addr.id).city, "Boston")
		# user was never touched
		self.assertEqual(User.get(id=user.id).name, "Bob")


class TestLock(TestSetup):
	def test_two_threads_lock_sequentially(self):
		"""Two threads acquiring a lock on the same DB record process one after the other."""
		import threading

		u = User(id=uid(), name="Lockable", age=0)
		u.commit()

		order = []
		errors = []

		def worker(thread_id):
			try:
				obj = User.get(id=u.id)
				with obj.lock(timeout=10000):
					order.append(f"t{thread_id}_start")
					time.sleep(1)  # hold the lock briefly
					order.append(f"t{thread_id}_end")
			except Exception as e:
				errors.append(e)

		t1 = threading.Thread(target=worker, args=(1,))
		t2 = threading.Thread(target=worker, args=(2,))
		t1.start()
		time.sleep(0.01)  # give t1 a head start so it acquires the lock first
		t2.start()
		t1.join(timeout=15)
		t2.join(timeout=15)

		self.assertFalse(errors, errors)
		self.assertEqual(len(order), 4)

		# Whichever thread went first, its _end must come before the other's _start
		first_thread = order[0][1]  # '1' or '2'
		second_thread = '2' if first_thread == '1' else '1'
		self.assertEqual(order, [
			f"t{first_thread}_start",
			f"t{first_thread}_end",
			f"t{second_thread}_start",
			f"t{second_thread}_end",
		])


class TestMongoStyleDeclarations(unittest.TestCase):
	"""Models shared with mongo_orm must work unchanged against pg_orm."""

	@classmethod
	def setUpClass(cls):
		MongoStyleModel.create_table()

	@classmethod
	def tearDownClass(cls):
		with MongoStyleModel._db_node_.use_conn() as conn:
			with conn.cursor() as cur:
				cur.execute("DROP TABLE IF EXISTS pg_test_mongo_style")
			conn.commit()

	def test_collection_name_is_used_as_table_name(self):
		self.assertEqual(MongoStyleModel._table_name_, "pg_test_mongo_style")

	def test_one_index_call_declares_several_indexes(self):
		names = {spec["name"] for spec in MongoStyleModel._indexes_}
		# the two declared indexes, plus the unique one on the fallback pk
		self.assertIn("pg_test_mongo_style_session_id_asc_user_id_asc", names)
		self.assertIn("pg_test_mongo_style_created_at_desc", names)
		self.assertIn("pg_test_mongo_style__id_asc", names)

	def test_pk_falls_back_to_declared_id_when_no_unique_index(self):
		self.assertEqual(MongoStyleModel._pk_attrs_, ["_id"])
		# the declared Attribute is kept — not replaced by a BIGSERIAL
		self.assertIs(MongoStyleModel._attrs_["_id"].type, str)

	def test_index_names_are_truncated_to_postgres_limit(self):
		for spec in MongoStyleModel._indexes_:
			self.assertLessEqual(len(spec["name"]), 63)

	def test_get_many_by_list(self):
		items = [MongoStyleModel(_id=uid(), session_id="s1").commit() for _ in range(3)]
		ids = [i._id for i in items]
		fetched = MongoStyleModel.get(ids + ["missing-id"])
		self.assertEqual(sorted(i._id for i in fetched), sorted(ids))
		self.assertEqual(MongoStyleModel.get([]), [])

	def test_set_creates_missing_parent_objects(self):
		item = MongoStyleModel(_id=uid(), session_id="s2").commit()
		# `data` holds no `agent` key yet — mongo would create the intermediate doc
		item.update({"$set": {"data.agent.disabled_until": 42}})
		self.assertEqual(
			MongoStyleModel.get(item._id).data, {"agent": {"disabled_until": 42}}
		)

	def test_inc_creates_missing_parent_objects(self):
		item = MongoStyleModel(_id=uid(), session_id="s3").commit()
		item.update({"$inc": {"data.retries": 1}})
		item.update({"$inc": {"data.retries": 1}})
		self.assertEqual(MongoStyleModel.get(item._id).data, {"retries": 2})

	def test_push_creates_missing_parent_objects(self):
		item = MongoStyleModel(_id=uid(), session_id="s4").commit()
		item.update({"$push": {"data.events": "a"}})
		item.update({"$push": {"data.events": "b"}})
		self.assertEqual(MongoStyleModel.get(item._id).data, {"events": ["a", "b"]})

	def test_update_reflects_into_the_object_without_refetching(self):
		'''update() must leave the object reporting what actually landed.'''
		item = MongoStyleModel(_id=uid(), session_id="s7", data={"keep": 1}).commit()

		item.update({"$set": {"data.verified": True, "session_id": "s7-renamed"}})
		# no re-read here — the object itself has to be current
		self.assertEqual(item.data, {"keep": 1, "verified": True})
		self.assertEqual(item.session_id, "s7-renamed")

		item.update({"$inc": {"data.hits": 2}})
		self.assertEqual(item.data["hits"], 2)

		item.update({"$push": {"data.events": "a"}})
		self.assertEqual(item.data["events"], ["a"])

		item.update({"$unset": {"data.keep": 1}})
		self.assertNotIn("keep", item.data)

		# and it matches what another reader sees
		self.assertEqual(MongoStyleModel.get(item._id).data, item.data)

	def test_update_reflects_a_concurrent_inc(self):
		'''$inc is computed by the DB, so the object takes the DB's answer.'''
		item = MongoStyleModel(_id=uid(), session_id="s8").commit()
		MongoStyleModel.get(item._id).update({"$inc": {"data.hits": 5}})

		item.update({"$inc": {"data.hits": 1}})
		self.assertEqual(item.data["hits"], 6)

	def test_commit_of_tracked_changes_reflects(self):
		item = MongoStyleModel(_id=uid(), session_id="s9").commit()
		loaded = MongoStyleModel.get(item._id)
		loaded.data["via"] = "attribute"
		loaded.commit()
		self.assertEqual(loaded.data, {"via": "attribute"})
		self.assertEqual(MongoStyleModel.get(item._id).data, {"via": "attribute"})

	def test_create_table_adds_a_column_the_table_is_missing(self):
		'''
		Adding a column=True attribute to a model that already has a table must
		reach the table — CREATE TABLE IF NOT EXISTS alone would leave it out and
		every later SELECT would fail on the missing column.
		'''
		table = MongoStyleModel._table_name_
		with MongoStyleModel._db_node_.use_conn() as conn:
			with conn.cursor() as cur:
				cur.execute(f"ALTER TABLE {table} DROP COLUMN IF EXISTS session_id")
			conn.commit()

		MongoStyleModel.create_table()

		item = MongoStyleModel(_id=uid(), session_id="restored").commit()
		self.assertEqual(MongoStyleModel.get(item._id).session_id, "restored")

	def test_query_by_dotted_jsonb_path(self):
		a = MongoStyleModel(_id=uid(), session_id="s6", data={"valid_until": 100}).commit()
		MongoStyleModel(_id=uid(), session_id="s6", data={"valid_until": 900}).commit()
		found = list(MongoStyleModel.query({
			"session_id": "s6", "data.valid_until": {"$lt": 500},
		}))
		self.assertEqual([i._id for i in found], [a._id])

		found = list(MongoStyleModel.query({"data.valid_until": 900}))
		self.assertEqual([i.data["valid_until"] for i in found], [900])

	def test_set_preserves_existing_siblings(self):
		item = MongoStyleModel(_id=uid(), session_id="s5", data={"keep": 1}).commit()
		item.update({"$set": {"data.added": 2}})
		self.assertEqual(
			MongoStyleModel.get(item._id).data, {"keep": 1, "added": 2}
		)


if __name__ == "__main__":
	unittest.main()
