import blaster  # gevent monkey-patch must happen first

import unittest
import time

from blaster.pg_orm import (
	Model, Attribute, INDEX,
	ASCENDING, DESCENDING,
	DatabaseNode, initialize_model,
	OptimisticLockError,
)
from blaster.tools import get_random_id

# ── DB connection ─────────────────────────────────────────────────────────────

_db = DatabaseNode(
	host="localhost",
	port=5499,
	user="postgres",
	password="postgres",
	db_name="postgres",
)

# ── Models ────────────────────────────────────────────────────────────────────

class User(Model):
	_collection_name_ = "pg_test_users"
	_db_node_ = _db

	id    = Attribute(str)
	name  = Attribute(str)
	age   = Attribute(int, default=0)
	score = Attribute(float, default=0.0)
	meta  = Attribute(dict, default={})
	tags  = Attribute(list, default=[])

	INDEX((id, ASCENDING), {"unique": True})
	INDEX((name, ASCENDING))
	INDEX((age, DESCENDING))


initialize_model(User)


# ── Helpers ───────────────────────────────────────────────────────────────────

def uid():
	return get_random_id()


def make_user(**kwargs):
	u = User(id=uid(), **kwargs)
	u.commit()
	return u


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestSetup(unittest.TestCase):
	@classmethod
	def setUpClass(cls):
		User.create_table()

	@classmethod
	def tearDownClass(cls):
		with _db.use_conn() as conn:
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

	def test_get_missing_returns_none(self):
		self.assertIsNone(User.get(id="does-not-exist"))

	def test_delete(self):
		u = make_user(name="ToDelete")
		u.delete()
		self.assertIsNone(User.get(id=u.id))

	def test_query_equality(self):
		_id = uid()
		User(id=_id, name="QueryMe", age=55).commit()
		results = list(User.query({"name": "QueryMe"}))
		self.assertTrue(any(r.id == _id for r in results))

	def test_query_operator_gt(self):
		_id1, _id2 = uid(), uid()
		User(id=_id1, name="Low",  age=10).commit()
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


class TestSetAttribute(TestSetup):
	def test_field_update_tracked(self):
		u = make_user(name="Before", age=10)
		u.name = "After"
		u.commit()

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.name, "After")
		# age unchanged
		self.assertEqual(fetched.age, 10)

	def test_multiple_field_updates(self):
		u = make_user(name="Multi", age=1, score=1.0)
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
		u = make_user(meta={"city": "NYC"})
		u.meta["city"] = "LA"
		u.commit()

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.meta["city"], "LA")

	def test_nested_dict_path_patch(self):
		u = make_user(meta={"addr": {"city": "NYC", "zip": "10001"}})
		u.meta["addr"]["city"] = "SF"
		u.commit()

		fetched = User.get(id=u.id)
		# Only city changed; zip must be intact
		self.assertEqual(fetched.meta["addr"]["city"], "SF")
		self.assertEqual(fetched.meta["addr"]["zip"], "10001")

	def test_dict_key_unset(self):
		u = make_user(meta={"keep": "yes", "drop": "no"})
		del u.meta["drop"]
		u.commit()

		fetched = User.get(id=u.id)
		self.assertIn("keep", fetched.meta)
		self.assertNotIn("drop", fetched.meta)

	def test_parent_path_supersedes_child(self):
		"""Setting a parent dict should supersede any pending child updates."""
		u = make_user(meta={"a": {"b": {"c": "old"}}})
		u.meta["a"]["b"]["c"] = "intermediate"   # records path (meta, a, b, c)
		u.meta["a"]["b"] = {"d": "final"}         # supersedes (meta, a, b, c)
		u.commit()

		fetched = User.get(id=u.id)
		self.assertNotIn("c", fetched.meta["a"]["b"])
		self.assertEqual(fetched.meta["a"]["b"]["d"], "final")

	def test_replace_whole_dict(self):
		u = make_user(meta={"old": 1})
		u.meta = {"new": 2}
		u.commit()

		fetched = User.get(id=u.id)
		self.assertNotIn("old", fetched.meta)
		self.assertEqual(fetched.meta["new"], 2)


class TestListTracking(TestSetup):
	def test_list_append(self):
		u = make_user(tags=[])
		u.tags.append("a")
		u.tags.append("b")
		u.commit()

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.tags, ["a", "b"])

	def test_list_insert(self):
		u = make_user(tags=["b", "c"])
		u.tags.insert(0, "a")
		u.commit()

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.tags, ["a", "b", "c"])

	def test_list_remove(self):
		u = make_user(tags=["a", "b", "c"])
		u.tags.remove("b")
		u.commit()

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.tags, ["a", "c"])

	def test_list_pop(self):
		u = make_user(tags=["x", "y", "z"])
		popped = u.tags.pop()
		self.assertEqual(popped, "z")
		u.commit()

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.tags, ["x", "y"])

	def test_list_multiple_ops_before_commit(self):
		u = make_user(tags=[])
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
	def test_concurrent_update_raises(self):
		u = make_user(name="Locked", age=1)

		copy1 = User.get(id=u.id)
		copy2 = User.get(id=u.id)

		copy1.name = "Winner"
		copy1.commit()

		copy2.name = "Loser"
		with self.assertRaises(OptimisticLockError):
			copy2.commit()

	def test_successful_sequential_updates(self):
		u = make_user(name="Seq", age=0)
		for i in range(1, 4):
			u.age = i
			u.commit()

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.age, 3)


class TestExplicitUpdate(TestSetup):
	def test_set_operator(self):
		u = make_user(name="Before", age=10)
		u._update({"$set": {"name": "After", "age": 99}})

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.name, "After")
		self.assertEqual(fetched.age, 99)

	def test_unset_operator(self):
		u = make_user(meta={"keep": 1, "drop": 2})
		u._update({"$unset": {"meta.drop": 1}})

		fetched = User.get(id=u.id)
		self.assertIn("keep", fetched.meta)
		self.assertNotIn("drop", fetched.meta)

	def test_inc_operator(self):
		u = make_user(score=10.0)
		u._update({"$inc": {"score": 5}})

		# Local state refreshed automatically after $inc
		self.assertAlmostEqual(u.score, 15.0, places=1)

		fetched = User.get(id=u.id)
		self.assertAlmostEqual(fetched.score, 15.0, places=1)

	def test_inc_from_zero(self):
		"""$inc on a field with no existing value defaults to 0."""
		_id = uid()
		User(id=_id, name="IncZero").commit()
		u = User.get(id=_id)
		u._update({"$inc": {"age": 7}})

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.age, 7)

	def test_set_and_inc_combined(self):
		u = make_user(name="Old", score=5.0)
		u._update({"$set": {"name": "New"}, "$inc": {"score": 10}})

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.name, "New")
		self.assertAlmostEqual(fetched.score, 15.0, places=1)

	def test_nested_set(self):
		u = make_user(meta={"a": 1, "b": 2})
		u._update({"$set": {"meta.b": 99}})

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.meta["a"], 1)
		self.assertEqual(fetched.meta["b"], 99)

	def test_extra_conditions_match(self):
		"""Update succeeds when extra condition matches."""
		u = make_user(name="CondOK", age=10)
		u._update({"$set": {"name": "Updated"}}, conditions={"age": "10"})

		fetched = User.get(id=u.id)
		self.assertEqual(fetched.name, "Updated")

	def test_extra_conditions_no_match_raises(self):
		"""Update raises when extra condition doesn't match (treated as lock conflict)."""
		u = make_user(name="CondFail", age=10)
		with self.assertRaises(OptimisticLockError):
			u._update({"$set": {"name": "ShouldNotUpdate"}}, conditions={"age": "999"})

	def test_no_op_when_empty(self):
		"""_update with empty updates dict does nothing."""
		u = make_user(name="NoOp")
		original_ts = u._
		u._update({})
		self.assertEqual(u._, original_ts)


class TestPkFromIndex(TestSetup):
	def test_pk_derived_from_unique_index(self):
		self.assertEqual(User._pk_attrs_, ["id"])

	def test_indexes_populated(self):
		index_names = {spec["name"] for spec in User._indexes_}
		self.assertIn("pg_test_users_id_uniq", index_names)
		self.assertIn("pg_test_users_name", index_names)
		self.assertIn("pg_test_users_age", index_names)


if __name__ == "__main__":
	unittest.main()
