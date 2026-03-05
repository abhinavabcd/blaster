import inspect
import json
from contextlib import contextmanager
from typing import TypeVar, Type, Iterator, Optional

import psycopg2
import psycopg2.pool
import psycopg2.extras

from .tools import cur_ms, all_subclasses
from .logging import LOG_WARN
from .config import IS_TEST

T = TypeVar("T", bound="Model")

ASCENDING = 1
DESCENDING = -1

EVENT_BEFORE_DELETE = -2
EVENT_AFTER_DELETE = -1
EVENT_BEFORE_UPDATE = 1
EVENT_AFTER_UPDATE = 2
EVENT_BEFORE_CREATE = 3
EVENT_AFTER_CREATE = 4
EVENT_ROW_AFTER_UPDATE = 5

_NOT_SET = object()

# pg type map for create_table
_PY_TO_PG_TYPE = {
	str: "TEXT",
	int: "BIGINT",
	float: "DOUBLE PRECISION",
	bool: "BOOLEAN",
}

# operator map for query()
_MONGO_TO_PG_OP = {
	"$gt": ">",
	"$lt": "<",
	"$gte": ">=",
	"$lte": "<=",
	"$ne": "!=",
}

# Declared indexes, keyed by _table_name_. Populated by INDEX() calls.
IndexesToCreate = {}

# Registry of named DatabaseNode instances.  Populated by DatabaseNode(name=...) or register_db_node().
_DB_NODES_: dict = {}


def INDEX(*indexes):
	"""
	Declare indexes for the enclosing Model class.
	Must be called inside the class body, after _table_name_ is set.

	Each positional arg is either:
	  - A (field_or_attr, direction) tuple   e.g. (user_id, ASCENDING)
	  - A dict of index options              e.g. {'unique': True}

	Example:
	    class User(Model):
	        _table_name_ = "users"
	        id   = Attribute(str)
	        name = Attribute(str)

	        INDEX((id, ASCENDING), {'unique': True})
	        INDEX((name, ASCENDING))
	"""
	locals_ = inspect.currentframe().f_back.f_locals
	table_name = locals_.get("_table_name_") or locals_.get("_collection_name_")
	if not table_name:
		return  # not a pg model — silently ignore (e.g. mongo-only models)
	IndexesToCreate.setdefault(table_name, [])
	IndexesToCreate[table_name].append(indexes)


def _json_default(obj):
	if isinstance(obj, PgDict):
		return dict(obj)
	if isinstance(obj, PgList):
		return list(obj)
	raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


# ──────────────────────────────────────────────────────────────────────────────
# PgDict — change-tracking dict
# ──────────────────────────────────────────────────────────────────────────────

class PgDict(dict):
	"""
	A dict that tracks mutations for diff-patching the __ JSONB column.

	_is_local = True  → new object: mutations are NOT sent to DB (whole doc sent on INSERT)
	_is_local = False → loaded object: each mutation notifies the parent model
	Mirrors MongoDict's _is_local / _model_obj / path pattern.
	"""
	_is_local = True

	def __init__(self, _parent, _path, initial_value=None):
		super().__init__()
		self._parent = _parent
		self._path = _path  # list of strings from root, e.g. ["meta", "addr"]
		self._is_local = True  # suppress notifications while processing initial_value
		if initial_value:
			for key, value in (initial_value.items() if isinstance(initial_value, dict) else initial_value):
				dict.__setitem__(self, key, self._wrap(key, value))
		# Mirror the model's create/load state:
		# new objects stay local (whole doc sent on insert); loaded objects are not local
		self._is_local = getattr(_parent, '_is_create_new_', True)

	def _wrap(self, key, value):
		"""Wrap nested dicts/lists so sub-mutations at any depth are tracked."""
		child_path = self._path + [str(key)]
		if isinstance(value, dict) and not isinstance(value, PgDict):
			return PgDict(self._parent, child_path, value)
		if isinstance(value, list) and not isinstance(value, PgList):
			return PgList(self._parent, child_path, value)
		return value

	def __setitem__(self, key, value):
		value = self._wrap(key, value)
		dict.__setitem__(self, key, value)
		if not self._is_local:
			self._parent._add_set_update(self._path + [str(key)], value)

	def __delitem__(self, key):
		self.pop(key)

	def pop(self, key, *args):
		popped = dict.pop(self, key, _NOT_SET)
		if popped is _NOT_SET:
			if args:
				return args[0]
			raise KeyError(key)
		if not self._is_local:
			self._parent._add_unset_update(self._path + [str(key)])
		# Return plain copies so callers get plain Python objects
		if isinstance(popped, PgDict):
			return dict(popped)
		if isinstance(popped, PgList):
			return list(popped)
		return popped

	def update(self, other=(), **kwargs):
		for key, value in (other.items() if isinstance(other, dict) else other):
			self[key] = value
		for key, value in kwargs.items():
			self[key] = value

	def setdefault(self, key, default=None):
		if key not in self:
			self[key] = default
		return self[key]

	def clear(self):
		dict.clear(self)
		if not self._is_local:
			self._parent._add_set_update(self._path, {})
		self._is_local = True  # treat as local after clear (like Mongo)

	def copy(self):
		return json.loads(json.dumps(dict(self), default=_json_default))

	def __reduce__(self):
		return (dict, (self.copy(),))


# ──────────────────────────────────────────────────────────────────────────────
# PgList — change-tracking list
# ──────────────────────────────────────────────────────────────────────────────

class PgList(list):
	"""
	A list that tracks any mutation by replacing the whole value at its path.

	_is_local = True  → new object: mutations are NOT sent to DB
	_is_local = False → loaded object: any mutation notifies parent via _add_set_update
	Mirrors MongoList's _is_local / _model_obj / path pattern.
	"""
	_is_local = True

	def __init__(self, _parent, _path, initial_value=None):
		super().__init__(initial_value or [])
		self._parent = _parent
		self._path = _path  # list of strings from root, e.g. ["tags"]
		self._is_local = getattr(_parent, '_is_create_new_', True)

	def _notify(self):
		if not self._is_local:
			self._parent._add_set_update(self._path, list(self))

	def append(self, value):
		super().append(value)
		self._notify()

	def extend(self, values):
		super().extend(values)
		self._notify()

	def insert(self, index, value):
		super().insert(index, value)
		self._notify()

	def remove(self, value):
		super().remove(value)
		self._notify()

	def pop(self, index=-1):
		result = super().pop(index)
		self._notify()
		return result

	def clear(self):
		super().clear()
		self._notify()

	def __setitem__(self, index, value):
		super().__setitem__(index, value)
		self._notify()

	def __delitem__(self, index):
		super().__delitem__(index)
		self._notify()

	def sort(self, **kwargs):
		super().sort(**kwargs)
		self._notify()

	def reverse(self):
		super().reverse()
		self._notify()

	def copy(self):
		return json.loads(json.dumps(list(self), default=_json_default))

	def __reduce__(self):
		return (list, (self.copy(),))


# ──────────────────────────────────────────────────────────────────────────────
# Attribute
# ──────────────────────────────────────────────────────────────────────────────

class Attribute:
	"""
	Field descriptor for Model classes.

	The primary key is no longer declared here — it is derived automatically
	from the first unique INDEX() declaration on the model.
	"""

	def __init__(self, _type, default=_NOT_SET, column=False, auto_increment=False, **kwargs):
		self.type = _type
		self.default = default
		self.column = column or auto_increment  # True → stored as a real DB column instead of inside __
		self.auto_increment = auto_increment  # True → BIGSERIAL; DB generates value on INSERT
		self.kwargs = kwargs
		# Set by __set_name__ when assigned inside a class body
		self.attr_name = None
		self.owner_cls = None

	def __set_name__(self, owner, name):
		self.attr_name = name
		self.owner_cls = owner

	def get_default(self):
		if self.default is _NOT_SET:
			return None
		if callable(self.default):
			return self.default()
		if isinstance(self.default, dict):
			return dict(self.default)
		if isinstance(self.default, list):
			return list(self.default)
		return self.default

	def coerce(self, value):
		if value is None:
			return None
		if self.type in (int, float, str, bool):
			try:
				return self.type(value)
			except (ValueError, TypeError):
				return value
		return value


# ──────────────────────────────────────────────────────────────────────────────
# DatabaseNode — connection pool
# ──────────────────────────────────────────────────────────────────────────────

class DatabaseNode:
	"""
	Manages a psycopg2 ThreadedConnectionPool for one PostgreSQL database.

	Args:
	    name:    Optional name to register this node in the global _DB_NODES_ registry
	             so models can reference it as a string: _db_node_ = "my_db"
	    replica: Optional DatabaseNode (or registered name) to use for read-only queries
	             when _replica=True is passed to query() / get().
	"""

	def __init__(self, host, port, user, password, db_name, min_conn=1, max_conn=20,
				 name: str = None, replica: "DatabaseNode | str" = None):
		self.host = host
		self.port = port
		self.user = user
		self.password = password
		self.db_name = db_name
		self._min_conn = min_conn
		self._max_conn = max_conn
		self._pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None
		self.replica = replica  # resolved lazily in _get_replica()
		if name:
			_DB_NODES_[name] = self

	def _get_replica(self) -> "DatabaseNode":
		"""Return the replica node, resolving a string name if needed."""
		r = self.replica
		if r is None:
			return self
		if isinstance(r, str):
			r = _DB_NODES_.get(r, self)
		return r

	def _get_pool(self):
		if self._pool is None:
			self._pool = psycopg2.pool.ThreadedConnectionPool(
				self._min_conn,
				self._max_conn,
				host=self.host,
				port=self.port,
				user=self.user,
				password=self.password,
				dbname=self.db_name,
				cursor_factory=psycopg2.extras.RealDictCursor,
			)
		return self._pool

	def get_conn(self):
		return self._get_pool().getconn()

	def put_conn(self, conn):
		self._get_pool().putconn(conn)

	@contextmanager
	def use_conn(self):
		conn = self.get_conn()
		try:
			yield conn
		finally:
			self.put_conn(conn)


# ──────────────────────────────────────────────────────────────────────────────
# Model
# ──────────────────────────────────────────────────────────────────────────────

class Model:
	_table_name_: str = None
	_db_node_: DatabaseNode = None
	_attrs_: dict = {}
	_pk_attrs_: list = []   # set by initialize_model from first unique INDEX
	_col_attrs_: dict = {}  # {name: Attribute} for column=True non-pk fields
	_indexes_: list = []    # parsed index specs, set by initialize_model
	_event_listeners_: dict = {}

	@classmethod
	def on(cls, events, func):
		"""Register func as a handler for one or more events on this model class."""
		if not isinstance(events, list):
			events = [events]
		for event in events:
			# Each class gets its own _event_listeners_ dict (not shared with base/siblings)
			if "_event_listeners_" not in cls.__dict__:
				cls._event_listeners_ = {}
			handlers = cls._event_listeners_.setdefault(event, [])
			handlers.append(func)

	@classmethod
	def _trigger_event(cls, event, *args):
		if handlers := cls._event_listeners_.get(event):
			for handler in handlers:
				handler(*args)

	def __init__(self, _is_create_new_=True, **kwargs):
		object.__setattr__(self, '_is_create_new_', _is_create_new_)
		object.__setattr__(self, '_initializing_', True)  # not initializing from db
		object.__setattr__(self, '_', 0)
		object.__setattr__(self, '_original_', 0)
		object.__setattr__(self, '_set_updates', {})
		object.__setattr__(self, '_unset_updates', set())

		for name, attr in self.__class__._attrs_.items():
			if(name in kwargs):
				val = kwargs[name]
			else:
				val = attr.get_default()
			setattr(self, name, val)

		object.__setattr__(self, '_initializing_', False)  # not initializing from db

	def __setattr__(self, name, value):
		_attr_obj = self.__class__._attrs_.get(name)
		if _attr_obj:
			_attr_obj_type = _attr_obj.type
			# Always wrap dict/list so sub-mutations at any depth are tracked.
			# _is_local on the wrapper mirrors _is_create_new_, so new-object
			# mutations are suppressed while loaded-object mutations notify.
			if _attr_obj_type is dict and not isinstance(value, PgDict):
				value = PgDict(self, [name], value or {})
			elif _attr_obj_type is list and not isinstance(value, PgList):
				value = PgList(self, [name], value or [])
			if object.__getattribute__(self, '_initializing_') is False:
				self._add_set_update([name], value)
		self.__dict__[name] = value

	# ── Change tracking ──────────────────────────────────────────────────────

	def _add_set_update(self, path, value):
		path_tuple = tuple(path)
		set_updates = object.__getattribute__(self, '_set_updates')
		superseded = [
			p for p in set_updates
			if len(p) > len(path_tuple) and p[:len(path_tuple)] == path_tuple
		]
		for p in superseded:
			del set_updates[p]
		unset_updates = object.__getattribute__(self, '_unset_updates')
		unset_updates.discard(path_tuple)
		set_updates[path_tuple] = value

	def _add_unset_update(self, path):
		path_tuple = tuple(path)
		set_updates = object.__getattribute__(self, '_set_updates')
		unset_updates = object.__getattribute__(self, '_unset_updates')
		set_updates.pop(path_tuple, None)
		unset_updates.add(path_tuple)

	# ── Serialization ────────────────────────────────────────────────────────

	def _to_doc(self):
		doc = {}
		inst = vars(self)  # instance __dict__ only; avoids picking up class-level Attribute objects
		col_attrs = self.__class__._col_attrs_
		for name in self.__class__._attrs_:
			if name in col_attrs:
				continue  # stored as a real column, not in __
			value = inst.get(name)
			if value is None:
				continue
			if isinstance(value, (dict, list)) and not value:
				continue  # skip empty containers (default {} / [])
			doc[name] = json.loads(json.dumps(value, default=_json_default))
		return doc

	def _from_doc(obj, row, partial=False) -> T:
		# LOADING FROM DB
		cls = obj.__class__
		_val = row["_"]
		object.__setattr__(obj, '_row_', row)
		object.__setattr__(obj, '_initializing_', True)
		object.__setattr__(obj, '_is_create_new_', False)
		object.__setattr__(obj, '_', _val)
		object.__setattr__(obj, '_original_', _val)
		object.__setattr__(obj, '_set_updates', {})
		object.__setattr__(obj, '_unset_updates', set())

		col_attrs = cls._col_attrs_
		for name, attr in cls._attrs_.items():
			# column=True fields are read directly from the row, not from __
			if(partial and name not in row): continue
			source = row if name in col_attrs else row["__"]
			value = source.get(name, _NOT_SET)
			if value is not _NOT_SET:  # exists
				value = attr.coerce(value)
			elif attr.default is not _NOT_SET:  # has default
				value = attr.get_default()

			if value is not _NOT_SET:  # exists
				path = [name]
				if isinstance(value, dict) and not isinstance(value, PgDict):
					value = PgDict(obj, path, value)
				elif isinstance(value, list) and not isinstance(value, PgList):
					value = PgList(obj, path, value)
				object.__setattr__(obj, name, value)
			else:  # does not exist in DB
				if attr.type is dict:
					object.__setattr__(obj, name, PgDict(obj, [name]))
				elif attr.type is list:
					object.__setattr__(obj, name, PgList(obj, [name]))

		object.__setattr__(obj, '_initializing_', False)
		return obj

	# ── SQL helpers ──────────────────────────────────────────────────────────

	@classmethod
	def _pk_conditions(cls):
		return " AND ".join(f"{pk} = %s" for pk in cls._pk_attrs_)

	def _pk_values(self):
		return [getattr(self, pk) for pk in self.__class__._pk_attrs_]

	@classmethod
	def _build_conditions_sql(cls, _query):
		"""
		Parse a MongoDB-style query dict into (conditions, params).
		Used by both query() and _update().

		Supported: equality, $gt, $lt, $gte, $lte, $ne, $in
		PK fields compare against real columns; others use JSONB (__->>'field').
		"""
		conditions = []
		params = []
		if not _query:
			return conditions, params

		for field, value in _query.items():
			is_real_col = field in cls._pk_attrs_ or field in cls._col_attrs_
			col_expr = field if is_real_col else f"(__->>'{field}')"
			attr = cls._attrs_.get(field)
			pg_type = _PY_TO_PG_TYPE.get(attr.type, "TEXT") if attr else "TEXT"

			if isinstance(value, dict):
				for op, op_val in value.items():
					if op == "$in":
						placeholders = ", ".join(["%s"] * len(op_val))
						conditions.append(f"{col_expr} IN ({placeholders})")
						if is_real_col:
							params.extend(op_val)
						else:
							params.extend(str(v) for v in op_val)
					elif op in _MONGO_TO_PG_OP:
						pg_op = _MONGO_TO_PG_OP[op]
						if is_real_col:
							conditions.append(f"{col_expr} {pg_op} %s")
							params.append(op_val)
						else:
							conditions.append(f"({col_expr})::{pg_type} {pg_op} %s")
							params.append(op_val)
					else:
						LOG_WARN("pg_orm_unknown_op", op=op, field=field)
			else:
				conditions.append(f"{col_expr} = %s")
				if is_real_col:
					params.append(value)
				else:
					params.append(str(value) if not isinstance(value, bool) else str(value).lower())

		return conditions, params

	@classmethod
	def _build_update_expr(cls, updates):
		"""
		Build chained jsonb_set / #- / $inc expression for patching the __ column.

		updates: {
		    "$set":   {path_or_dotpath: value, ...},   e.g. {"name": "x", "addr.city": "LA"}
		    "$unset": {path_or_dotpath: 1, ...},
		    "$inc":   {path_or_dotpath: delta, ...},
		}

		For $inc, the current value is always read from the original __ column so that
		$set and $inc on different fields in the same call compose correctly.

		Returns (expr, params, has_inc).
		"""
		expr = "__"
		params = []
		has_inc = bool(updates.get("$inc"))

		for field, value in updates.get("$set", {}).items():
			path = list(field) if isinstance(field, (list, tuple)) else field.split(".")
			expr = f"jsonb_set({expr}, %s::text[], %s::jsonb, true)"
			params.append(path)
			params.append(json.dumps(value, default=_json_default))

		for field in updates.get("$unset", {}):
			path = list(field) if isinstance(field, (list, tuple)) else field.split(".")
			expr = f"({expr} #- %s::text[])"
			params.append(path)

		for field, delta in updates.get("$inc", {}).items():
			path = list(field) if isinstance(field, (list, tuple)) else field.split(".")
			# Read from original __ (not expr) so $set and $inc on different fields compose cleanly
			expr = (
				f"jsonb_set({expr}, %s::text[], "
				f"(COALESCE((__ #>> %s::text[])::numeric, 0) + %s)::text::jsonb, true)"
			)
			params.extend([path, path, delta])

		return expr, params, has_inc

	# ── CRUD ─────────────────────────────────────────────────────────────────

	def commit(self, conditions=None, force=False):
		"""
		Persist this object: INSERT if new, UPDATE otherwise.

		conditions: passed to update() for conditional commits (see update()).
		force:      on INSERT duplicate-key conflict, load the existing row and
		            apply the new fields on top of it instead of raising.
		"""
		if object.__getattribute__(self, '_is_create_new_'):
			self._insert(force=force)
		else:
			self.update(conditions=conditions)
		return self

	def _insert(self, force=False):
		cls = self.__class__
		cls._trigger_event(EVENT_BEFORE_CREATE, self)

		doc = self._to_doc()
		new_ts = cur_ms()
		pk_attrs = cls._pk_attrs_
		col_attrs = cls._col_attrs_

		inst = vars(self)
		pk_values = [inst.get(pk) for pk in pk_attrs]
		col_names = list(col_attrs.keys())
		col_values = [inst.get(n) for n in col_names]

		all_cols = ", ".join(pk_attrs + col_names + ["_", "__"])
		all_placeholders = ", ".join(["%s"] * (len(pk_attrs) + len(col_names)) + ["%s", "%s::jsonb"])
		returning = "_, __" + (", " + ", ".join(col_names) if col_names else "")

		sql = (
			f"INSERT INTO {cls._table_name_} "
			f"({all_cols}) "
			f"VALUES ({all_placeholders}) "
			f"RETURNING {returning}"
		)

		try:
			with cls._db_node_.use_conn() as conn:
				with conn.cursor() as cur:
					cur.execute(sql, pk_values + col_values + [new_ts, json.dumps(doc, default=_json_default)])
					row = cur.fetchone()
				conn.commit()
		except psycopg2.errors.UniqueViolation:
			if not force:
				raise
			# Load the conflicting row, then apply whatever fields this object had set
			existing = cls.get(**{pk: inst.get(pk) for pk in pk_attrs})
			if existing is None:
				raise  # row vanished between INSERT and GET — re-raise original
			self._from_doc(existing._row_)
			# Build $set from the fields that were explicitly provided on this new object
			set_payload = {}
			for name, attr in cls._attrs_.items():
				if name in pk_attrs:
					continue
				v = inst.get(name)
				if v is None:
					continue
				if isinstance(v, (dict, list)) and not v:
					continue
				set_payload[name] = v
			if set_payload:
				self.update({"$set": set_payload})
			return

		self._from_doc(row)  # update local state with any DB defaults, coerced types, etc.
		cls._trigger_event(EVENT_AFTER_CREATE, self)

	@classmethod
	def _split_updates(cls, updates):
		"""
		Partition updates into column-level (real DB column) and JSONB-level parts.
		column=True fields and top-level-only paths route to column-level.
		"""
		col_attrs = cls._col_attrs_

		def _top(field):
			"""Return (top_field_name, is_top_level_only)."""
			if isinstance(field, tuple):
				return field[0], len(field) == 1
			parts = field.split(".")
			return parts[0], len(parts) == 1

		col_set, col_inc, col_unset = {}, {}, set()
		json_set, json_inc, json_unset = {}, {}, {}

		for field, value in updates.get("$set", {}).items():
			top, is_top = _top(field)
			if top in col_attrs and is_top:
				col_set[top] = value
			else:
				json_set[field] = value

		for field in updates.get("$unset", {}):
			top, is_top = _top(field)
			if top in col_attrs and is_top:
				col_unset.add(top)
			else:
				json_unset[field] = 1

		for field, delta in updates.get("$inc", {}).items():
			top, is_top = _top(field)
			if top in col_attrs and is_top:
				col_inc[top] = delta
			else:
				json_inc[field] = delta

		json_updates = {}
		if json_set:   json_updates["$set"]   = json_set
		if json_unset: json_updates["$unset"] = json_unset
		if json_inc:   json_updates["$inc"]   = json_inc

		return col_set, col_inc, col_unset, json_updates

	def update(self, updates=None, conditions=None, callback=None):
		"""
		Apply updates to this record with optimistic locking on _.

		updates: {
		    "$set":   {"field": value, "nested.field": value, ...},
		    "$unset": {"field": 1, ...},
		    "$inc":   {"field": delta, ...},
		}
		conditions: extra WHERE filters beyond pk + _ (same syntax as query())
		    e.g. {"status": "active", "score": {"$lt": 100}}
		callback:   called with (self, result_row) after the UPDATE succeeds but
		            before COMMIT. If it raises, the transaction is rolled back
		            and the exception is re-raised.

		When updates is None, tracked changes from attribute mutations are used.
		column=True fields are updated via direct SQL columns; others via JSONB.
		"""
		# Build updates from tracked changes if not provided explicitly
		if updates is None:
			set_updates = object.__getattribute__(self, '_set_updates')
			unset_updates = object.__getattribute__(self, '_unset_updates')
			if not set_updates and not unset_updates:
				return
			updates = {}
			if set_updates:
				updates["$set"] = dict(set_updates)
			if unset_updates:
				updates["$unset"] = {path: 1 for path in unset_updates}

		if not any(updates.get(op) for op in ("$set", "$unset", "$inc")):
			return

		cls = self.__class__
		cls._trigger_event(EVENT_BEFORE_UPDATE, self)
		col_attrs = cls._col_attrs_
		col_set, col_inc, col_unset, json_updates = cls._split_updates(updates)

		new_ts = cur_ms()
		original_ts = self._original_

		set_clauses = []
		all_params = []

		# JSONB expression (only when there are non-column updates)
		has_jsonb_inc = bool(json_updates.get("$inc"))
		if any(json_updates.get(op) for op in ("$set", "$unset", "$inc")):
			expr, jparams, _ = cls._build_update_expr(json_updates)
			set_clauses.append(f"__ = {expr}")
			all_params.extend(jparams)

		# Direct column SET / NULL / INC
		for field, value in col_set.items():
			set_clauses.append(f"{field} = %s")
			all_params.append(col_attrs[field].coerce(value))
		for field in col_unset:
			set_clauses.append(f"{field} = NULL")
		for field, delta in col_inc.items():
			set_clauses.append(f"{field} = COALESCE({field}, 0) + %s")
			all_params.append(delta)

		if not set_clauses:
			return

		set_clauses.append("_ = %s")
		all_params.append(new_ts)

		pk_conditions = cls._pk_conditions()
		pk_values = self._pk_values()
		extra_conds, extra_params = cls._build_conditions_sql(conditions or {})
		where_parts = [pk_conditions, "_ = %s"] + extra_conds
		where = " AND ".join(where_parts)

		# Always RETURNING col attrs so local state stays in sync after column INC
		returning_cols = ["_"]
		if has_jsonb_inc:
			returning_cols.append("__")
		returning_cols.extend(col_attrs.keys())
		returning = "RETURNING " + ", ".join(returning_cols)

		sql = (
			f"UPDATE {cls._table_name_} "
			f"SET {', '.join(set_clauses)} "
			f"WHERE {where} {returning}"
		)
		base_params = all_params + pk_values
		fixed_extra = extra_params

		_MAX_RETRIES = 3
		for attempt in range(_MAX_RETRIES):
			with cls._db_node_.use_conn() as conn:
				with conn.cursor() as cur:
					cur.execute(sql, base_params + [original_ts] + fixed_extra)
					result = cur.fetchone()
				if result is None:
					if conditions:
						return False
					# optimistic lock conflict — refetch and retry
					if attempt < _MAX_RETRIES - 1:
						fresh = cls.get(**{pk: getattr(self, pk) for pk in cls._pk_attrs_})
						if fresh is None:
							raise OptimisticLockError(
								f"{cls._table_name_}: record deleted during retry "
								f"(pk={self._pk_values()})"
							)
						original_ts = fresh._original_
						continue
					raise OptimisticLockError(
						f"{cls._table_name_}: update conflict after {_MAX_RETRIES} retries "
						f"(pk={self._pk_values()}, _={self._original_})"
					)
				if callback:
					try:
						callback(self, result)
					except Exception as ex:
						conn.rollback()
						raise ex
				conn.commit()
			original_row = dict(self._row_) if hasattr(self, '_row_') else None
			self._from_doc(result, partial=True)
			cls._trigger_event(EVENT_ROW_AFTER_UPDATE, self, original_row, result)
			cls._trigger_event(EVENT_AFTER_UPDATE, self)
			return True

	def delete(self):
		cls = self.__class__
		cls._trigger_event(EVENT_BEFORE_DELETE, self)
		pk_conditions = cls._pk_conditions()
		pk_values = self._pk_values()
		sql = f"DELETE FROM {cls._table_name_} WHERE {pk_conditions}"
		with cls._db_node_.use_conn() as conn:
			with conn.cursor() as cur:
				cur.execute(sql, pk_values)
			conn.commit()
		cls._trigger_event(EVENT_AFTER_DELETE, self)

	@classmethod
	def get(cls: Type[T], _replica: bool = False, **pk_kwargs) -> Optional[T]:
		"""
		Fetch a single record by primary key fields.

		_replica=True routes the read to the replica node (if configured on _db_node_).
		"""
		node = cls._db_node_._get_replica() if _replica else cls._db_node_
		pk_values = [pk_kwargs[pk] for pk in cls._pk_attrs_]
		pk_conditions = cls._pk_conditions()
		col_names = list(cls._col_attrs_.keys())
		select_cols = "_, __" + (", " + ", ".join(col_names) if col_names else "")
		sql = f"SELECT {select_cols} FROM {cls._table_name_} WHERE {pk_conditions} LIMIT 1"
		with node.use_conn() as conn:
			with conn.cursor() as cur:
				cur.execute(sql, pk_values)
				row = cur.fetchone()
		if row is None:
			return None
		return cls.__new__(cls)._from_doc(row)

	@classmethod
	def query(
		cls: Type[T],
		_query: dict = None,
		sort=None,
		limit: int = None,
		offset: int = None,
		_replica: bool = False,
	) -> Iterator[T]:
		"""
		Query records with MongoDB-style filter syntax.

		Supported operators: $gt, $lt, $gte, $lte, $ne, $in
		PK fields are compared against real columns; all others via JSONB.

		_replica=True routes the read to the replica node (if configured on _db_node_).

		Example:
		    User.query({'age': {'$gte': 18}}, sort=[('name', 1)], limit=10, _replica=True)
		"""
		node = cls._db_node_._get_replica() if _replica else cls._db_node_
		conditions, params = cls._build_conditions_sql(_query)
		where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

		order_parts = []
		if sort:
			for field, direction in sort:
				dir_str = "ASC" if direction >= 0 else "DESC"
				if field in cls._pk_attrs_ or field in cls._col_attrs_ or field == '_':
					order_parts.append(f"{field} {dir_str}")
				else:
					order_parts.append(f"(__->>'{field}') {dir_str}")
		order = f"ORDER BY {', '.join(order_parts)}" if order_parts else ""

		limit_clause = f"LIMIT {int(limit)}" if limit is not None else ""
		offset_clause = f"OFFSET {int(offset)}" if offset is not None else ""

		col_names = list(cls._col_attrs_.keys())
		select_cols = "_, __" + (", " + ", ".join(col_names) if col_names else "")
		sql = f"SELECT {select_cols} FROM {cls._table_name_} {where} {order} {limit_clause} {offset_clause}".strip()

		with node.use_conn() as conn:
			with conn.cursor() as cur:
				cur.execute(sql, params)
				rows = cur.fetchall()

		for row in rows:
			yield cls.__new__(cls)._from_doc(row)

	@classmethod
	def _build_create_table_sql(cls):
		"""Return the CREATE TABLE SQL string (does not execute it)."""
		pk_attrs = cls._pk_attrs_
		col_defs = []
		for pk in pk_attrs:
			attr = cls._attrs_[pk]
			pg_type = "BIGSERIAL" if attr.auto_increment else _PY_TO_PG_TYPE.get(attr.type, "TEXT")
			col_defs.append(f"    {pk} {pg_type}")
		for name, attr in cls._col_attrs_.items():
			pg_type = "BIGSERIAL" if attr.auto_increment else _PY_TO_PG_TYPE.get(attr.type, "TEXT")
			col_defs.append(f"    {name} {pg_type}")
		col_defs.append("    _ BIGINT NOT NULL DEFAULT 0")
		col_defs.append("    __ JSONB NOT NULL DEFAULT '{}'")
		col_defs.append(f"    PRIMARY KEY ({', '.join(pk_attrs)})")
		return (
			f"CREATE TABLE IF NOT EXISTS {cls._table_name_} (\n"
			+ ",\n".join(col_defs)
			+ "\n)"
		)

	@classmethod
	def create_table(cls):
		"""
		Create the table and all declared indexes if they don't already exist.
		initialize_model() must be called first so _pk_attrs_ is populated.
		"""
		table_sql = cls._build_create_table_sql()
		with cls._db_node_.use_conn() as conn:
			with conn.cursor() as cur:
				cur.execute(table_sql)
				for index_spec in cls._indexes_:
					cur.execute(index_spec['sql'])
			conn.commit()


# ──────────────────────────────────────────────────────────────────────────────
# Exceptions
# ──────────────────────────────────────────────────────────────────────────────

class OptimisticLockError(Exception):
	pass


class MissingTableError(Exception):
	pass


# ──────────────────────────────────────────────────────────────────────────────
# initialize_model
# ──────────────────────────────────────────────────────────────────────────────


def _make_index_name(table, fields, explicit_unique):
	"""Deterministic index name: {table}_{f1}_{f2}[_uniq].
	_uniq suffix is only added when unique=True is explicitly declared in props.
	"""
	suffix = "_uniq" if explicit_unique else ""
	return table + "_" + "_".join(f for f, _ in fields) + suffix


def _parse_index_declaration(raw_index, attrs_to_name):
	"""
	Parse one INDEX() argument group into (fields, properties).

	raw_index is a tuple of positional args passed to INDEX(), e.g.:
	    ((user_id, ASCENDING), (created_at, DESCENDING), {'unique': True})

	Returns:
	    fields: list of (field_name, direction)
	    props:  dict  e.g. {'unique': True}
	"""
	fields = []
	props = {}
	for item in raw_index:
		if isinstance(item, dict):
			props.update(item)
			continue
		# item is (field_or_attr, direction) or just field_or_attr
		if isinstance(item, tuple):
			field_ref, direction = item[0], item[1] if len(item) > 1 else ASCENDING
		else:
			field_ref, direction = item, ASCENDING

		# Resolve Attribute object → name
		if isinstance(field_ref, Attribute):
			field_name = attrs_to_name.get(id(field_ref)) or field_ref.attr_name
		else:
			field_name = field_ref

		if field_name:
			fields.append((field_name, direction))

	return fields, props


def _table_exists(db_node, table_name):
	"""Return True/False if the table exists, or None if the DB is unreachable."""
	try:
		with db_node.use_conn() as conn:
			with conn.cursor() as cur:
				cur.execute(
					"SELECT 1 FROM pg_class WHERE relname = %s AND relkind = 'r'",
					[table_name],
				)
				return cur.fetchone() is not None
	except Exception:
		return None


def _fetch_existing_indexes(db_node, table_name):
	"""
	Return a dict {indexname: is_unique} for all non-primary indexes on the table.
	Returns None if the table doesn't exist yet.
	"""
	sql = """
		SELECT i.relname AS indexname, ix.indisunique AS is_unique
		FROM pg_class t
		JOIN pg_index ix ON t.oid = ix.indrelid
		JOIN pg_class i ON i.oid = ix.indexrelid
		WHERE t.relname = %s
		  AND t.relkind = 'r'
		  AND NOT ix.indisprimary
	"""
	try:
		with db_node.use_conn() as conn:
			with conn.cursor() as cur:
				cur.execute(sql, [table_name])
				rows = cur.fetchall()
		return {row['indexname']: row['is_unique'] for row in rows}
	except Exception:
		return None


_loading_errors = []


def initialize_model(model_cls):
	"""
	Scan the class MRO for Attribute definitions, parse INDEX() declarations,
	derive _pk_attrs_ from the first unique index, and compare against existing
	DB indexes (printing warnings on drift).

	Must be called once after the model class is defined.

	Example:
	    class User(Model):
	        _table_name_ = "users"
	        _db_node_ = my_db_node

	        id         = Attribute(str)
	        name       = Attribute(str)
	        age        = Attribute(int, default=0)
	        meta       = Attribute(dict, default={})
	        tags       = Attribute(list, default=[])

	        INDEX((id,   ASCENDING), {'unique': True})
	        INDEX((name, ASCENDING))

	    initialize_model(User)
	"""

	# ── 0. Resolve _db_node_ (string name lookup + default fallback) ─────────
	db_node = model_cls._db_node_
	if isinstance(db_node, str):
		db_node = _DB_NODES_.get(db_node)
		model_cls._db_node_ = db_node

	# ── 1. Collect Attribute definitions ────────────────────────────────────
	attrs = {}
	for klass in reversed(model_cls.__mro__):
		for name, value in klass.__dict__.items():
			if isinstance(value, Attribute):
				attrs[name] = value
	model_cls._attrs_ = attrs

	# Build a lookup from Attribute object id → field name (for INDEX resolution)
	attrs_to_name = {id(attr): name for name, attr in attrs.items()}

	# ── 2. Parse declared indexes ────────────────────────────────────────────
	raw_indexes = IndexesToCreate.get(model_cls._table_name_, [])
	table = model_cls._table_name_

	parsed_indexes = []   # list of {'fields', 'unique', 'name', 'sql'}
	pk_attrs = None

	for raw_index in raw_indexes:
		fields, props = _parse_index_declaration(raw_index, attrs_to_name)
		if not fields:
			continue

		# Unique by default; only non-unique when explicitly {"unique": False}
		is_unique = props.get('unique', True)
		# _uniq suffix only when unique=True is explicitly declared
		explicit_unique = props.get('unique') is True
		condition = props.get('condition')  # optional WHERE clause for partial index
		index_name = _make_index_name(table, fields, explicit_unique)

		# Build SQL column expressions
		# pk_attrs is not resolved yet here; we use a placeholder set and fix after
		col_exprs = []
		for field, direction in fields:
			dir_str = "ASC" if direction >= 0 else "DESC"
			# We don't know pk_attrs yet; treat all as JSONB initially.
			# After pk_attrs is set we regenerate if needed (below).
			col_exprs.append((field, direction, dir_str))

		# First unique index → pk_attrs
		if is_unique and pk_attrs is None:
			pk_attrs = [f for f, _ in fields]

		parsed_indexes.append({
			'fields': fields,
			'unique': is_unique,
			'condition': condition,
			'name': index_name,
			'col_exprs': col_exprs,     # resolved to SQL below
		})

	# ── 3. Derive pk_attrs ───────────────────────────────────────────────────

	# ── 3. Derive pk_attrs ───────────────────────────────────────────────────
	if not pk_attrs:
		_id_attr = model_cls._attrs_["_id"] = Attribute(int, auto_increment=True)
		attrs_to_name[id(_id_attr)] = "_id"
		pk_attrs = ["_id"]

	model_cls._pk_attrs_ = pk_attrs

	# ── 3b. Collect column=True attrs (non-pk real columns) ──────────────────
	model_cls._col_attrs_ = {
		name: attr
		for name, attr in attrs.items()
		if attr.column and name not in pk_attrs
	}

	# ── 4. Build final index SQL now that pk_attrs is known ──────────────────
	indexes = []
	for spec in parsed_indexes:
		col_parts = []
		for field, direction, dir_str in spec['col_exprs']:
			if field in pk_attrs or field == '_':
				col_parts.append(f"{field} {dir_str}")
			else:
				col_parts.append(f"(__->>'{field}') {dir_str}")

		unique_clause = "UNIQUE " if spec['unique'] else ""
		where_clause = f" WHERE {spec['condition']}" if spec.get('condition') else ""
		index_sql = (
			f"CREATE {unique_clause}INDEX IF NOT EXISTS {spec['name']} "
			f"ON {table} ({', '.join(col_parts)}){where_clause}"
		)
		indexes.append({
			'fields': spec['fields'],
			'unique': spec['unique'],
			'condition': spec.get('condition'),
			'name': spec['name'],
			'sql': index_sql,
		})

	model_cls._indexes_ = indexes

	# ── 5. Ensure table exists; compare indexes ──────────────────────────────
	if not model_cls._db_node_:
		return

	exists = _table_exists(model_cls._db_node_, table)
	if exists is None:
		return  # DB unreachable — skip checks silently

	if not exists:
		if IS_TEST:
			model_cls.create_table()
			# indexes were just created; nothing left to compare
		else:
			create_sql = model_cls._build_create_table_sql()
			index_sqls = "\n".join(spec['sql'] + ";" for spec in indexes)
			_loading_errors.append(MissingTableError(
				f"Table '{table}' does not exist. Create it with:\n\n"
				f"{create_sql};\n\n{index_sqls}"
			))
	else:
		existing = _fetch_existing_indexes(model_cls._db_node_, table)
		if existing is not None:
			declared_names = {spec['name'] for spec in indexes}
			existing_names = set(existing.keys())

			# Indexes in DB but not declared → probably stale, warn
			for name in existing_names - declared_names:
				LOG_WARN(
					"pg_orm_extra_index",
					model=model_cls.__name__,
					table=table,
					index=name,
					desc=(
						f"Index '{name}' exists in DB but is not declared in the ORM. "
						f"Drop it? DROP INDEX {name};"
					)
				)

			# Declared but not in DB → missing, warn
			for spec in indexes:
				if spec['name'] not in existing_names:
					LOG_WARN(
						"pg_orm_missing_index",
						model=model_cls.__name__,
						table=table,
						index=spec['name'],
						desc=(
							f"Index '{spec['name']}' is declared but missing in DB. "
							f"Create it? {spec['sql']}"
						)
					)
					continue

				# Check if unique flag drifted
				db_is_unique = existing[spec['name']]
				if bool(db_is_unique) != spec['unique']:
					LOG_WARN(
						"pg_orm_index_changed",
						model=model_cls.__name__,
						table=table,
						index=spec['name'],
						declared_unique=spec['unique'],
						db_unique=db_is_unique,
						desc=(
							f"Index '{spec['name']}' unique flag differs "
							f"(declared={spec['unique']}, db={db_is_unique}). "
							f"Recreate: DROP INDEX {spec['name']}; {spec['sql']}"
						)
					)


def initialize_postgres(db_nodes):

	# initialize control db
	if(isinstance(db_nodes, dict)):
		db_nodes = [DatabaseNode(**db_nodes)]
	elif(isinstance(db_nodes, list)):
		db_nodes = [DatabaseNode(**db_node) for db_node in db_nodes]
	else:
		raise Exception(
			f"argument must be a list of dicts, or a single dict, not {type(db_nodes)}"
		)
	# set default db node for each class; skip mongo-only models (no _table_name_)
	for cls in all_subclasses(Model):
		if not getattr(cls, "_table_name_", None):
			continue
		if not getattr(cls, "_db_node_", None):
			cls._db_node_ = db_nodes[0]
		initialize_model(cls)

	if(_loading_errors):
		for _loading_error in _loading_errors:
			print(str(_loading_error))
			print("---------")
		raise Exception("initialize_postgres has errors: Check error logs: ")
