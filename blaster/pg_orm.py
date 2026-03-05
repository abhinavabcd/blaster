import inspect
import json
from contextlib import contextmanager
from typing import TypeVar, Type, Iterator, Optional

import psycopg2
import psycopg2.pool
import psycopg2.extras

from .tools import cur_ms
from .logging import LOG_WARN, LOG_ERROR

T = TypeVar("T", bound="Model")

ASCENDING = 1
DESCENDING = -1

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

# Declared indexes, keyed by _collection_name_. Populated by INDEX() calls.
IndexesToCreate = {}


def INDEX(*indexes):
	"""
	Declare indexes for the enclosing Model class.
	Must be called inside the class body, after _collection_name_ is set.

	Each positional arg is either:
	  - A (field_or_attr, direction) tuple   e.g. (user_id, ASCENDING)
	  - A dict of index options              e.g. {'unique': True}

	Example:
	    class User(Model):
	        _collection_name_ = "users"
	        id   = Attribute(str)
	        name = Attribute(str)

	        INDEX((id, ASCENDING), {'unique': True})
	        INDEX((name, ASCENDING))
	"""
	collection_name = inspect.currentframe().f_back.f_locals["_collection_name_"]
	IndexesToCreate.setdefault(collection_name, [])
	IndexesToCreate[collection_name].append(indexes)


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
	"""A dict that tracks mutations for diff-patching the __ JSONB column."""

	def __init__(self, data=None, *, _parent=None, _path=None):
		super().__init__()
		self._parent = _parent
		self._path = _path or []
		self._is_local = True
		if data:
			for key, value in (data.items() if isinstance(data, dict) else data):
				super().__setitem__(key, self._wrap(key, value))

	def _wrap(self, key, value):
		path = self._path + [str(key)]
		if isinstance(value, dict) and not isinstance(value, PgDict):
			return PgDict(value, _parent=self._parent, _path=path)
		if isinstance(value, list) and not isinstance(value, PgList):
			return PgList(value, _parent=self._parent, _path=path)
		return value

	def _notify_set(self, key):
		parent = self._parent
		if parent is not None and not getattr(parent, '_initializing_', True):
			parent._add_set_update(self._path + [str(key)], self[key])

	def _notify_del(self, key):
		parent = self._parent
		if parent is not None and not getattr(parent, '_initializing_', True):
			parent._add_unset_update(self._path + [str(key)])

	def __setitem__(self, key, value):
		super().__setitem__(key, self._wrap(key, value))
		self._notify_set(key)

	def __delitem__(self, key):
		super().__delitem__(key)
		self._notify_del(key)

	def update(self, other=(), **kwargs):
		for key, value in (other.items() if isinstance(other, dict) else other):
			self[key] = value
		for key, value in kwargs.items():
			self[key] = value

	def pop(self, key, *args):
		existed = key in self
		result = super().pop(key, *args)
		if existed:
			self._notify_del(key)
		return result

	def setdefault(self, key, default=None):
		if key not in self:
			self[key] = default
		return self[key]


# ──────────────────────────────────────────────────────────────────────────────
# PgList — change-tracking list
# ──────────────────────────────────────────────────────────────────────────────

class PgList(list):
	"""A list that tracks any mutation by replacing the whole value at its path."""

	def __init__(self, data=None, *, _parent=None, _path=None):
		super().__init__(data or [])
		self._parent = _parent
		self._path = _path or []
		self._is_local = True

	def _notify(self):
		parent = self._parent
		if parent is not None and not getattr(parent, '_initializing_', True):
			parent._add_set_update(self._path, list(self))

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


# ──────────────────────────────────────────────────────────────────────────────
# Attribute
# ──────────────────────────────────────────────────────────────────────────────

class Attribute:
	"""
	Field descriptor for Model classes.

	The primary key is no longer declared here — it is derived automatically
	from the first unique INDEX() declaration on the model.
	"""

	def __init__(self, _type, default=_NOT_SET, **kwargs):
		self.type = _type
		self.default = default
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
	"""Manages a psycopg2 ThreadedConnectionPool for one PostgreSQL database."""

	def __init__(self, host, port, user, password, db_name, min_conn=1, max_conn=20):
		self.host = host
		self.port = port
		self.user = user
		self.password = password
		self.db_name = db_name
		self._min_conn = min_conn
		self._max_conn = max_conn
		self._pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None

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
	_collection_name_: str = None
	_db_node_: DatabaseNode = None
	_attrs_: dict = {}
	_pk_attrs_: list = []   # set by initialize_model from first unique INDEX
	_indexes_: list = []    # parsed index specs, set by initialize_model

	def __init__(self, _is_create_new_=True, **kwargs):
		object.__setattr__(self, '_initializing_', True)
		object.__setattr__(self, '_is_create_new_', _is_create_new_)
		object.__setattr__(self, '_', 0)
		object.__setattr__(self, '_original_', 0)
		object.__setattr__(self, '_set_updates', {})
		object.__setattr__(self, '_unset_updates', set())

		for name, attr in self.__class__._attrs_.items():
			default = attr.get_default()
			if default is not None:
				self._init_attr(name, default)

		for name, value in kwargs.items():
			if name in self.__class__._attrs_:
				self._init_attr(name, value)

		object.__setattr__(self, '_initializing_', False)

	def _init_attr(self, name, value):
		attr = self.__class__._attrs_.get(name)
		if attr is not None:
			value = attr.coerce(value)
		value = self._wrap_value(name, value)
		object.__setattr__(self, name, value)

	def _wrap_value(self, name, value):
		path = [name]
		if isinstance(value, dict) and not isinstance(value, PgDict):
			return PgDict(value, _parent=self, _path=path)
		if isinstance(value, list) and not isinstance(value, PgList):
			return PgList(value, _parent=self, _path=path)
		return value

	def __setattr__(self, name, value):
		if name.startswith('_') or name not in self.__class__._attrs_:
			object.__setattr__(self, name, value)
			return

		attr = self.__class__._attrs_[name]
		value = attr.coerce(value)
		value = self._wrap_value(name, value)
		object.__setattr__(self, name, value)

		if not self._initializing_:
			self._add_set_update([name], value)

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
		for name in self.__class__._attrs_:
			value = inst.get(name)
			if value is not None:
				doc[name] = json.loads(json.dumps(value, default=_json_default))
		return doc

	@classmethod
	def _from_doc(cls: Type[T], doc: dict, _val: int) -> T:
		obj = cls.__new__(cls)
		object.__setattr__(obj, '_initializing_', True)
		object.__setattr__(obj, '_is_create_new_', False)
		object.__setattr__(obj, '_', _val)
		object.__setattr__(obj, '_original_', _val)
		object.__setattr__(obj, '_set_updates', {})
		object.__setattr__(obj, '_unset_updates', set())

		for name, attr in cls._attrs_.items():
			value = doc.get(name)
			if value is not None:
				value = attr.coerce(value)
			elif attr.default is not _NOT_SET:
				value = attr.get_default()

			if value is not None:
				path = [name]
				if isinstance(value, dict) and not isinstance(value, PgDict):
					value = PgDict(value, _parent=obj, _path=path)
				elif isinstance(value, list) and not isinstance(value, PgList):
					value = PgList(value, _parent=obj, _path=path)
				object.__setattr__(obj, name, value)

		for name in cls._attrs_:
			val = getattr(obj, name, None)
			if isinstance(val, (PgDict, PgList)):
				val._is_local = False

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
			is_pk = field in cls._pk_attrs_
			col_expr = field if is_pk else f"(__->>'{field}')"
			attr = cls._attrs_.get(field)
			pg_type = _PY_TO_PG_TYPE.get(attr.type, "TEXT") if attr else "TEXT"

			if isinstance(value, dict):
				for op, op_val in value.items():
					if op == "$in":
						placeholders = ", ".join(["%s"] * len(op_val))
						conditions.append(f"{col_expr} IN ({placeholders})")
						if is_pk:
							params.extend(op_val)
						else:
							params.extend(str(v) for v in op_val)
					elif op in _MONGO_TO_PG_OP:
						pg_op = _MONGO_TO_PG_OP[op]
						if is_pk:
							conditions.append(f"{col_expr} {pg_op} %s")
							params.append(op_val)
						else:
							conditions.append(f"({col_expr})::{pg_type} {pg_op} %s")
							params.append(op_val)
					else:
						LOG_WARN("pg_orm_unknown_op", op=op, field=field)
			else:
				conditions.append(f"{col_expr} = %s")
				if is_pk:
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

	def commit(self):
		"""Persist this object: INSERT if new, UPDATE otherwise."""
		if self._is_create_new_:
			self.insert()
		else:
			self.update()

	def insert(self):
		doc = self._to_doc()
		new_ts = cur_ms()
		cls = self.__class__
		pk_attrs = cls._pk_attrs_

		pk_cols = ", ".join(pk_attrs)
		pk_placeholders = ", ".join(["%s"] * len(pk_attrs))
		pk_values = [getattr(self, pk) for pk in pk_attrs]

		sql = (
			f"INSERT INTO {cls._collection_name_} "
			f"({pk_cols}, _, __) "
			f"VALUES ({pk_placeholders}, %s, %s::jsonb)"
		)

		with cls._db_node_.use_conn() as conn:
			with conn.cursor() as cur:
				cur.execute(sql, pk_values + [new_ts, json.dumps(doc, default=_json_default)])
			conn.commit()

		object.__setattr__(self, '_', new_ts)
		object.__setattr__(self, '_original_', new_ts)
		object.__setattr__(self, '_is_create_new_', False)
		object.__setattr__(self, '_set_updates', {})
		object.__setattr__(self, '_unset_updates', set())

	def update(self, updates=None, conditions=None):
		"""
		Apply updates to this record with optimistic locking on _.

		updates: {
		    "$set":   {"field": value, "nested.field": value, ...},
		    "$unset": {"field": 1, ...},
		    "$inc":   {"field": delta, ...},
		}
		conditions: extra WHERE filters beyond pk + _ (same syntax as query())
		    e.g. {"status": "active", "score": {"$lt": 100}}

		When updates is None, tracked changes from attribute mutations are used.
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

		new_ts = cur_ms()
		original_ts = self._original_
		cls = self.__class__

		expr, update_params, has_inc = cls._build_update_expr(updates)
		pk_conditions = cls._pk_conditions()
		pk_values = self._pk_values()

		extra_conds, extra_params = cls._build_conditions_sql(conditions or {})
		where_parts = [pk_conditions, "_ = %s"] + extra_conds
		where = " AND ".join(where_parts)

		# Return __ too when $inc is used so we can refresh local state
		returning = "RETURNING __, _" if has_inc else "RETURNING _"
		sql = f"UPDATE {cls._collection_name_} SET __ = {expr}, _ = %s WHERE {where} {returning}"
		all_params = update_params + [new_ts] + pk_values + [original_ts] + extra_params

		with cls._db_node_.use_conn() as conn:
			with conn.cursor() as cur:
				cur.execute(sql, all_params)
				result = cur.fetchone()
			if result is None:
				raise OptimisticLockError(
					f"{cls._collection_name_}: update conflict or record not found "
					f"(pk={self._pk_values()}, _={original_ts})"
				)
			conn.commit()

		object.__setattr__(self, '_', new_ts)
		object.__setattr__(self, '_original_', new_ts)
		object.__setattr__(self, '_set_updates', {})
		object.__setattr__(self, '_unset_updates', set())

		# Refresh local state for $inc fields from the returned doc
		if has_inc and result.get('__'):
			object.__setattr__(self, '_initializing_', True)
			for name in updates.get("$inc", {}):
				top_field = name.split(".")[0] if isinstance(name, str) else name[0]
				if top_field in cls._attrs_:
					value = result['__'].get(top_field)
					if value is not None:
						self._init_attr(top_field, value)
			object.__setattr__(self, '_initializing_', False)

	def delete(self):
		cls = self.__class__
		pk_conditions = cls._pk_conditions()
		pk_values = self._pk_values()
		sql = f"DELETE FROM {cls._collection_name_} WHERE {pk_conditions}"
		with cls._db_node_.use_conn() as conn:
			with conn.cursor() as cur:
				cur.execute(sql, pk_values)
			conn.commit()

	@classmethod
	def get(cls: Type[T], **pk_kwargs) -> Optional[T]:
		"""Fetch a single record by primary key fields."""
		pk_values = [pk_kwargs[pk] for pk in cls._pk_attrs_]
		pk_conditions = cls._pk_conditions()
		sql = f"SELECT _, __ FROM {cls._collection_name_} WHERE {pk_conditions} LIMIT 1"
		with cls._db_node_.use_conn() as conn:
			with conn.cursor() as cur:
				cur.execute(sql, pk_values)
				row = cur.fetchone()
		if row is None:
			return None
		return cls._from_doc(row['__'], row['_'])

	@classmethod
	def query(
		cls: Type[T],
		_query: dict = None,
		sort=None,
		limit: int = None,
		offset: int = None,
	) -> Iterator[T]:
		"""
		Query records with MongoDB-style filter syntax.

		Supported operators: $gt, $lt, $gte, $lte, $ne, $in
		PK fields are compared against real columns; all others via JSONB.

		Example:
		    User.query({'age': {'$gte': 18}, 'active': True}, sort=[('name', 1)], limit=10)
		"""
		conditions, params = cls._build_conditions_sql(_query)
		where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

		order_parts = []
		if sort:
			for field, direction in sort:
				dir_str = "ASC" if direction >= 0 else "DESC"
				if field in cls._pk_attrs_ or field == '_':
					order_parts.append(f"{field} {dir_str}")
				else:
					order_parts.append(f"(__->>'{field}') {dir_str}")
		order = f"ORDER BY {', '.join(order_parts)}" if order_parts else ""

		limit_clause = f"LIMIT {int(limit)}" if limit is not None else ""
		offset_clause = f"OFFSET {int(offset)}" if offset is not None else ""

		sql = f"SELECT _, __ FROM {cls._collection_name_} {where} {order} {limit_clause} {offset_clause}".strip()

		with cls._db_node_.use_conn() as conn:
			with conn.cursor() as cur:
				cur.execute(sql, params)
				rows = cur.fetchall()

		for row in rows:
			yield cls._from_doc(row['__'], row['_'])

	@classmethod
	def create_table(cls):
		"""
		Create the table if it doesn't exist.
		initialize_model() must be called first so _pk_attrs_ is populated.
		"""
		pk_attrs = cls._pk_attrs_
		col_defs = []
		for pk in pk_attrs:
			attr = cls._attrs_[pk]
			pg_type = _PY_TO_PG_TYPE.get(attr.type, "TEXT")
			col_defs.append(f"    {pk} {pg_type}")
		col_defs.append("    _ BIGINT NOT NULL DEFAULT 0")
		col_defs.append("    __ JSONB NOT NULL DEFAULT '{}'")
		col_defs.append(f"    PRIMARY KEY ({', '.join(pk_attrs)})")

		table_sql = (
			f"CREATE TABLE IF NOT EXISTS {cls._collection_name_} (\n"
			+ ",\n".join(col_defs)
			+ "\n)"
		)

		with cls._db_node_.use_conn() as conn:
			with conn.cursor() as cur:
				cur.execute(table_sql)
				# Create all declared indexes
				for index_spec in cls._indexes_:
					cur.execute(index_spec['sql'])
			conn.commit()


# ──────────────────────────────────────────────────────────────────────────────
# Exceptions
# ──────────────────────────────────────────────────────────────────────────────

class OptimisticLockError(Exception):
	pass


# ──────────────────────────────────────────────────────────────────────────────
# initialize_model
# ──────────────────────────────────────────────────────────────────────────────


def _make_index_name(table, fields, unique):
	"""Deterministic index name: {table}_{f1}_{f2}[_uniq]."""
	suffix = "_uniq" if unique else ""
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


def initialize_model(model_cls):
	"""
	Scan the class MRO for Attribute definitions, parse INDEX() declarations,
	derive _pk_attrs_ from the first unique index, and compare against existing
	DB indexes (printing warnings on drift).

	Must be called once after the model class is defined.

	Example:
	    class User(Model):
	        _collection_name_ = "users"
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
	raw_indexes = IndexesToCreate.get(model_cls._collection_name_, [])
	table = model_cls._collection_name_

	parsed_indexes = []   # list of {'fields', 'unique', 'name', 'sql'}
	pk_attrs = None

	for raw_index in raw_indexes:
		fields, props = _parse_index_declaration(raw_index, attrs_to_name)
		if not fields:
			continue

		is_unique = props.get('unique', False)
		index_name = _make_index_name(table, fields, is_unique)

		# Build SQL column expressions
		# pk_attrs is not resolved yet here; we use a placeholder set and fix after
		col_exprs = []
		for field, direction in fields:
			dir_str = "ASC" if direction >= 0 else "DESC"
			# We don't know pk_attrs yet; treat all as JSONB initially.
			# After pk_attrs is set we regenerate if needed (below).
			col_exprs.append((field, direction, dir_str))

		unique_clause = "UNIQUE " if is_unique else ""

		# First unique index → pk_attrs
		if is_unique and pk_attrs is None:
			pk_attrs = [f for f, _ in fields]

		parsed_indexes.append({
			'fields': fields,
			'unique': is_unique,
			'name': index_name,
			'col_exprs': col_exprs,     # resolved to SQL below
		})

	# ── 3. Derive pk_attrs ───────────────────────────────────────────────────
	if not pk_attrs:
		if attrs:
			pk_attrs = [next(iter(attrs))]
			LOG_WARN(
				"pg_orm_no_unique_index",
				model=model_cls.__name__,
				desc=f"No unique INDEX found; falling back to first attribute '{pk_attrs[0]}' as pk"
			)
		else:
			pk_attrs = []

	model_cls._pk_attrs_ = pk_attrs

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
		index_sql = (
			f"CREATE {unique_clause}INDEX IF NOT EXISTS {spec['name']} "
			f"ON {table} ({', '.join(col_parts)})"
		)
		indexes.append({
			'fields': spec['fields'],
			'unique': spec['unique'],
			'name': spec['name'],
			'sql': index_sql,
		})

	model_cls._indexes_ = indexes

	# ── 5. Compare against existing DB indexes ───────────────────────────────
	if not model_cls._db_node_:
		return

	existing = _fetch_existing_indexes(model_cls._db_node_, table)
	if existing is None:
		# Table doesn't exist yet — nothing to compare
		return

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
