"""SQLite ORM with the same model, Attribute, INDEX and dbnode API as pg_orm.

Example::

    db = DatabaseNode("app.sqlite3", name="app")
    class User(Model):
        _table_name_ = "users"
        _db_node_ = "app"  # a node instance also works
        id = Attribute(str)
        meta = Attribute(dict)
        INDEX(id, {"unique": True})
    initialize_sqlite(db)
    User(id="alice", meta={"visits": 1}).commit()

Each node owns one SQLite connection. Concurrent writes are queued and grouped
into transactions (up to batch_size, after batch_interval seconds). commit(),
update() and delete() wait for the transaction to commit; they never acknowledge
uncommitted data. Sequential calls therefore commit individually. Use concurrent
gevent jobs to benefit from batching. A savepoint isolates each queued operation:
one invalid write does not discard other successful writes in the batch.

File databases use WAL and synchronous=NORMAL by default. WAL permits external
readers during writes; NORMAL may lose recent commits after power failure, while
preserving database consistency. SQLite in-memory databases cannot use WAL.
Callbacks run inside the writer transaction; same-node nested ORM operations join
that transaction and roll back together on callback failure. Cross-node callbacks
are not distributed transactions. Call close() before discarding a node. Nodes
are process-local; create new nodes after forking.

Schema setup is explicit and additive: initialize_sqlite() creates tables/indexes
and create_table() adds missing ordinary columns. Type changes need migrations.
Requires SQLite 3.35+ with JSON functions. Raw INDEX SQL uses SQLite syntax.
No PostgreSQL or psycopg2 is required.
"""
import hashlib
import inspect
import json
import re
import sqlite3
import time
from contextlib import contextmanager
from typing import TypeVar

import gevent
from gevent.event import AsyncResult
from gevent.lock import RLock
from gevent.queue import Queue

from .tools import cur_ms, all_subclasses

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
_DB_NODES_ = {}
IndexesToCreate = {}
_SQL_TYPES = {str: "TEXT", int: "INTEGER", float: "REAL", bool: "INTEGER", bytes: "BLOB"}
_PG_IDENTIFIER_PATTERN = r'(?:"(?:[^"]|"")*"|[A-Za-z_][A-Za-z0-9_$]*)'
_RAW_INDEX_PATTERN = re.compile(
    rf'^\s*CREATE\s+(?P<unique>UNIQUE\s+)?INDEX\s+'
    rf'(?:IF\s+NOT\s+EXISTS\s+)?(?P<name>{_PG_IDENTIFIER_PATTERN})\s+ON\b',
    re.IGNORECASE | re.DOTALL,
)


def SHARD_BY(*args, **kwargs):
    """Compatibility no-op; each table belongs to a single dbnode."""


def _quote(name):
    return '"' + name.replace('"', '""') + '"'


def _decode(row):
    if row is None:
        return None
    row = dict(row)
    if "__" in row:
        row["__"] = json.loads(row["__"])
    return row



def _is_field_spec(item):
    """
    True when `item` names a single indexed field, i.e. one of:
        name_or_attr                      e.g. user_id
        (name_or_attr, direction)         e.g. (user_id, ASCENDING)

    False when it is a nested index declaration (mongo_orm style), i.e. a tuple
    holding several fields and/or its own options dict:
        (session_id, user_id)
        ((created_at, -1), {"unique": False})
    """
    if not isinstance(item, tuple):
        return True  # a bare field reference
    if len(item) > 2 or not item:
        return False  # more than (field, direction) => a nested declaration
    if not isinstance(item[0], (Attribute, str)):
        return False  # e.g. ((created_at, -1), ...) — first element is a field spec
    # (field,) or (field, direction); anything else in slot 1 (an Attribute,
    # a nested tuple, an options dict) means this is a nested declaration.
    return len(item) == 1 or isinstance(item[1], int)

def _is_raw_index_sql(item):
    return (
        isinstance(item, str)
        and re.match(r'^\s*CREATE\s+(?:UNIQUE\s+)?INDEX\b', item, re.IGNORECASE) is not None
    )

def INDEX(*indexes):
    """
    Declare indexes for the enclosing Model class.
    Must be called inside the class body, after _table_name_/_collection_name_ is set.

    Three declaration styles are accepted, and may be mixed in one call.

    raw SQLite SQL — a string beginning with CREATE INDEX.

    sqlite_orm style — the whole call declares ONE (possibly compound) index, where
    each positional arg is either a field spec or an options dict:

        INDEX((id, ASCENDING), {'unique': True})
        INDEX((city, ASCENDING), (sort_order, DESCENDING), {'unique': False})

    mongo_orm style — each positional arg is itself a complete index, so one
    call declares SEVERAL indexes. This keeps models written against
    mongo_orm.INDEX working unchanged:

        INDEX(
            (session_id, user_id),
            (user_id, _type, (updated_at, -1), {"unique": False}),
        )
    """
    locals_ = inspect.currentframe().f_back.f_locals
    table_name = locals_.get("_table_name_") or locals_.get("_collection_name_")
    if not table_name:
        return  # not a SQLite model — silently ignore (e.g. mongo-only models)

    declarations = []
    own_index = []  # field specs/options belonging to this call itself (pg style)
    for item in indexes:
        if _is_raw_index_sql(item):
            declarations.append(item)
        elif isinstance(item, dict) or _is_field_spec(item):
            own_index.append(item)
        else:
            declarations.append(item)  # a complete nested index (mongo style)
    if any(not isinstance(item, dict) for item in own_index):
        declarations.insert(0, tuple(own_index))

    IndexesToCreate.setdefault(table_name, [])
    IndexesToCreate[table_name].extend(declarations)

def _json_default(obj):
    if isinstance(obj, SqliteDict):
        return dict(obj)
    if isinstance(obj, SqliteList):
        return list(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

class SqliteDict(dict):
    """
    A dict that tracks mutations for diff-patching the __ JSON column.

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
        if isinstance(value, dict) and not isinstance(value, SqliteDict):
            return SqliteDict(self._parent, child_path, value)
        if isinstance(value, list) and not isinstance(value, SqliteList):
            return SqliteList(self._parent, child_path, value)
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
        if isinstance(popped, SqliteDict):
            return dict(popped)
        if isinstance(popped, SqliteList):
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
            # Keep the live value so additions before commit are included in the
            # replacement, and keep tracking subsequent nested mutations.
            self._parent._add_set_update(self._path, self)

    def copy(self):
        return json.loads(json.dumps(dict(self), default=_json_default))

    def __reduce__(self):
        return (dict, (self.copy(),))

class SqliteList(list):
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

class Attribute:
    """
    Field descriptor for Model classes.

    The primary key is derived from the shortest unconditional unique INDEX()
    declaration, falling back to _id.
    """

    def __init__(self, _type, default=_NOT_SET, column=_NOT_SET, auto_increment=False, **kwargs):
        self.type = _type
        self.default = default
        if(column is _NOT_SET):
            column = True if _type in (int, float, str, bool, bytes) else False
        self.column = column or auto_increment  # True → stored as a real DB column instead of inside __
        self.auto_increment = auto_increment  # True → INTEGER PRIMARY KEY AUTOINCREMENT; DB generates value on INSERT
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
        if self.type in (int, float, str, bool, bytes):
            try:
                return self.type(value)
            except (ValueError, TypeError):
                return value
        return value

def with_lock(func):
    """Decorator that acquires a lock on self before calling func and releases it after."""
    def wrapper(self, *args, **kwargs):
        try:
            self.lock()
            return func(self, *args, **kwargs)
        except TimeoutError:
            return None
        finally:
            self.unlock()
    return wrapper

class OptimisticLockError(Exception):
    pass

class MissingTableError(Exception):
    pass

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

def _parse_raw_index_sql(index_sql):
    """Return ORM index metadata extracted from a raw CREATE INDEX statement."""
    match = _RAW_INDEX_PATTERN.match(index_sql)
    if match is None:
        raise ValueError("Raw index SQL must begin with a valid CREATE INDEX statement")
    normalized_sql = re.sub(r'\n\s*', ' ', index_sql).strip()
    index_name = match.group("name")
    if index_name.startswith('"'):
        index_name = index_name[1:-1].replace('""', '"')
    return {
        'fields': [],
        'unique': bool(match.group("unique")),
        'condition': None,
        'name': index_name,
        'sql': normalized_sql,
        'raw': True,
    }

class Model:
    _table_name_ = None
    _db_node_ = None
    _attrs_ = {}
    _pk_attrs_ = []
    _col_attrs_ = {}
    _indexes_ = []
    _event_listeners_ = {}


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
        object.__setattr__(self, '_', 0)  # persisted version/timestamp; updates advance it monotonically
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
            if _attr_obj_type is dict and not isinstance(value, SqliteDict):
                value = SqliteDict(self, [name], value or {})
            elif _attr_obj_type is list and not isinstance(value, SqliteList):
                value = SqliteList(self, [name], value or [])
            if object.__getattribute__(self, '_initializing_') is False:
                self._add_set_update([name], value)
        self.__dict__[name] = value


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


    def to_dict(self):
        """
        The record as a plain dict: every attribute that has a value, with
        dicts/lists unwrapped from their change-tracking wrappers.

        Mirrors mongo_orm.Model.to_dict so models shared between the two ORMs
        can call it (and super() into it) either way.
        """
        _json = {}
        for name in self.__class__._attrs_:
            value = getattr(self, name, None)
            if value is None:
                continue
            if isinstance(value, SqliteDict):
                value = value.copy()
            elif isinstance(value, SqliteList):
                value = value.copy()
            _json[name] = value
        if (_id := _json.get("_id")) is not None:
            _json["_id"] = str(_id)
        return _json


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


    @property
    def updated_at(self):
        # get "_" attr
        return getattr(self, '_', None)


    def _from_doc(obj, row, partial=False) -> T:
        # LOADING FROM DB
        cls = obj.__class__
        _val = row["_"]
        object.__setattr__(obj, '_row_', row)
        object.__setattr__(obj, '_initializing_', True)
        object.__setattr__(obj, '_is_create_new_', False)
        object.__setattr__(obj, '_', _val)
        object.__setattr__(obj, '_set_updates', {})
        object.__setattr__(obj, '_unset_updates', set())

        col_attrs = cls._col_attrs_
        for name, attr in cls._attrs_.items():
            # Primary-key and column=True fields are read directly from the row,
            # rather than from __.
            if partial:
                if name in cls._pk_attrs_ or name in col_attrs:
                    if name not in row: continue
                else:
                    if "__" not in row: continue
            source = row if name in cls._pk_attrs_ or name in col_attrs else row["__"]
            value = source.get(name, _NOT_SET)
            if value is not _NOT_SET:  # exists
                value = attr.coerce(value)
            elif attr.default is not _NOT_SET:  # has default
                value = attr.get_default()

            if value is not _NOT_SET:  # exists
                path = [name]
                if isinstance(value, dict) and not isinstance(value, SqliteDict):
                    value = SqliteDict(obj, path, value)
                elif isinstance(value, list) and not isinstance(value, SqliteList):
                    value = SqliteList(obj, path, value)
                object.__setattr__(obj, name, value)
            else:  # does not exist in DB
                if attr.type is dict:
                    object.__setattr__(obj, name, SqliteDict(obj, [name]))
                elif attr.type is list:
                    object.__setattr__(obj, name, SqliteList(obj, [name]))

        object.__setattr__(obj, '_initializing_', False)
        return obj


    def _pk_values(self):
        return [getattr(self, pk) for pk in self.__class__._pk_attrs_]


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


    class _LockHandle:
        def __init__(self, model_obj, name, acquired):
            self._model_obj = model_obj
            self._name = name
            self._acquired = acquired
            self._released = False

        def __bool__(self):
            return self._acquired

        def __enter__(self):
            if not self._acquired:
                raise TimeoutError("Lock not acquired")
            return self._model_obj

        def __exit__(self, exc_type, exc, tb):
            if self._acquired and not self._released:
                self._model_obj.unlock(name=self._name)
                self._released = True
            return False

        def unlock(self, force=False):
            if self._acquired and not self._released:
                self._model_obj.unlock(name=self._name, force=force)
                self._released = True


    def reload(self, projections=None, if_none=False):
        """Refresh this instance and return it, discarding changes to loaded fields.

        Projections accept field names or Attribute objects, as in get().
        if_none limits the refresh to missing/None values and empty strings or
        containers; zero and False are preserved. A missing row raises LookupError.
        """
        cls = self.__class__
        if projections is not None:
            if isinstance(projections, (str, Attribute)):
                projections = [projections]
            else:
                projections = list(projections)
            cls._build_select_columns(projections)  # validate before filtering
            projections = list(dict.fromkeys(
                field.attr_name if isinstance(field, Attribute) else field
                for field in projections
            ))
        if if_none:
            fields = cls._attrs_ if projections is None else projections
            projections = [
                name for name in fields
                if (value := self.__dict__.get(name)) is None
                or (isinstance(value, (str, bytes, dict, list, tuple, set)) and not value)
            ]
        if projections == []:
            return self

        fresh = cls.get(*self._pk_values(), projections=projections)
        if fresh is None:
            raise LookupError(f"{cls.__name__} {self._pk_values()!r} not found")
        row = fresh._row_
        if projections is None:
            # Remove stale values for fields no longer present in the row.
            for name in cls._attrs_:
                self.__dict__.pop(name, None)
            return self._from_doc(row)

        object.__setattr__(self, '_initializing_', True)
        object.__setattr__(self, '_is_create_new_', False)
        try:
            for name in projections:
                attr = cls._attrs_[name]
                source = row if name in cls._pk_attrs_ or name in cls._col_attrs_ else row['__']
                value = attr.coerce(source[name]) if name in source else attr.get_default()
                setattr(self, name, value)
        finally:
            object.__setattr__(self, '_initializing_', False)
        for path in list(self._set_updates):
            if path[0] in projections:
                del self._set_updates[path]
        self._unset_updates.difference_update(
            path for path in list(self._unset_updates) if path[0] in projections
        )
        object.__setattr__(self, '_', row['_'])
        previous_row = getattr(self, '_row_', {})
        merged_row = {**previous_row, **row}
        merged_json = dict(previous_row.get('__', {}))
        for name in projections:
            if name not in cls._pk_attrs_ and name not in cls._col_attrs_:
                merged_json.pop(name, None)
        merged_json.update(row['__'])
        merged_row['__'] = merged_json
        object.__setattr__(self, '_row_', merged_row)
        return self

    @classmethod
    def _field_sql(cls, field):
        if isinstance(field, Attribute):
            field = field.attr_name
        if field in cls._pk_attrs_ or field in cls._col_attrs_ or field == '_':
            return _quote(field)
        # Quote JSON keys independently; bind data values separately everywhere.
        path = '$' + ''.join('.' + json.dumps(p) for p in field.split('.'))
        return "json_extract(__, '" + path.replace("'", "''") + "')"

    @classmethod
    def _build_conditions_sql(cls, query):
        conditions, params = [], []
        for field, value in (query or {}).items():
            if field in ('$or', '$and'):
                groups = []
                for sub in value:
                    cond, values = cls._build_conditions_sql(sub)
                    groups.append('(' + (' AND '.join(cond) or '1') + ')')
                    params.extend(values)
                conditions.append('(' + (' OR ' if field == '$or' else ' AND ').join(groups) + ')' if groups else ('0' if field == '$or' else '1'))
                continue
            expr = cls._field_sql(field)
            operations = value if isinstance(value, dict) else {'$eq': value}
            for op, operand in operations.items():
                if op in ('$in', '$nin'):
                    operands = list(operand)
                    conditions.append(f"{expr} {'NOT IN' if op == '$nin' else 'IN'} ({','.join('?' for _ in operands)})")
                    params.extend(operands)
                elif op == '$exists':
                    conditions.append(f"{expr} IS {'NOT ' if operand else ''}NULL")
                else:
                    operators = {'$eq': '=', '$ne': '!=', '$gt': '>', '$gte': '>=', '$lt': '<', '$lte': '<='}
                    if op not in operators:
                        raise ValueError(f'Unsupported query operator: {op}')
                    if operand is None and op in ('$eq', '$ne'):
                        conditions.append(f"{expr} IS {'NOT ' if op == '$ne' else ''}NULL")
                    else:
                        conditions.append(f'{expr} {operators[op]} ?')
                        params.append(operand)
        return conditions, params

    @classmethod
    def _build_select_columns(cls, projections):
        # Projection validation is shared with reload(). Actual projection happens
        # after decoding so missing JSON keys stay missing rather than becoming null.
        if projections is None:
            return '*', []
        if isinstance(projections, (str, Attribute)):
            projections = [projections]
        names = []
        for field in projections:
            name = field.attr_name if isinstance(field, Attribute) else field
            if not isinstance(name, str):
                raise TypeError('projections must contain field names or Attribute objects')
            if name not in cls._attrs_:
                raise ValueError(f'{cls.__name__} has no field named {name!r}')
            names.append(name)
        return names, []

    @classmethod
    def _project(cls, row, projections):
        if projections is None:
            return row
        names, _ = cls._build_select_columns(projections)
        selected = set(names) | set(cls._pk_attrs_) | {'_'}
        result = {key: val for key, val in row.items() if key in selected}
        result['__'] = {key: val for key, val in row['__'].items() if key in names}
        return result

    @classmethod
    def get(cls, *pk_args, _replica=False, projections=None, **pk_kwargs):
        cls._build_select_columns(projections)
        if len(pk_args) == 1 and isinstance(pk_args[0], (list, set)):
            queries = [p if isinstance(p, dict) else {cls._pk_attrs_[0]: p} for p in pk_args[0]]
            return list(cls.query({'$or': queries}, projections=projections, _replica=_replica)) if queries else []
        if pk_args:
            if len(pk_args) != len(cls._pk_attrs_):
                raise TypeError(f'{cls.__name__}.get() expected {len(cls._pk_attrs_)} primary-key values')
            query = dict(zip(cls._pk_attrs_, pk_args))
        else:
            query = {pk: pk_kwargs[pk] for pk in cls._pk_attrs_}
        return next(cls.query(query, limit=1, projections=projections, _replica=_replica), None)

    @classmethod
    def query(cls, _query=None, sort=None, limit=None, offset=None, _replica=False, projections=None):
        cls._build_select_columns(projections)
        cond, params = cls._build_conditions_sql(_query)
        sql = f'SELECT * FROM {_quote(cls._table_name_)}'
        if cond:
            sql += ' WHERE ' + ' AND '.join(cond)
        if sort:
            sql += ' ORDER BY ' + ', '.join(cls._field_sql(f) + (' ASC' if d >= 0 else ' DESC') for f, d in sort)
        if limit is not None or offset is not None:
            sql += ' LIMIT ?'
            params.append(int(limit) if limit is not None else -1)
        if offset is not None:
            sql += ' OFFSET ?'
            params.append(int(offset))
        for row in cls._db_node_.fetch_rows(sql, params, use_replica=_replica):
            yield cls.__new__(cls)._from_doc(cls._project(row, projections))

    def _insert(self, force=False):
        cls = type(self)
        cls._trigger_event(EVENT_BEFORE_CREATE, self)
        values = {name: vars(self).get(name) for name in cls._pk_attrs_ + list(cls._col_attrs_)}
        values = {name: value for name, value in values.items() if not (cls._attrs_[name].auto_increment and value is None)}
        values.update({'_': cur_ms(), '__': json.dumps(self._to_doc(), default=_json_default)})
        sql = f'INSERT INTO {_quote(cls._table_name_)} ({", ".join(map(_quote, values))}) VALUES ({", ".join("?" for _ in values)}) RETURNING *'
        try:
            row = cls._db_node_.write(lambda conn: _decode(conn.execute(sql, list(values.values())).fetchone()))
        except sqlite3.IntegrityError:
            if not force:
                raise
            payload = self.to_dict()
            existing = cls.get(*self._pk_values())
            if existing is None:
                raise
            self._from_doc(existing._row_)
            self.update({'$set': {k: v for k, v in payload.items() if k not in cls._pk_attrs_}})
            return
        cls._db_node_._remember(self)
        self._from_doc(row)
        cls._trigger_event(EVENT_AFTER_CREATE, self)

    def update(self, updates=None, conditions=None, callback=None):
        """Apply tracked changes or $set/$unset/$inc/$push atomically.

        The writer reads the latest document under BEGIN IMMEDIATE before
        patching, preserving unrelated concurrent edits and atomic increments.
        A callback exception rolls back this operation and same-node nested writes.
        """
        if updates is None:
            updates = {'$set': dict(self._set_updates), '$unset': {p: 1 for p in self._unset_updates}}
        if not any(updates.values()):
            return
        if set(updates) - {'$set', '$unset', '$inc', '$push'}:
            raise ValueError('Unsupported update operator')
        cls = type(self)
        cls._trigger_event(EVENT_BEFORE_UPDATE, self)
        where, params = cls._build_conditions_sql(dict(zip(cls._pk_attrs_, self._pk_values())))
        extra, extras = cls._build_conditions_sql(conditions)
        sql = f'SELECT * FROM {_quote(cls._table_name_)} WHERE ' + ' AND '.join(where + extra)
        def apply(conn):
            row = _decode(conn.execute(sql, params + extras).fetchone())
            if row is None:
                if conditions:
                    return None
                raise OptimisticLockError(f'{cls._table_name_}: record deleted during update')
            for op, changes in updates.items():
                for field, value in changes.items():
                    path = list(field) if isinstance(field, tuple) else field.split('.')
                    top = path[0]
                    if top not in cls._attrs_:
                        raise ValueError(f'{cls.__name__} has no field named {top!r}')
                    real = top in cls._pk_attrs_ or top in cls._col_attrs_
                    target = row if real else row['__']
                    for part in path[:-1]:
                        if isinstance(target, list):
                            target = target[int(part)]
                        else:
                            if part not in target or target[part] is None:
                                target[part] = {}
                            target = target[part]
                    key = int(path[-1]) if isinstance(target, list) else path[-1]
                    old = target[key] if isinstance(target, list) else target.get(key)
                    if op == '$unset':
                        if isinstance(target, list):
                            target[key] = None
                        elif real:
                            target[key] = None
                        else:
                            target.pop(key, None)
                    elif op == '$inc':
                        target[key] = (old or 0) + value
                    elif op == '$push':
                        target[key] = list(old or []) + [value]
                    else:
                        target[key] = cls._attrs_[top].coerce(value) if real else json.loads(json.dumps(value, default=_json_default))
            row['_'] = max(cur_ms(), row['_'] + 1)
            columns = list(cls._pk_attrs_) + list(cls._col_attrs_) + ['_', '__']
            vals = [json.dumps(row[c], default=_json_default) if c == '__' else row[c] for c in columns]
            conn.execute(f'UPDATE {_quote(cls._table_name_)} SET ' + ', '.join(_quote(c) + '=?' for c in columns) + ' WHERE ' + ' AND '.join(where), vals + params)
            if callback:
                callback(self, row)
            return row
        row = cls._db_node_.write(apply)
        if row is None:
            return False
        original = getattr(self, '_row_', None)
        cls._db_node_._remember(self)
        self._from_doc(row)
        cls._trigger_event(EVENT_ROW_AFTER_UPDATE, self, original, row)
        cls._trigger_event(EVENT_AFTER_UPDATE, self)
        return True

    def delete(self):
        cls = type(self)
        cls._trigger_event(EVENT_BEFORE_DELETE, self)
        cond, params = cls._build_conditions_sql(dict(zip(cls._pk_attrs_, self._pk_values())))
        cls._db_node_.write(lambda conn: conn.execute(f'DELETE FROM {_quote(cls._table_name_)} WHERE ' + ' AND '.join(cond), params).rowcount)
        cls._trigger_event(EVENT_AFTER_DELETE, self)

    def lock(self, name='', timeout=5000, silent=False, can_hold_until=120000):
        """Claim a persisted lease atomically; expiry and timeout are milliseconds."""
        key = '_lock_' + name
        locks = self.__dict__.setdefault('_locks', {})
        deadline = cur_ms() + timeout
        cls = type(self)
        cond, params = cls._build_conditions_sql(dict(zip(cls._pk_attrs_, self._pk_values())))
        path = '$.' + json.dumps(key)
        while cur_ms() < deadline:
            now = cur_ms()
            expires = now + can_hold_until
            if self._is_create_new_:
                acquired = key not in locks
            else:
                def claim(conn):
                    return conn.execute(f'UPDATE {_quote(cls._table_name_)} SET __=json_set(__, ?, ?), _=max(_+1, ?) WHERE ' + ' AND '.join(cond) + ' AND (json_extract(__, ?) IS NULL OR json_extract(__, ?) < ?) RETURNING _', [path, expires, now, *params, path, path, now]).fetchone()
                acquired = cls._db_node_.write(claim)
            if acquired:
                if not self._is_create_new_:
                    object.__setattr__(self, '_', acquired['_'])
                locks[key] = expires
                return self._LockHandle(self, name, True)
            time.sleep(0.01)
        if not silent:
            raise TimeoutError(f'Lock timed out: {cls.__name__} {self._pk_values()}')
        return self._LockHandle(self, name, False)

    def unlock(self, name='', force=False):
        key = '_lock_' + name
        locks = self.__dict__.setdefault('_locks', {})
        if not force and key not in locks:
            return
        if not self._is_create_new_:
            cls = type(self)
            cond, params = cls._build_conditions_sql(dict(zip(cls._pk_attrs_, self._pk_values())))
            path = '$.' + json.dumps(key)
            sql = f'UPDATE {_quote(cls._table_name_)} SET __=json_remove(__, ?), _=max(_+1, ?) WHERE ' + ' AND '.join(cond)
            values = [path, cur_ms(), *params]
            if not force:
                sql += ' AND json_extract(__, ?)=?'
                values.extend([path, locks[key]])
            cls._db_node_.write(lambda conn: conn.execute(sql, values).rowcount)
        locks.pop(key, None)

    @classmethod
    def _build_create_table_sql(cls):
        columns = []
        for name in cls._pk_attrs_ + list(cls._col_attrs_):
            attr = cls._attrs_[name]
            if attr.auto_increment:
                if cls._pk_attrs_ != [name] or attr.type is not int:
                    raise ValueError('SQLite auto_increment requires a single integer primary key')
                definition = 'INTEGER PRIMARY KEY AUTOINCREMENT'
            else:
                definition = _SQL_TYPES.get(attr.type, 'TEXT')
                if name in cls._pk_attrs_:
                    definition += ' NOT NULL'
            columns.append(_quote(name) + ' ' + definition)
        columns += ['_ INTEGER NOT NULL DEFAULT 0', "__ TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(__))"]
        if not any(cls._attrs_[pk].auto_increment for pk in cls._pk_attrs_):
            columns.append('PRIMARY KEY (' + ', '.join(map(_quote, cls._pk_attrs_)) + ')')
        return f'CREATE TABLE IF NOT EXISTS {_quote(cls._table_name_)} (' + ', '.join(columns) + ')'

    @classmethod
    def create_table(cls):
        """Create table/indexes, adding missing ordinary columns without dropping data."""
        def create(conn):
            conn.execute(cls._build_create_table_sql())
            existing = {row['name'] for row in conn.execute(f'PRAGMA table_info({_quote(cls._table_name_)})')}
            for name, attr in cls._col_attrs_.items():
                if name not in existing:
                    conn.execute(f'ALTER TABLE {_quote(cls._table_name_)} ADD COLUMN {_quote(name)} {_SQL_TYPES.get(attr.type, "TEXT")}')
            for spec in cls._indexes_:
                conn.execute(spec['sql'])
        cls._db_node_.write(create)


class DatabaseNode:
    """One SQLite database, connection and batched writer per node.

    path (or db_name) is a filesystem filename or SQLite URI. batch_interval
    defaults to 1ms and batch_size to 100. Setting interval=0 still groups jobs
    already queued. use_conn() is a serialized escape hatch for raw SQL; callers
    must commit or roll back before leaving. Prefer write(callable) for mutations
    so they use the default batching and per-operation savepoints.
    """
    def __init__(self, path=None, *, db_name=None, name=None, replica=None,
                 batch_size=100, batch_interval=0.001, timeout=5.0, uri=False):
        if batch_size < 1 or batch_interval < 0:
            raise ValueError('batch_size must be positive and batch_interval nonnegative')
        self.path = str(path if path is not None else db_name if db_name is not None else ':memory:')
        self.db_name = self.path
        self.replica = replica
        self.batch_size = batch_size
        self.batch_interval = batch_interval
        self._mutex = RLock()
        self._queue = Queue()
        self._worker = None
        self._undo = []
        self._depth = 0
        self._closed = False
        self._conn = sqlite3.connect(self.path, timeout=timeout, uri=uri, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute('PRAGMA journal_mode=WAL')
        self._conn.execute('PRAGMA synchronous=NORMAL')
        self._conn.execute('PRAGMA foreign_keys=ON')
        if name:
            _DB_NODES_[name] = self

    def _get_replica(self):
        if isinstance(self.replica, str):
            return _DB_NODES_[self.replica]
        return self.replica or self

    def write(self, operation):
        """Run operation(connection), returning only after its batch is committed."""
        if gevent.getcurrent() is self._worker:
            self._depth += 1
            savepoint = f'orm_nested_{self._depth}'
            mark = len(self._undo)
            self._conn.execute('SAVEPOINT ' + savepoint)
            try:
                return operation(self._conn)
            except Exception:
                self._conn.execute('ROLLBACK TO ' + savepoint)
                self._restore(mark)
                raise
            finally:
                self._conn.execute('RELEASE ' + savepoint)
                self._depth -= 1
        if self._closed:
            raise RuntimeError('DatabaseNode is closed')
        result = AsyncResult()
        self._queue.put((operation, result))
        if self._worker is None:
            self._worker = gevent.spawn(self._drain)
        return result.get()

    def _remember(self, obj):
        # Nested callbacks update their model before the surrounding commit.
        # Restore these objects too if that surrounding transaction fails.
        if gevent.getcurrent() is self._worker:
            self._undo.append((obj, dict(vars(obj))))

    def _restore(self, mark=0):
        for obj, state in reversed(self._undo[mark:]):
            obj.__dict__.clear()
            obj.__dict__.update(state)
        del self._undo[mark:]

    def _drain(self):
        try:
            while not self._queue.empty():
                if self.batch_interval:
                    gevent.sleep(self.batch_interval)
                jobs = [self._queue.get_nowait() for _ in range(min(self.batch_size, self._queue.qsize()))]
                outcomes = []
                with self._mutex:
                    try:
                        self._conn.execute('BEGIN IMMEDIATE')
                        for operation, result in jobs:
                            mark = len(self._undo)
                            self._conn.execute('SAVEPOINT orm_operation')
                            try:
                                value = operation(self._conn)
                            except Exception as exc:
                                self._conn.execute('ROLLBACK TO orm_operation')
                                self._restore(mark)
                                outcomes.append((result, None, exc))
                            else:
                                outcomes.append((result, value, None))
                            finally:
                                self._conn.execute('RELEASE orm_operation')
                        self._conn.commit()
                        self._undo.clear()
                    except Exception as exc:
                        self._conn.rollback()
                        self._restore()
                        outcomes = [(result, None, exc) for _, result in jobs]
                for result, value, error in outcomes:
                    if error is not None:
                        result.set_exception(error)
                    else:
                        result.set(value)
        finally:
            self._worker = None

    def get_conn(self):
        """Acquire serialized raw access; pair with put_conn() in a finally block."""
        self._mutex.acquire()
        if self._closed and gevent.getcurrent() is not self._worker:
            self._mutex.release()
            raise RuntimeError('DatabaseNode is closed')
        return self._conn

    def put_conn(self, conn):
        """Release raw access, rolling back an unfinished external transaction."""
        if conn is not self._conn:
            raise ValueError('Connection does not belong to this node')
        try:
            if gevent.getcurrent() is not self._worker and conn.in_transaction:
                conn.rollback()
        finally:
            self._mutex.release()

    @contextmanager
    def use_conn(self):
        """Serialize raw access; rollback any unfinished transaction on exit."""
        conn = self.get_conn()
        try:
            yield conn
        finally:
            self.put_conn(conn)

    def fetch_rows(self, sql, params=None, use_replica=False):
        node = self._get_replica() if use_replica else self
        with node.use_conn() as conn:
            rows = [_decode(row) for row in conn.execute(sql, params or [])]
        return iter(rows)

    def close(self):
        """Drain pending writes and close; further operations raise RuntimeError."""
        if gevent.getcurrent() is self._worker:
            raise RuntimeError('Cannot close a node inside its write callback')
        self._closed = True
        if self._worker is not None:
            self._worker.join()
        with self._mutex:
            self._conn.close()


def initialize_model(model_cls):
    """Resolve a table's node, attributes, primary key and declared indexes."""
    cls = model_cls
    cls._table_name_ = cls._table_name_ or getattr(cls, '_collection_name_', None)
    if not cls._table_name_:
        raise ValueError(f'{cls.__name__} requires _table_name_ or _collection_name_')
    if isinstance(cls._db_node_, str):
        cls._db_node_ = _DB_NODES_[cls._db_node_]
    if cls._db_node_ is None:
        raise ValueError(f'{cls.__name__} requires a DatabaseNode')
    attrs = {}
    for base in reversed(cls.__mro__):
        attrs.update({name: attr for name, attr in vars(base).items() if isinstance(attr, Attribute)})
    cls._attrs_ = attrs
    lookup = {id(attr): name for name, attr in attrs.items()}
    specs, raw_specs = [], []
    pk = None
    for declaration in IndexesToCreate.get(cls._table_name_, []):
        if _is_raw_index_sql(declaration):
            raw_specs.append(_parse_raw_index_sql(declaration))
            continue
        fields, props = _parse_index_declaration(declaration, lookup)
        if not fields:
            continue
        unique = props.get('unique', True)
        if unique and not props.get('condition') and (pk is None or len(fields) < len(pk)):
            pk = [field for field, _ in fields]
        specs.append({'fields': fields, 'unique': unique, 'condition': props.get('condition')})
    if pk is None:
        pk = ['_id']
        if '_id' not in attrs:
            attr = attrs['_id'] = Attribute(int, auto_increment=True)
            attr.__set_name__(cls, '_id')
            setattr(cls, '_id', attr)
        else:
            specs.insert(0, {'fields': [('_id', 1)], 'unique': True, 'condition': None})
    for name in pk:
        if name not in attrs:
            raise ValueError(f'Primary key {name!r} is not a declared Attribute')
    cls._pk_attrs_ = pk
    cls._col_attrs_ = {name: attr for name, attr in attrs.items() if attr.column and name not in pk}
    for spec in specs:
        name = cls._table_name_ + '_' + '_'.join(f'{f}_{"asc" if d >= 0 else "desc"}' for f, d in spec['fields'])
        if len(name) > 63:
            name = name[:54] + '_' + hashlib.md5(name.encode()).hexdigest()[:8]
        spec['name'] = name
        fields = ', '.join(cls._field_sql(f) + (' ASC' if d >= 0 else ' DESC') for f, d in spec['fields'])
        spec['sql'] = f'CREATE {"UNIQUE " if spec["unique"] else ""}INDEX IF NOT EXISTS {_quote(name)} ON {_quote(cls._table_name_)} ({fields})'
        if spec['condition']:
            spec['sql'] += ' WHERE ' + spec['condition']
    cls._indexes_ = specs + raw_specs
    cls.create_table()


def initialize_sqlite(db_nodes):
    """Initialize defined models; unassigned tables use the first supplied node.

    Accept a DatabaseNode, filename, options dict, or a list of nodes/dicts.
    Named model assignments resolve strictly; unknown names raise KeyError.
    Returns the nodes so applications can close them during shutdown.
    """
    if not isinstance(db_nodes, (list, tuple)):
        db_nodes = [db_nodes]
    nodes = [node if isinstance(node, DatabaseNode) else DatabaseNode(**node) if isinstance(node, dict) else DatabaseNode(node) for node in db_nodes]
    if not nodes:
        raise ValueError('At least one database node is required')
    for cls in all_subclasses(Model):
        if not (getattr(cls, '_table_name_', None) or getattr(cls, '_collection_name_', None)):
            continue
        if cls._db_node_ is None:
            cls._db_node_ = nodes[0]
        initialize_model(cls)
    return nodes
