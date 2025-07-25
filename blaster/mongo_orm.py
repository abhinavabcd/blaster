import inspect
import json
import heapq
import types
import pymongo
import random
import ujson  # it's fastest even to copy as it's a C extension
from functools import cmp_to_key
from contextlib import ExitStack
from typing import TypeVar, Iterator, Type
# from pymongo.read_concern import ReadConcern
# from pymongo.write_concern import WriteConcern
from bson.objectid import ObjectId
from itertools import chain
from collections import OrderedDict
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, PyMongoError
from pymongo import ReturnDocument
from .tools import all_subclasses, \
	cur_ms, list_diff2, batched_iter, _OBJ_END_, \
	get_by_key_path
from .config import IS_DEV, MONGO_WARN_MAX_RESULTS_RATE, \
	MONGO_MAX_RESULTS_AT_HIGH_SCAN_RATE, \
	DEBUG_PRINT_LEVEL, IS_TEST
from gevent.threading import Thread
from gevent import time
from .logging import LOG_WARN, LOG_ERROR, LOG_DEBUG

_loading_errors = {}


EVENT_BEFORE_DELETE = -2
EVENT_AFTER_DELETE = -1
EVENT_BEFORE_UPDATE = 1
EVENT_AFTER_UPDATE = 2
EVENT_BEFORE_CREATE = 3
EVENT_AFTER_CREATE = 4
EVENT_MONGO_AFTER_UPDATE = 5

# just a random constant object to check for non existent
# keys without catching KeyNotExist exception
# given a list of [(filter, iter), ....] , flattens them

_NOT_EXISTS_QUERY = {"$exists": False}  # just a constant

LIST_OR_DICT_TYPE = frozenset((list, dict))


# utility to print collection name + node for logging
def COLLECTION_NAME(collection):
	_nodes = collection.database.client.nodes or (("unknown", 0),)
	for _node in _nodes:
		return "{:s}.{:s}.{:d}".format(collection.full_name, _node[0], _node[1])
	return collection.full_name


def str_validator(_attr, val, obj):
	max_len = getattr(_attr, "max_len", 4096)
	if(val and len(val) > max_len):
		val = val[:max_len]
	return val


def dict_validator(_attr, val, obj):
	return val if val is not None else {}


def list_validator(_attr, val, obj):
	return val if val is not None else []


DEFAULT_VALIDATORS = {
	str: str_validator,
	dict: dict_validator,
	list: list_validator
}


class Attribute(object):
	_type = None
	_validator = None

	def __init__(self, _type, validator=_OBJ_END_, **kwargs):
		self._type = _type
		self._validator = DEFAULT_VALIDATORS.get(_type)\
			if validator is _OBJ_END_ else validator
		self.__dict__.update(kwargs)


__all_models__ = {}


class MongoList(list):
	_is_local = True  # not tied to db
	_model_obj = None
	path = ""  # empty string becuase it doesn't break pickling

	def __init__(self, _model_obj, path, initial_value):
		self._is_local = True  # initially true so __setitem__ doesn't update db query
		self._model_obj = _model_obj
		self.path = path
		if(initial_value):
			for item in initial_value:
				self.append(item)
		self._is_local = _model_obj._is_create_new_  # Hack for new object
		if(_model_obj._is_create_new_):
			_model_obj._if_non_empty_set_query_update[path] = self

	def __setitem__(self, k, v):
		_model_obj = self._model_obj
		if(not self._is_local):
			_model_obj._set_query_updates[self.path + "." + str(k)] = v
		elif(_model_obj._initializing_):
			# recursively create custom dicts/list when
			# loading object from db
			if(isinstance(v, dict)):
				v = MongoDict(_model_obj, self.path + "." + str(k), v)
			elif(isinstance(v, list)):
				v = MongoList(_model_obj, self.path + "." + str(k), v)
		super(MongoList, self).__setitem__(k, v)

	def remove(self, item):  # can raise value exception
		super(MongoList, self).remove(item)
		_pull = self._model_obj._other_query_updates.get("$pull")
		if(_pull is None):
			self._model_obj._other_query_updates["$pull"] = _pull = {}
		value_to_remove = _pull.get(self.path, _OBJ_END_)
		if(value_to_remove is _OBJ_END_):
			_pull[self.path] = item  # first item to remove
		else:
			# if there are multiple items to remove, use $in
			if(
				isinstance(value_to_remove, dict)
				and "$in" in value_to_remove
			):
				_pull[self.path]["$in"].append(item)
			else:
				_pull[self.path] = {"$in": [value_to_remove, item]}

	def pop(self, i=None):
		if(i is None):
			i = 1
			ret = super(MongoList, self).pop()
		elif(i == 0):
			ret = super(MongoList, self).pop(0)  # remove first item
			i = -1
		else:
			raise Exception(
				"can only remove first or last elements, supported arg is 0 or None"
			)

		if(not self._is_local):
			_pop = self._model_obj._other_query_updates.get("$pop")
			if(_pop is None):
				self._model_obj._other_query_updates["$pop"] = _pop = {}
			_pop[self.path] = i
		# convert them to original object types
		if(isinstance(ret, list)):
			return list(ret)
		if(isinstance(ret, dict)):
			return dict(ret)
		return ret

	def insert(self, pos, item):
		if(not self._is_local):
			_push = self._model_obj._other_query_updates.get("$push")
			if(_push is None):
				self._model_obj._other_query_updates["$push"] = _push = {}

			_push_path_values = _push.get(self.path)
			if(not _push_path_values):
				_push[self.path] = _push_path_values = {"$position": pos, "$each": [item]}
			elif(_push_path_values.get("$position") == pos):
				_push_path_values["$each"].append(item)
			else:
				raise Exception("You may be concurrently updating the list with insert/append. Commit it and update")
		super(MongoList, self).insert(pos, item)

	def append(self, item):
		if(self._is_local):
			if(isinstance(item, dict)):
				item = MongoDict(self._model_obj, self.path + "." + str(len(self)), item)
			elif(isinstance(item, list)):
				item = MongoList(self._model_obj, self.path + "." + str(len(self)), item)

		super(MongoList, self).append(item)

		if(not self._is_local):
			_push = self._model_obj._other_query_updates.get("$push")
			if(_push is None):
				_push = self._model_obj._other_query_updates["$push"] = {}

			_push_path_values = _push.get(self.path)
			if(not _push_path_values):
				_push[self.path] = _push_path_values = {"$each": [item]}
			else:
				_push_path_values["$each"].append(item)

	def extend(this, items):
		for i in items:
			this.append(i)

	def clear(self):
		super().clear()
		self._model_obj._set_query_updates[self.path] = self
		self._is_local = True  # Hack for new object

	def copy(self) -> dict:
		ret = get_by_key_path(self._model_obj._original_doc, self.path)
		if(ret is not None):
			return ujson.loads(ujson.dumps(ret))
		return []  # return empty list if not found


class MongoDict(dict):
	_is_local = True  # indicates it's not tied to db
	_model_obj = None
	path = ""  # empty string becuase it doesn't break pickling

	def __init__(self, _model_obj, path, initial_value):
		self._is_local = True   # initialy starts as local object while initializing
		self._model_obj = _model_obj
		self.path = path
		if(initial_value):
			self.update(initial_value)
		self._is_local = _model_obj._is_create_new_  # Hack for new object
		if(_model_obj._is_create_new_):
			_model_obj._if_non_empty_set_query_update[path] = self

	def __setitem__(self, k, v):
		_model_obj = self._model_obj
		if(not self._is_local):
			_model_obj._set_query_updates_with_no_conflicts(self.path + "." + str(k), v)
		elif(_model_obj._initializing_):
			# recursively create custom dicts/list when
			# loading object from db
			if(isinstance(v, dict)):
				v = MongoDict(_model_obj, self.path + "." + str(k), v)
			elif(isinstance(v, list)):
				v = MongoList(_model_obj, self.path + "." + str(k), v)

		super(MongoDict, self).__setitem__(k, v)

	def __delitem__(self, k):
		self.pop(k)

	def pop(self, k, default=None):
		# not in initializing mode
		popped_val = super(MongoDict, self).pop(k, _OBJ_END_)
		if(popped_val is _OBJ_END_):
			return default
		# if not initializing and has value set remove it form mongo
		elif(not self._is_local):
			new_path = self.path + "." + str(k)
			_unset = self._model_obj._other_query_updates.get("$unset")
			if(not _unset):
				_unset = self._model_obj._other_query_updates["$unset"] = {}
			_unset[new_path] = ""
		# convert them to original object types
		if(isinstance(popped_val, list)):
			return list(popped_val)
		if(isinstance(popped_val, dict)):
			return dict(popped_val)
		return popped_val

	def update(self, another):
		for k, v in another.items():
			# calls __setitem__ again
			self[k] = v
		# allow chaining
		return self

	def clear(self):
		super().clear()
		self._model_obj._set_query_updates_with_no_conflicts(self.path, self)  # self as it's already a mongo dict
		self._is_local = True  # Hack for new object

	def copy(self) -> dict:
		ret = get_by_key_path(self._model_obj._original_doc, self.path)
		if(ret is not None):
			return ujson.loads(ujson.dumps(ret))
		return {}  # return empty dict if not found


# utility function to cache and return a session for transaction
def _with_session(collection, _transaction, exit_stack):
	dbnode = collection._db_node_
	mclient = dbnode.mongo_client

	if(_transaction is None or not dbnode.replicaset):
		return None

	if(_stack := _transaction.get("stack")):  # reuse top level exit stack
		exit_stack = _stack
	else:
		_transaction["stack"] = exit_stack
	session = _transaction.get(mclient)
	if(not session):
		_transaction[mclient] = session = mclient.start_session()
		exit_stack.enter_context(session)

	if((_tracked_transactions := _transaction.get("transactions")) is None):  # cleared when failed
		_transaction["transactions"] = _tracked_transactions = OrderedDict()

	if(session not in _tracked_transactions):
		if(session.in_transaction):
			session.abort_transaction()
		_tracked_transactions[session] = session.start_transaction()
	return session


def _commit_transaction(_transaction):
	if(not _transaction):
		return

	for session in reversed(list(_transaction["transactions"].keys())):
		while(True):
			try:
				session.commit_transaction()
				break
			except PyMongoError as exc:
				if (exc.has_error_label("UnknownTransactionCommitResult")):
					# Retry the commit.
					continue
				raise
	_transaction.pop("transactions", None)


def _abort_transaction(_transaction):
	if(not _transaction):
		return
	# sometimes many transient errors and exits with cleanedup
	if(_transactions := _transaction.get("transactions")):
		for session in reversed(list(_transactions.keys())):
			session.abort_transaction()

	_transaction.pop("transactions", None)


ModelType = TypeVar('ModelType', bound='Model')  # use string


class Model(object):
	# ###class level###
	_attrs = None
	_pk_attrs = None

	_cache_ = None
	# used to identify if it's a primary or secondary shard
	_is_secondary_shard = False

	# ###instance level###
	_is_create_new_ = True
	# this means the object is being initialized by this orm,
	# and not available to user yet
	_initializing_ = False
	# pending query updates are stored in this
	_set_query_updates = None
	_if_non_empty_set_query_update = None
	_other_query_updates = None
	_original_doc = None
	# we execute all functions inside this list,
	# and use the return value to update
	_insert_result = None

	# initialize a new object
	def __init__(self, _is_create_new_=True, **values):

		# defaults
		self._is_create_new_ = _is_create_new_
		self._initializing_ = not _is_create_new_
		self._set_query_updates = {}
		self._if_non_empty_set_query_update = {}
		self._other_query_updates = {}
		cls = self.__class__
		for k, v in cls._attrs_.items():
			self.__dict__[k] = None  # set all initial attributes to None

		if(_is_create_new_):
			# setup default values
			self.__check_and_set_initial_defaults(values)  # from empty doc

		# update given values
		for k, v in values.items():
			setattr(self, k, v)

	def get_id(self):
		return str(self._id)

	# Usage:
	# YourTable.get(a=c, b=d)
	# YourTable.get({"p": "q", "r": "s"}) # this will kick in cache use
	# YourTable.get(SOMEID) # this will kick in cache use
	@classmethod
	def get(cls: Type[ModelType], _pk=None, use_cache=True, **kwargs) -> ModelType:
		if(_pk is not None):
			is_single_item = False
			_pks = None
			if(not isinstance(_pk, (list, set))):
				is_single_item = True
				_pks = [_pk]
			else:
				_pks = _pk

			if(not _pks):
				return []

			from_cache = []
			# handle _ids fetch separately so to speedup

			is_already_fetching = {}
			to_fetch_pks = []
			to_fetch_ids = []
			for _pk in _pks:
				_id = None  # string based id
				_pk_tuple = None  # used to query in cache
				if(not _pk):
					continue
				elif(isinstance(_pk, str)):
					_pk_tuple = (_pk,)
					_id = _pk
					_pk = None  # reset it to null as we are fetching it via ids
				else:  # dict
					_pk_tuple = []
					for _k in cls._pk_attrs:
						_pk_tuple.append(_pk[_k])  # should present
					_pk_tuple = tuple(_pk_tuple)

				# check already in cache
				_doc_in_cache = None
				if(use_cache is True):
					use_cache = cls._cache_
				elif(use_cache is False):
					use_cache = None

				if(use_cache is not None):
					# user supplied cache
					_doc_in_cache = use_cache.get(_pk_tuple)

				if(not _doc_in_cache):
					if(is_already_fetching.get(_pk_tuple)):
						continue
					is_already_fetching[_pk_tuple] = True
					if(_pk):
						# remove duplicate pk query check if already fetching
						to_fetch_pks.append(_pk)
					if(_id):
						to_fetch_ids.append(_id)

				else:
					from_cache.append(cls(False)._reset(_doc_in_cache))

			_query = {"$or": to_fetch_pks}
			if(to_fetch_ids):
				to_fetch_pks.append({"_id": {"$in": to_fetch_ids}})

			# query all items
			ret = list(chain(from_cache, cls.query(_query)))

			# user supplied cache
			if(use_cache is not None):
				for _item in ret:
					use_cache[_item.pk_tuple()] = _item._original_doc

			if(is_single_item):
				if(ret):
					return ret[0]
				else:
					return None
			# multiple items, reorder
			# put them into id and pk_map
			# convert them to list by querying again from map
			pk_map = {}
			for item in ret:
				if(item):
					pk_map[item.pk_tuple()] = item
			id_map = {}
			for item in ret:
				if(item):
					id_map[item._id] = item
			ret = []
			for _pk in _pks:
				if(not _pk):
					continue
				elif(isinstance(_pk, str)):  # possibly id
					item = id_map.get(_pk)
				else:
					_pk_tuple = []
					for _k in cls._pk_attrs:
						_pk_tuple.append(_pk.get(_k))
					_pk_tuple = tuple(_pk_tuple)
					item = pk_map.get(_pk_tuple)
				if(item):
					ret.append(item)

			return ret

		elif(kwargs):
			# assuming we want a single item
			cache_key = None
			if(use_cache and use_cache not in (True, False)):
				cache_key = tuple((k, str(v)) for k, v in sorted(kwargs.items()))
			if(cache_key is not None and (ret := use_cache.get(cache_key))):
				return ret

			ret = list(cls.query(kwargs))
			if(ret):
				if(cache_key is not None):
					use_cache[cache_key] = ret[0]
				return ret[0]

		return None

	def __validate_setattr(self, _attr_obj, k, v):
		# setting manually (not loading from db) -> try implicit conversions
		# check with validator if it exists
		if((_attr_validator := _attr_obj._validator) is not None):
			v = _attr_validator(_attr_obj, v, self)

		_attr_obj_type = _attr_obj._type
		_v_type = type(v)
		if((v is not None) and _v_type is not _attr_obj_type):
			if(_attr_obj_type in (int, float)):  # force cast between int/float
				# force cast between int/float
				v = _attr_obj_type(v or 0)
			elif(_attr_obj_type == str):  # force cast to string
				v = str(v)
			elif(_attr_obj_type is ObjectId and k == "_id"):
				pass  # could be anything don't try to cast
			elif(_attr_obj_type in LIST_OR_DICT_TYPE):
				if(
					not (
						_v_type in (MongoList, MongoDict)
						and (v._model_obj is self)  # setting from same object, all is good
					)
				):
					v = _attr_obj_type(v)
			else:
				raise TypeError(
					"Type mismatch in {}, {}: should be {} but got {}".format(
						type(self), k, _attr_obj_type, str(type(v))
					)
				)
		return v

	def __setattr__(self, k, v):
		_attr_obj = self.__class__._attrs_.get(k)
		if(_attr_obj):
			# change type of objects when setting them
			_attr_obj_type = _attr_obj._type
			if(not self._initializing_):
				self._set_query_updates[k] = self.__validate_setattr(_attr_obj, k, v)
			else:  # LOADING DATA FROM DB
				# while initializing it, we tranform db data to objects fields
				if(_attr_obj_type in LIST_OR_DICT_TYPE):
					if(isinstance(v, dict)):
						v = MongoDict(self, k, v)
					if(isinstance(v, list)):
						v = MongoList(self, k, v)

		self.__dict__[k] = v

	def to_dict(self):
		_json = {}
		if(original_doc := self._original_doc):
			for k in self.__class__._attrs_.keys():
				if((v := original_doc.get(k, _OBJ_END_)) is not _OBJ_END_):
					_json[k] = v
		elif(self._set_query_updates):
			for k in self.__class__._attrs_.keys():
				if((v := self._set_query_updates.get(k, _OBJ_END_)) is not _OBJ_END_):
					_json[k] = v
		if((_id := _json.get("_id")) is not None):
			_json['_id'] = str(_id)
		return _json

	def pk_tuple(self):
		return tuple(getattr(self, k) for k in self.__class__._pk_attrs.keys())

	def pk(self):
		ret = OrderedDict()
		for k in self.__class__._pk_attrs.keys():
			ret[k] = getattr(self, k)
		return ret

	def reload_from_db(self):
		cls = self.__class__
		primary_shard_collection = cls.get_collection(getattr(self, cls._shard_key_ or "_id"))
		_doc_in_db = primary_shard_collection.find_one({"_id": self._id})
		if(not _doc_in_db):  # moved out of shard or pk changed
			raise Exception(f"Document {self._id} pk:{self.pk()} not found")
		# 2. update our local copy
		self.reset_and_update_cache(_doc_in_db)
		return self

	# check all defaults and see if not present in doc, set them on the object
	def __check_and_set_initial_defaults(self, doc):
		for attr_name, attr_obj in self.__class__._attrs_.items():
			_default = getattr(attr_obj, "default", _OBJ_END_)
			# if the value is not the document initialise defaults
			if(attr_name not in doc):
				# if there is a default value
				if(_default is not _OBJ_END_):
					if(isinstance(_default, types.FunctionType)):
						_default = _default()  # call the function
					if(attr_obj._type == dict):
						_default = dict(_default)  # copy
					elif(attr_obj._type == list):
						_default = list(_default)  # copy
					# set that attr so it's set also in db on next update
					setattr(self, attr_name, _default)
				else:
					# default isn't given
					# we create save on write defaults for dict/list objects
					if(attr_obj._type == dict):
						self.__dict__[attr_name] = MongoDict(self, attr_name, {})  # copy
					elif(attr_obj._type == list):
						self.__dict__[attr_name] = MongoList(self, attr_name, [])  # copy
					else:
						self.__dict__[attr_name] = None

	def _reset(self, doc):
		self._is_create_new_ = False  # this is not a new object
		self._initializing_ = True

		self.__check_and_set_initial_defaults(doc)
		for k, v in doc.items():
			setattr(self, k, v)

		self._initializing_ = False
		self._original_doc = doc
		return self

	def reset_and_update_cache(self, doc):
		cls = self.__class__
		# remove old pk_tuple in cache
		if(cls._cache_):
			del cls._cache_[self.pk_tuple()]

		self._reset(doc)

		# update in cache
		if(cls._cache_):
			cls._cache_[self.pk_tuple()] = doc

	def _set_query_updates_with_no_conflicts(self, path, v):
		# if there is a subpath of this path, remove it from updates
		conflicting_keys_updates_to_remove = None
		path_len = len(path)
		for _path in self._set_query_updates.keys():
			if(path_len < len(_path) and _path.startswith(path)):  # prefix
				if(conflicting_keys_updates_to_remove is None):
					conflicting_keys_updates_to_remove = []
				conflicting_keys_updates_to_remove.append(_path)

		# remove conflicting keys if found
		if(conflicting_keys_updates_to_remove is not None):
			for _path in conflicting_keys_updates_to_remove:
				del self._set_query_updates[_path]

		self._set_query_updates[path] = v

	'''given a shard key, says which database node it resides in'''
	@classmethod
	def get_db_node(_Model, shard_key):
		shard_key = str(shard_key) if shard_key is not None else ""
		# do a bin search to find (i, j] segment
		# unfortunately if j == len(nodes) shouldn't happen, because the ring
		# should cover full region
		_db_nodes = _Model._db_nodes_
		i = 0
		j = n = len(_db_nodes)
		while(j - i > 1):
			mid = i + (j - i) // 2
			if(shard_key > _db_nodes[mid].pos):
				i = mid
			else:
				j = mid
		return _db_nodes[j] if j < n else _db_nodes[0]

	# basically finds the node where the shard_key resides
	# and returns a actual pymongo collection, shard_key must be a string
	@classmethod
	def get_collection(_Model, shard_key):
		return _Model.get_db_node(shard_key).get_collection(_Model)

	# returns all nodes and connections to Model inside them
	@classmethod
	def get_collection_on_all_db_nodes(_Model):
		return map(lambda db_node: db_node.get_collection(_Model), _Model._db_nodes_)

	# give a shard key from query we return
	# mongo collections list
	@classmethod
	def get_collections_to_query(cls, _query, sort):
		shard_key = _query.get(cls._shard_key_, _OBJ_END_)

		if(shard_key is _OBJ_END_):
			return None

		# if it's a secondary shard check if we can perform the query on this Model
		if(cls._is_secondary_shard):
			# check if we can construct a secondary shard query
			for query_attr_name, query_attr_val in _query.items():
				# if attr is not present in the secondary shard
				if(query_attr_name not in cls._attrs_):
					return None

			if(sort):
				# check if we can perform sort on the secondary index
				for sort_key, sort_direction in sort:
					if(sort_key not in cls._attrs_):
						# cannot perform sort
						return None

		if(isinstance(shard_key, (str, int))):
			return [cls.get_collection(shard_key)]

		elif(isinstance(shard_key, dict)):
			_in_values = shard_key.get("$in", _OBJ_END_)
			if(_in_values is not _OBJ_END_):
				collections_to_update = {}
				# group all shards into one set
				for shard_key_value in _in_values:
					_collection = cls.get_collection(shard_key_value)
					collections_to_update[id(_collection)] = _collection
				return collections_to_update.values()

			_eq_value = shard_key.get("$eq", _OBJ_END_)
			if(_eq_value is not _OBJ_END_):
				return [cls.get_collection(_eq_value)]

		return None

	def update(
		self, _update_query, conditions=None,
		after_mongo_update=None, _transaction=None,
		include_pending_updates=True,
		**kwargs
	):
		cls = self.__class__

		cls._trigger_event(EVENT_BEFORE_UPDATE, self)

		if((_set_query := _update_query.get("$set")) is not None):
			# validate
			for _path, _val in _set_query.items():
				if("." not in _path):
					if(_attr_obj := cls._attrs_.get(_path)):
						_set_query[_path] = self.__validate_setattr(_attr_obj, _path, _val)

		if(include_pending_updates):
			if(self._if_non_empty_set_query_update):
				# check an update them on $set query
				for _path in list(self._if_non_empty_set_query_update.keys()):
					if(_val := self._if_non_empty_set_query_update.pop(_path)):
						self._set_query_updates[_path] = _val
			if(_set_query_updates := self._set_query_updates):
				if(_set_query is not None):
					# IMP: Copy and update, because if the update fails,
					# _set_query_updates is not carried on
					_update_query["$set"] = {**_set_query_updates, **_set_query}
				else:
					_update_query["$set"] = _set_query_updates

			if(self._other_query_updates):
				for _ukey, _uval in self._other_query_updates.items():
					_u_query = _update_query.get(_ukey) or {}  # from update query
					_update_query[_ukey] = {**_uval, **_u_query}

		if(not _update_query):
			return True  # nothing to update, hence true
		if(
			_transaction is True
			or (after_mongo_update and _transaction is None)
		):
			_transaction = {}

		is_new_transaction = (
			_transaction is not None
			and not _transaction.get("transactions")
		)
		break_with_exception = None
		is_updated = False
		conditions_have_failed = False
		with ExitStack() as stack:
			for _update_retry_count in range(3):
				try:
					# create or reuse stack(when nested)
					original_doc = self._original_doc
					updated_doc = None
					_query = {
						"_id": self._id,
						"_": original_doc.get("_", _NOT_EXISTS_QUERY)  # IMP to check
					}

					# update with more given conditions,
					# (it's only for local<->remote comparision)
					if(conditions):
						_query.update(conditions)

					# get the shard where current object is
					primary_shard_key = original_doc[cls._shard_key_ or "_id"]
					primary_shard_collection = cls.get_collection(primary_shard_key)

					#: VVIMPORTANT: set `_` -> to current timestamp
					_to_set = _update_query.get("$set", _OBJ_END_)
					if(_to_set is _OBJ_END_):
						_update_query["$set"] = _to_set = {}

					_to_set["_"] = max(
						original_doc.get("_", 0) + 1,
						int(time.time() * 1000)
					)

					# query and update the document
					IS_DEV \
						and DEBUG_PRINT_LEVEL > 8 \
						and LOG_DEBUG(
							"MONGO", description="update before and query",
							model=cls.__name__, collection=COLLECTION_NAME(primary_shard_collection),
							query=str(_query), original_doc=str(original_doc),
							update_query=str(_update_query)
						)

					# 1: try updating
					updated_doc = primary_shard_collection.find_one_and_update(
						_query,
						_update_query,
						return_document=ReturnDocument.AFTER,
						session=_with_session(primary_shard_collection, _transaction, stack),
						**kwargs
					)

					IS_DEV \
						and DEBUG_PRINT_LEVEL > 8\
						and LOG_DEBUG(
							"MONGO", description="after update",
							model=cls.__name__, collection=COLLECTION_NAME(primary_shard_collection),
							updated_doc=str(updated_doc)
						)

					if(not updated_doc):
						# update was unsuccessful
						# is this concurrent update by someone else?
						_update_retry_count and time.sleep(0.03 * _update_retry_count + random.random() / 10)  # 100 ms randomized delay
						# 1. fetch from db again
						_doc_in_db = primary_shard_collection.find_one({"_id": self._id})
						if(not _doc_in_db):  # moved out of shard or pk changed
							raise Exception(f"Document {self._id} pk:{self.pk()} not found")

						# 2. update our local copy
						self.reset_and_update_cache(_doc_in_db)

						# 3. check if basic consistency between local and remote
						if(_doc_in_db.get("_", _OBJ_END_) != original_doc.get("_", _OBJ_END_)):
							continue  # can retry

						# cannot retry, may be the given conditions could have failed
						conditions_have_failed = True
						break

					# doc successfully updated
					# check if need to migrate to another shard if sharding is enabled
					if(cls._shard_key_ is not None):
						_new_primary_shard_key = updated_doc[cls._shard_key_]
						if(_new_primary_shard_key != primary_shard_key):
							new_primary_shard_collection = cls.get_collection(_new_primary_shard_key)
							if(new_primary_shard_collection != primary_shard_collection):
								# migrate to new shard
								# will crash if duplicate _id key
								new_primary_shard_collection.insert_one(updated_doc)
								primary_shard_collection.delete_one({"_id": self._id})

					# 1. propagate the updates to secondary shards
					for _secondary_shard_key, _shard in cls._secondary_shards_.items():
						_SecondayModel = _shard._Model_

						_old_shard_key_val = original_doc.get(_secondary_shard_key)
						_new_shard_key_val = updated_doc.get(_secondary_shard_key)
						# 2. check if shard key has been changed
						_old_secondary_collection = None
						_new_secondary_collection = None

						if(_old_shard_key_val is not None):
							_old_secondary_collection = _SecondayModel\
								.get_collection(_old_shard_key_val)
						if(_new_shard_key_val is not None):
							_new_secondary_collection = _SecondayModel\
								.get_collection(_new_shard_key_val)

						# 2.1. If old shard exists, delete that secondary doc
						if(
							_old_secondary_collection is not None
							and (
								_old_secondary_collection != _new_secondary_collection
							)
						):
							_old_secondary_collection.delete_one({"_id": self._id})

						# 2.2.  if new shard key exists insert into appropriate node
						# insert only if non null
						if(_new_secondary_collection is not None):
							_to_insert = {}
							is_shard_data_changed = False
							for _k in _shard.attrs:
								_v = updated_doc.get(_k, _OBJ_END_)
								if(_v is not _OBJ_END_):
									_to_insert[_k] = _v
								if(_v != original_doc.get(_k, _OBJ_END_)):
									# changed during update
									is_shard_data_changed = True

							# nothing changed in the shard
							# don't replace/update
							if(not is_shard_data_changed):
								continue

							_to_insert["_"] = updated_doc["_"]
							# insert the doc in new collection
							try:
								_new_secondary_collection.replace_one(
									{
										"_id": self._id,
										"_": {"$not": {"$gte": updated_doc["_"]}}
									},
									_to_insert,
									upsert=True
								)
							except DuplicateKeyError as ex:
								LOG_ERROR(
									"MONGO",
									desc="upserting failed(concurrent update)/duplicate entry on seconday shard",
									ex=str(ex),
									model=cls.__name__,
									collection=COLLECTION_NAME(_new_secondary_collection),
								)
							except Exception as ex:
								# updating to secondary failed
								LOG_ERROR(
									"MONGO",
									desc="secondary not be propagated",
									ex=str(ex),
									model=cls.__name__,
									collection=COLLECTION_NAME(_new_secondary_collection),
									secondary_doc=str(_to_insert)
								)

					# call any explicit callback
					after_mongo_update and after_mongo_update(
						self, original_doc, updated_doc,
						_transaction=_transaction
					)
					# publish update event
					cls._trigger_event(
						EVENT_MONGO_AFTER_UPDATE,
						self, original_doc, updated_doc
					)

					# reset all values
					self.reset_and_update_cache(updated_doc)
					# waint on threads

					is_new_transaction and _commit_transaction(_transaction)
					is_updated = True

					# cleanup if pending updates already applied
					if(include_pending_updates):
						self._set_query_updates.clear()
						self._other_query_updates.clear()

					# triggered after update event
					cls._trigger_event(EVENT_AFTER_UPDATE, self)
					# whoever created the transaction should commit it
					break  # UPDATE SUCCESS
				except PyMongoError as ex:
					if(
						is_new_transaction
						and ex.has_error_label("TransientTransactionError")
					):
						# RETRY
						time.sleep(0.03 * _update_retry_count)
						_transaction.pop("transactions", None)
						continue
					raise ex  # re-raise the exception
				except Exception as ex:
					break_with_exception = ex  # any other exception abort and raise
					break

			if(not is_updated):
				is_new_transaction and _abort_transaction(_transaction)

		if(break_with_exception):
			raise break_with_exception
		if(is_updated):
			return True
		if(not conditions_have_failed):
			Exception("Concurrent update may have caused this query to fail")
		return False  # conditions failed

	@classmethod
	def from_doc(cls, doc):
		return cls(False)._reset(doc)

	@classmethod
	def query(
		cls: Type[ModelType],
		_query,
		sort=None,
		offset=None,
		limit=None,
		force_primary=False,
		**kwargs
	) -> Iterator[ModelType]:
		queries = None
		# split or queries to separate it to shards
		if(not isinstance(_query, list)):
			if((or_queries := _query.pop("$or", None)) is not None):  # top level $or query
				if(not _query):
					queries = or_queries
				else:  # merge remaining query with all $or queries
					queries = []
					for or_query in or_queries:
						queries.append({**_query, **or_query})
			else:
				queries = [_query]
		else:
			queries = _query  # already a list of queries

		# if there is nothing to query return empty
		if(not queries):
			return []

		# normalize sort param to [('attr_name', ASCENDING/DESCENDING)...]
		if(sort):
			if(not isinstance(sort, list)):
				sort = [sort]
			_sort_keys = []
			for _sort_key_tuple in sort:
				_ordering = 1
				_sort_key_name = None
				if(not isinstance(_sort_key_tuple, tuple)):
					_sort_key_name = _sort_key_tuple
				else:
					_sort_key_name = _sort_key_tuple[0]
					_ordering = _sort_key_tuple[1]
				_sort_key_name = cls._attrs_to_name.get(_sort_key_name) \
					or _sort_key_name
				_sort_keys.append((_sort_key_name, _ordering))
			sort = _sort_keys

		# collection: [_query,...]
		collections_to_query = {}
		for _query in queries:
			# try checking if we can query primary
			collection_shards = cls.get_collections_to_query(_query, sort)
			if(collection_shards):
				for collection_shard in collection_shards:
					_key = id(collection_shard)
					shard_and_queries = collections_to_query.get(_key)
					if(shard_and_queries is None):
						collections_to_query[_key]\
							= shard_and_queries \
							= (collection_shard, [], cls)
					shard_and_queries[1].append(_query)

			elif(not force_primary):  # try if we can query it on secondary shards
				for _secondary_shard_key, _shard in cls._secondary_shards_.items():
					secondary_collection_shards = _shard._Model_.get_collections_to_query(_query, sort)
					if(not secondary_collection_shards):
						continue
					for collection_shard in secondary_collection_shards:
						_key = id(collection_shard)
						shard_and_queries = collections_to_query.get(_key)
						if(shard_and_queries is None):
							collections_to_query[_key] \
								= shard_and_queries \
								= (collection_shard, [], _shard._Model_)
						shard_and_queries[1].append(_query)

					break  # important

		# if we did not find any possible shard to query
		# we query all primary shards and assemble queries using chain
		# which will be dead slow and possibly fucked up!
		if(not collections_to_query):
			collections_to_query = {
				id(x): (x, queries, cls)
				for x in cls.get_collection_on_all_db_nodes()
			}

		def count_documents(_collection, _query, offset, limit):
			kwargs = {}
			if(offset):
				kwargs["skip"] = offset
			if(limit):
				kwargs["limit"] = limit
			return _collection.count_documents(_query, **kwargs)

		class SortKey(object):
			def __init__(self, obj):
				self.obj = obj

			def __lt__(self, other_obj):
				if(not sort):
					return True
				for sort_key, sort_direction in sort:
					value1 = getattr(self.obj, sort_key, None)
					value2 = getattr(other_obj, sort_key, None)
					if(sort_direction > 0):
						if(value1 == value2):
							continue
						if(value1 is None or value2 is None):
							return True if not value1 else False
						return value1 < value2
					else:
						if(value1 == value2):
							continue
						if(value1 is None or value2 is None):
							return False if not value1 else True
						return value1 > value2
				return True

		# does local sorting on results from multiple query iterators using a heap
		class MultiCollectionQueryResultIterator:
			query_count_funcs = None
			buffer = None
			results_returned = 0
			__start_timestamp = 0

			def __init__(self):
				self.query_count_funcs = []
				self._cursors = []
				self.results_returned = 0
				self.__start_timestamp = 0
				self.buffer = []
				heapq.heapify(self.buffer)

			def add(self, query_result_iter, _cursor, query_count_func=None):
				self.push_query_iter_into_heap(query_result_iter)
				self._cursors.append(_cursor)
				if(query_count_func):
					self.query_count_funcs.append(query_count_func)

			def count(self):
				return sum([x() for x in self.query_count_funcs])

			def distinct(self, key):
				return chain.from_iterable(
					map(lambda x: x.distinct(key), self._cursors)
				)

			def push_query_iter_into_heap(self, _query_result_iter):
				try:
					next_value = next(_query_result_iter)
					heapq.heappush(
						self.buffer,
						(SortKey(next_value), _query_result_iter, next_value)
					)
				except StopIteration:
					pass

			def __iter__(self):
				return self

			def __next__(self):
				_ret = None
				try:
					sort_value, _query_result_iter, _ret = heapq.heappop(self.buffer)
					self.push_query_iter_into_heap(_query_result_iter)
				except IndexError:
					raise StopIteration

				self.results_returned += 1
				if(self.results_returned % MONGO_WARN_MAX_RESULTS_RATE == 0):
					cur_timestamp = time.time()
					if(not self.__start_timestamp):
						self.__start_timestamp = cur_timestamp

					elif(self.results_returned > MONGO_WARN_MAX_RESULTS_RATE * (cur_timestamp - self.__start_timestamp)):
						LOG_WARN(
							"mongo_results_high_rate",
							desc=f"scanned {cls._collection_name_with_shard_}/{cls.__name__} {_queries}: {self.results_returned}"
						)
						if(self.results_returned > MONGO_MAX_RESULTS_AT_HIGH_SCAN_RATE):  # high scan rate for 1000 results, must be doing something wrong
							raise Exception(f"Scanned {MONGO_MAX_RESULTS_AT_HIGH_SCAN_RATE} results at max scan rate")

				return _ret

		multi_collection_query_result_iterator = MultiCollectionQueryResultIterator()

		def query_collection(_Model, _collection, _query, offset=None):
			IS_DEV \
				and DEBUG_PRINT_LEVEL > 8 \
				and LOG_DEBUG(
					"MONGO",
					description="querying", model=_Model.__name__,
					collection=COLLECTION_NAME(_collection),
					query=str(_query), sort=str(sort), offset=str(offset), limit=str(limit)
				)

			projection = kwargs.get("projection")
			ret = _collection.find(_query, **kwargs)
			if(offset):  # from global arg
				ret = ret.skip(offset)
			if(limit):
				ret = ret.limit(limit)
			if(sort):  # from global arg
				ret = ret.sort(sort)

			_cursor = ret  # keep track of this for distinct

			# we queried from the secondary shard, will not have all fields
			# requery again
			if(_Model._is_secondary_shard):
				# do a requery to fetch full document
				# TODO: make it batch wise fetch
				def batched_requery_iter(ret, n=200):
					for docs in batched_iter(ret, n):
						# use this to put them in the same order after or query
						_ids_order = [_doc["_id"] for _doc in docs]

						if(cls._shard_key_ != "_id"):
							_query = {
								"$or": [
									{cls._shard_key_: _doc[cls._shard_key_], "_id": _doc["_id"]} for _doc in docs
								]
							}
						else:
							_query = {"$or": [{"_id": _doc["_id"]} for _doc in docs]}

						IS_DEV\
							and DEBUG_PRINT_LEVEL > 8\
							and LOG_DEBUG(
								"MONGO", description="requerying",
								model=cls.__name__, query=str(_query)
							)
						_requeried_from_primary = {_doc._id: _doc for _doc in cls.query(_query, force_primary=True)}
						for _id in _ids_order:
							item = _requeried_from_primary.get(_id)
							if(not item):
								IS_DEV\
									and DEBUG_PRINT_LEVEL > 8\
									and LOG_DEBUG(
										"MONGO", description="missing from primary",
										model=cls.__name__, _id=str(_id)
									)
								continue
							yield item
				ret = batched_requery_iter(ret)
			else:
				def doc_to_obj(doc):
					ret = cls(False)  # not a new item
					if(projection is not None):
						ret._reset(doc)
					else:
						ret.reset_and_update_cache(doc)
					return ret

				ret = map(doc_to_obj, ret)

			multi_collection_query_result_iterator.add(
				ret,
				_cursor,
				lambda: count_documents(_collection, _query, offset, limit)  # query count func
			)

		threads = []
		for _, shard_and_queries in collections_to_query.items():
			_collection_shard, _queries, _Model = shard_and_queries
			new_query = None
			if(len(_queries) == 1):
				new_query = _queries[0]
			else:
				new_query = {"$or": _queries}

			if(offset and len(collections_to_query) > 1):
				offset = None
				LOG_WARN(
					"MONGO",
					description=(
						"Cannot use offset when query spans multiple collections shards, "
						"rethink your query or change sharding"
					),
					model=_Model.__name__
				)

			thread = Thread(
				target=query_collection,
				args=(_Model, _collection_shard, new_query),
				kwargs={"offset": offset}
			)
			thread.start()
			threads.append(thread)

		# wait on sub queries to finish
		for thread in threads:
			thread.join()

		return multi_collection_query_result_iterator

	'''
		usually we call this while compile time,
		perfomance wise is not an issue
	'''
	@classmethod
	def on(cls, events, func):
		if(not isinstance(events, list)):
			events = [events]
		for event in events:
			event_listeners = getattr(cls, "_event_listeners_", _OBJ_END_)
			if(event_listeners is _OBJ_END_):
				event_listeners = {}
				setattr(cls, "_event_listeners_", event_listeners)
			if((handlers := event_listeners.get(event)) is None):
				event_listeners[event] = handlers = []
			handlers.append(func)

	@classmethod
	def _trigger_event(cls, event, *obj):
		if(handlers := cls._event_listeners_.get(event)):
			for handler in handlers:
				handler(*obj)

	def before_update(self):
		pass

	@property
	def updated_at(self):
		return self._original_doc.get("_", 0)

	def commit(
		self: ModelType, force=False, ignore_exceptions=None,
		conditions=None, _transaction=None
	) -> ModelType:
		cls = self.__class__
		is_committed = False
		break_with_ex = None
		if(self._is_create_new_):  # try inserting
			cls._trigger_event(EVENT_BEFORE_CREATE, self)

			if(self._if_non_empty_set_query_update):
				for _path in list(self._if_non_empty_set_query_update.keys()):
					if(_val := self._if_non_empty_set_query_update.pop(_path)):
						self._set_query_updates[_path] = _val

			if(not self._set_query_updates):
				return self  # nothing to update

			if(
				self._id is None
				and (
					cls._shard_key_ == "_id"  # need to explicitly set _id
					or cls._attrs_["_id"]._type != ObjectId
				)
			):
				raise Exception("Need to specify _id")

			self._set_query_updates["_"] = int(time.time() * 1000)

			primary_shard_collection = cls.get_collection(getattr(self, cls._shard_key_ or "_id"))
			IS_DEV \
				and DEBUG_PRINT_LEVEL > 8 \
				and LOG_DEBUG(
					"MONGO", description="new object values",
					model=cls.__name__, collection=COLLECTION_NAME(primary_shard_collection),
					set_query=str(self._set_query_updates)
				)
			if(cls._secondary_shards_ and _transaction is None):
				_transaction = {}

			is_new_transaction = (
				_transaction is not None
				and _transaction.get("transactions") is None
			)				# find which shard we should insert
			with ExitStack() as stack:
				for _insert_retry_count in range(3):
					try:
						self._insert_result = primary_shard_collection.insert_one(
							self._set_query_updates,
							session=_with_session(primary_shard_collection, _transaction, stack)
						)

						self._id = self._set_query_updates["_id"] = self._insert_result.inserted_id

						# insert in secondary shards now
						for _secondary_shard_key, _shard in cls._secondary_shards_.items():
							_SecondaryModel = _shard._Model_
							_shard_key_val = self._set_query_updates.get(_secondary_shard_key)
							if(_shard_key_val is not None):
								_to_insert = {}
								for attr in _shard.attrs:
									_attr_val = self._set_query_updates.get(attr, _OBJ_END_)
									if(_attr_val is not _OBJ_END_):
										_to_insert[attr] = _attr_val
								secondary_collection = _SecondaryModel.get_collection(_shard_key_val)
								if(_to_insert):
									secondary_collection.insert_one(
										_to_insert,
										session=_with_session(secondary_collection, _transaction, stack)
									)
						# set _id updates
						is_new_transaction and _commit_transaction(_transaction)
						is_committed = True
						# set original doc and custom dict and set fields
						# copy the dict to another
						self.reset_and_update_cache(dict(self._set_query_updates))
						# hook to do something for newly created db entries
						cls._trigger_event(EVENT_AFTER_CREATE, self)
						break
					except DuplicateKeyError as ex:

						if(ignore_exceptions and DuplicateKeyError in ignore_exceptions):
							break  # no exception

						if(not force):
							raise(ex)  # re reaise

						# get original doc from mongo shard
						doc_in_db = primary_shard_collection.find_one(ex.details["keyValue"])
						self.reset_and_update_cache(doc_in_db)
						IS_DEV \
							and DEBUG_PRINT_LEVEL > 8 \
							and LOG_DEBUG(
								"MONGO", description="created a duplicate, refetching and updating",
								model=cls.__name__,
								collection=COLLECTION_NAME(primary_shard_collection),
								pk=str(self.pk())
							)

						# remove _id, not need to update it
						if("_id" in self._set_query_updates):
							del self._set_query_updates["_id"]
						break  # CONTINUE TO UPDATE AS A EXISTING OBJECT
					except PyMongoError as ex:
						if(is_new_transaction and ex.has_error_label("TransientTransactionError")):
							# RETRY
							time.sleep(0.03 * _insert_retry_count)
							_transaction.pop("transactions", None)
							continue
						raise ex  # re-raise
					except Exception as ex:
						break_with_ex = ex
						break

				if(not is_committed):
					is_new_transaction and _abort_transaction(_transaction)  # abort transaction

		if(not self._is_create_new_ and not is_committed):  # try updating
			# automatically picks up _set_query_updates inside .update function
			is_committed = self.update({}, conditions=conditions)
			if(not is_committed and not conditions):
				# if it's used for creating a new object, but we couldn't update it
				raise Exception(
					"MONGO: Couldn't commit, either a concurrent update modified this or query has issues",
					self.pk(), self._set_query_updates, self._other_query_updates
				)
		if(not is_committed):
			if(break_with_ex):
				raise break_with_ex
			return None

		# clear
		self._set_query_updates.clear()
		self._other_query_updates.clear()
		return self

	def delete(self):
		_Model = self.__class__
		if(_Model._is_secondary_shard):
			raise Exception("MONGO: Cannot delete secondary shard item")
		# Note: when we know the _id and the shard we basically delete them by _id

		_Model._trigger_event(EVENT_BEFORE_DELETE, self)

		_delete_query = {"_id": self._id}
		# delete from all secondary shards
		for _shard_key_name, _shard in _Model._secondary_shards_.items():
			_SecondaryModel = _shard._Model_
			_shard_key_val = self._original_doc.get(_shard_key_name)
			if(_shard_key_val):
				_SecondaryModel\
					.get_collection(_shard_key_val)\
					.delete_one(_delete_query)

		# find which pimary shard it belongs to and delete it there
		collection_shard = _Model.get_collection(self._original_doc[_Model._shard_key_ or "_id"])
		collection_shard.delete_one(_delete_query)
		IS_DEV \
			and DEBUG_PRINT_LEVEL > 8 \
			and LOG_DEBUG(
				"MONGO", description="deleting from primary",
				model=_Model.__name__, collection=COLLECTION_NAME(collection_shard),
				delete_query=str(_delete_query)
			)
		if(_Model._cache_):
			del _Model._cache_[self.pk_tuple()]

		_Model._trigger_event(EVENT_AFTER_DELETE, self)

	# utility to keep track of locks
	__locks = None

	# timeout -> can try to get lock until
	# can_hold_until -> after acquired, how long we can hold it,
	# - beyong that time, we know other can interfere
	def lock(self, name="", timeout=5000, silent=False, can_hold_until=2 * 60 * 1000):
		# return true for objects not yet in db too
		lock_name = f"_lock_{name}"
		start_timestamp = cur_ms()

		if(self.__locks is None):
			self.__locks = {}

		# non db lock
		if(not self._original_doc):
			# keep waiting until lock cleared
			while(
				lock_name in self.__locks
				and (cur_timestamp := cur_ms()) - start_timestamp < timeout
			):
				time.sleep(1)
			if(lock_name in self.__locks):
				if(not silent):
					raise TimeoutError(
						"locking timedout on non-db new object {}:{}".format(
							self.__class__,
							self.pk_tuple()
						)
					)
				return False

			self.__locks[lock_name] = None
			return True  # no lock needed

		# db lock
		count = 0
		while((cur_timestamp := cur_ms()) - start_timestamp < timeout):
			count += 1
			locked_until = cur_timestamp + can_hold_until
			if(
				self.update(
					{"$set": {lock_name: locked_until}},
					conditions={
						"$or": [
							{lock_name: None},
							{lock_name: {"$lt": cur_timestamp}}  # locked before 2 minutes
						]
					},
					include_pending_updates=False
				)
			):
				self.__locks[lock_name] = locked_until
				return True
			time.sleep(0.2 * count)  # wait

		if(not silent):
			raise TimeoutError(
				"locking timedout {}:{}".format(
					self.__class__,
					self.pk_tuple()
				)
			)

		return False

	def unlock(self, name="", force=False):
		lock_name = f"_lock_{name}"
		if(not force and lock_name not in self.__locks):
			# not acqurired by us and no force
			return
		# check if it's a db lock
		if(self._original_doc):
			self.update(
				{"$unset": {lock_name: True}},
				conditions=None if force else {lock_name: self.__locks[lock_name]},  # if locked by us
				include_pending_updates=False
			)
		self.__locks.pop(lock_name, None)


# decorator to prevent concurrent updates
# uses the first argument to lock
def with_lock(func):
	def wrapper(self, *args, **kwargs):
		try:
			self.lock()
			return func(self, *args, **kwargs)
		except TimeoutError:
			return None
		finally:
			self.unlock()
	return wrapper


class SecondaryShard:
	attrs = None
	indexes = None
	collection_name = None
	_Model_ = None

	def __init__(self):
		self.attrs = {}
		self.indexes = []


# control
class CollectionTracker(Model):
	_db_name_ = "control"
	_collection_name_ = "collection_tracker"

	_id = Attribute(str)  # db_name__collection_name
	db_nodes = Attribute(list)
	is_primary_shard = Attribute(int)
	primary_shard_key = Attribute(str)
	secondary_shard_keys = Attribute(list)
	pk_attrs = Attribute(list)
	attrs = Attribute(list)
	created_at = Attribute(int, default=cur_ms)


# tags shard keys to the attributes and use it when intializing the model
def SHARD_BY(primary=None, secondary=None):
	if(primary and isinstance(primary, Attribute)):
		primary.is_primary_shard_key = True
	if(secondary):
		for secondary_shard_attr in secondary:
			if(secondary_shard_attr and isinstance(secondary_shard_attr, Attribute)):
				secondary_shard_attr.is_secondary_shard_key = True


IndexesToCreate = {}


# tags indexes_to_create to attributes and
# retrieve them when initializing the model
def INDEX(*indexes):
	collection_name = inspect.currentframe().f_back.f_locals["_collection_name_"]
	IndexesToCreate[collection_name] = indexes


_cached_mongo_clients = {}


# DatabaseNode is basically a server or a replicaset
class DatabaseNode:

	_cached_pymongo_collections = None
	# mongo connection
	mongo_client = None
	hosts = None
	replicaset = None
	username = None
	password = None
	# default db name
	db_name = None
	pos = None

	# this initializes the tables in all nodes
	def __init__(
		self, host=None, replicaset=None,
		username=None, password=None, db_name=None,
		hosts=None, pos="", **_
	):
		if(isinstance(host, str)):
			hosts = [host]
		hosts.sort()
		self.hosts = hosts
		self.username = username
		self.db_name = db_name
		self.replicaset = replicaset
		self.pos = pos
		# use list of hosts, replicaset and username as cache key
		# so as not to create multiple clients
		_mongo_clients_cache_key = (",".join(sorted(hosts)), username or "")
		self.mongo_client = _cached_mongo_clients.get(_mongo_clients_cache_key)
		if(not self.mongo_client):
			self.mongo_client \
				= _cached_mongo_clients[_mongo_clients_cache_key] \
				= MongoClient(
					host=hosts, replicaSet=replicaset,
					username=username, password=password,
					connectTimeoutMS=5000,
					serverSelectionTimeoutMS=5000,
					maxIdleTimeMS=120000,
					waitQueueTimeoutMS=60000,
					maxPoolSize=500
				)

		self._cached_pymongo_collections = {}

	def to_dict(self):
		return {
			"hosts": self.hosts,
			"replicaset": self.replicaset,
			"username": self.username,
			"db_name": self.db_name
		}

	# returns collection on the given DatabaseNode
	# slight optimization to cache and return
	def get_collection(self, _Model):
		ret = self._cached_pymongo_collections.get(_Model, None)
		if(ret is None):
			self._cached_pymongo_collections[_Model] \
				= ret \
				= self.mongo_client[_Model._db_name_][_Model._collection_name_with_shard_]
			ret._db_node_ = self
		return ret


def initialize_model(_Model):

	if(isinstance(_Model, list)):
		for _m in _Model:
			initialize_model(_m)
		return

	# defaults, do not change the code below
	_Model._shard_key_ = getattr(_Model, "_shard_key_", None)
	_Model._secondary_shards_ = {}

	_Model._indexes_ = getattr(_Model, "_indexes_", None)\
		or IndexesToCreate.get(_Model._collection_name_) or []

	# temp usage _id_attr
	_id_attr = Attribute(ObjectId)  # default is of type objectId
	_Model._attrs_ = _model_attrs = {"_id": _id_attr}
	_Model._pk_attrs = None
	'''it's used for translating attr objects/name to string names'''
	_Model._attrs_to_name = attrs_to_name = {_id_attr: '_id', '_id': '_id'}

	# parse all attributes
	_mro = _Model.__mro__
	for _M in reversed(_mro[1:_mro.index(Model)]):  # parent classes until Model class reversed
		if(_Model._collection_name_ == _M._collection_name_):
			return  # NOT A MODEL, NO NEED TO INITIALIZE AGAIN
		_Model._attrs_.update(_M._attrs_)
		_Model._attrs_to_name_.update(_M._attrs_to_name_)
		_Model._indexes_ += _M._indexes_
		_Model._shard_key_ = _M._shard_key_
		_Model._secondary_shards_ = {k: SecondaryShard() for k in _M._secondary_shards_}

	for k, v in filter(
		lambda kv: isinstance(kv[1], Attribute),
		_Model.__dict__.items()
	):  # scan attributes
		_model_attrs[k] = v
		# helps convert string/attr to string
		attrs_to_name[v] = k
		attrs_to_name[k] = k
		# track models this attr belongs to

		# check if it has any shard, then extract indexes,
		# shard key tagged to attributes
		if(not _Model._is_secondary_shard):  # very important check
			# check indexes_to_create
			if(getattr(v, "is_primary_shard_key", False) is True):
				delattr(v, "is_primary_shard_key")  # delete so that it's not used again after first time
				_Model._shard_key_ = k
			elif(getattr(v, "is_secondary_shard_key", False) is True):
				delattr(v, "is_secondary_shard_key")
				_Model._secondary_shards_[k] = SecondaryShard()
				# because we don't need it again after first time

	IS_DEV \
		and DEBUG_PRINT_LEVEL > 8 \
		and print(
			"\n\n#MONGO: Initializing Model", _Model,
			_Model._indexes_, _Model._shard_key_, _Model._secondary_shards_
		)

	_Model._event_listeners_ = getattr(_Model, "_event_listeners_", {})
	# some default event
	_Model.on(EVENT_BEFORE_UPDATE, lambda obj: obj.before_update())

	_pymongo_indexes_to_create = {}

	_indexes_list = []
	for _index in _Model._indexes_:
		_index_properties = {}
		_index_keys = []
		if(isinstance(_index, dict)):
			_index_properties.update(_index.pop("properties", None) or {})
			_index = _index.get("keys")

		if(not isinstance(_index, tuple)):
			_index = (_index,)

		for _a in _index:
			_attr_name = _a
			_ordering = pymongo.ASCENDING
			if(isinstance(_a, tuple)):
				_a, _ordering = _a
			if(isinstance(_a, Attribute)):
				_attr_name = attrs_to_name[_a]
			if(isinstance(_a, str)):
				_attr_name = _a
			if(isinstance(_a, dict)):
				_index_properties = _a
				continue

			_index_keys.append((_attr_name, _ordering))
		_indexes_list.append((tuple(_index_keys), _index_properties))

	# sort shortest first and grouped by shard keys first
	def index_cmp(index_a, index_b):
		index_a = index_a[0]  # just the index keys
		index_b = index_b[0]  # just the index keys

		if((_diff := len(index_a) - len(index_b)) != 0):
			return _diff  # shorter wins

		if(index_a[0] != index_b[0]):  # cmp by shard key
			return -1 if index_a[0] < index_b[0] else 1
		return 0

	_indexes_list.sort(key=cmp_to_key(index_cmp))   # sort and group by indexes first key

	for _index_keys, _index_properties in _indexes_list:
		# mix with defaults
		_index_properties = {"unique": True, **_index_properties}
		# convert to tuple again
		is_unique_index = _index_properties.get("unique", True) is not False
		_index_shard_key = _index_keys[0][0]  # shard key is the first mentioned in index

		# adjust the pk of the table accordingly, mostly useful for primary shard
		if(_secondary_shard := _Model._secondary_shards_.get(_index_shard_key)):  # belongs to seconday index
			for _attr_key, _ordering in _index_keys:
				if("." in _attr_key):
					_attr_key = _attr_key.split(".")[0]
				_secondary_shard.attrs[_attr_key] = getattr(_Model, _attr_key)
			# create _index_ for secondary shards
			_secondary_shard.indexes.append({
				"keys": _index_keys,
				"properties": _index_properties
			})
		else:  # create it on main table
			_pymongo_indexes_to_create[_index_keys] = _index_properties

		if(_Model._shard_key_ == _index_shard_key):   # set pk_attrs for caching
			if(_Model._pk_attrs is None and is_unique_index):
				_Model._pk_attrs = _pk_attrs = OrderedDict()
				for i in _index_keys:  # first unique index as pk
					_pk_attrs[i[0]] = 1

	_pymongo_indexes_to_create.pop((('_id', 1),), None)	 # remove default index
	if(not _Model._pk_attrs):  # create default _pk_attrs
		_Model._pk_attrs = OrderedDict(_id=True)

	# set collection name to include shard_keys

	_Model._collection_name_with_shard_ = _Model._collection_name_

	# append shard id to table name as an indicator that table is sharded
	if(_Model._shard_key_ is not None):
		_Model._collection_name_with_shard_ += "_shard_" + _Model._shard_key_

	# find tracking nodes
	_Model._collection_tracker_key_ = "{:s}__{:s}".format(
		_Model._db_name_,
		_Model._collection_name_with_shard_
	)

	# just keep a track, for random use cases to retrive by name
	__all_models__[_Model._collection_name_with_shard_] = _Model

	IS_DEV \
		and DEBUG_PRINT_LEVEL > 8 \
		and print(
			"\n\n#MONGO collection tracker key",
			_Model._collection_tracker_key_
		)

	# FIND ALL DB NODES THIS TABLE IS SHARED TO
	if(_Model not in [CollectionTracker]):
		collection_tracker = CollectionTracker.get(_Model._collection_tracker_key_)
		if(collection_tracker):
			_Model._db_nodes_ = tuple(
				DatabaseNode(**_db_node)
					for _db_node in collection_tracker.db_nodes
			)
			if(
				not _Model._is_secondary_shard
				and _Model._secondary_shards_
				and collection_tracker.primary_shard_key != _Model._shard_key_
			):
				raise Exception(
					"\n\n#MONGO_EXCEPTION: Primary shard key changed for ",
					_Model,
					"It has secondary shards, that point to primary shard key. ",
					"You will have to drop shard secondary shards and force reindex everything again. "
				)
		else:
			# just single db node, initialize on the default db node given
			init_db_node = CollectionTracker._db_nodes_[0]
			_Model._db_nodes_ = (  # tuple
				DatabaseNode(
					**{
						"hosts": init_db_node.hosts,
						"replicaset": init_db_node.replicaset,
						"pos": "",
						"username": init_db_node.username,
						"password": init_db_node.password,
						"db_name": _Model._db_name_
					}
				),
			)

		# check if new attribute added to secondary shard

	# TODO: create or delete index using control jobs
	# list existing indexes
	existing_indexes = {}
	# check if indexes on each node are same
	for i in range(len(_Model._db_nodes_)):
		current_node_collection = _Model._db_nodes_[i].get_collection(_Model)
		indexesA = existing_indexes \
			= current_node_collection.index_information()
		if(i > 0):
			# compare with previous node
			previous_node_collection = _Model._db_nodes_[i - 1].get_collection(_Model)
			indexesB \
				= previous_node_collection.index_information()
			if(indexesA != indexesB):
				LOG_ERROR(
					"mongo_indexes_error", desc="Indexes are different on nodes",
					collection_a=COLLECTION_NAME(current_node_collection),
					collection_b=COLLECTION_NAME(previous_node_collection),
					index_a=json.dumps(indexesA) if indexesA is not None else None,
					index_b=json.dumps(indexesB) if indexesB is not None else None
				)

	# check unused indexes
	existing_indexes = {
		tuple([  # list of tuples
			(x, int(y) if isinstance(y, float) else y)  # sometimes it's float so convert to int
				for x, y in _index["key"]
		]): (_name, _index) for _name, _index in existing_indexes.items()
	}  # key: ((_id, 1)).., value: {uqniue: False} or None

	# remove default for comparision
	existing_indexes.pop((('_id', 1),), None)

	_new_indexes, _delete_indexes = list_diff2(
		list(_pymongo_indexes_to_create.keys()),
		list(existing_indexes.keys())
	)
	for _index_keys in _delete_indexes:
		LOG_WARN(
			"mongo_indexes",
			desc=(
				f"index {_index_keys} not declared in orm, delete it on db? "
				f'db.{_Model._collection_name_with_shard_}.dropIndex("{existing_indexes[_index_keys][0]}")'
			)
		)

	if(IS_TEST):
		# create indexes in test mode automatically
		for pymongo_index, mongo_index_args in _pymongo_indexes_to_create.items():
			DEBUG_PRINT_LEVEL > 3 and print(
				"\n\n#MONGO: creating_indexes automatically in test mode", _Model, pymongo_index, mongo_index_args
			)
			# in each node create indexes
			for db_node in _Model._db_nodes_:
				db_node.get_collection(_Model).create_index(pymongo_index, **mongo_index_args)

	if(_new_indexes and not IS_TEST):
		for _index in _new_indexes:
			missing_index_creation_string = (
				f"db.{_Model._collection_name_with_shard_}"
				f".createIndex({json.dumps({x: y for x, y in _index})}, "
				f"{json.dumps(_pymongo_indexes_to_create[_index])})"
			)
			LOG_ERROR("missing_mongo_indexes", desc=missing_index_creation_string)
			if("missing_mongo_indexes" not in _loading_errors):
				_loading_errors["missing_mongo_indexes"] = []
			_loading_errors["missing_mongo_indexes"].append(missing_index_creation_string)

	# check if our options are different from remote options
	for _index, _options in _pymongo_indexes_to_create.items():
		if(existing_index := existing_indexes.get(_index)):
			# _options = dict(_options)  # copy
			existing_options = dict(existing_index[1])  # copy
			# existing_options.pop("v")
			# existing_options.pop("key")
			# ignore some default
			for prop, _default in (("unique", False), ("partialFilterExpression", None)):
				existing_prop_val = existing_options.get(prop) or _default
				new_prop_val = _options.get(prop) or _default
				if(existing_prop_val != new_prop_val):
					index_options_changed = (
						f"{existing_options} -> {_options} : you can delete and recreate "
						f'Delete: db.{_Model._collection_name_with_shard_}.dropIndex("{existing_index[0]}") '
						f"Create: db.{_Model._collection_name_with_shard_}"
						f".createIndex({json.dumps({x: y for x, y in _index})}, "
						f"{json.dumps(_options)})"  # options
					)
					LOG_ERROR("index_options_changed", desc=index_options_changed)
					if("index_options_changed" not in _loading_errors):
						_loading_errors["index_options_changed"] = []
					_loading_errors["index_options_changed"].append(index_options_changed)
					break

	# create secondary shards
	for _secondary_shard_key in list(_Model._secondary_shards_.keys()):  # iterate on copy
		_secondary_shard = _Model._secondary_shards_[_secondary_shard_key]
		if(not _secondary_shard.attrs):
			_Model._secondary_shards_.pop(_secondary_shard_key)
			LOG_ERROR(
				"MONGO", description="not creating secondary shards without any related index",
				model=_Model.__name__,
				secondary_shard_key=_secondary_shard_key
			)
			continue

		class_attrs = {
			"_db_name_": _Model._db_name_,  # same as from primary
			"_indexes_": _secondary_shard.indexes,
			"_shard_key_": _secondary_shard_key,
			# collection name is same as from primary,
			# it's updated accordingly when initializing
			"_collection_name_": _Model._collection_name_,
			"_is_secondary_shard": True,
			"_db_nodes_": None,
			"_primary_model_class_": _Model  # reference
		}

		# add primary shard key, _id to refer to the primary shard and _id of doc
		_secondary_shard.attrs[_Model._shard_key_] = _Model._attrs_[_Model._shard_key_]
		_secondary_shard.attrs['_id'] = _Model._attrs_["_id"]

		class_attrs.update(_secondary_shard.attrs)
		# also include the primary key of the primary shard
		# into secondary shards

		_secondary_shard._Model_ = type(
			"{:s}_{:s}".format(_Model.__name__, _secondary_shard_key.upper()),
			(Model,),  # base class
			class_attrs
		)
		# initialize this new model
		initialize_model(_secondary_shard._Model_)


# initialize control Tr
# initialize all other nodes
def initialize_mongo(control_db_nodes, default_db_name=None):

	default_db_name = default_db_name or "temp_db"
	# initialize control db
	if(isinstance(control_db_nodes, dict)):
		control_db_nodes = [DatabaseNode(**control_db_nodes)]
	elif(isinstance(control_db_nodes, list)):
		control_db_nodes = [DatabaseNode(**db_node) for db_node in control_db_nodes]
	else:
		raise Exception(
			f"argument must be a list of dicts, or a single dict, not {type(control_db_nodes)}"
		)

	# initialize control db
	CollectionTracker._db_nodes_ = control_db_nodes
	initialize_model(CollectionTracker)

	# set default db name for each class
	for cls in all_subclasses(Model):
		if(not getattr(cls, "_db_name_", None)):
			cls._db_name_ = default_db_name
		initialize_model(cls)

	if(_loading_errors):
		raise Exception("initialize_mongo has errors: Check error logs: " + str(_loading_errors))


# Initially intended to be < 500 line and more for educating
# about sharding
# How this works:
# - Create Collection by giving it a set of nodes to start with
#   and a primary shard key and secondary shard keys
# - Primary shards contain full document
# - Secondary shard contain subset of attributes(that are used for querying) of the document
#   all those attributes are indexed
# - When you query(primary shard), it looks if we can narrow down to specific nodes
#   and run query on them
# - We return a combined cusor from all nodes we queried
# - We analyse the query and sometimes we decide the query
#   can run on a secondary shard, we query, get the cursor
#   and batch fetch original docs from the primary when returning
# - When you update document, we identify shard it belongs to
#   and update that doc, then get the diff that has changed
#   and apply that diff on all secondary shards
# - when you apply a sort param, and query spans multiple nodes
# 	we create a small heap ( = number of nodes queried) and advance cusors accordingly


# Optionally:
# Limitations:
# - when query spans multiple nodes, you cannot apply offset
# - You can't update multiple documents at once

# when we say collection it it's actual pymongo collection on db server
# DbNode -> mongo server which contains collections


# Internal Notes:
# ############## Using a transaction
# _mongo_client = cls.get_db_node(self._original_doc[cls._shard_key_]).mongo_client
# with _mongo_client.start_session() as session:
# 	#
# 	return session.with_transaction(
# 		lambda session: some_func, # transaction updates inside this func
# 		read_concern=ReadConcern('local'),
# 		write_concern=WriteConcern("majority", wtimeout=5000),
# 		read_preference=ReadPreference.PRIMARY
# 	)
# ##############
