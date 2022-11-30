from os import environ
import json
import heapq
import types
import pymongo
from contextlib import ExitStack
import metrohash
from typing import TypeVar, Iterator
# from pymongo.read_concern import ReadConcern
# from pymongo.write_concern import WriteConcern
from bson.objectid import ObjectId
from itertools import chain
from collections import OrderedDict
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo import ReturnDocument, ReadPreference
from .tools import ExpiringCache, all_subclasses,\
	cur_ms, list_diff2, batched_iter
from .config import IS_DEV, MONGO_WARN_THRESHOLD_MANY_RESULTS_FETCHED, MONGO_WARN_MAX_QUERY_TIME_SECONDS
from gevent.threading import Thread
from gevent import time
from .logging import LOG_APP_INFO, LOG_WARN, LOG_SERVER, LOG_ERROR

_loading_errors = {}

MONGO_DEBUG_LEVEL =  int(environ.get("MONGO_ORM_DEBUG_LEVEL") or 1)

EVENT_BEFORE_DELETE = -2
EVENT_AFTER_DELETE = -1
EVENT_BEFORE_UPDATE = 1
EVENT_AFTER_UPDATE = 2
EVENT_BEFORE_CREATE = 3
EVENT_AFTER_CREATE = 4
EVENT_MONGO_AFTER_UPDATE = 5

# just a random constant object to check for non existent
# keys without catching KeyNotExist exception
_OBJ_END_ = object()
# given a list of [(filter, iter), ....] , flattens them

_NOT_EXISTS_QUERY = {"$exists": False}  # just a constant


# utility to print collection name + node for logging
def COLLECTION_NAME(collection):
	_nodes = collection.database.client.nodes or (("unknown", 0),)
	for _node in _nodes:
		return "{:s}.{:s}.{:d}".format(collection.full_name, _node[0], _node[1])
	return collection.full_name

def str_validator(_attr, val, obj):
	max_len = getattr(_attr, "max_len", 2048)
	if(val and len(val) > max_len):
		val = val[:max_len]
	return val

DEFAULT_VALIDATORS = {
	str: str_validator
}

class Attribute(object):
	_type = None
	_validator = None
	_models = None
	def __init__(self, _type, validator=None, **kwargs):
		self._type = _type
		self._validator = validator or DEFAULT_VALIDATORS.get(_type)
		self._models = set()
		self.__dict__.update(kwargs)


__all_models__ = {}


class MongoList(list):
	_initializing = True
	_model_obj = None
	path = ""  # empty string becuase it doesn't break pickling

	def __init__(self, _model_obj, path, initial_value):
		self._initializing = True
		self._model_obj = _model_obj
		self.path = path
		if(initial_value):
			for item in initial_value:
				self.append(item)
		self._initializing = False

	def __setitem__(self, k, v):
		if(not self._initializing):
			self._model_obj._set_query_updates[self.path + "." + str(k)] = v
		if(self._initializing):
			# recursively create custom dicts/list when
			# loading object from db
			if(isinstance(v, dict)):
				v = MongoDict(self._model_obj, self.path + "." + str(k), v)
			elif(isinstance(v, list)):
				v = MongoList(self._model_obj, self.path + "." + str(k), v)
		super(MongoList, self).__setitem__(k, v)

	def remove(self, item):  # can raise value exception
		super(MongoList, self).remove(item)
		_pull = self._model_obj._other_query_updates.get("$pull")
		if(_pull == None):
			self._model_obj._other_query_updates["$pull"] = _pull = {}
		value_to_remove = _pull.get(self.path, _OBJ_END_)
		if(value_to_remove == _OBJ_END_):
			_pull[self.path] = item  # first item to remove
		else:
			if(
				isinstance(value_to_remove, dict)
				and "$in" in value_to_remove
			):
				_pull[self.path]["$in"].append(item)
			else:
				_pull[self.path] = {"$in": [value_to_remove, item]}

	def pop(self, i=None):
		if(i == None):
			i = 1
			ret = super(MongoList, self).pop()
		elif(i == 0):
			ret = super(MongoList, self).pop(0)  # remove first item
			i = -1
		else:
			raise Exception(
				"can only remove first or last elements, supported arg is 0 or None"
			)

		if(not self._initializing):
			_pop = self._model_obj._other_query_updates.get("$pop")
			if(_pop == None):
				self._model_obj._other_query_updates["$pop"] = _pop = {}
			_pop[self.path] = i
		# convert them to original object types
		if(isinstance(ret, list)):
			return list(ret)
		if(isinstance(ret, dict)):
			return dict(ret)
		return ret

	def insert(self, pos, arr):
		if(not isinstance(arr, list)):
			arr = [arr]
		for i in reversed(arr):
			super().insert(pos, i)

		if(not self._initializing):
			_push = self._model_obj._other_query_updates.get("$push")
			if(_push == None):
				self._model_obj._other_query_updates["$push"] = _push = {}
			_push[self.path] = {"$position": pos, "$each": arr}

	def append(self, item):
		if(self._initializing):
			if(isinstance(item, dict)):
				item = MongoDict(self._model_obj, self.path + "." + str(len(self)), item)
			elif(isinstance(item, list)):
				item = MongoList(self._model_obj, self.path + "." + str(len(self)), item)

		super(MongoList, self).append(item)

		if(not self._initializing):
			_push = self._model_obj._other_query_updates.get("$push")
			if(_push == None):
				_push = self._model_obj._other_query_updates["$push"] = {}
			_push[self.path] = item

	def extend(this, items):
		for i in items:
			this.append(i)

	def clear(self):
		super().clear()
		self._model_obj._set_query_updates[self.path] = self
		self._initializing = True  # Hack for new object
 

class MongoDict(dict):
	_initializing = True
	_model_obj = None
	path = ""  # empty string becuase it doesn't break pickling

	def __init__(self, _model_obj, path, initial_value):
		self._initializing = True
		self._model_obj = _model_obj
		self.path = path
		if(initial_value):
			self.update(initial_value)
		self._initializing = False

	def __setitem__(self, k, v):
		if(not self._initializing):
			self._model_obj._set_query_updates[self.path + "." + str(k)] = v
		if(self._initializing):
			# recursively create custom dicts/list when
			# loading object from db
			if(isinstance(v, dict)):
				v = MongoDict(self._model_obj, self.path + "." + str(k), v)
			elif(isinstance(v, list)):
				v = MongoList(self._model_obj, self.path + "." + str(k), v)

		super(MongoDict, self).__setitem__(k, v)

	def pop(self, k, default=None):
		# not in initializing mode
		popped_val = super(MongoDict, self).pop(k, _OBJ_END_)
		if(popped_val == _OBJ_END_):
			return default
		# if not initializing and has value set remove it form mongo
		elif(not self._initializing):
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
		self._model_obj._set_query_updates[self.path] = self
		self._initializing = True  # Hack for new object

# utility function to cache and return a session for transaction
def _with_transaction(collection, _transaction, exit_stack):
	dbnode = collection._db_node_
	mclient = dbnode.mongo_client
	session = _transaction.get(mclient)
	if(not session):
		_transaction[mclient] = session =  mclient.start_session()
		exit_stack.enter_context(session)
		if(dbnode.replicaset): # TODO: do better to indentify replicaset
			exit_stack.enter_context(session.start_transaction())
	return session

ModelType = TypeVar('ModelType', bound='Model') # use string

class Model(object):
	# ###class level###
	_attrs = None
	_pk_attrs = None

	__cache__ = None
	# used to identify if it's a primary or secondary shard
	_is_secondary_shard = False

	# ###instance level###
	__is_new = True
	# this means the object is being initialized by this orm,
	# and not available to user yet
	_initializing = False
	# pending query updates are stored in this
	_set_query_updates = None
	_other_query_updates = None
	_original_doc = None
	# we execute all functions inside this list,
	# and use the return value to update
	_insert_result = None
	_pk = None
	_json = None

	# initialize a new object
	def __init__(self, _is_new_=True, **values):

		# defaults
		self.__is_new = _is_new_
		self._initializing = not _is_new_
		self._set_query_updates = {}
		self._other_query_updates = {}
		cls = self.__class__
		for k, v in cls._attrs_.items():
			self.__dict__[k] = None  # set all initial attributes to None

		if(_is_new_):
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
	def get(cls: ModelType, _pk=None, use_cache=True, **kwargs) -> ModelType:
		if(_pk != None):
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
					if(cls.__cache__):
						_doc_in_cache = cls.__cache__.get(_pk_tuple)
				elif(use_cache):
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
			if(use_cache and use_cache != True):
				for _item in ret:
					use_cache.set(_item.pk_tuple(), _item._original_doc)

			if(is_single_item):
				if(ret):
					return ret[0]
				else:
					return None
			# multiple items, reorder
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
			ret = list(cls.query(kwargs))
			if(ret):
				return ret[0]

		return None

	def __setattr__(self, k, v):
		_attr_obj = self.__class__._attrs_.get(k)
		if(_attr_obj):
			# change type of objects when setting them
			_attr_obj_type = _attr_obj._type
			# impllicit type corrections for v
			if(v != None and not isinstance(v, _attr_obj_type)):
				if(_attr_obj_type == int or _attr_obj_type == float):
					# force cast between int/float
					v = _attr_obj_type(v or 0)
				elif(_attr_obj_type == str):  # force cast to string
					v = str(v)
				elif(_attr_obj_type == ObjectId and k == "_id"):
					pass  # could be anything don't try to cast
				else:
					if(not self._initializing):
						# we are setting it wrong on a new object
						# raise
						raise TypeError(
							"Type mismatch in {}, {}: should be {} but got {}".format(
								type(self), k, _attr_obj_type, str(type(v))
							)
						)
					# else:
						# already in db, so let's wait for application to crash
			if(self._initializing):
				# while initializing it, we tranform db data to objects fields
				if(_attr_obj_type == dict):
					if(isinstance(v, dict)):
						v = MongoDict(self, k, v)
				elif(_attr_obj_type == list):
					if(isinstance(v, list)):
						v = MongoList(self, k, v)

			else: # initialized object
				# check with validator	
				_attr_validator = _attr_obj._validator
				if(_attr_validator):
					v = _attr_validator(_attr_obj, v, self)
				self._set_query_updates[k] = v

		self.__dict__[k] = v

	def to_dict(self, r=None):
		_json = {}
		if(isinstance(r, str)):
			r = [r]

		for k, _attr in self.__class__._attrs_.items():
			v = getattr(self, k, _OBJ_END_)
			if(v != _OBJ_END_):
				attr_read_auth: set = getattr(_attr, "r", None)
				if(r and attr_read_auth):
					for i in r:
						if(i in attr_read_auth):
							_json[k] = v
							break
				else:
					_json[k] = v

		_id = getattr(self, '_id', _OBJ_END_)
		if(_id != _OBJ_END_):
			_json['_id'] = str(_id)
		return _json

	@classmethod
	def pk_from_doc(cls, doc):
		ret = OrderedDict()
		for attr in cls._pk_attrs:
			ret[attr] = doc.get(attr)
		return ret

	def pk_tuple(self):
		return tuple(self.pk().values())

	def pk(self, renew=False):
		if(not self._pk or renew or self.__is_new):
			ret = OrderedDict()
			for k in self.__class__._pk_attrs.keys():
				ret[k] = getattr(self, k)
			self._pk = ret
		return self._pk

	# check all defaults and see if not present in doc, set them on the object
	def __check_and_set_initial_defaults(self, doc):
		for attr_name, attr_obj in self.__class__._attrs_.items():
			_default = getattr(attr_obj, "default", _OBJ_END_)
			# if the value is not the document initialise defaults
			if(attr_name not in doc):
				# if there is a default value
				if(_default != _OBJ_END_):
					if(isinstance(_default, types.FunctionType)):
						_default = _default()
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


	# when retrieving objects from db
	# creates a new Model object
	@classmethod
	def get_instance_from_document(cls, doc):
		ret = cls(False)  # not a new item
		ret.reset_and_update_cache(doc)
		return ret

	def _reset(self, doc):
		self.__is_new = False  # this is not a new object
		self._initializing = True

		self.__check_and_set_initial_defaults(doc)
		for k, v in doc.items():
			setattr(self, k, v)

		self._initializing = False
		self._original_doc = doc
		# renew the pk!
		self.pk(True)
		return self

	def reset_and_update_cache(self, doc):
		cls = self.__class__
		# remove old pk_tuple in cache
		if(cls.__cache__):
			cls.__cache__.delete(self.pk_tuple())

		self._reset(doc)

		# update in cache
		if(cls.__cache__):
			cls.__cache__.set(self.pk_tuple(), doc)

	'''given a shard key, says which database node it resides in'''
	@classmethod
	def get_db_node(_Model, shard_key):
		shard_key = str(shard_key) if shard_key != None else ""
		# get 16 bit int
		shard_key = int.from_bytes(metrohash.metrohash64(shard_key.encode()), 'big')
		# do a bin search to find (i, j] segment
		# unfortunately if j == len(nodes) shouldn't happen, because the ring
		# should cover full region
		_db_nodes = _Model._db_nodes_
		i = 0
		j = n = len(_db_nodes)
		while(j - i > 1):
			mid = i + (j - i) // 2
			if(shard_key > _db_nodes[mid].at):
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
		if(
			shard_key == None
			or shard_key == _OBJ_END_
		):
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
			if(_in_values != _OBJ_END_):
				collections_to_update = {}
				# group all shards into one set
				for shard_key_value in _in_values:
					_collection = cls.get_collection(shard_key_value)
					collections_to_update[id(_collection)] = _collection
				return collections_to_update.values()

			_eq_value = shard_key.get("$eq", _OBJ_END_)
			if(_eq_value != _OBJ_END_):
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

		if(include_pending_updates):
			if(self._set_query_updates):
				_in_query = _update_query.get("$set") or {}
				_to_set = dict(self._set_query_updates)
				_to_set.update(_in_query)  # overwrite from query
				_update_query["$set"] = _to_set
			if(self._other_query_updates):
				for _ukey, _uval in self._other_query_updates.items():
					_to_update = dict(_uval)
					_in_query = _update_query.get(_ukey) or {}  # from update query
					_to_update.update(_in_query)  # overwrite
					_update_query[_ukey] = _to_update

		if(not _update_query):
			return True  # nothing to update, hence true

		with ExitStack() as stack:
			for _update_retry_count in range(3):
				original_doc = self._original_doc
				updated_doc = None
				_query = {
					"_id": self._id,
					"_": original_doc.get("_", _NOT_EXISTS_QUERY) # IMP to check
				}

				# update with more given conditions,
				# (it's only for local<->remote comparision)
				if(conditions):
					_query.update(conditions)

				# get the shard where current object is
				primary_shard_key = original_doc[cls._shard_key_]
				primary_collection = cls.get_collection(primary_shard_key)
				# query and update the document
				IS_DEV \
					and MONGO_DEBUG_LEVEL > 1 \
					and LOG_SERVER(
						"MONGO", description="update before and query",
						model=cls.__name__, collection=COLLECTION_NAME(primary_collection),
						query="_query", original_doc=str(original_doc),
						update_query=str(_update_query)
					)

				if(
					_transaction == True 
					or (after_mongo_update and _transaction == None)
				):
					_transaction = {}  # use transaction

				#: VVIMPORTANT: set `_` -> to current timestamp
				_to_set = _update_query.get("$set", _OBJ_END_)
				if(_to_set == _OBJ_END_):
					_update_query["$set"] = _to_set = {}
				_to_set["_"] = max(original_doc.get("_", 0) + 1, int(time.time()* 1000))

				# 1: try updating
				updated_doc = primary_collection.find_one_and_update(
					_query,
					_update_query,
					return_document=ReturnDocument.AFTER,
					session=(
						_with_transaction(primary_collection, _transaction, stack)
						if _transaction != None
						else None
					),
					**kwargs
				)

				IS_DEV \
					and MONGO_DEBUG_LEVEL > 1\
					and LOG_SERVER(
						"MONGO", description="after update",
						model=cls.__name__, collection=COLLECTION_NAME(primary_collection),
						updated_doc=str(updated_doc)
					)

				if(not updated_doc):
					# update was unsuccessful
					# is this concurrent update by someone else?
					# is this because of more conditions given by user?
					_update_retry_count and time.sleep(0.03 * _update_retry_count)
					# 1. fetch from db again
					_doc_in_db = primary_collection.find_one({"_id": self._id})
					if(not _doc_in_db):  # moved out of shard or pk changed
						return False
					# 2. update our local copy
					self.reset_and_update_cache(_doc_in_db)
					can_retry = False
					# 3. check if basic consistency between local and remote
					if(_doc_in_db.get("_", _OBJ_END_) != original_doc.get("_", _OBJ_END_)):
						can_retry = True

					if(can_retry):
						continue  # retry again
					# cannot retry
					return False

				# doc successfully updated
				# check if need to migrate to another shard
				_new_primary_shard_key = updated_doc.get(cls._shard_key_)
				if(_new_primary_shard_key != primary_shard_key):
					# primary shard key has changed
					new_primary_collection = cls.get_collection(_new_primary_shard_key)
					if(new_primary_collection != primary_collection):
						# migrate to new shard
						# will crash if duplicate _id key
						new_primary_collection.insert_one(updated_doc)
						primary_collection.delete_one({"_id": self._id})

				# 1. propagate the updates to secondary shards
				for _secondary_shard_key, _shard in cls._secondary_shards_.items():
					_SecondayModel = _shard._Model_

					_old_shard_key_val = original_doc.get(_secondary_shard_key)
					_new_shard_key_val = updated_doc.get(_secondary_shard_key)
					# 2. check if shard key has been changed
					_old_secondary_collection = None
					_new_secondary_collection = None

					if(_old_shard_key_val != None):
						_old_secondary_collection = _SecondayModel\
							.get_collection(_old_shard_key_val)
					if(_new_shard_key_val != None):
						_new_secondary_collection = _SecondayModel\
							.get_collection(_new_shard_key_val)

					# 2.1. If old shard exists, delete that secondary doc
					if(
						_old_secondary_collection
						and (
							_old_secondary_collection != _new_secondary_collection
						)
					):
						_old_secondary_collection.delete_one({"_id": self._id})

					# 2.2.  if new shard key exists insert into appropriate node
					# insert only if non null
					if(_new_secondary_collection):
						_to_insert = {}
						is_shard_data_changed = False
						for _k in _shard.attrs:
							_v = updated_doc.get(_k, _OBJ_END_)
							if(_v != _OBJ_END_):
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
					original_doc, updated_doc
				)

				# reset all values
				self.reset_and_update_cache(updated_doc)
				# waint on threads

				# cleanup if pending updates already applied
				if(include_pending_updates):
					self._set_query_updates.clear()
					self._other_query_updates.clear()

				# triggered after update event
				cls._trigger_event(EVENT_AFTER_UPDATE, self)
				return True


	@classmethod
	def query(
		cls,
		_query,
		sort=None,
		projection=None,
		offset=None,
		limit=None,
		read_preference=ReadPreference.PRIMARY,
		force_primary=False,
		**kwargs
	) -> Iterator[ModelType]:
		queries = None
		# split or queries to separate it to shards
		if(not isinstance(_query, list)):
			if("$or" in _query):
				queries = _query["$or"]
			else:
				queries = [_query]
		else:
			queries = _query

		# if there is nothing to query return empty
		if(not queries):
			return []

		# normalize sort param to [('attr_name', ASCENDING/DESCENDING)...]
		if(sort):
			if(not isinstance(sort, list)):
				sort = [sort]
			_sort_keys = []
			for _sort_key_tuple in sort:
				if(not isinstance(_sort_key_tuple, tuple)):
					_sort_keys.append(
						(cls._attrs_to_name[_sort_key_tuple], 1)
					)
				else:
					_sort_keys.append(
						(cls._attrs_to_name[_sort_key_tuple[0]], _sort_key_tuple[1])
					)
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
					if(shard_and_queries == None):
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
						if(shard_and_queries == None):
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
						if(value1 == None or value2 == None):
							return True if not value1 else False
						return value1 < value2
					else:
						if(value1 == value2):
							continue
						if(value1 == None or value2 == None):
							return False if not value1 else True
						return value1 > value2
				return True

		# does local sorting on results from multiple query iterators using a heap
		class MultiCollectionQueryResult:
			query_result_iters = None
			query_count_funcs = None
			buffer = None
			results_returned = 0

			def __init__(self):
				self.query_result_iters = []
				self.query_count_funcs = []
				self.results_returned = 0

			def add(self, query_result_iter, query_count_func=None):
				self.query_result_iters.append(query_result_iter)
				if(query_count_func):
					self.query_count_funcs.append(query_count_func)

			def count(self):
				ret = 0
				for count_func in self.query_count_funcs:
					ret += count_func()
				return ret

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
				# if(len(self.query_result_iters) == 1):
				# 	return self.query_result_iters[0]
				# else we sort results for each if there is a sorting key given
				self.buffer = []
				heapq.heapify(self.buffer)
				for _query_result_iter in self.query_result_iters:
					self.push_query_iter_into_heap(_query_result_iter)
				return self

			def __next__(self):
				_ret = None
				try:
					sort_value, _query_result_iter, _ret = heapq.heappop(self.buffer)
					self.push_query_iter_into_heap(_query_result_iter)
				except IndexError:
					raise StopIteration

				self.results_returned += 1
				if(self.results_returned % MONGO_WARN_THRESHOLD_MANY_RESULTS_FETCHED == 0):
					LOG_WARN(
						"mongo_results_many",
						desc=f"scanned {cls._collection_name_with_shard_}/{cls.__name__} {_queries}: {self.results_returned}"
					)

				return _ret

		multi_collection_query_result = MultiCollectionQueryResult()

		def query_collection(_Model, _collection, _query, offset=None):
			IS_DEV \
				and MONGO_DEBUG_LEVEL > 1 \
				and LOG_SERVER(
					"MONGO",
					description="querying", model=_Model.__name__,
					collection=COLLECTION_NAME(_collection),
					query=str(_query), sort=str(sort), offset=str(offset), limit=str(limit)
				)

			_start_time = time.time()
			
			ret = _collection.find(_query, **kwargs)
			if(sort):  # from global arg
				ret = ret.sort(sort)
			if(offset):  # from global arg
				ret = ret.skip(offset)
			if(limit):
				ret = ret.limit(limit)

			if((_elapsed_time:= (time.time() - _start_time)) > MONGO_WARN_MAX_QUERY_TIME_SECONDS):
				query_plan = ret.explain()["queryPlanner"]
				LOG_WARN(
					"mongo_perf",
					desc=f"query took longer than {MONGO_WARN_MAX_QUERY_TIME_SECONDS} seconds",
					elapsed_millis=int(_elapsed_time * 1000),
					plan_type=query_plan["winningPlan"]["stage"],
					query=query_plan["parsedQuery"]
				)

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
							and MONGO_DEBUG_LEVEL > 1\
							and LOG_SERVER(
								"MONGO", description="requerying",
								model=cls.__name__, query=str(_query)
							)
						_requeried_from_primary = {_doc._id: _doc for _doc in cls.query(_query, force_primary=True)}
						for _id in _ids_order:
							item = _requeried_from_primary.get(_id)
							if(not item):
								IS_DEV\
									and MONGO_DEBUG_LEVEL > 1\
									and LOG_SERVER(
										"MONGO", description="missing from primary",
										model=cls.__name__, _id=str(_id)
									)
								continue
							yield item
				ret = batched_requery_iter(ret)
			else:
				ret = map(cls.get_instance_from_document, ret)

			multi_collection_query_result.add(
				ret,
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

		return multi_collection_query_result

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
			if(event_listeners == _OBJ_END_):
				event_listeners = {}
				setattr(cls, "_event_listeners_", event_listeners)
			handlers = event_listeners.get(event)
			if(handlers == None):
				event_listeners[event] = handlers = []
			handlers.append(func)

	@classmethod
	def _trigger_event(cls, event, *obj):
		handlers = cls._event_listeners_.get(event)
		if(not handlers):
			return
		for handler in handlers:
			handler(*obj)

	def before_update(self):
		pass

	@property
	def updated_at(self):
		return self._original_doc.get("_")

	def commit(
		self, force=False, ignore_exceptions=None,
		conditions=None, _transaction=None
	):
		cls = self.__class__
		committed = False

		if(self.__is_new):  # try inserting
			cls._trigger_event(EVENT_BEFORE_CREATE, self)
			if(not self._set_query_updates):
				return self  # nothing to update
			shard_key_name = cls._shard_key_
			if(
				self._id == None
				and (
					(cls._is_sharding_enabled_ and shard_key_name == "_id")
					or cls._attrs_["_id"]._type != ObjectId
				)
			):
				raise Exception("Need to specify _id")

			self._set_query_updates["_"] = int(time.time() * 1000)
			primary_collection = cls.get_collection(
				getattr(self, shard_key_name)
			)
			IS_DEV \
				and MONGO_DEBUG_LEVEL > 1 \
				and LOG_SERVER(
					"MONGO", description="new object values",
					model=cls.__name__, collection=COLLECTION_NAME(primary_collection),
					set_query=str(self._set_query_updates)
				)
			try:
				# find which shard we should insert to and insert into that
				with ExitStack() as stack:
					_transaction = _transaction if _transaction != None else {}

					self._insert_result = primary_collection.insert_one(
						self._set_query_updates,
						session=(
							_with_transaction(primary_collection, _transaction, stack)
							if cls._secondary_shards_ 
							else None 
						)
					)

					self._id = self._set_query_updates["_id"] = self._insert_result.inserted_id

					# insert in secondary shards now
					for _secondary_shard_key, _shard in cls._secondary_shards_.items():
						_SecondaryModel = _shard._Model_
						_shard_key_val = self._set_query_updates.get(_secondary_shard_key)
						if(_shard_key_val != None):
							_to_insert = {}
							for attr in _shard.attrs:
								_attr_val = self._set_query_updates.get(attr, _OBJ_END_)
								if(_attr_val != _OBJ_END_):
									_to_insert[attr] = _attr_val
							secondary_collection = _SecondaryModel.get_collection(_shard_key_val)
							if(_to_insert):
								secondary_collection.insert_one(
									_to_insert,
									session=_with_transaction(secondary_collection, _transaction, stack)
								)

				# set _id updates
				committed = True
				# set original doc and custom dict and set fields
				# copy the dict to another
				self.reset_and_update_cache(dict(self._set_query_updates))
				# hook to do something for newly created db entries
				cls._trigger_event(EVENT_AFTER_CREATE, self)

			except DuplicateKeyError as ex:

				if(ignore_exceptions and DuplicateKeyError in ignore_exceptions):
					return None

				if(not force):
					raise(ex)  # re reaise

				# get original doc from mongo shard
				doc_in_db = primary_collection.find_one(self.pk())
				self.reset_and_update_cache(doc_in_db)
				IS_DEV \
					and MONGO_DEBUG_LEVEL > 1 \
					and LOG_SERVER(
						"MONGO", description="created a duplicate, refetching and updating",
						model=cls.__name__,
						collection=COLLECTION_NAME(primary_collection),
						pk=str(self.pk())
					)

				# remove _id, not need to update it
				if("_id" in self._set_query_updates):
					del self._set_query_updates["_id"]

		if(not self.__is_new and not committed):  # try updating
			# automatically picks up _set_query_updates inside .update function
			is_committed = self.update({}, conditions=conditions)
			if(not is_committed and not conditions):
				# if it's used for creating a new object, but we couldn't update it
				raise Exception(
					"MONGO: Couldn't commit, either a concurrent update modified this or query has issues",
					self.pk(), self._set_query_updates, self._other_query_updates
				)

		# clear and reset pk to new
		self.pk(renew=True)
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
		collection_shard = _Model.get_collection(self._original_doc[_Model._shard_key_])
		collection_shard.delete_one(_delete_query)
		IS_DEV \
			and MONGO_DEBUG_LEVEL > 1 \
			and LOG_SERVER(
				"MONGO", description="deleting from primary",
				model=_Model.__name__, collection=COLLECTION_NAME(collection_shard),
				delete_query=str(_delete_query)
			)
		if(_Model.__cache__):
			_Model.__cache__.delete(self.pk_tuple())

		_Model._trigger_event(EVENT_AFTER_DELETE, self)

	# utility to lock
	__locked_until = None

	# timeout -> can try to get lock until
	# can_hold_until -> after acquired, how long we can hold it,
	# - beyong that time, we know other can interfere
	def lock(self, timeout=5000, silent=False, can_hold_until=2 * 60 * 1000):
		# return true for objects not yet in db too
		start_timestamp = cur_ms()
		if(
			(self.__locked_until and self.__locked_until >= start_timestamp)
			or not self._original_doc
		):
			return True

		count = 0
		while((cur_timestamp:= cur_ms()) - start_timestamp < timeout):
			count += 1
			locked_until = cur_timestamp + can_hold_until
			if(
				self.update(
					{"$set": {"locked": locked_until}},
					conditions={
						"$or": [
							{"locked": None},
							{"locked": {"$lt": cur_timestamp}}  # locked before 2 minutes
						]
					},
					include_pending_updates=False
				)
			):
				self.__locked_until = locked_until
				break
			time.sleep(0.2 * count)  # wait

		if(not self.__locked_until):
			if(not silent):
				raise TimeoutError(
					"locking timedout {}:{}".format(
						self.__class__,
						self.pk_tuple()
					)
				)
			return False

		return True

	def unlock(self, force=False):
		if(not self.__locked_until and not force):
			# not acqurired by us and no force
			return

		self.update(
			{"$unset": {"locked": ""}},
			conditions=None if force else {"locked": self.__locked_until}, # if locked by us
			include_pending_updates=False
		)
		self.__locked_until = None

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


# tags indexes_to_create to attributes and
# retrieve them when initializing the model
def INDEX(*indexes):
	for index_key_set in indexes:
		first_key_of_index_key_set = index_key_set[0]
		if(isinstance(first_key_of_index_key_set, tuple)):
			# when it has a sorting order as second key
			first_key_of_index_key_set = first_key_of_index_key_set[0]

		first_key_of_index_key_set._indexes_to_create = getattr(first_key_of_index_key_set, "_indexes_to_create", [])
		first_key_of_index_key_set._indexes_to_create.append(index_key_set)

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
	at = None

	# this initializes the tables in all nodes
	def __init__(
		self, host=None, replicaset=None,
		username=None, password=None, db_name=None,
		hosts=None,
		replica_set=None, at=0 # cleanup
	):
		if(isinstance(host, str)):
			hosts = [host]
		hosts.sort()
		self.hosts = hosts
		if(list(filter(lambda _host: "replSet=" in _host, hosts))):
			raise Exception("use replicaset argument to indicate replicaset")
		self.replicaset = replicaset
		self.username = username
		self.db_name = db_name
		# use list of hosts, replicaset and username as cache key
		# so as not to create multiple clients
		_mongo_clients_cache_key = (",".join(sorted(hosts)), replicaset or "", username or "")
		self.mongo_client = _cached_mongo_clients.get(_mongo_clients_cache_key)
		if(not self.mongo_client):
			self.mongo_client \
				= _cached_mongo_clients[_mongo_clients_cache_key] \
				= MongoClient(
					host=hosts, replicaSet=replicaset,
					username=username, password=password
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
		if(ret == None):
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
	_Model._shard_key_ = getattr(_Model, "_shard_key_", "_id")
	_Model._secondary_shards_ = {}
	_Model._indexes_ = getattr(_Model, "_indexes_", [("_id",)])
	# create a default cache if nothing specified
	_Model_cache = getattr(_Model, "_cache_", _OBJ_END_)
	if(_Model_cache == _OBJ_END_ and not _Model._is_secondary_shard):
		_Model.__cache__ = ExpiringCache(10000)  # default cache

	# temp usage _id_attr
	_id_attr = Attribute(ObjectId)  # default is of type objectId
	_Model._attrs_ = _model_attrs = {"_id": _id_attr}
	_Model._pk_attrs = None
	'''it's used for translating attr objects/name to string names'''
	_Model._attrs_to_name = attrs_to_name = {_id_attr: '_id', '_id': '_id'}
	is_primary_sharding_enabled = False
	# parse all attributes
	for k, v in _Model.__dict__.items():
		if(isinstance(v, Attribute)):
			# preprocess any attribute arguments
			if(getattr(v, "r", None)):  # auth levels needed to read this attr
				if(isinstance(v.r, str)):
					v.r = [v.r]
				v.r = set(v.r)

			_model_attrs[k] = v
			# helps convert string/attr to string
			attrs_to_name[v] = k
			attrs_to_name[k] = k
			# track models this attr belongs to
			v._models.add(_Model)

			# check if it has any shard, then extract indexes,
			# shard key tagged to attributes
			if(not _Model._is_secondary_shard):  # very important check
				# check indexes_to_create
				_indexes_to_create = getattr(v, "_indexes_to_create", None)
				if(_indexes_to_create):
					# because we don't need it again after first time
					delattr(v, "_indexes_to_create")
					_Model._indexes_.extend(_indexes_to_create)

				is_primary_shard_key = getattr(v, "is_primary_shard_key", False)
				is_secondary_shard_key = getattr(v, "is_secondary_shard_key", False)
				if(is_primary_shard_key is True):
					_Model._shard_key_ = k
					# because we don't need it again after first time
					#  and won't rewrite secondary shard keys
					delattr(v, "is_primary_shard_key")
					is_primary_sharding_enabled = True
				elif(is_secondary_shard_key is True):
					_Model._secondary_shards_[k] = SecondaryShard()
					# because we don't need it again after first time
					delattr(v, "is_secondary_shard_key")


	IS_DEV \
		and MONGO_DEBUG_LEVEL > 1 \
		and print(
			"\n\n#MONGO: Initializing Model", _Model,
			_Model._indexes_, _Model._shard_key_, _Model._secondary_shards_
		)

	_Model._event_listeners_ = getattr(_Model, "_event_listeners_", {})
	# some default event
	_Model.on(EVENT_BEFORE_UPDATE, lambda obj: obj.before_update())

	_pymongo_indexes_to_create = {}
	_Model._pk_is_unique_index = False

	_indexes_list = []
	for _index in _Model._indexes_:
		_index_properties = {}
		_index_keys = []
		if(isinstance(_index, dict)):
			_index_properties.update(_index.pop("properties") or {})
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

	# sort shortest first and grouped by keys first
	_indexes_list.sort(key=lambda x: tuple(k[0] for k in x[0]))
	for _index_keys, _index_properties in _indexes_list:
		# mix with defaults
		_index_properties = {"unique": True, **_index_properties}
		# convert to tuple again
		is_unique_index = _index_properties.get("unique", True) is not False
		_index_shard_key = _index_keys[0][0]  # shard key is the first mentioned in index

		# adjust the pk of the table accordingly, mostly useful for primary shard
		if(_Model._shard_key_ == _index_shard_key):
			# index belong to primary shard
			if(
				not _Model._pk_attrs
				or (is_unique_index and not _Model._pk_is_unique_index)
			):
				_Model._pk_is_unique_index = is_unique_index
				_Model._pk_attrs = _pk_attrs = OrderedDict()
				for i in _index_keys:  # first unique index
					_pk_attrs[i[0]] = 1

		if(_Model._shard_key_ != _index_shard_key):
			# secondary shard tables
			_secondary_shard = _Model._secondary_shards_.get(_index_shard_key)

			if(_secondary_shard):
				for _attr_name, _ordering in _index_keys:
					_secondary_shard.attrs[_attr_name] = getattr(
						_Model,
						_attr_name
					)
				# create _index_ for secondary shards
				_secondary_shard.indexes.append({
					"keys": _index_keys,
					"properties": _index_properties
				})

		if(
			_index_shard_key not in _Model._secondary_shards_
			and _index_shard_key != "_id"
		):
			# this indes should go to to the primary index
			_pymongo_indexes_to_create[_index_keys] = _index_properties

	if(not _Model._pk_attrs):  # create default _pk_attrs
		_Model._pk_attrs = OrderedDict(_id=True)

	# set collection name to include shard_keys

	_Model._collection_name_with_shard_ = _Model._collection_name_

	_Model._is_sharding_enabled_ = False  # default
	# append shard id to table name as an indicator that table is sharded
	if(_Model._is_secondary_shard or is_primary_sharding_enabled):
		_Model._collection_name_with_shard_ += "_shard_" + _Model._shard_key_
		_Model._is_sharding_enabled_ = True

	# find tracking nodes
	_Model._collection_tracker_key_ = "{:s}__{:s}".format(
		_Model._db_name_,
		_Model._collection_name_with_shard_
	)

	# just keep a track, for random use cases to retrive by name
	__all_models__[_Model._collection_name_with_shard_] = _Model

	IS_DEV \
		and MONGO_DEBUG_LEVEL > 1 \
		and print(
			"\n\n#MONGO collection tracker key",
			_Model._collection_tracker_key_
		)

	if(_Model not in [CollectionTracker]):
		collection_tracker = CollectionTracker.get(_Model._collection_tracker_key_)
		if(
			not collection_tracker
			or not collection_tracker.db_nodes
			or not collection_tracker.primary_shard_key
		):
			print(
				"\n\n#MONGOORM_IMPORTANT_INFO : "
				"Collection tracker entry not present for '{:s}'.."
				"creating table in control node for this time in database '{:s}'. "
				"You may want to talk to dba to move to a proper node".format(
					_Model.__name__, _Model._db_name_
				)
			)

			# check if the _Model already has _db_nodes_
			_collection_tracker_node = CollectionTracker._db_nodes_[0]
			db_node = {
				"hosts": _collection_tracker_node.hosts,
				"replicaset": _collection_tracker_node.replicaset,
				"at": 0,
				"username": _collection_tracker_node.username,
				"password": _collection_tracker_node.password,
				"db_name": _Model._db_name_
			}

			collection_tracker = CollectionTracker(
				_id=_Model._collection_tracker_key_,
				db_nodes=[db_node],
				is_primary_shard=1 if not _Model._is_secondary_shard else 0,
				primary_shard_key=_Model._shard_key_,
				secondary_shard_keys=list(_Model._secondary_shards_.keys()),
				pk_attrs=list(_Model._pk_attrs.keys()),
				attrs=list(_Model._attrs_.keys())
			).commit(force=True)

		# version < 101 support
		if(not collection_tracker.attrs):
			collection_tracker.attrs = list(_Model._attrs_.keys())
			collection_tracker.commit()

		_Model._db_nodes_ = tuple(
			DatabaseNode(**_db_node)
			for _db_node in collection_tracker.db_nodes
		)
		# TODO: find new secondary shards by comparing 
		# collection_tracker.secondary_shard_keys, _Model._secondary_shards_.keys()
		# and create a job to create and reindex all data to secondary index
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
					index_a=json.dumps(indexesA) if indexesA != None else None,
					index_b=json.dumps(indexesB) if indexesB != None else None
				)

	# check unused indexes
	existing_indexes = {
		tuple([(x, int(y)) for x, y in _index["key"]]): (_name, _index) # sometimes it's float so convert to int
		for _name, _index in existing_indexes.items()
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
	if(_new_indexes):
		for _index in _new_indexes:
			LOG_ERROR(
				"missing_mongo_indexes",
				desc=(
					f"db.{_Model._collection_name_with_shard_}"
					f".createIndex({json.dumps({x:y for x,y in _index})}, "
					f"{json.dumps(_pymongo_indexes_to_create[_index])})"
				)
			)
			_loading_errors["missing_mongo_indexes"] = True

	# for pymongo_index, mongo_index_args in _pymongo_indexes_to_create.items():
	# 	IS_DEV \
	# 		and MONGO_DEBUG_LEVEL > 1 \
	# 		and print(
	# 			"\n\n#MONGO: creating_indexes", _Model, pymongo_index, mongo_index_args
	# 		)
	# 	# in each node create indexes
	# 	for db_node in _Model._db_nodes_:
	# 		db_node.get_collection(_Model).create_index(pymongo_index, **mongo_index_args)

	# create secondary shards
	for _secondary_shard_key in list(_Model._secondary_shards_.keys()): # iterate on copy
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
def initialize_mongo(db_nodes, default_db_name=None):

	default_db_name = default_db_name or "temp_db"
	# initialize control db
	if(isinstance(db_nodes, dict)):
		db_nodes = [DatabaseNode(**db_nodes)]
	elif(isinstance(db_nodes, list)):
		db_nodes = [DatabaseNode(**db_node) for db_node in db_nodes]
	else:
		raise Exception(
			"argument must be a list of dicts, or a single dict, not %s"%(type(db_nodes))
		)
	# check connection to mongodb
	[db_node.mongo_client.server_info() for db_node in db_nodes]

	# initialize control db
	CollectionTracker._db_nodes_ = db_nodes
	initialize_model([CollectionTracker])

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
