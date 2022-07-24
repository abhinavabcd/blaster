import heapq
import types
import traceback
import pymongo
from typing import TypeVar
import metrohash
# from pymongo.read_concern import ReadConcern
# from pymongo.write_concern import WriteConcern
from bson.objectid import ObjectId
from itertools import chain
from collections import OrderedDict
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo import ReturnDocument, ReadPreference
from .tools import ExpiringCache, all_subclasses,\
	cur_ms, list_diff2, batched_iter, get_by_key_list
from .config import DEBUG_LEVEL as BLASTER_DEBUG_LEVEL, IS_DEV
from gevent.threading import Thread
from gevent import time
from .logging import LOG_WARN, LOG_SERVER, LOG_ERROR

T = TypeVar("T")

_VERSION_ = 101
MONGO_DEBUG_LEVEL = BLASTER_DEBUG_LEVEL
EVENT_BEFORE_DELETE = -2
EVENT_AFTER_DELETE = -1
EVENT_BEFORE_UPDATE = 1
EVENT_AFTER_UPDATE = 2
EVENT_BEFORE_CREATE = 3
EVENT_AFTER_CREATE = 4
EVENT_MONGO_AFTER_UPDATE = 5

# just a random constant object to check for non existent keys without catching KeyNotExist exception
_OBJ_END_ = object()
# given a list of [(filter, iter), ....] , flattens them


# utility to print collection name + node for logging
def COLLECTION_NAME(collection):
	_nodes = collection.database.client.nodes or (("unknown", 0),)
	for _node in _nodes:
		return "{:s}.{:s}.{:d}".format(collection.full_name, _node[0], _node[1])
	return collection.full_name


class Attribute(object):
	_type = None

	def __init__(self, _type, **kwargs):
		self._type = _type
		self.__dict__.update(kwargs)


__all_models__ = {}


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
	def get(cls, _pk=None, use_cache=True, **kwargs):
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

	'''Create a custom dict object, when you set an item on this dict,
		we basically mark pending_updates to mongo
	'''
	def get_custom_dict(self, path, _obj):

		_initializing = True

		class DictObj(dict):
			def __setitem__(this, k, v):
				if(not _initializing):
					self._set_query_updates[path + "." + str(k)] = v
				if(_initializing):
					if(isinstance(v, dict)):
						v = self.get_custom_dict(path + "." + str(k), v)
					elif(isinstance(v, list)):
						v = self.get_custom_list(path + "." + str(k), v)

				super(DictObj, this).__setitem__(k, v)

			def pop(this, k, default=None):
				# not in initializing mode
				popped_val = super(DictObj, this).pop(k, _OBJ_END_)
				if(popped_val == _OBJ_END_):
					return default
				# if not initializing and has value set remove it form mongo
				elif(not _initializing):
					new_path = path + "." + str(k)
					_unset = self._other_query_updates.get("$unset")
					if(not _unset):
						_unset = self._other_query_updates["$unset"] = {}
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

		ret = DictObj().update(_obj)
		_initializing = False
		return ret

	def get_custom_list(self, path, _list_obj):
		_initializing = True

		class ListObj(list):
			def __setitem__(this, k, v):
				if(not _initializing):
					self._set_query_updates[path + "." + str(k)] = v
				if(_initializing):
					if(isinstance(v, dict)):
						v = self.get_custom_dict(path + "." + str(k), v)
					elif(isinstance(v, list)):
						v = self.get_custom_list(path + "." + str(k), v)
				super(ListObj, this).__setitem__(k, v)

			def remove(this, item):  # can raise value exception
				super(ListObj, this).remove(item)
				_pull = self._other_query_updates.get("$pull")
				if(_pull == None):
					self._other_query_updates["$pull"] = _pull = {}
				already_pulled_values = _pull.get(path, _OBJ_END_)
				if(already_pulled_values == _OBJ_END_):
					_pull[path] = item
				else:
					if(
						isinstance(already_pulled_values, dict) 
						and "$in" in already_pulled_values
					):
						_pull[path]["$in"].append(item)
					else:
						_pull[path]["$in"] = [already_pulled_values, item]

			def pop(this, i=None):
				if(i == None):
					i = 1
					ret = super(ListObj, this).pop()
				elif(i == 0):
					ret = super(ListObj, this).pop(0)  # remove first item
					i = -1
				else:
					raise Exception(
						"can only remove first or last elements, supported arg is 0 or None"
					)

				if(not _initializing):
					_pop = self._other_query_updates.get("$pop")
					if(_pop == None):
						self._other_query_updates["$pop"] = _pop = {}
					_pop[path] = i
				# convert them to original object types
				if(isinstance(ret, list)):
					return list(ret)
				if(isinstance(ret, dict)):
					return dict(ret)
				return ret

			def append(this, item):
				if(_initializing):
					if(isinstance(item, dict)):
						item = self.get_custom_dict(path + "." + str(len(this)), item)
					elif(isinstance(item, list)):
						item = self.get_custom_list(path + "." + str(len(this)), item)

				super(ListObj, this).append(item)

				if(not _initializing):
					_push = self._other_query_updates.get("$push")
					if(_push == None):
						_push = self._other_query_updates["$push"] = {}
					_push[path] = item

			def extend(this, items):
				for i in items:
					this.append(i)

		ret = ListObj()
		for item in _list_obj:
			ret.append(item)
		_initializing = False
		return ret

	def __setattr__(self, k, v):
		_attr_obj = self.__class__._attrs_.get(k)
		if(_attr_obj):
			# change type of objects when setting them
			_attr_obj_type = _attr_obj._type
			# impllicit type corrections for v
			if(v != None and not isinstance(v, _attr_obj_type)):
				if(_attr_obj_type == int or _attr_obj_type == float):  # force cast between int/float
					v = _attr_obj_type(v or 0)
				elif(_attr_obj_type == str):  # force cast to string
					v = str(v)
				elif(_attr_obj_type == ObjectId and k == "_id"):
					pass  # could be anything don't try to cast
				else:
					if(not self._initializing):
						# we are setting it wrong on a new/initialized object
						# raise
						raise TypeError(
							"Type mismatch in {}, {}: should be {} but got {}".format(
								type(self), k, _attr_obj_type, str(type(v))
							)
						)
					# else:
						# already in db, so let's wait for application to crash

			if(self._initializing):
				if(not self.__is_new):
					if(_attr_obj_type == dict):
						if(isinstance(v, dict)):
							v = self.get_custom_dict(k, v)

					elif(_attr_obj_type == list):
						if(isinstance(v, list)):
							v = self.get_custom_list(k, v)
			else:
				cur_value = None
				if(not self.__is_new):
					cur_value = self._original_doc.get(k, _OBJ_END_)
				else:
					cur_value = getattr(self, k, _OBJ_END_)  # existing value

				# bug fix when cur_value = [] and setting to []
				# we should set it as query update
				if(cur_value != v or not cur_value):
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
			# if the value is not the document and have a default do the $set
			if(_default != _OBJ_END_ and attr_name not in doc):
				if(isinstance(_default, types.FunctionType)):
					_default = _default()
				# if there is a default value
				if(attr_obj._type == dict):
					self._set_query_updates[attr_name] = _default = dict(_default)  # copy
				elif(attr_obj._type == list):
					self._set_query_updates[attr_name] = _default = list(_default)  # copy
				else:
					self._set_query_updates[attr_name] = _default  # copy

				setattr(self, attr_name, _default)

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
			# _attrs_to_backfill = getattr(cls, "_attrs_to_backfill_", None)
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

	# for example, you can say update only when someother field > 0
	def update(self, _update_query, conditions=None, cb=None, **kwargs):
		cls = self.__class__

		if(not _update_query):
			return

		_NOT_EXISTS_QUERY = {"$exists": False}  # just a constant	
		cls._trigger_event(EVENT_BEFORE_UPDATE, self)

		# this is the dict of local value to compare to remote db before updating
		_local_vs_remote_consistency_checks = None
		for _update_retry_count in range(3):

			updated_doc = None
			_query = {"_id": self._id}
			# no additional conditions, retries ensure the state of
			# current object in memory matches remote before updating

			# consistency between local copy and that on the db,
			# to check concurrent updates
			_local_vs_remote_consistency_checks = {}
			# check all the fields that we are about to update
			# to match with remote
			for _k, _v in _update_query.items():
				if(_k[0] == "$"):  # it should start with $
					for p, q in _v.items():
						# consistency checks of the current document
						# to that exists in db
						_local_vs_remote_consistency_checks[p] \
							= get_by_key_list(self._original_doc, p.split("."), default=_NOT_EXISTS_QUERY)

			# override with any given conditions
			_query.update(_local_vs_remote_consistency_checks)
			# update with more given conditions,
			# but don't update _local_vs_remote_consistency_checks
			# (it's only for local<->remote comparision)
			if(conditions):
				_query.update(conditions)

			# get the shard where current object is
			primary_shard_key = self._original_doc[cls._shard_key_]
			primary_collection_shard = cls.get_collection(primary_shard_key)
			# query and update the document
			IS_DEV \
				and MONGO_DEBUG_LEVEL > 1 \
				and LOG_SERVER(
					"MONGO", description="update before and query",
					model=cls.__name__, collection=COLLECTION_NAME(primary_collection_shard),
					query="_query", original_doc=str(self._original_doc),
					update_query=str(_update_query)
				)

			# 1: try updating
			updated_doc = primary_collection_shard.find_one_and_update(
				_query,
				_update_query,
				return_document=ReturnDocument.AFTER,
				**kwargs
			)

			IS_DEV \
				and MONGO_DEBUG_LEVEL > 1\
				and LOG_SERVER(
					"MONGO", description="after update",
					model=cls.__name__, collection=COLLECTION_NAME(primary_collection_shard),
					updated_doc=str(updated_doc)
				)

			if(updated_doc):
				# check if need to migrate to another shard
				_new_primary_shard_key = updated_doc.get(cls._shard_key_)
				if(_new_primary_shard_key != primary_shard_key):
					# primary shard key has changed
					new_primary_collection_shard = cls.get_collection(_new_primary_shard_key)
					if(new_primary_collection_shard != primary_collection_shard):
						# migrate to new shard
						# will crash if duplicate _id key
						new_primary_collection_shard.insert_one(updated_doc)
						primary_collection_shard.delete_one({"_id": self._id})

				# 1. propagate the updates to secondary shards
				for _secondary_shard_key, _shard in cls._secondary_shards_.items():
					_SecondayModel = _shard._Model_

					_old_shard_key_val = self._original_doc.get(_secondary_shard_key, _OBJ_END_)
					_new_shard_key_val = updated_doc.get(_secondary_shard_key, _OBJ_END_)
					# 2. check if shard key has been changed
					if(_secondary_shard_key != _new_shard_key_val):
						# 2.1. If old shard exists, delete that secondary doc
						if(_old_shard_key_val != _OBJ_END_):
							_SecondayModel\
								.get_collection(_old_shard_key_val)\
								.delete_one({"_id": self._id})

						# 2.2.  if new shard key exists insert into appropriate node
						if(_new_shard_key_val != _OBJ_END_ and _new_shard_key_val):
							_to_insert = {}
							for _k in _shard.attrs:
								_v = updated_doc.get(_k, _OBJ_END_)
								if(_v != _OBJ_END_):
									_to_insert[_k] = _v

							_seconday_inserted = None
							_secondary_collection = _SecondayModel\
								.get_collection(_new_shard_key_val)
							_id_key = {"_id": self._id}
							for _secondary_insert_retries in range(3):
								try:
									_seconday_inserted = _secondary_collection\
										.replace_one(_id_key, _to_insert, upsert=True)
								except DuplicateKeyError:
									LOG_WARN(
										"MONGO", description="secondary upsert failed.. retrying",
										model=cls.__name__,
									)
									time.sleep(_secondary_insert_retries * 0.02)

							if(not _secondary_shard_key):
								LOG_ERROR(
									"MONGO", description="couldn't insert to secondary",
									collection=COLLECTION_NAME(_secondary_collection),
									key=str(_id_key)
								)
								raise Exception("couldn't insert to secondary")

					#  3. if shard hasn't changed and exists patch
					elif(_new_shard_key_val != _OBJ_END_ and _new_shard_key_val):
						_set = {}
						_unset = {}
						_conditions = {"_id": updated_doc["_id"]}
						_to_patch = {}
						attrs_to_backfill = getattr(_shard, "_attrs_to_backfill_", None)
						# 3.1. update only those values that have been checked on primary
						# and changed
						for _k, _old_val in _local_vs_remote_consistency_checks.items():
							if(_k in _shard.attrs):
								_new_val = updated_doc.get(_k, _OBJ_END_)
								if(_new_val != _OBJ_END_):
									_set[_k] = _new_val
								else:
									_unset[_k] = True

								# if not in migrating/backfilling, use it for consistency check
								if(not (attrs_to_backfill and _k in attrs_to_backfill)):
									_conditions[_k] = _old_val

						if(_set):
							_to_patch["$set"] = _set
						if(_unset):
							_to_patch["$unset"] = _unset
						if(_to_patch):
							is_secondary_shard_updated = False
							_secondary_collection = _SecondayModel\
								.get_collection(_new_shard_key_val)
							for _retry_count in range(1, 4):
								is_secondary_shard_updated = _secondary_collection\
									.find_one_and_update(_conditions, _to_patch)
								if(is_secondary_shard_updated):
									break
								time.sleep(_retry_count * 0.03)

							if(not is_secondary_shard_updated):
								LOG_ERROR(
									"MONGO",
									description="secondary couldn't be propagated",
									model=cls.__name__, collection=COLLECTION_NAME(_secondary_collection),
									secondary_pk=str(_conditions),
									secondary_updates=str(_to_patch)
								)

				cb and cb(self, self._original_doc, updated_doc)
				
				cls._trigger_event(EVENT_MONGO_AFTER_UPDATE, self._original_doc, updated_doc)

				# reset all values
				self.reset_and_update_cache(updated_doc)
				# waint on threads

				# triggered after update event
				cls._trigger_event(EVENT_AFTER_UPDATE, self)
				return True

			# update was unsuccessful
			# is this concurrent update by someone else?
			# is this because of more conditions given by user?
			# 1. fetch from db again
			_update_retry_count and time.sleep(0.03 * _update_retry_count)
			_doc_in_db = primary_collection_shard.find_one({"_id": self._id})
			if(not _doc_in_db):  # moved out of shard or pk changed
				return False
			# 2. update our local copy
			self.reset_and_update_cache(_doc_in_db)
			can_retry = False
			# 3. check if basic consistency between local and remote
			for _k, _v in _local_vs_remote_consistency_checks.items():
				remote_db_val = get_by_key_list(
					_doc_in_db, _k.split("."),
					default=_NOT_EXISTS_QUERY
				)
				if(_v != remote_db_val):
					LOG_WARN(
						"MONGO", description="remote was modified retrying", model=cls.__name__,
						collection=COLLECTION_NAME(primary_collection_shard), _query=str(_query),
						remote_value=remote_db_val, local_value=str(_v), _key=_k
					)
					can_retry = True  # local copy consistency check has failed, retry again
					break

			if(not can_retry):
				return False

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
	):
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
			# try checking if we can query rimary
			collection_shards = cls.get_collections_to_query(_query, sort) or []
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
			collections_to_query = {id(x): (x, queries, cls) for x in cls.get_collection_on_all_db_nodes()}

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

			def __init__(self):
				self.query_result_iters = []
				self.query_count_funcs = []

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
				if(len(self.query_result_iters) == 1):
					return self.query_result_iters[0]
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

			ret = _collection.find(_query, **kwargs)
			if(sort):  # from global arg
				ret = ret.sort(sort)
			if(offset):  # from global arg
				ret = ret.skip(offset)
			if(limit):
				ret = ret.limit(limit)

			# we queried from the secondary shard, will not have all fields
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
		for _collection_shard_id, shard_and_queries in collections_to_query.items():
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
					description="Cannot use offset when query spans multiple collections shards, improve your query",
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
			try:
				handler(*obj)
			except Exception as ex:
				LOG_ERROR(
					"MONGO",
					description="error processing trigger {}".format(event),
					exception_str=str(ex),
					stacktrace_string=traceback.format_exc()
				)

	def before_update(self):
		pass

	def commit(self, force=False, ignore_exceptions=None, conditions=None):
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

			_collection_shard = cls.get_collection(
				str(getattr(self, shard_key_name))
			)
			IS_DEV \
				and MONGO_DEBUG_LEVEL > 1 \
				and LOG_SERVER(
					"MONGO", description="new object values",
					model=cls.__name__, collection=COLLECTION_NAME(_collection_shard),
					set_query=str(self._set_query_updates)
				)
			try:
				# find which shard we should insert to and insert into that
				self._insert_result = _collection_shard.insert_one(
					self._set_query_updates
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

						_to_insert and _SecondaryModel\
							.get_collection(_shard_key_val)\
							.insert_one(_to_insert)

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
				# and update any other fields
				self.reset_and_update_cache(
					_collection_shard.find_one(self.pk())
				)
				IS_DEV \
					and MONGO_DEBUG_LEVEL > 1 \
					and LOG_SERVER(
						"MONGO", description="created a duplicate, refetching and updating",
						model=cls.__name__,
						collection=COLLECTION_NAME(_collection_shard),
						pk=str(self.pk())
					)

				# try removing all primary keys
				# although this is unnecessary i feel it's better this way
				if("_id" in self._set_query_updates):
					del self._set_query_updates["_id"]

				for k in list(self._set_query_updates.keys()):
					if(k in cls._pk_attrs):
						del self._set_query_updates[k]

		if(not self.__is_new and not committed):  # try updating
			_update_query = {}
			if(self._set_query_updates):
				_update_query["$set"] = self._set_query_updates
			if(self._other_query_updates):
				_update_query.update(self._other_query_updates)
			if(not _update_query and not force):
				return self  # nothing to update

			is_committed = self.update(_update_query, conditions=conditions)
			if(not is_committed):
				# if it's used for creating a new object, but we couldn't update it
				raise Exception(
					"MONGO: Couldn't commit, either a concurrent update modified this or query has issues",
					self.pk(), _update_query
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
		while((cur_timestamp := cur_ms()) - start_timestamp < timeout):
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
					}
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
			conditions=None if force else {"locked": self.__locked_until}
		)
		self.__locked_until = None


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


# Control Jobs
# - every time a job is completed, check if it has parent,
# - goto parent and check if all child jobs are finished and mark it complete
class ControlJobs(Model):
	_db_name_ = "control"
	_collection_name_ = "jobs_v2"

	# JOB TYPES
	RESHARD = 0
	CREATE_SECONDARY_SHARD = 1
	ADD_NEW_ATTRIBUTES_TO_SECONDARY_SHARD = 2

	# attributes
	parent__id = Attribute(str)
	num_child_jobs = Attribute(int, default=0)  # just for reconcillation
	_type = Attribute(int)
	db = Attribute(str)
	collection = Attribute(str)
	uid = Attribute(str)  # unique identifier not to duplicate jobs
	status = Attribute(int, default=0)  # 0=>not started,1=>progress,2=>completed

	worker_id = Attribute(str)  # worker id
	# contract that worker should update every few millis
	worker_should_update_within_ms = Attribute(int, default=60000)
	# work data
	data = Attribute(dict)
	data1 = Attribute(dict)
	created_at = Attribute(int, default=cur_ms)
	updated_at = Attribute(int, default=cur_ms)

	INDEX(
		(db, collection, _type, uid, status),
		(db, collection, _type, status, {"unique": False}),
		(parent__id, {"unique": False})
	)

	def before_update(self):
		self.updated_at = cur_ms()

	def run(self):
		if(self._type == ControlJobs.ADD_NEW_ATTRIBUTES_TO_SECONDARY_SHARD):
			_SecondaryShardModel = __all_models__[self.collection]
			_PrimaryShardModel = _SecondaryShardModel._primary_model_class_
			# TODO: make this run on multiple nodes
			# by running query on specific nodes
			_secondary_shard_key = _SecondaryShardModel._shard_key_
			for db_node in _PrimaryShardModel._db_nodes_:
				print("querying", _PrimaryShardModel, "from", db_node.hosts, " to reindex")
				i = 0
				for _doc in db_node.get_collection(_PrimaryShardModel).find({}):
					# delete and reinsert full doc in the shard
					_shard_key_val = _doc.get(_secondary_shard_key)
					if(_shard_key_val):
						# 1. we have it linked in the
						# secondary shard, delete there
						_collection = _SecondaryShardModel\
							.get_collection(_shard_key_val)
						_collection.delete_one({"_id": _doc["_id"]})
						# 2. Insert
						_to_insert = {}
						for attr in _SecondaryShardModel._attrs_:
							_attr_val = _doc.get(attr, _OBJ_END_)
							if(_attr_val != _OBJ_END_):
								_to_insert[attr] = _attr_val
							_collection.insert_one(_to_insert)
					i += 1
					if(i % 1000 == 0):
						print("propagated", i, "records to secondary shard")
				print("propagated", i, "records to secondary shard")
			self.status = 1  # completed
			# remove the attributes
			collection_tracker_entry = CollectionTracker.get(
				_SecondaryShardModel._collection_tracker_key_
			)
			for _attr in self.data["to_remove_attrs"]:
				collection_tracker_entry.attrs.remove(_attr)
			collection_tracker_entry.commit()
			# add the new attributes
			collection_tracker_entry.attrs.extend(self.data["to_add_attrs"])
			collection_tracker_entry.commit()

		if(self._type == ControlJobs.CREATE_SECONDARY_SHARD):
			pass


_cached_mongo_clients = {}


# DatabaseNode is basically a server or a replicaset
class DatabaseNode:

	_cached_pymongo_collections = None
	# mongo connection
	mongo_connection = None
	hosts = None
	replica_set = None
	username = None
	password = None
	# default db name
	db_name = None
	at = None

	# this initializes the tables in all nodes
	def __init__(
		self, host=None, replica_set=None,
		username=None, password=None, db_name=None,
		hosts=None, at=0
	):
		if(isinstance(host, str)):
			hosts = [host]
		hosts.sort()
		self.hosts = hosts
		self.replica_set = replica_set
		self.username = username
		self.db_name = db_name
		self.at = at
		# use list of hosts, replicaset and username as cache key
		# so as not to create multiple clients
		_mongo_clients_cache_key = (",".join(sorted(hosts)), replica_set or "", username or "")
		self.mongo_connection = _cached_mongo_clients.get(_mongo_clients_cache_key)
		if(not self.mongo_connection):
			self.mongo_connection \
				= _cached_mongo_clients[_mongo_clients_cache_key] \
				= MongoClient(
					host=hosts, replicaSet=replica_set,
					username=username, password=password
				)

		self._cached_pymongo_collections = {}

	def to_dict(self):
		return {
			"hosts": self.hosts,
			"replica_set": self.replica_set,
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
				= self.mongo_connection[_Model._db_name_][_Model._collection_name_with_shard_]
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
	if(_Model_cache == _OBJ_END_):
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
			attrs_to_name[v] = k
			# dumb , but it's one time thing
			# and also helps converting if any given attributes as strings
			attrs_to_name[k] = k

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

	_pymongo_indexes_to_create = []
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
			_pymongo_indexes_to_create.append((_index_keys, _index_properties))

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

	if(_Model not in [CollectionTracker, ControlJobs]):
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
				"replica_set": _collection_tracker_node.replica_set,
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

		if(not _Model._is_secondary_shard):
			# create diff jobs, check if new shard is indicated
			to_create_secondary_shard_key, to_delete_secondary_shard_key = list_diff2(
				list(_Model._secondary_shards_.keys()),
				collection_tracker.secondary_shard_keys
			)
			for shard_key_name in to_create_secondary_shard_key:
				try:
					ControlJobs(
						_id=str(cur_ms()),
						db=_Model._db_name_,
						collection=_Model._collection_name_with_shard_,
						_type=ControlJobs.CREATE_SECONDARY_SHARD,
						uid=shard_key_name
					).commit()
					IS_DEV \
						and MONGO_DEBUG_LEVEL > 1 \
						and print(
							"\n\n#MONGO: create a control job to create secondary shard",
							_Model, shard_key_name
						)
				except DuplicateKeyError:
					print(
						(
							"Secondary shard not synced yet, queries on this shard won't "
							"give results for previously created entries, please propagate all data to this shard. "
							"Add mark this ControlJob as complete"
						), _Model, shard_key_name
					)

		if(_Model._is_secondary_shard):
			to_add_attrs, to_remove_attrs = list_diff2(
				list(_Model._attrs_.keys()),
				collection_tracker.attrs
			)
			if(to_add_attrs or to_remove_attrs):
				try:
					ControlJobs(
						_id=str(cur_ms()),
						db=_Model._db_name_,
						collection=_Model._collection_name_with_shard_,
						_type=ControlJobs.ADD_NEW_ATTRIBUTES_TO_SECONDARY_SHARD,
						uid="",
						data={"to_add_attrs": to_add_attrs, "to_remove_attrs": to_remove_attrs}
					).commit()
					IS_DEV \
						and MONGO_DEBUG_LEVEL > 1 \
						and print(
							"\n\n#MONGO: created a control job to change attributes",
							_Model, to_add_attrs, to_remove_attrs
						)
					raise Exception(
						"Attributes have been changed on the Secondary shard, "
						"you need to fill the attribute in the shard "
					)
				except DuplicateKeyError:
					LOG_ERROR(
						"MONGO",
						description=(
							"There is already a pending job to add {}, remove {} attributes in {}"
							"queries involving these attrs will not give correct results"
							"perform this to correct ::-> "
							"ControlJobs.get(db='{:s}', collection='{:s}', _type={:d}, uid='').run()"
						).format(
							to_add_attrs, to_remove_attrs, _Model,
							# attrs
							_Model._db_name_, _Model._collection_name_with_shard_,
							ControlJobs.ADD_NEW_ATTRIBUTES_TO_SECONDARY_SHARD
						)
					)
					# for now do not use it for consistency checks
					_Model._attrs_to_backfill_ = {}
					for _attr_name in to_add_attrs:
						_Model._attrs_to_backfill_[_attr_name] = _Model._attrs_[_attr_name]

		# check if new attribute added to secondary shard

	# TODO: create or delete index using control jobs
	for pymongo_index, additional_mongo_index_args in _pymongo_indexes_to_create:
		mongo_index_args = {"unique": True}
		mongo_index_args.update(**additional_mongo_index_args)

		IS_DEV \
			and MONGO_DEBUG_LEVEL > 1 \
			and print(
				"\n\n#MONGO: creating_indexes", _Model, pymongo_index, mongo_index_args
			)
		# in each node create indexes
		for db_node in _Model._db_nodes_:
			db_node.get_collection(_Model).create_index(pymongo_index, **mongo_index_args)

	# create secondary shards
	for _secondary_shard_key, _secondary_shard in _Model._secondary_shards_.items():
		if(not _secondary_shard.attrs):
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
		raise Exception("argument must be a list of dicts, or a single dict")
	# check connection to mongodb
	[db_node.mongo_connection.server_info() for db_node in db_nodes]

	# initialize control db
	CollectionTracker._db_nodes_ = db_nodes
	ControlJobs._db_nodes_ = db_nodes
	initialize_model([CollectionTracker, ControlJobs])

	# set default db name for each class
	for cls in all_subclasses(Model):
		if(not getattr(cls, "_db_name_", None)):
			cls._db_name_ = default_db_name
		initialize_model(cls)


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


# Optionally:
# - when you apply a sort param, and query spans multiple nodes
# we create a small heap ( = number of nodes queries) and advance cusors accordingly
# Limitations:
# - when query spans multiple nodes, you can apply offset on the query
# - You can't run update query that spans multiple documents
# - if you corrupt data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
# when we say collection it it's actual pymongo collection on db server
# DbNode -> mongo server which contains collections


# Internal Notes:
# ############## Using a transaction
# _mongo_client = cls.get_db_node(self._original_doc[cls._shard_key_]).mongo_connection
# with _mongo_client.start_session() as session:
# 	#
# 	return session.with_transaction(
# 		lambda session: some_func, # transaction updates inside this func
# 		read_concern=ReadConcern('local'),
# 		write_concern=WriteConcern("majority", wtimeout=5000),
# 		read_preference=ReadPreference.PRIMARY
# 	)
# ##############
