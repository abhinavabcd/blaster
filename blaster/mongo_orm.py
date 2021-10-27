import heapq
import types
import json
import pymongo
from datetime import datetime
#from bson.objectid import ObjectId
from itertools import chain
from collections import OrderedDict
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo import ReturnDocument, ReadPreference
from .common_funcs_and_datastructures import jump_hash, ExpiringCache,\
	cur_ms, list_diff2, batched_iter, get_by_key_list
from .config import DEBUG_LEVEL as BLASTER_DEBUG_LEVEL, IS_DEV
from gevent.threading import Thread
from .logging import LOG_WARN, LOG_SERVER, LOG_ERROR

_VERSION_ = 101
MONGO_DEBUG_LEVEL = BLASTER_DEBUG_LEVEL
EVENT_BEFORE_DELETE = -2
EVENT_AFTER_DELETE = -1
EVENT_BEFORE_UPDATE = 1
EVENT_AFTER_UPDATE = 2
EVENT_BEFORE_CREATE = 3
EVENT_AFTER_CREATE = 4
EVENT_MONGO_AFTER_UPDATE = 5

#just a random constant object to check for non existent keys without catching KeyNotExist exception
_OBJ_END_ = object()
#given a list of [(filter, iter), ....] , flattens them

##utility to print collection name + node for logging
def COLLECTION_NAME(collection):
	_nodes = collection.database.client.nodes or (("unknown", "unknown"),)
	for _node in _nodes:
		return "%s.%s.%d"%(collection.full_name, _node[0], _node[1])
	return collection.full_name
	
class Attribute(object):
	_type = None

	def __init__(self, _type, **kwargs):
		self._type = _type
		self.__dict__.update(kwargs)

class Model(object):
	#class level
	_attrs = None
	_pk_attrs = None

	__cache__ = None
	_is_secondary_shard = False # used to identify if it's a primary or secondary shard
	
	#instance level
	__is_new = True
	_set_query_updates = None
	_other_query_updates = None
	_original_doc = None
	#we execute all functions inside this list, and use the return value to update
	_insert_result = None
	#this means the object is being initialized by this orm, and not available to user yet
	_initializing = False
	_pk = None
	_json = None

	#initialize a new object
	def __init__(self, _is_new_=True, **values):

		#defaults
		self.__is_new = _is_new_
		self._initializing = not _is_new_
		self._set_query_updates = {}
		self._other_query_updates = {}
		cls = self.__class__
		for k, v in cls._attrs.items():
			self.__dict__[k] = None # set all initial attributes to None

		if(_is_new_):
			#setup default values
			self.__check_and_set_initial_defaults(values) # from empty doc

		#update given values
		for k, v in values.items():
			setattr(self, k, v)

	def get_id(self):
		return str(self._id)

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
			#handle _ids fetch separately so to speedup

			is_already_fetching = {}
			to_fetch_pks = []
			to_fetch_ids = []
			for _pk in _pks:
				_id = None # string based id
				_pk_tuple = None
				if(not _pk):
					continue
				elif(isinstance(_pk, str)):
					_pk_tuple = (_pk,)
					_id = _pk
					_pk = None # reset it to null as we are fetching it via ids
				else: # dict
					_pk_tuple = []
					for _k in cls._pk_attrs:
						_pk_tuple.append(_pk[_k])
					_pk_tuple = tuple(_pk_tuple)

				#check already in cache
				_item_in_cache = None
				if(use_cache and cls.__cache__):
					_item_in_cache = cls.__cache__.get(_pk_tuple)

				if(not _item_in_cache):
					if(is_already_fetching.get(_pk_tuple)):
						continue
					is_already_fetching[_pk_tuple] = True
					if(_pk):
						#remove duplicate pk query check if already fetching
						to_fetch_pks.append(_pk)
					if(_id):
						to_fetch_ids.append(_id)

				else:
					from_cache.append(_item_in_cache)

			_query = {"$or": to_fetch_pks}
			if(to_fetch_ids):
				to_fetch_pks.append({"_id": {"$in": to_fetch_ids}})

			ret = list(chain(from_cache, cls.query(_query)))
			if(is_single_item):
				if(ret):
					return ret[0]
				else:
					return None
			#multiple items, reorder
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
				elif(isinstance(_pk, str)): # possibly id
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
			#assuming we want a single item
			ret = list(cls.query(kwargs))
			if(ret):
				return ret[0]

		return None

	'''Create a custom dict object, when you set an item on this dict, we basically mark pending_updates to mongo'''
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
				#not in initializing mode
				popped_val = super(DictObj, this).pop(k, _OBJ_END_)
				if(popped_val == _OBJ_END_):
					return default
				#if not initializing and has value set remove it form mongo
				elif(not _initializing):
					new_path = path + "." + str(k)
					_unset = self._other_query_updates.get("$unset")
					if(not _unset):
						_unset = self._other_query_updates["$unset"] = {}
					_unset[new_path] = ""
				#convert them to original object types
				if(isinstance(popped_val, list)):
					return list(popped_val)
				if(isinstance(popped_val, dict)):
					return dict(popped_val)
				return popped_val

			def update(self, another):
				for k, v in another.items():
					#calls __setitem__ again
					self[k] = v
				#allow chaining
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

			def remove(this, item): # can raise value exception
				super(ListObj, this).remove(item)
				# reset full array very inefficient : (
				_pull = self._other_query_updates.get("$pull")
				if(_pull == None):
					self._other_query_updates["$pull"] = _pull = {}
				already_pulled_values = _pull.get(path, _OBJ_END_)
				if(already_pulled_values == _OBJ_END_):
					_pull[path] = item
				else:
					if(isinstance(already_pulled_values, dict) and "$in" in already_pulled_values):
						_pull[path]["$in"].append(item)
					else:
						_pull[path]["$in"] = [already_pulled_values, item]

			def pop(this, i=None):
				if(i == None):
					i = 1
					ret = super(ListObj, this).pop()
				elif(i == 0):
					ret = super(ListObj, this).pop(0) # remove first item
					i = -1
				else:
					raise Exception("can only remove first or last elements, supported arg is 0 or None")

				if(not _initializing):
					_pop = self._other_query_updates.get("$pop")
					if(_pop == None):
						self._other_query_updates["$pop"] = _pop = {}
					_pop[path] = i
				#convert them to original object types
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

		ret = ListObj()
		for item in _list_obj:
			ret.append(item)
		_initializing = False
		return ret

	def __setattr__(self, k, v):
		_attr_type_obj = self.__class__._attrs.get(k)
		if(_attr_type_obj):
			#change type of objects when setting them
			if(v and not isinstance(v, _attr_type_obj._type)):
				if(_attr_type_obj._type in (int, float)): # force cast between int/float
					v = _attr_type_obj._type(v)
				elif(_attr_type_obj._type == str): # force cast to string
					v = str(v)
				elif(not self._initializing):
					raise Exception("Type mismatch in %s, %s: should be %s but got %s"%(type(self), k, _attr_type_obj._type, str(type(v))))

			if(self._initializing):
				if(not self.__is_new):
					if(_attr_type_obj._type == dict):
						if(isinstance(v, dict)):
							v = self.get_custom_dict(k, v)

					elif(_attr_type_obj._type == list):
						if(isinstance(v, list)):
							v = self.get_custom_list(k, v)
			else:
				cur_value = None
				if(not self.__is_new):
					cur_value = self._original_doc.get(k, _OBJ_END_)
				else:
					cur_value = getattr(self, k, _OBJ_END_) # existing value

				if(cur_value != v):
					self._set_query_updates[k] = v
		self.__dict__[k] = v

	def to_dict(self):
		_json = {}
		for k in self.__class__._attrs:
			v = getattr(self, k, _OBJ_END_)
			if(v != _OBJ_END_):
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

	#check all defaults and see if not present in doc, set them on the object
	def __check_and_set_initial_defaults(self, doc):
		for attr_name, attr_obj in self.__class__._attrs.items():
			_default = getattr(attr_obj, "default", _OBJ_END_)
			#if the value is not the document and have a default do the $set
			if(_default != _OBJ_END_ and attr_name not in doc):
				if(isinstance(_default, types.FunctionType)):
					_default = _default()
				#if there is a default value
				if(attr_obj._type == dict):
					self._set_query_updates[attr_name] = _default = dict(_default) # copy
				elif(attr_obj._type == list):
					self._set_query_updates[attr_name] = _default = list(_default) # copy
				else:
					self._set_query_updates[attr_name] = _default # copy

				setattr(self, attr_name, _default)


	#when retrieving objects from db
	@classmethod
	def get_instance_from_document(cls, doc):
		ret = cls(False) # not a new item

		#set defaults
		ret.__check_and_set_initial_defaults(doc)

		for k, v in doc.items():
			setattr(ret, k, v)

		ret._original_doc = doc
		#get and cache pk
		ret.pk(renew=True)
		#important flag to indicat initiazation finished
		ret._initializing = False
		if(cls.__cache__):
			cls.__cache__.set(ret.pk_tuple(), ret)
		return ret

	def reinitialize_from_doc(self, doc):
		cls = self.__class__
		#remove existing pk_tuple in cache
		cls.remove_from_cache(self)
		self.__is_new = False # this is not a new object
		self._initializing = True

		self.__check_and_set_initial_defaults(doc)
		for k, v in doc.items():
			setattr(self, k, v)

		self._initializing = False
		self._original_doc = doc
		#renew the pk!
		self.pk(True)
		if(cls.__cache__):
			cls.__cache__.set(self.pk_tuple(), self)


	# basically finds the node where the shard_key resides
	# and returns a actual pymongo collection, shard_key must be a string
	@classmethod
	def get_collection(Model, shard_key):
		shard_key = str(shard_key) if shard_key != None else ""
		node_with_data_index = jump_hash(shard_key.encode(), len(Model._db_nodes_))
		db_node = Model._db_nodes_[node_with_data_index]
		return db_node.get_collection(Model)

	# returns all nodes and connections to Model inside them
	@classmethod
	def get_collection_on_all_db_nodes(Model):
		return map(lambda db_node: db_node.get_collection(Model), Model._db_nodes_)

	#give a shard key from query we return multiple connect
	@classmethod
	def get_collections_to_query(cls, _query, sort):
		shard_key = _query.get(cls._shard_key_, _OBJ_END_)
		if(shard_key == None
			or shard_key == _OBJ_END_
		):
			return None

		#if it's a secondary shard check if we can perform the query on this Model
		if(cls._is_secondary_shard):
			#check if we can construct a secondary shard query
			for query_attr_name, query_attr_val in _query.items():
				attr_exists_in_shard = cls._attrs.get(query_attr_name, _OBJ_END_)
				if(attr_exists_in_shard == _OBJ_END_):
					return None

			if(sort):
				#check if we can perform sort on the secondary index
				for sort_key, sort_direction in sort:
					if(sort_key not in cls._attrs):
						#cannot perform sort
						return None

		if(isinstance(shard_key, (str, int))):
			return [cls.get_collection(shard_key)]

		elif(isinstance(shard_key, dict)):
			_in_values = shard_key.get("$in", _OBJ_END_)
			if(_in_values != _OBJ_END_):
				collections_to_update = {}
				#group all shards into one set
				for shard_key_value in _in_values:
					_collection = cls.get_collection(shard_key_value)
					collections_to_update[id(_collection)] = _collection
				return collections_to_update.values()

			_eq_value = shard_key.get("$eq", _OBJ_END_)
			if(_eq_value != _OBJ_END_):
				return [cls.get_collection(_eq_value)]

	@classmethod
	def propagate_update_to_secondary_shards(cls, before_doc, after_doc, force=False, specific_shards=None):
		#CONS of this multi table sharding:
		#- Primary index updated but secondary constraint can fail
		#- in that case, the data exists in primary shard, but it's not valid data
		#TODO:
		# idea1: move this to transactions if we have secondary shards ?
		# idea2: revert the original update
		#update all secondary shards
		for shard_key in (specific_shards or cls._secondary_shards_.keys()):
			#actual secondary shard
			shard = cls._secondary_shards_[shard_key]

			secondary_Model = shard._Model_

			#shard keys
			_before_shard_key = before_doc.get(shard_key)
			_after_shard_key = after_doc.get(shard_key)

			if(_before_shard_key == None and _after_shard_key == None):
				#this never existing in this secondary shard
				continue

			is_shard_key_changed = (_before_shard_key != _after_shard_key)

			_before_id = before_doc.get("_id") # if this exists then it's existing document
			#Note: all secondary index data is retrieved by _id, but sharded by their shard_key
			_secondary_pk = {"_id": _before_id}
			_secondary_values_to_set = {}
			

			#secondary collection which this doc to
			_secondary_collection = (_before_shard_key != None) and secondary_Model.get_collection(_before_shard_key)

			#shard key has changed
			if(is_shard_key_changed or force):
				# belonged to a shard delete it
				if(_secondary_collection and _before_id != None):
					_secondary_collection.find_one_and_delete(_secondary_pk)
					IS_DEV and MONGO_DEBUG_LEVEL > 1 and LOG_SERVER("MONGO", description="deleting from secondary",
						seconday_pk=_secondary_pk, model=secondary_Model.__name__, collection=COLLECTION_NAME(_secondary_collection)
					)


				if(_after_shard_key != None and after_doc["_id"]):
					for attr, _attr_obj in shard.attrs.items():
						_secondary_values_to_set[attr] = after_doc.get(attr)
					#just force set _id again
					_secondary_values_to_set["_id"] = after_doc["_id"]
					#find new shard
					_secondary_collection = secondary_Model.get_collection(
						_secondary_values_to_set[shard_key] # same as after_shard_key
					)
					#insert new one
					IS_DEV and MONGO_DEBUG_LEVEL > 1 and LOG_SERVER("MONGO", description="deleting and inserting new secondary",
						secondary_pk=str(_secondary_pk), values_to_set=str(_secondary_values_to_set),
						model=secondary_Model.__name__, collection=COLLECTION_NAME(_secondary_collection)
					)
					_secondary_collection.insert_one(_secondary_values_to_set)
				
			else:

				#check for any attrs to backfill, for these values we don't do
				#consistency check, but just progate setting the value"
				attrs_to_backfill = getattr(shard, "_attrs_to_backfill_", None)

				_secondary_values_to_unset = {}
				#if someone else has modified
				#old values check in pk will not overwrite it
				for attr in shard.attrs:
					old_value = before_doc.get(attr, _OBJ_END_)
					new_value = after_doc.get(attr, _OBJ_END_)
					if(old_value != new_value):
						if(new_value == _OBJ_END_): # removed after update
							_secondary_values_to_unset[attr] = new_value # unset this value as it doesn't exist anymore
						else:
							_secondary_values_to_set[attr] = new_value
						#also add this for query for a little bit of more consistency check in query
						if(old_value != _OBJ_END_ and old_value != new_value): # means it exists and changed, add this to consistency checks
							if(attrs_to_backfill and attr in attrs_to_backfill):
								# can be non existing, null or old_value,
								#there is an issue where we might miss consistency when original doc has non-null, but seconday shard has null value,
								#Any way this is way better
								_secondary_pk[attr] = {"$in": [None, old_value]}
							else:
								_secondary_pk[attr] = old_value

				if(_secondary_values_to_set or _secondary_values_to_unset):
					_secondary_updates = {}
					if(_secondary_values_to_set):
						_secondary_updates["$set"] = _secondary_values_to_set
					if(_secondary_values_to_unset):
						_secondary_updates["$unset"] = _secondary_values_to_unset
					IS_DEV and MONGO_DEBUG_LEVEL > 1 and LOG_SERVER("MONGO", description="updating secondary",
						model=cls.__name__, collection=COLLECTION_NAME(_secondary_collection),
						secondary_pk=str(_secondary_pk), secondary_updates=str(_secondary_updates)
					)
					#find that doc and update
					secondary_shard_update_return_value = _secondary_collection.find_one_and_update(
						_secondary_pk,
						_secondary_updates
					)

					if(not secondary_shard_update_return_value):
						LOG_WARN("MONGO", description="secondary couldn't be propagated, probably due to concurrent update",
								model=cls.__name__, collection=COLLECTION_NAME(_secondary_collection), secondary_model=str(secondary_Model), secondary_pk=str(_secondary_pk),
								secondary_updates=str(_secondary_updates)
							)

	#_add_query is more query other than pk
	# for example, you can say update only when someother field > 0

	def update(self, _update_query, more_conditions=None, **kwargs):
		cls = self.__class__

		if(not _update_query):
			return
		
		cls._trigger_event(EVENT_BEFORE_UPDATE, self)

		retries = 3
		_consistency_checks = None # this is the dict of modified fields
		while(retries > 0):
			retries -= 1

			updated_doc = None
			_query = dict(self.pk())
			#no additional conditions, retries ensure the state of
			#current object with us matches remote before updateing

			_consistency_checks = {} # consistency between local copy and that on the db, to check concurrent updates
			#all the fields that we are supposed to update
			for _k, _v in _update_query.items():
				if(_k[0] == "$"): # it should start with $
					for p, q in _v.items():
						#consistency checks of the current document
						#to that exists in db
						existing_attr_val = self._original_doc.get(p, _OBJ_END_)
						if(existing_attr_val != _OBJ_END_):
							_consistency_checks.update({p: existing_attr_val})

			#override with any given conditions
			_query.update(_consistency_checks)
			#update with more given conditions
			if(more_conditions):
				_consistency_checks.update(more_conditions)

			#get the shard where current object is
			primary_shard_key = self._original_doc[cls._shard_key_]
			primary_collection_shard = cls.get_collection(
				primary_shard_key
			)
			#query and update the document
			IS_DEV and MONGO_DEBUG_LEVEL > 1 and LOG_SERVER("MONGO", description="update before and query",
				model=cls.__name__, collection=COLLECTION_NAME(primary_collection_shard), query="_query", original_doc=str(self._original_doc), update_query=str(_update_query)
			)
			updated_doc = primary_collection_shard.find_one_and_update(
				_query,
				_update_query,
				return_document=ReturnDocument.AFTER,
				**kwargs
			)
			IS_DEV and MONGO_DEBUG_LEVEL > 1 and LOG_SERVER("MONGO", description="after update",
				model=cls.__name__, collection=COLLECTION_NAME(primary_collection_shard),
				updated_doc=str(updated_doc)
			)

			if(updated_doc):
				break # no retry again , all is good

			#update was unsuccessful
			#is this concurrent update by someone else?
			#is this because of more conditions given by user?
			#1. fetch from db again
			_doc_in_db = primary_collection_shard.find_one(self.pk())
			if(not _doc_in_db): # moved out of shard or pk changed
				return False
			#2. update our local copy
			self.reinitialize_from_doc(_doc_in_db)
			can_retry = False
			#3. check if basic consistency checks failed
			for _k, _v in _consistency_checks.items():
				remote_db_val = get_by_key_list(self._original_doc, *_k.split("."))
				if(_v != remote_db_val):
					can_retry = True # local copy consistency check has failed, retry again
					LOG_WARN("MONGO", description="concurrent update",
						model=cls.__name__,
						collection=COLLECTION_NAME(primary_collection_shard),
						_query=_query,
						remote_value=remote_db_val,
						local_value=_v,
						_key=_k
					)

			if(not can_retry):
				return False

		#propage the updates to secondary shards
		cls.propagate_update_to_secondary_shards(
			self._original_doc, # latest from db
			updated_doc
		)

		cls._trigger_event(EVENT_MONGO_AFTER_UPDATE, self._original_doc, updated_doc)

		#reset all values
		self.reinitialize_from_doc(updated_doc)
		#waint on threads
		
		#triggered after update event
		cls._trigger_event(EVENT_AFTER_UPDATE, self)
		return True

	@classmethod
	def query(cls,
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

		#normalize sort param to [('attr_name', ASCENDING/DESCENDING)...]
		if(sort):
			if(not isinstance(sort, list)):
				sort = [sort]
			_sort_keys = []
			for _sort_key_tuple in sort:
				if(not isinstance(_sort_key_tuple, tuple)):
					_sort_keys.append((cls._attrs_to_name[_sort_key_tuple], 1))
				else:
					_sort_keys.append((cls._attrs_to_name[_sort_key_tuple[0]], _sort_key_tuple[1]))
			sort = _sort_keys

		# collection: [_query,...]
		collections_to_query = {}
		for _query in queries:
			is_querying_on_secondary_shard = False
			if(not force_primary):
				#try if we can query it on secondary shards
				for secondary_shard_key_name, _shard in cls._secondary_shards_.items():
					secondary_collection_shards = _shard._Model_.get_collections_to_query(_query, sort)
					if(not secondary_collection_shards):
						continue
					is_querying_on_secondary_shard = True
					for collection_shard in secondary_collection_shards:
						_key = id(collection_shard)
						shard_and_queries = collections_to_query.get(_key)
						if(shard_and_queries == None):
							collections_to_query[_key] \
								= shard_and_queries \
								= (collection_shard, [], _shard._Model_)
						shard_and_queries[1].append(_query)

					break # important

			if(not is_querying_on_secondary_shard):
				#query on the primary shard
				collection_shards = cls.get_collections_to_query(_query, sort) or []
				for collection_shard in collection_shards:
					_key = id(collection_shard)
					shard_and_queries = collections_to_query.get(_key)
					if(shard_and_queries == None):
						collections_to_query[_key]\
							= shard_and_queries \
							= (collection_shard, [], cls)
					shard_and_queries[1].append(_query)

		#if we did not find any possible shard to query
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

		#does local sorting on results from multiple query iterators using a heap
		class MultiCollectionQueryResult:
			query_result_iters = None
			query_count_iters = None
			buffer = None

			def __init__(self):
				self.query_result_iters = []
				self.query_count_iters = []

			def add(self, query_result_iter, query_count_iter=None):
				self.query_result_iters.append(query_result_iter)
				if(query_count_iter):
					self.query_count_iters.append(query_count_iter)

			def count(self):
				ret = 0
				for count_func in self.query_count_iters:
					ret += count_func()
				return ret

			def push_query_iter_into_heap(self, _query_result_iter):
				try:
					next_value = next(_query_result_iter)
					heapq.heappush(self.buffer, (SortKey(next_value), _query_result_iter, next_value))
				except StopIteration:
					pass

			def __iter__(self):
				if(len(self.query_result_iters) == 1):
					return self.query_result_iters[0]
				#else we sort results for each if there is a sorting key given
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

			IS_DEV and MONGO_DEBUG_LEVEL > 1 and LOG_SERVER("MONGO",
				description="querying", model=_Model.__name__, collection=COLLECTION_NAME(_collection),
				query=str(_query), sort=str(sort), offset=str(offset), limit=str(limit)
			)

			ret = _collection.find(_query, **kwargs)
			if(sort): # from global arg
				ret = ret.sort(sort)
			if(offset): # from global arg
				ret = ret.skip(offset)
			if(limit):
				ret = ret.limit(limit)


			# we queried from the secondary shard, will not have all fields
			if(_Model._is_secondary_shard):
				# do a requery to fetch full document
				#TODO: make it batch wise fetch
				def batched_requery_iter(ret, n=200):
					for docs in batched_iter(ret, n):
						#use this to put them in the same order after or query
						_ids_order = [_doc["_id"] for _doc in docs]

						if(cls._shard_key_ != "_id"):
							_query = {
								"$or": [
									{cls._shard_key_: _doc[cls._shard_key_], "_id": _doc["_id"]} for _doc in docs
								]
							}
						else:
							_query = {"$or": [{"_id": _doc["_id"]} for _doc in docs]}

						IS_DEV and MONGO_DEBUG_LEVEL > 1 and LOG_SERVER("MONGO", description="requerying", model=cls.__name__, query=str(_query))
						_requeried_from_primary = {_doc._id: _doc for _doc in cls.query(_query, force_primary=True)}
						for _id in _ids_order:
							item = _requeried_from_primary.get(_id)
							if(not item):
								IS_DEV and MONGO_DEBUG_LEVEL > 1 and LOG_SERVER("MONGO", description="missing from primary", model=cls.__name__, _id=str(_id))
								continue
							yield item
				ret = batched_requery_iter(ret)
			else:
				ret = map(cls.get_instance_from_document, ret)

			multi_collection_query_result.add(ret, lambda: count_documents(_collection, _query, offset, limit))

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
					"MONGO", description="Cannot use offset when query spans multiple collections shards, improve your query",
					model=_Model.__name__
				)

			thread = Thread(
				target=query_collection,
				args=(_Model, _collection_shard, new_query),
				kwargs={"offset": offset}
			)
			thread.start()
			threads.append(thread)
							


		#wait on sub queries to finish
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

	def commit(self, force=False, ignore_exceptions=None):
		cls = self.__class__
		committed = False

		if(self.__is_new): # try inserting
			cls._trigger_event(EVENT_BEFORE_CREATE, self)
			if(not self._set_query_updates):
				return self # nothing to update
			shard_key_name = cls._shard_key_
			if(shard_key_name == "_id" and self._id == None):
				raise Exception("Need to sepcify _id when sharded by _id")

			_collection_shard = cls.get_collection(
				str(getattr(self, shard_key_name))
			)
			IS_DEV and MONGO_DEBUG_LEVEL > 1 and LOG_SERVER("MONGO", description="new object values",
				model=cls.__name__, collection=COLLECTION_NAME(_collection_shard), set_query=str(self._set_query_updates)
			)
			try:
				#find which shard we should insert to and insert into that
				self._insert_result = _collection_shard.insert_one(
					self._set_query_updates
				)

				self._id = self._set_query_updates["_id"] = self._insert_result.inserted_id

				# update secondary shards now
				cls.propagate_update_to_secondary_shards({}, self._set_query_updates)

				#set _id updates
				committed = True
				# set original doc and custom dict and set fields
				#copy the dict to another
				self.reinitialize_from_doc(dict(self._set_query_updates))
				#hook to do something for newly created db entries
				cls._trigger_event(EVENT_AFTER_CREATE, self)

			except DuplicateKeyError as ex:

				if(ignore_exceptions and DuplicateKeyError in ignore_exceptions):
					return None

				if(not force):
					raise(ex) # re reaise

				#get original doc from mongo shard
				# and update any other fields
				self.reinitialize_from_doc(
					_collection_shard.find_one(self.pk())
				)
				IS_DEV and MONGO_DEBUG_LEVEL > 1 and LOG_SERVER("MONGO", description="created a duplicate, refetching and updating",
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
				return self # nothing to update

			is_committed = self.update(_update_query)
			if(not is_committed):
				raise Exception("MONGO: Couldn't commit, either a concurrent update modified this or query has issues", self.pk(), _update_query)

		#clear and reset pk to new
		self.pk(renew=True)
		#clear
		self._set_query_updates.clear()
		self._other_query_updates.clear()

		return self

	def delete(self):
		_Model = self.__class__
		if(_Model._is_secondary_shard):
			raise Exception("MONGO: Cannot delete secondary shard item")
		#Note: when we know the _id and the shard we basically delete them by _id

		_Model._trigger_event(EVENT_BEFORE_DELETE, self)

		_Model.propagate_update_to_secondary_shards(self._original_doc, {})

		#find which pimary shard it belongs to and delete it there
		collection_shard = _Model.get_collection(self._original_doc[_Model._shard_key_])
		_delete_query = {"_id": self._id}
		collection_shard.delete_one(_delete_query)
		IS_DEV and MONGO_DEBUG_LEVEL > 1 and LOG_SERVER("MONGO", description="deleting from primary",
			model=_Model.__name__, collection=COLLECTION_NAME(collection_shard), delete_query=str(_delete_query)
		)
		if(_Model.__cache__):
			_Model.__cache__.delete(self.pk_tuple())

		_Model._trigger_event(EVENT_AFTER_DELETE, self)


	@classmethod
	def remove_from_cache(cls, *_objs):
		if(not cls.__cache__):
			return
		for _obj in _objs:
			cls.__cache__.delete(_obj.pk_tuple())


class SecondaryShard:
	attrs = None
	indexes = None
	collection_name = None
	_Model_ = None

	def __init__(self):
		self.attrs = {}
		self.indexes = []


#control
class CollectionTracker(Model):
	_db_name_ = "control"
	_collection_name_ = "collection_tracker"

	_id = Attribute(str) # db_name__collection_name
	db_nodes = Attribute(list)
	is_primary_shard = Attribute(int)
	primary_shard_key = Attribute(str)
	secondary_shard_keys = Attribute(list)
	pk_attrs = Attribute(list)
	attrs = Attribute(list)

#tags shard keys to the attributes and use it when intializing the model
def SHARD_BY(primary=None, secondary=None):
	if(primary and isinstance(primary, Attribute)):
		primary.is_primary_shard_key = True
	if(secondary):
		for secondary_shard_attr in secondary:
			if(secondary_shard_attr and isinstance(secondary_shard_attr, Attribute)):
				secondary_shard_attr.is_secondary_shard_key = True

#tags indexes_to_create to attributes and retrieve them when initializing the model
def INDEX(*indexes):
	for index_key_set in indexes:
		first_key_of_index_key_set = index_key_set[0]
		if(isinstance(first_key_of_index_key_set, tuple)): # when it has a sorting order as second key
			first_key_of_index_key_set = first_key_of_index_key_set[0]

		first_key_of_index_key_set._indexes_to_create = getattr(first_key_of_index_key_set, "_indexes_to_create", [])
		first_key_of_index_key_set._indexes_to_create.append(index_key_set)


'''
Control Jobs
- 	every time a job is completed, check if it has parent,
	goto parent and check if all child jobs are finished and mark it complete
'''
class ControlJobs(Model):
	_db_name_ = "control"
	_collection_name_ = "jobs_v2"


	##JOB TYPES
	RESHARD = 0
	CREATE_SECONDARY_SHARD = 1
	ADD_NEW_ATTRIBUTES_TO_SECONDARY_SHARD = 2
	

	##attributes
	parent__id = Attribute(str)
	num_child_jobs = Attribute(int, default=0) # just for reconcillation
	_type = Attribute(int) # reshard=>0, create_secondary_index=>1, create_primary_index=>2
	db = Attribute(str)
	collection = Attribute(str)
	uid = Attribute(str) # unique identifier not to duplicate jobs
	status = Attribute(int, default=0) # 0=>not started, 1=>progress, 2=>completed

	worker_id = Attribute(str) # worker id
	worker_should_update_within_ms = Attribute(int, default=60000) # contract that worker should update every few millis
	#work data
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


_cached_mongo_clients = {}
#DatabaseNode is basically a server
class DatabaseNode:

	_cached_pymongo_collections = {}
	#mongo connection
	mongo_connection = None
	hosts = None
	replica_set = None
	username = None
	password = None
	#default db name
	db_name = None

	#this initializes the tables in all nodes
	def __init__(self, hosts=None, replica_set=None, username=None, password=None, db_name=None):
		if(isinstance(hosts, str)):
			hosts = [hosts]
		hosts.sort()
		self.hosts = hosts
		self.replica_set = replica_set
		self.username = username
		self.db_name = db_name
		# use list of hosts, replicaset and username as cache key
		# so as not to create multiple clients
		_mongo_clients_cache_key = (",".join(hosts), replica_set or "" , username or "")
		self.mongo_connection = _cached_mongo_clients.get(_mongo_clients_cache_key)
		if(not self.mongo_connection):
			self.mongo_connection = _cached_mongo_clients[_mongo_clients_cache_key] = MongoClient(host=hosts, replicaSet=replica_set, username=username, password=password)


	def to_dict(self):
		return {
			"hosts": self.hosts,
			"replica_set": self.replica_set,
			"username": self.username,
			"db_name": self.db_name
		}

	#returns collection on the given DatabaseNode
	def get_collection(self, Model):
		ret = self._cached_pymongo_collections.get(Model)
		if(not ret):
			self._cached_pymongo_collections[Model] \
				= ret \
				= self.mongo_connection[Model._db_name_][Model._collection_name_with_shard_]
		return ret


def initialize_model(Model):

	if(isinstance(Model, list)):
		for _m in Model:
			initialize_model(_m)
		return

	#defaults, do not change the code below
	Model._shard_key_ = getattr(Model, "_shard_key_", "_id")
	Model._secondary_shards_ = {}
	Model._indexes_ = getattr(Model, "_indexes_", [("_id",)])
	#create a default cache
	Model.__cache__ = getattr(Model, "_cache_", ExpiringCache(10000))

	# temp usage _id_attr
	_id_attr = Attribute(str) # default is of type objectId
	Model._attrs = _model_attrs = {"_id": _id_attr}
	Model._pk_attrs = None
	'''it's used for translating attr objects/name to string names'''
	Model._attrs_to_name = attrs_to_name = {_id_attr: '_id', '_id': '_id'}
	is_sharding_enabled = False
	for k, v in Model.__dict__.items():
		if(isinstance(v, Attribute)):
			_model_attrs[k] = v
			attrs_to_name[v] = k
			# dumb , but it's one time thing
			# and also helps converting if any given attributes as strings
			attrs_to_name[k] = k

			#check if it has any shard, then extract indexes, shard key tagged to attributes
			if(not Model._is_secondary_shard): # very important check
				#check indexes_to_create
				_indexes_to_create = getattr(v, "_indexes_to_create", None)
				if(_indexes_to_create):
					delattr(v, "_indexes_to_create") # because we don't need it again after first time
					Model._indexes_.extend(_indexes_to_create)

				is_primary_shard_key = getattr(v, "is_primary_shard_key", False)
				is_secondary_shard_key = getattr(v, "is_secondary_shard_key", False)
				if(is_primary_shard_key is True):
					Model._shard_key_ = k
					delattr(v, "is_primary_shard_key") # because we don't need it again after first time and won't rewrite secondary shard keys
				elif(is_secondary_shard_key is True):
					Model._secondary_shards_[k] = SecondaryShard()
					delattr(v, "is_secondary_shard_key") # because we don't need it again after first time

				is_sharding_enabled = is_sharding_enabled or (is_primary_shard_key or is_secondary_shard_key)

	IS_DEV and MONGO_DEBUG_LEVEL > 1 and print("\n\n#MONGO: Initializing Model", Model,
		Model._indexes_, Model._shard_key_, Model._secondary_shards_
	)

	Model._event_listeners_ = getattr(Model, "_event_listeners_", {})
	#some default event
	Model.on(EVENT_BEFORE_UPDATE, lambda obj: obj.before_update())

	_pymongo_indexes_to_create = []
	Model._pk_is_unique_index = False
	for _index in Model._indexes_:
		mongo_index_args = {}
		pymongo_index = []
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
				mongo_index_args = _a
				continue

			pymongo_index.append((_attr_name, _ordering))

		#convert to tuple again
		pymongo_index = tuple(pymongo_index)
		is_unique_index = mongo_index_args.get("unique", True) is not False

		_index_shard_key = pymongo_index[0][0] # shard key is the first mentioned key in index

		if(Model._shard_key_ != _index_shard_key):
			#secondary shard tables
			_secondary_shard = Model._secondary_shards_.get(_index_shard_key)

			if(_secondary_shard):
				for _attr_name, _ordering in pymongo_index:
					_secondary_shard.attrs[_attr_name] = getattr(
						Model,
						_attr_name
					)
				#create _index_ for secondary shards
				_secondary_shard_index = list(pymongo_index)
				_secondary_shard_index.append(mongo_index_args)
				_secondary_shard.indexes.append(
					tuple(_secondary_shard_index)
				)

		ignore_index_creation = False
		#check if index it belongs to primary shard
		if(	Model._shard_key_ == _index_shard_key):
			#index belong to primary shard
			if(not Model._pk_attrs
				or (is_unique_index and not Model._pk_is_unique_index)
			):
				Model._pk_is_unique_index = is_unique_index
				Model._pk_attrs = _pk_attrs = OrderedDict()
				for i in pymongo_index: # first unique index
					_pk_attrs[i[0]] = 1
		elif(_index_shard_key in Model._secondary_shards_):
			#this index goes into the secondary table`
			#Ex: Table A is sharded by x then we created indexes that has x on A_shard_x table only
			ignore_index_creation = True

		#check for indexes to ignore
		if(pymongo_index[0][0] == "_id"):
			ignore_index_creation = True

		#create the actual index
		if(not ignore_index_creation):
			_pymongo_indexes_to_create.append((pymongo_index, mongo_index_args))


	if(not Model._pk_attrs): # create default _pk_attrs
		Model._pk_attrs = OrderedDict(_id=True)

	# set collection name to include shard_keys

	Model._collection_name_with_shard_ = Model._collection_name_
	#append shard id to table name if it's secondary shard or primary and sharding enabled
	if(Model._is_secondary_shard or is_sharding_enabled):
		Model._collection_name_with_shard_ += "_shard_" + Model._shard_key_


	##find tracking nodes
	Model._collection_tracker_key_ = "%s__%s"%(Model._db_name_, Model._collection_name_with_shard_)

	IS_DEV and MONGO_DEBUG_LEVEL > 1 and print("\n\n#MONGO collection tracker key", Model._collection_tracker_key_)

	if(Model not in [CollectionTracker, ControlJobs]):
		collection_tracker = CollectionTracker.get(Model._collection_tracker_key_)
		if(not collection_tracker
			or not collection_tracker.db_nodes
			or not collection_tracker.primary_shard_key
		):
			print(
				"\n\n#MONGOORM_IMPORTANT_INFO : "
				"Collection tracker entry not present for '%s'.."
				"creating table in control node for this time in database '%s'. "
				"You may want to talk to dba to move to a proper node"%(Model.__name__, Model._db_name_)
			)

			#check if the Model already has _db_nodes_
			db_nodes = getattr(Model, "_db_nodes_", None) or [
						{
							"hosts": init_db_nodes.hosts,
							"replica_set": init_db_nodes.replica_set,
							"username": init_db_nodes.username,
							"db_name": Model._db_name_
						} for init_db_nodes in CollectionTracker._db_nodes_
					]

			collection_tracker = CollectionTracker(
				_id=Model._collection_tracker_key_,
				db_nodes=db_nodes,
				is_primary_shard=1 if not Model._is_secondary_shard else 0,
				primary_shard_key=Model._shard_key_,
				secondary_shard_keys=list(Model._secondary_shards_.keys()),
				pk_attrs=list(Model._pk_attrs.keys()),
				attrs=list(Model._attrs.keys())
			).commit(force=True)

		#version < 101 support
		if(not collection_tracker.attrs):
			collection_tracker.attrs = list(Model._attrs.keys())
			collection_tracker.commit()

		Model._db_nodes_ = tuple(DatabaseNode(**_db_node) for _db_node in collection_tracker.db_nodes)
		#TODO: find new secondary shards by comparing collection_tracker.secondary_shard_keys, Model._secondary_shards_.keys()
		#and create a job to create and reindex all data to secondary index
		if(not Model._is_secondary_shard
			and Model._secondary_shards_
			and collection_tracker.primary_shard_key != Model._shard_key_
		):
			raise Exception("\n\n#MONGO_EXCEPTION: Primary shard key changed for ", Model, "It has secondary shards, that point to primary shard key. You will have to drop shard secondary shards and force reindex everything again")


		if(not Model._is_secondary_shard):
			#create diff jobs, check if new shard is indicated
			to_create_secondary_shard_key, to_delete_secondary_shard_key = list_diff2(
				list(Model._secondary_shards_.keys()),
				collection_tracker.secondary_shard_keys
			)
			for shard_key_name in to_create_secondary_shard_key:
				try:
					ControlJobs(
						_id=str(cur_ms()),
						db=Model._db_name_,
						collection=Model._collection_name_with_shard_,
						_type=ControlJobs.CREATE_SECONDARY_SHARD,
						uid=shard_key_name
					).commit()
					IS_DEV and MONGO_DEBUG_LEVEL > 1 and print("\n\n#MONGO: create a control job to create secondary shard", Model, shard_key_name)
				except DuplicateKeyError:
					print("Secondary shard not created yet, queries on this shard won't give any results, please propagate all data to this shard", Model, shard_key_name)

		if(Model._is_secondary_shard):
			to_add_attrs, to_remove_attrs = list_diff2(
				list(Model._attrs.keys()),
				collection_tracker.attrs
			)
			if(to_add_attrs or to_remove_attrs):
				try:
					ControlJobs(
						_id=str(cur_ms()),
						db=Model._db_name_,
						collection=Model._collection_name_with_shard_,
						_type=ControlJobs.ADD_NEW_ATTRIBUTES_TO_SECONDARY_SHARD,
						uid="",
						data={"to_add_attrs": to_add_attrs, "to_remove_attrs": to_remove_attrs}
					).commit()
					IS_DEV and MONGO_DEBUG_LEVEL > 1 \
						and print("\n\n#MONGO: created a control job to change attributes", Model, to_add_attrs, to_remove_attrs)
					raise Exception("Attributes have been changed on the Secondary shard, you need to backfill the data by setting _is_migrating_ = True")
				except DuplicateKeyError:
					LOG_ERROR(
						"MONGO",
						desc="There is already a pending job to update attributes, it needs to be finished ",
						model=Model.__name__
					)
					Model._attrs_to_backfill_ = {}
					for _attr_name in to_add_attrs:
						Model._attrs_to_backfill_[_attr_name] = Model._attrs[_attr_name]

		#check if new attribute added to secondary shard


	#TODO: create or delete index using control jobs
	for pymongo_index, additional_mongo_index_args in _pymongo_indexes_to_create:
		mongo_index_args = {"unique": True}
		mongo_index_args.update(**additional_mongo_index_args)

		IS_DEV and MONGO_DEBUG_LEVEL > 1 and print("\n\n#MONGO: creating_indexes", Model, pymongo_index, mongo_index_args)
		#in each node create indexes
		for db_node in Model._db_nodes_:
			db_node.get_collection(Model).create_index(pymongo_index, **mongo_index_args)
	
	#create secondary shards
	for _secondary_shard_key, _secondary_shard in Model._secondary_shards_.items():
		if(not Model._pk_is_unique_index):
			raise Exception("Cannot have secondary shard keys for non unique indexes! %s"%(Model,))

		class_attrs = {
			"_indexes_": _secondary_shard.indexes,
			"_shard_key_": _secondary_shard_key,
			"_collection_name_": Model._collection_name_,
			"_is_secondary_shard": True,
			"_db_nodes_": None
		}

		# add primary shard attributes of the main class to
		# secondary shards too, to identify the original document shard
		_secondary_shard.attrs[Model._shard_key_] = getattr(Model, Model._shard_key_)

		secondary_id_attr = getattr(Model, '_id', None)
		if(secondary_id_attr):
			_secondary_shard.attrs['_id'] = secondary_id_attr

		class_attrs.update(_secondary_shard.attrs)
		#also include the primary key of the primary shard
		#into secondary shards

		_secondary_shard._Model_ = type(
			"%s_%s"%(Model.__name__, _secondary_shard_key.upper()),
			(Model,),
			class_attrs
		)
		#initialize this new model
		initialize_model(_secondary_shard._Model_)


#initialize control Tr
#initialize all other nodes
def initialize_mongo(init_db_nodes, default_db_name=None):

	default_db_name = default_db_name or "temp_db"
	#initialize control db
	if(isinstance(init_db_nodes, dict)):
		init_db_nodes = DatabaseNode(**init_db_nodes)
	#check connection to mongodb
	init_db_nodes.mongo_connection.server_info()

	#initialize control db
	CollectionTracker._db_nodes_ = [init_db_nodes]
	ControlJobs._db_nodes_ = [init_db_nodes]
	initialize_model([CollectionTracker, ControlJobs])

	#set default db name for each class
	for cls in Model.__subclasses__():
		if(not getattr(cls, "_db_name_", None)):
			cls._db_name_ = default_db_name
		initialize_model(cls)



# Initially intended to be < 500 line and crazy scalable like
# add more nodes and it automatically shards
# - Complete client side sharding, control database,
# - You can Configure mongo as a sharded cluster.Just pass the configuration server to
# - init_mongo_cluster({"uri": "mongodb://localhost:27017", "db_name": "yourdb"})

#reshard-restart-reshard

#when we say collection it it's actual pymongo collection on a perticular db node
