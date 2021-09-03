import heapq
import gevent
import types
import pymongo
from bson.objectid import ObjectId
from itertools import chain
from collections import OrderedDict
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo import ReturnDocument, ReadPreference
from .common_funcs_and_datastructures import jump_hash, ExpiringCache, cur_ms, list_diff2, batched_iter, run_in_partioned_queues
from .config import DEBUG_LEVEL as BLASTER_DEBUG_LEVEL, IS_DEV

_VERSION_ = 100
MONGO_DEBUG_LEVEL = BLASTER_DEBUG_LEVEL
EVENT_BEFORE_DELETE = -2
EVENT_AFTER_DELETE = -1
EVENT_BEFORE_UPDATE = 1
EVENT_AFTER_UPDATE = 2
EVENT_BEFORE_CREATE = 3
EVENT_AFTER_CREATE = 4

#just a random constant object to check for non existent keys without catching KeyNotExist exception
_OBJ_END_ = object()
#given a list of [(filter, iter), ....] , flattens them

class Attribute(object):
	_type = None

	def __init__(self, _type, **kwargs):
		self._type = _type
		self.__dict__.update(kwargs)

class Model(object):
	#class level
	_attrs = None
	_pk_attrs = None

	__cache__ = ExpiringCache(10000)
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
	_is_secondary_shard = False # used to identify if it's a primary or secondary shard

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
	def get(cls, _pk=None, use_the_cache=True, **kwargs):
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
				if(use_the_cache and cls.__cache__):
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
				self._set_query_updates[path] = this

			def pop(this, i=None):
				if(i == None):
					i = 1
					ret = super(ListObj, this).pop()
				else:
					ret = super(ListObj, this).pop(0)
					i = -1
				if(not _initializing):
					_pop = self._other_query_updates.get("$pop")
					if(_pop == None):
						_pop = self._other_query_updates["$pop"] = {}
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
				cur_value = getattr(self, k, _OBJ_END_)
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
	def get_collections_from_shard_key(cls, shard_key):
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

		elif(shard_key == None):
			IS_DEV and MONGO_DEBUG_LEVEL > 1 and print(
				"None type shard keys will be ignored and wont be available for query! Let me know feedback!"
			)
			return []

		raise Exception("Shard keys must be integers or strings or {'$in': [VALUES]}: got %s"%(str(shard_key),))

	@classmethod
	def get_collections_from_query(cls, _query):
		shard_key = _query.get(cls._shard_key_)

		if(shard_key):
			return cls.get_collections_from_shard_key(shard_key)

		#could'nt finds shards, return all shards
		return cls.get_collection_on_all_db_nodes()


	@classmethod
	def propagate_update_to_secondary_shards(cls, before_doc, after_doc):
		#CONS of this multi table sharding:
		#- Primary index updated but secondary constraint can fail
		#- in that case, the data exists in primary shard, but it's not valid data
		#TODO:
		# idea1: move this to transactions if we have secondary shards ?
		# idea2: revert the original update
		#update all secondary shards
		for shard_key, shard in cls._secondary_shards_.items():
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
			if(is_shard_key_changed):
				# belonged to a shard delete it
				if(_secondary_collection and _before_id != None):
					_secondary_collection.find_one_and_delete(_secondary_pk)
					IS_DEV and MONGO_DEBUG_LEVEL > 1 and print("#MONGO: deleting from secondary",
						_secondary_pk, secondary_Model._collection_name_with_shard_
					)


				if(_after_shard_key != None and after_doc["_id"]):
					for attr, _attr_obj in shard.attributes.items():
						_secondary_values_to_set[attr] = after_doc.get(attr)
					#just force set _id again
					_secondary_values_to_set["_id"] = after_doc["_id"]
					#find new shard
					_secondary_collection = secondary_Model.get_collection(
						_secondary_values_to_set[shard_key] # same as after_shard_key
					)
					#insert new one
					IS_DEV and MONGO_DEBUG_LEVEL > 1 and print("#MONGO: deleting and inserting new secondary",
						_secondary_pk, _secondary_values_to_set
					)
					_secondary_collection.insert_one(_secondary_values_to_set)
				
			else:
				_secondary_values_to_unset = {}
				#if someone else has modified
				#old values check in pk will not overwrite it
				for attr in shard.attributes:
					old_value = before_doc.get(attr, _OBJ_END_)
					new_value = after_doc.get(attr, _OBJ_END_)
					if(old_value != new_value):
						if(new_value == _OBJ_END_): # removed after update
							_secondary_values_to_unset[attr] = new_value # unset this value as it doesn't exist anymore
						else:
							_secondary_values_to_set[attr] = new_value
						#also add this for query for a little bit of more consistency check in query
						if(old_value != _OBJ_END_): # means it exists
							_secondary_pk[attr] = old_value

				if(_secondary_values_to_set or _secondary_values_to_unset):
					_secondary_updates = {}
					if(_secondary_values_to_set):
						_secondary_updates["$set"] = _secondary_values_to_set
					if(_secondary_values_to_unset):
						_secondary_updates["$unset"] = _secondary_values_to_unset
					IS_DEV and MONGO_DEBUG_LEVEL > 1 and print("#MONGO: updating secondary", _secondary_pk, _secondary_updates)
					#find that doc and update
					seconday_shard_update_return_value = _secondary_collection.find_one_and_update(
						_secondary_pk,
						_secondary_updates
					)

					if(not seconday_shard_update_return_value):
						print("#MONGO:WARNING!! secondary couldn't be propagated, probably due to concurrent update",
								cls, _secondary_pk, _secondary_updates
							)

	#_add_query is more query other than pk
	# for example, you can say update only when someother field > 0

	def update(self, _update_query, more_conditions=None, **kwargs):
		cls = self.__class__
		
		cls._trigger_event(EVENT_BEFORE_UPDATE, self)

		updated_doc = None
		_query = dict(self.pk())
		if(more_conditions == None):
			more_conditions = {}

		#TODO: add existing values to more_conditions to be
		# super consistent on updates and probably
		# raise concurrent update exception if not modified
		_query.update(more_conditions)

		#get the shard where current object is
		primary_shard_key = getattr(self, cls._shard_key_)
		primary_collection_shard = cls.get_collection(
			primary_shard_key
		)
		#query and update the document
		IS_DEV and MONGO_DEBUG_LEVEL > 1 and print(
			"#MONGO: update before and query",
			cls, _query, self._original_doc, _update_query
		)
		updated_doc = primary_collection_shard.find_one_and_update(
			_query,
			_update_query,
			return_document=ReturnDocument.AFTER,
			**kwargs
		)
		IS_DEV and MONGO_DEBUG_LEVEL > 1 and print("#MONGO: after update::", cls, updated_doc)

		if(not updated_doc):
			#update was unsuccessful
			return False

		#propage the updates to secondary shards
		run_in_partioned_queues(
			primary_shard_key, cls.propagate_update_to_secondary_shards, self._original_doc, updated_doc
		)

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
		_no_requery=False,
		read_preference=ReadPreference.PRIMARY,
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

		#map of collection: [_query]
		collections_to_query = {}
		for _query in queries:
			not_possible_in_the_secondary_shard = True
			shard_key = _query.get(cls._shard_key_, _OBJ_END_)
			if(shard_key == _OBJ_END_):
				# querying on secondary shards as main shard key doesn't exist
				# so try each secondary shard and test if we can query on it
				# otherwise we go with primary shard
				for secondary_shard_key_name, _shard in cls._secondary_shards_.items():
					secondary_shard_key = _query.get(secondary_shard_key_name, _OBJ_END_)
					if(secondary_shard_key == _OBJ_END_):
						continue
					#check if we can construct a secondary shard query
					#extract keys from the query to create new secondary shard query
					not_possible_in_the_secondary_shard = False
					if(sort):
						#check if we can perform sort on the secondary index
						for sort_key, sort_direction in sort:
							if(sort_key not in _shard.attributes):
								#cannot perform sort
								not_possible_in_the_secondary_shard = True

					secondary_shard_query = {}
					for query_attr_name, query_attr_val in _query.items():
						attr_exists_in_shard = _shard.attributes.get(query_attr_name, _OBJ_END_)
						if(attr_exists_in_shard == _OBJ_END_):
							not_possible_in_the_secondary_shard = True
							break
						secondary_shard_query[query_attr_name] = query_attr_val

					if(not_possible_in_the_secondary_shard):
						#try next secondary shard
						continue

					secondary_collection_shards = _shard._Model_.get_collections_from_shard_key(secondary_shard_key)
					for collection_shard in secondary_collection_shards:
						_key = id(collection_shard)
						shard_and_queries = collections_to_query.get(_key)
						if(shard_and_queries == None):
							collections_to_query[_key] \
								= shard_and_queries \
								= (collection_shard, [], _shard._Model_)
						shard_and_queries[1].append(secondary_shard_query)
					break

			if(shard_key != _OBJ_END_ and not_possible_in_the_secondary_shard):
				#query on the primary shard
				collection_shards = cls.get_collections_from_shard_key(shard_key)
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

		def query_collection(_Model, _collection, _query, offset=None):

			IS_DEV and MONGO_DEBUG_LEVEL > 1 and print(
				"#MONGO: querying", _Model, _collection, _query, sort, offset, limit
			)

			ret = _collection.find(_query, **kwargs)
			if(sort): # from global arg
				ret = ret.sort(sort)
			if(offset): # from global arg
				ret = ret.skip(offset)
			if(limit):
				ret = ret.limit(limit)


			# we queried from the secondary shard, will not have all fields
			if(_Model._is_secondary_shard and not _no_requery):
				# do a requery to fetch full document
				#TODO: make it batch wise fetch
				def batched_requery_iter(ret, n=200):
					for docs in batched_iter(ret, n):
						_ids_order = [_doc["_id"] for _doc in docs]
						_query = {"$or": [dict(cls.pk_from_doc(_doc)) for _doc in docs]}

						IS_DEV and MONGO_DEBUG_LEVEL > 1 and print("#MONGO: requerying", cls, _query)
						_requeried_from_primary = {_doc._id: _doc for _doc in cls.query(_query)}
						for _id in _ids_order:
							item = _requeried_from_primary.get(_id)
							if(not item):
								IS_DEV and MONGO_DEBUG_LEVEL > 1 and print("#MONGO: WARNING: missing from primary", _id)
								continue
							yield item
				ret = batched_requery_iter(ret)
			else:
				ret = map(cls.get_instance_from_document, ret)

			return ret, lambda: count_documents(_collection, _query, offset, limit)

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
				print("Warning: Cannot use offset when query spans multiple collections shards, improve your query")

			thread = gevent.spawn(
				query_collection,
				_Model,
				_collection_shard,
				new_query,
				offset=offset
			)
			threads.append(thread)
							
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


		#waint on threads and return cursors
		multi_collection_query_result = MultiCollectionQueryResult()

		for thread in gevent.joinall(threads):
			multi_collection_query_result.add(thread.value[0], thread.value[1])

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
	def _trigger_event(cls, event, obj):
		handlers = cls._event_listeners_.get(event)
		if(not handlers):
			return
		for handler in handlers:
			handler(obj)

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

			IS_DEV and MONGO_DEBUG_LEVEL > 1 and print("#MONGO: new object values", cls, self._set_query_updates)
			self.__is_new = False
			_collection_shard = cls.get_collection(
				str(getattr(self, shard_key_name))
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
				IS_DEV and MONGO_DEBUG_LEVEL > 1 and print("#MONGO: created a duplicate, refetching and updating",
					self.pk()
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

			self.update(_update_query) # , hint=self.__class__._pk_attrs)


		#clear and reset pk to new
		self.pk(renew=True)
		#clear
		self._set_query_updates.clear()
		self._other_query_updates.clear()

		return self

	def delete(self):
		_Model = self.__class__
		if(_Model._is_secondary_shard):
			raise Exception("Cannot delete secondary shard item")
		#Note: when we know the _id and the shard we basically delete them by _id

		_Model._trigger_event(EVENT_BEFORE_DELETE, self)

		_Model.propagate_update_to_secondary_shards(self._original_doc, {})

		#find which pimary shard it belongs to and delete it there
		collection_shard = _Model.get_collection(getattr(self, _Model._shard_key_))
		_delete_query = {"_id": self._id}
		collection_shard.delete_one(_delete_query)
		IS_DEV and MONGO_DEBUG_LEVEL > 1 and print("#MONGO: deleting from primary",
				_Model, _delete_query
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
	attributes = None
	indexes = None
	collection_name = None
	_Model_ = None

	def __init__(self):
		self.attributes = {}
		self.indexes = []


#control
class CollectionTracker(Model):
	_db_name_ = "control"
	_collection_name_ = "collection_tracker"

	_id = Attribute(str) # db_name__collection_name
	db_nodes = Attribute(list)
	primary_shard_key = Attribute(str)
	secondary_shard_keys = Attribute(list)
	pk_attrs = Attribute(list)


#tags shard keys to the attributes and use it when intializing the model
def SHARD_BY(primary=None, secondary=None, Model=None):
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
	_collection_name_ = "jobs"

	parent__id = Attribute(str)
	num_child_jobs = Attribute(int, default=0) # just for reconcillation
	_type = Attribute(int) # reshard=>0, create_secondary_index=>1, create_primary_index=>2
	db = Attribute(str)
	collection = Attribute(str)
	status = Attribute(int, default=0) # 0=>not started, 1=>progress, 2=>completed
	uid = Attribute(str) # unique identifier not to duplicate jobs

	worker_id = Attribute(str) # worker id
	worker_should_update_within_ms = Attribute(int, default=60000) # contract that worker should update every few millis
	#work data
	data = Attribute(dict)
	data1 = Attribute(dict)
	created_at = Attribute(int, default=cur_ms)
	updated_at = Attribute(int, default=cur_ms)

	INDEX(
		(db, collection, _type, uid),
		(db, collection, _type, status, {"unique": False}),
		(parent__id, {"unique": False})
	)

	SHARD_BY(primary=db)



	##Constants
	CREATE_SECONDARY_SHARD = 1

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

	#defaults
	Model._shard_key_ = getattr(Model, "_shard_key_", "_id")
	Model._secondary_shards_ = {}
	Model._indexes_ = getattr(Model, "_indexes_", [("_id",)])

	# temp usage _id_attr
	_id_attr = Attribute(str) # default is of type objectId
	Model._attrs = _model_attrs = {"_id": _id_attr}
	Model._pk_attrs = None
	'''it's used for translating attr objects/name to string names'''
	Model._attrs_to_name = attrs_to_name = {_id_attr: '_id', '_id': '_id'}
	for k, v in Model.__dict__.items():
		if(isinstance(v, Attribute)):
			_model_attrs[k] = v
			attrs_to_name[v] = k
			# dumb , but it's one time thing
			# and also helps converting if any given attributes as strings
			attrs_to_name[k] = k

			#check if it has any shard, then extract indexes, shard key tagged to attributes
			if(not Model._is_secondary_shard): # very importat check
				#check indexes_to_create
				_indexes_to_create = getattr(v, "_indexes_to_create", None)
				if(_indexes_to_create):
					delattr(v, "_indexes_to_create") # because we don't need it again after first time
					Model._indexes_.extend(_indexes_to_create)

				is_primary_shard_key = getattr(v, "is_primary_shard_key", False)
				is_secondary_shard_key = getattr(v, "is_secondary_shard_key", False)
				if(is_primary_shard_key is True):
					Model._shard_key_ = k
					delattr(v, "is_primary_shard_key") # because we don't need it again after first time
				elif(is_secondary_shard_key is True):
					Model._secondary_shards_[k] = SecondaryShard()
					delattr(v, "is_secondary_shard_key") # because we don't need it again after first time

	IS_DEV and MONGO_DEBUG_LEVEL > 1 and print("#MONGO: Initializing Model", Model,
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
			_seconday_shard = Model._secondary_shards_.get(_index_shard_key)

			if(_seconday_shard):
				for _attr_name, _ordering in pymongo_index:
					_seconday_shard.attributes[_attr_name] = getattr(
						Model,
						_attr_name
					)
				#create _index_ for seconday shards
				_secondary_shard_index = list(pymongo_index)
				_secondary_shard_index.append(mongo_index_args)
				_seconday_shard.indexes.append(
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
	Model._collection_name_with_shard_ = Model._collection_name_ + "_shard_" + Model._shard_key_

	##find tracking nodes
	Model._collection_tracker_key_ = "%s__%s"%(Model._db_name_, Model._collection_name_with_shard_)

	IS_DEV and MONGO_DEBUG_LEVEL > 1 and print("#MONGO collection tracker key", Model._collection_tracker_key_)

	if(Model not in [CollectionTracker, ControlJobs]):
		collection_tracker = CollectionTracker.get(Model._collection_tracker_key_)
		if(not collection_tracker
			or not collection_tracker.db_nodes
			or not collection_tracker.primary_shard_key
			#to support _VERSION_ < 100
			or (not Model._is_secondary_shard and not collection_tracker.pk_attrs) # secondary shard keys are there and not pk_attrs
		):
			print(
				"#MONGOORM_IMPORTANT_INFO : "
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
				primary_shard_key=Model._shard_key_,
				secondary_shard_keys=list(Model._secondary_shards_.keys()),
				pk_attrs=list(Model._pk_attrs.keys())
			).commit(force=True)

		Model._db_nodes_ = [DatabaseNode(**_db_node) for _db_node in collection_tracker.db_nodes]
		#TODO: find new secondary shards by comparing collection_tracker.secondary_shard_keys, Model._secondary_shards_.keys()
		#and create a job to create and reindex all data to secondary index
		if(collection_tracker.primary_shard_key != Model._shard_key_):
			raise Exception("#MONGO_EXCEPTION: Primary shard key changed for ", Model)

		if(not Model._is_secondary_shard and tuple(collection_tracker.pk_attrs) != tuple(Model._pk_attrs.keys())):
			raise Exception("#MONGO_EXCEPTION: primary keys cannot be changed for a primary shard, " +\
					"they are used to retrieve back original documents from secondary shards, if you absolutely " + \
					"have to, reindex all data again", Model
				)

		#create diff jobs
		to_create_secondary_shard_key, to_delete_secondary_shard_key = list_diff2(
			Model._secondary_shards_.keys(),
			collection_tracker.secondary_shard_keys
		)
		for shard_key in to_create_secondary_shard_key:
			try:
				ControlJobs(
					db=Model._db_name_,
					collection=Model._collection_name_with_shard_,
					_type=ControlJobs.CREATE_SECONDARY_SHARD,
					uid=shard_key
				).commit()
				IS_DEV and MONGO_DEBUG_LEVEL > 1 and print("#MONGO: create a control job to create secondary sharding", Model, shard_key)
			except DuplicateKeyError as ex:
				pass

	#TODO: create or delete index using control jobs
	for pymongo_index, additional_mongo_index_args in _pymongo_indexes_to_create:
		mongo_index_args = {"unique": True}
		mongo_index_args.update(**additional_mongo_index_args)

		IS_DEV and MONGO_DEBUG_LEVEL > 1 and print("#MONGO: creating_indexes", Model, pymongo_index, mongo_index_args)
		#in each node create indexes
		for db_node in Model._db_nodes_:
			db_node.get_collection(Model).create_index(pymongo_index, **mongo_index_args)
	
	#create secondary shards
	for _seconday_shard_key, _seconday_shard in Model._secondary_shards_.items():
		if(not Model._pk_is_unique_index):
			raise Exception("Cannot have secondary shard keys for non unique indexes! %s"%(Model,))

		class_attrs = {
			"_indexes_": _seconday_shard.indexes,
			"_shard_key_": _seconday_shard_key,
			"_collection_name_": Model._collection_name_,
			"_is_secondary_shard": True,
			"_db_nodes_": None
		}
		# add primary key attributes of the main class to
		# secondary shards too

		for attr_name in Model._pk_attrs:
			_seconday_shard.attributes[attr_name] = getattr(Model, attr_name)

		secondary_id_attr = getattr(Model, '_id', None)
		if(secondary_id_attr):
			_seconday_shard.attributes['_id'] = secondary_id_attr

		class_attrs.update(_seconday_shard.attributes)
		#also include the primary key of the primary shard
		#into secondary shards

		_seconday_shard._Model_ = type(
			"%s_%s"%(Model.__name__, _seconday_shard_key.upper()),
			(Model,),
			class_attrs
		)
		#initialize this new model
		initialize_model(_seconday_shard._Model_)


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
