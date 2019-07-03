import types
import pymysql
from itertools import chain
from databases import OrderedDict
import gevent
from gevent.queue import Queue

from .common_funcs_and_datastructures import jump_hash


class Attribute(object):
	_type = None

	def __init__(self, _type, **kwargs):
		self._type = _type
		self.__dict__.update(kwargs)

class Model(object):
	#class level
	_attrs = None
	_pk_attrs = None

	#instance level
	__is_new = True
	_pending_updates = None

	_inserted_id = None
	_init = False

	_json = None

	def get_custom_dict(self, path, dict_obj):

		_init = True

		class DictObj(dict):
			def __setitem__(this, k, v):
				if(not _init):
					new_path = path + "." + k
					self._pending_updates[new_path] = v
					if(isinstance(v, dict)):
						v = self.get_custom_dict(new_path, v)
				super(DictObj, this).__setitem__(k, v)

		ret = DictObj(**dict_obj)
		_init = False
		return ret

	def __setattr__(self, k, v):
		if(k in self.__class__._attrs):
			_attr_type_obj = self.__class__._attrs[k]
			if(_attr_type_obj._type == dict and isinstance(v, dict)):
				v = self.get_custom_dict(k, v)

			if(not self._init):
				self._pending_updates[k] = v
		self.__dict__[k] = v

	def to_json(self):
		self._json = _json = self._json or {}
		for k in self.__class__._attrs:
			if(k in self.__dict__):
				_json[k] = getattr(self, k , None)
		return _json

	def pk(self):
		_id = getattr(self, "_id", None)
		if(_id):
			return {"_id": _id}
		ret = {}
		for k in self.__class__._pk_attrs.keys():
			ret[k] = getattr(self, k)
		return ret

	#when retrieving objects from db
	@classmethod
	def get_instance_from_document(cls, doc):
		ret = cls()
		ret.__is_new = False
		ret._init = True
		ret._pending_updates = {}
		for k, v in cls._attrs.items():
			ret.__dict__[k] = None # set all initial attributes to None
		for k, v in doc.items():
			setattr(ret, k, v)
		ret._init = False
		return ret

	#used for creating new entities
	@classmethod
	def create_doc(cls, **values):
		ret = cls()
		ret._pending_updates = {}
		ret._pending_updates.update(cls.get_default_values())
		for k, v in cls._attrs.items():
			ret.__dict__[k] = None # set all initial attributes to None
		for k, v in values.items(): # set the given values
			setattr(ret, k, v)
		return ret

	@classmethod
	def get_default_values(cls):
		ret = {}
		for k, v in cls._attrs.items():
			default = getattr(v, "default", None)
			if(default):
				if(isinstance(default, types.FunctionType)):
					default = default()
				ret[k] = default
		return ret

	@classmethod
	def get_shard_key_from_query(cls, _query):
		_shard_keys = cls._shard_keys_
		if(not _shard_keys):
			return ""
		ret = []
		for k in cls._shard_keys_:
			val = _query.get(k)
			if(not val):
				return None
			ret.append(val) # must exist
		return "__".join(ret)

	def get_shard_key(self):
		_shard_keys = self.__class__._shard_keys_
		if(not _shard_keys):
			return ""
		ret = []
		for k in _shard_keys:
			ret.append(getattr(self, k))
		return "__".join(ret)

	@classmethod
	def get_databases_to_query(cls, _query, shard_keys_array):
		databases_to_update = None
		if(shard_keys_array):
			databases_set = {}
			for _shard_keys in shard_keys_array:
				_collection = None
				if(isinstance(_shard_keys, str)): # assuming single primary key
					_collection = get_collection(cls, _shard_keys)
				else:
					_collection = get_collection(cls, "__".join(_shard_keys))
				databases_set[id(_collection)] = _collection
			databases_to_update = databases_set.values()
		else:
			shard_key = cls.get_shard_key_from_query(_query)
			if(shard_key):
				databases_to_update = [get_collection(cls, shard_key)]
			else:
				#update in all collection
				databases_to_update = get_databases_all_nodes(cls)

		return databases_to_update

	@classmethod
	def update(cls, _query, update_values, shard_keys_array=None, **kwargs):
		databases_to_update = cls.get_databases_to_query(_query, shard_keys_array)

		multi_update_response = []

		def query_collection(_collection, _query, update_values):
			multi_update_response.append(_collection.update_many(_query, update_values, **kwargs))

		threads = [gevent.spawn(query_collection, _collection, _query, update_values)
													for _collection in databases_to_update
				]
		#waint on threads and return cursors
		gevent.joinall(threads)
		return multi_update_response

	@classmethod
	def query(cls, _query, sort=None, projection=None, shard_keys_array=None, **kwargs):
		databases_to_query = cls.get_databases_to_query(_query, shard_keys_array)

		multi_iterator = []

		def query_collection(_collection, _query, projection, sort):
			ret = _collection.find(_query, projection, **kwargs)
			if(sort):
				ret = ret.sort(sort)
			multi_iterator.append(ret)


		threads = [gevent.spawn(query_collection, _collection, _query, projection, sort)
													for _collection in databases_to_query
				]
		#waint on threads and return cursors
		gevent.joinall(threads)

		return map(cls.get_instance_from_document, chain(*multi_iterator))

	def commit(self, force=False):
		committed = False
		if(self.__is_new): # try inserting
			self.__is_new = False
			try:
				self._inserted_id = get_collection(self.__class__, self.get_shard_key()).insert_one(self._pending_updates)
				committed = True
			except DuplicateKeyError as ex:
				if(not force):
					raise(ex) # re reaise

				# try remove the primary key from pending updates
				if("_id" in self._pending_updates):
					del self._pending_updates["_id"]

				for k in list(self._pending_updates.keys()):
					if(k in self.__class__._pk_attrs):
						del self._pending_updates[k]

		if(not self.__is_new and not committed and self._pending_updates):  # try updated
			get_collection(self.__class__, self.get_shard_key()).update(self.pk(), {"$set": self._pending_updates}) # , hint=self.__class__._pk_attrs)

		self._pending_updates.clear()
		return self

	def delete(self):
		get_collection(self.__class__, self.get_shard_key()).delete_one(self.pk())



class GetCusror:

	def __init__(self, conn):
		self.conn = conn
		self.mysql_conn = None

	def __enter__(self):
		self.mysql_conn = self.conn.get_db()
		return self.mysql_conn

	def __exit__(self, exc_type, exc_val, exc_tb):
		self.conn.release_db(self.mysql_conn)


'''
Single connection object abstracting pool of connections
'''
class Connection:

	db_args = None
	conn_pool = None

	def __init__(self, **db_args):
		self.db_args = db_args
		self.conn_pool = Queue()


	def get_db(self): # cache and get collection? needed?
		conn = self.conn_pool.get(block=False)
		if(not conn):
			conn = pymysql.connect(**self.db_args)
		return conn

	def release_db(self, conn): # cache and get collection? needed?
		self.conn_pool.put(conn)
	

	def inititalize(self, Model):
		if(isinstance(Model, list)):
			for _m in Model:
				self.inititalize(_m)
			return

		Model._attrs = _model_attrs = {}
		Model._pk_attrs = None

		attrs_to_name = {}
		for k, v in Model.__dict__.items():
			if(isinstance(v, Attribute)):
				_model_attrs[k] = v
				attrs_to_name[v] = k
				attrs_to_name[k] = k # dumb , but it's one time thing and also helps converting if any given attributes as strings

		# ensure indexes created and all databases loaded to memory
		_model_indexes = getattr(Model, "_index_", [])
		Model._shard_keys_ = getattr(Model, "_shard_keys_", ())
		Model._cluster_ = getattr(Model, "_cluster_", "default")

		_mysql_indexes_to_create = []
		for _index in _model_indexes:
			mysql_index = []
			if(not isinstance(_index, tuple)):
				_index = (_index,)
			for _a in _index:
				_attr_name = _a
				_ordering = 'ASC'
				if(isinstance(_a, tuple)):
					_a, _ordering = _a
				if(isinstance(_a, Attribute)):
					_attr_name = attrs_to_name[_a]

				mysql_index.append((_attr_name, _ordering))


			if(not Model._pk_attrs):
				Model._pk_attrs = _pk_attrs = OrderedDict()
				for i in mysql_index: # first unique index
					_pk_attrs[i[0]] = 1

			_mysql_indexes_to_create.append(mysql_index)

		# set collection name to include shard_keys
		if(Model._shard_keys_):
			Model._collection_name_ = Model._collection_name_ + "_shard_" + ("__".join(map(lambda x: attrs_to_name[x], Model._shard_keys_)))
		# create indices in mysql
		_index_sql = []
		_index_name = ""
		for mysql_index in _mysql_indexes_to_create:
			with GetCusror(self) as cursor:
				_sql = []
				for attr_name, order in mysql_index:
					_index_name += ("__" + attr_name)
					_sql.append("%s %s"%(attr_name, order))
			_index_sql.append("(%s)", ", ".join(_sql))

			cursor.execute("ALTER TABLE CREATE INDEX %s %s", (_index_name, _index_sql))


		if(not Model._pk_attrs): # create default _pk_attrs
			Model._pk_attrs = OrderedDict(_id=True)


''''
	cluster -> (node1 + node2 + node3) (each node could be replica set)
	data is client level sharded within node1, node2, node3
	queries are assembled as a chained iterator
'''
_mysql_clusters = {}
def init_mysql_cluster(nodes, cluster="default"):
	if(not isinstance(nodes, list)):
		nodes = [nodes]
	db_nodes = []
	for node in nodes:
		if(isinstance(node, dict)):
			node = Connection(**node)
		db_nodes.append(node)
	_mysql_clusters[cluster] = db_nodes


'''
	returns the collection from a perticular node depending on shard key
	shard_key=__1pk__2pk... underscore joined keys of a single primary key
'''
def get_collection(Model, shard_key):
	cluster_nodes = _mysql_clusters[Model._cluster_]
	_node_with_data = jump_hash(shard_key, len(cluster_nodes))
	return cluster_nodes[_node_with_data].get_collection(Model)

def get_databases_all_nodes(Model):
	cluster_nodes = _mysql_clusters[Model._cluster_]
	return map(lambda node: node.get_collection(Model), cluster_nodes)




# - Supports client-side sharding,
# - Doesn't autoshardi, you have to redistribute data if you add more db's later.

# - You can Configure mysql as a sharded cluster. Just pass the configuration server to 
# - init_mysql_cluster({"host": "", "port": 27017, "db_name": "yourdb", "user_name": "", "password": ""})
