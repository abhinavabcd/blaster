import typing
import ujson as json
from typing import get_type_hints
from functools import partial
#bare minimum validations and schema generators
_OBJ_END_ = object()
Optional = typing.Optional


class Int:
	def __init__(self, one_of=None, _min=_OBJ_END_, _max=_OBJ_END_, default=_OBJ_END_):
		self._min = _min
		self._max = _max
		self.one_of = set(one_of) if one_of else None
		self._default = default
		#make schema
		self._schema_ = _schema = {"type": "integer"}
		if(default != _OBJ_END_):
			_schema["default"] = default
		if(self._min != _OBJ_END_):
			_schema["minimum"] = self._min
		if(self._max != _OBJ_END_):
			_schema["maximum"] = self._max
		if(self.one_of):
			_schema["enum"] = list(self.one_of)

	def validate(self, e):
		if(e == None):
			if(self._default != _OBJ_END_):
				return self._default
			raise Exception("should be int")
		if(self._min != _OBJ_END_ and e < self._min):
			raise Exception("should be minlen %d"%(self._min))
		if(self._max != _OBJ_END_ and e > self._max):
			raise Exception("more than maxlen %d"%(self._max))
		if(self.one_of and e not in self.one_of):
			raise Exception("should be one of %s"%(str(self.one_of)))
		return e



class Str:
	def __init__(self, one_of=None, minlen=0, maxlen=4294967295, default=_OBJ_END_):
		self.minlen = minlen
		self.maxlen = maxlen
		self.one_of = set(one_of) if one_of else None
		self._default = default
		#make schema
		self._schema_ = _schema = {"type": "integer"}
		if(default != _OBJ_END_):
			_schema["default"] = default
		_schema = {"type": "string"}
		if(self.minlen):
			_schema["minimum"] = self.minlen
		if(self.maxlen and self.maxlen < 4294967295):
			_schema["maximum"] = self.maxlen
		if(self.one_of):
			_schema["enum"] = list(self.one_of)

	def validate(self, e):
		if(e == None):
			if(self._default != _OBJ_END_):
				return self._default
			raise Exception("should be int")
		if(len(e) < self.minlen):
			raise Exception("should be minlen %s"%(self.minlen))
		if(len(e) > self.maxlen):
			raise Exception("more than maxlen %s"%(self.maxlen))
		if(self.one_of and e not in self.one_of):
			raise Exception("should be one of %s"%(str(self.one_of)))
		return e

class Array:
	def __init__(self, _types, default=_OBJ_END_):
		self._types = _types if isinstance(_types, (list, tuple)) else (_types,)
		self._default = default

		#ceontents validation
		_s, _v = schema(self._types)

		self._schema_ = _schema = {"type": "array", "items": _s}
		if(default != _OBJ_END_):
			_schema["default"] = default
		#this is spefic validataion
		self.validate = partial(
			array_validation,
			mix=isinstance(self._types, tuple),
			default=self._default,
			**_v.keywords
		)



class Object:
	def __init__(self, default=_OBJ_END_, _required_=None, **keys):
		self._keys = keys
		self._default = default
		self._required = set(_required_) if _required_ else set()

		#instance specific schema
		self._properties = {}
		self._validations = {}
		for k, _type in keys.items():
			_s, _v = schema(_type)
			self._properties[k] = _s
			self._validations[k] = _v

		self._schema_ = _schema = {"type": "object", "properties": self._properties}
		if(default != _OBJ_END_):
			_schema["default"] = default

	#validates Object(a=str, b=int, h=Test)
	def validate(self, e):
		if(not isinstance(e, dict)):
			return json.loads(e)

		k = None
		attr_value = None
		try:
			for k, attr_validation in self._validations.items():
				attr_value = e.get(k, None)
				_validated_attr = attr_validation(attr_value)

				if(attr_value == None and k in self._required):
					raise Exception("Field is required")

				if(_validated_attr != attr_value):
					e[k] = _validated_attr
			return e
		except Exception as ex:
			raise Exception({
				"exception": (ex.args and ex.args[0]) or "Validation failed",
				"key": k,
				"value": attr_value,
				"schema": self._properties[k]
			})


def item_validation(e, simple_types=(), complex_validations=(), nullable=True):
	if(e == None and not nullable):
		raise Exception("Cannot be none")

	valid = False
	if(simple_types and isinstance(e, simple_types)):
		valid = True
	if(complex_validations):
		for _complex_validation in complex_validations:
			if(_complex_validation(e)):
				valid = True
				break

	if(valid):
		return e

	if(nullable):
		return None

	raise Exception("Cannot be none")


def array_validation(arr, simple_types=(), complex_validations=(), default=_OBJ_END_, mix=False, nullable=True):
	#sequece
	if(arr == None and not nullable):
		if(default != _OBJ_END_):
			return default
		raise Exception("Cannot be none")
	if(not isinstance(arr, list)):
		arr = json.loads(arr)

	filter_nones = False
	_prev_type = _OBJ_END_
	for i in range(len(arr)):
		valid = False
		e = arr[i]
		if(simple_types and isinstance(e, simple_types)):
			valid = True
		if(complex_validations):
			for _complex_validation in complex_validations:
				if(_complex_validation(e)):
					valid = True
		#check types should not mixed
		if(not mix): # single type, so check type matches with previous
			_cur_type = type(e)
			if(_prev_type == _OBJ_END_):
				_prev_type = _cur_type
			elif(_prev_type != _cur_type):
				valid = False
					
		if(not valid):
			arr[i] = None
			filter_nones = True

	if(filter_nones and not nullable): # null not allowed
		n = len(arr)
		i = n - 1
		while(i >= 0):
			if(arr[i] == None):
				arr.pop(i)
			i -= 1

	return arr


'''
	given any instance/class, returns schema, _validation function
'''
def schema(x):
	if(isinstance(x, type) and issubclass(x, Object) and x != Object):
		ret = schema.defs.get(x.__name__)
		if(ret):
			return {"schema": {"$ref": "#/definitions/" + x.__name__}}, x.validate

		_validations = {}
		_properties = {}
		for k, v in x.__dict__.items():
			_schema_and_validation = schema(v)
			if(_schema_and_validation):
				_properties[k], _validations[k] = _schema_and_validation

		#all are required unless marked as Optional
		_required = set(_properties.keys()) - set(k for k, v in get_type_hints(x).items() if v == Optional)

		def _validate_instance(e):
			if(not isinstance(e, x)):
				return None
			k = None
			attr_value = None
			try:
				for k, attr_validation in _validations.items():
					attr_value = getattr(e, k, None)
					_validated_attr = attr_validation(attr_value)

					if(_validated_attr == None and k in _required):
						raise Exception("Field is required")

					if(_validated_attr != attr_value):
						setattr(e, k, _validated_attr)
			except Exception as ex:
				raise Exception({
					"exception": (ex.args and ex.args[0]) or "Validation failed",
					"key": k,
					"value": attr_value,
					"schema": _properties[k]
				})

			return e

		#cache and return
		ret = schema.defs[x.__name__] = {
				"type": "object",
			}
		_title = getattr(x, "_title_", None)
		_description = getattr(x, "_description_", None)
		if(_title):
			ret["title"] = _title
		if(_description):
			ret["description"] = getattr(x, "_description_", None)
				
		if(_properties):
			ret["properties"] = _properties

		if(_required):
			ret["required"] = list(_required)
		
		#set these class level attributes
		x.validate = _validate_instance
		x._required = _required
		x._schema_ = ret

		return ret, _validate_instance

	#special for tuples and list
	elif(isinstance(x, (list, tuple))): # x = [int, str]->oneof, (int, str)->anyof
		is_nullable = False
		if(None in x):
			x.pop(x.index(None))
			is_nullable = True

		simple_types = [] # str, int, etc
		complex_validations = [] # array, object, SomeObject
		_schemas = []
		for _type in x: #
			_s, _v = schema(_type)
			_schemas.append(_s)

			if(_type in (int, str, float)):
				simple_types.append(_type)
			else:
				complex_validations.append(_type)

		ret = None
		if(len(x) == 1):
			ret = _schemas[0]
		elif(isinstance(x, list)):
			ret = {"oneOf": _schemas}
		else:
			ret = {"anyOf": _schemas}

		if(is_nullable):
			ret["nullable"] = True

		return ret, partial(item_validation, simple_types=tuple(simple_types), complex_validations=complex_validations, nullable=is_nullable)

	elif(x == int):
		return {"type": "integer"}, lambda e: e and int(e)

	elif(x == float):
		return {"type": "number", "format": "float"}, lambda e: e and float(e)

	elif(x == str or x == Str):
		return {"type": "string"}, lambda e: e and str(e)

	elif(isinstance(x, Str)):
		return x._schema_, x.validate

	elif(isinstance(x, Int)):
		return x._schema_, x.validate

	elif(x == Array or x == list): # genric
		return {"type": "array"}, (lambda e: e if isinstance(e, list) else None)

	elif(x == Object or x == dict):
		return {"type": "object"}, (lambda e: e if isinstance(e, dict) else None)

	elif(isinstance(x, Array)): # Array(_types)
		return x._schema_, x.validate

	elif(isinstance(x, Object)): # Object(id=Array)
		return x._schema_, x.validate

	else:
		return None


schema.defs = {}

def all_subclasses(cls):
	return set(cls.__subclasses__()).union(
		[s for c in cls.__subclasses__() for s in all_subclasses(c)]
	)


schema.init = lambda: [schema(x) for x in all_subclasses(Object)]




if __name__ == "__main__":
	class Test2(Object):
		a = Array(int)

		def __init__(self):
			self.a = [100]

	class Test(Object):
		a = Array
		b = Array((int, str))
		c = Object
		d = int
		e = Object(p=int, q=str, r=Array([int, str]))
		f = Test2
		g = Str(minlen=10)

		def __init__(self, **kwargs):
			self.a = [1, 2, 3, "damg"]
			self.b = [1, 2]
			self.c = {"a" : "hello"}
			self.d = 1
			self.e = {"p": 1, "q": [100], "r": ["hello"]}
			self.f = Test2()
			self.g = "abhinav reddy"

	import json
	schema.init()

	print(json.dumps(schema.defs, indent=4))
	i = Test()
	Test.validate(i)
	print(i.a, i.b, i.c, i.d, i.e, i.f.a, i.g)
