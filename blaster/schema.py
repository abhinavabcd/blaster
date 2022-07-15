import typing
import re
from base64 import b64decode
from datetime import datetime
import ujson as json
from typing import get_type_hints
from functools import partial
# bare minimum validations and schema generators
_OBJ_END_ = object()


def TYPE_ERR(msg):
	raise TypeError(msg)


class _Optional:
	def __init__(self, *types):
		self._types = types

	def __getitem__(self, *keys):
		return _Optional(*keys)


class _Required:
	def __init__(self, *types):
		self._types = types

	def __getitem__(self, *keys):
		return _Required(*keys)


Optional = _Optional()
Required = _Required()


class Int:
	def __init__(self, one_of=None, _min=_OBJ_END_, _max=_OBJ_END_, default=_OBJ_END_, _name=None):
		self._min = _min
		self._max = _max
		self.one_of = set(one_of) if one_of else None
		self._default = default
		self._name = _name
		# make schema
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
			raise TypeError("should be int")
		if(self._min != _OBJ_END_ and e < self._min):
			raise TypeError("should be minlen {:d}".format(self._min))
		if(self._max != _OBJ_END_ and e > self._max):
			raise TypeError("more than maxlen {:d}".format(self._max))
		if(self.one_of and e not in self.one_of):
			raise TypeError("should be one of {:d}".format(str(self.one_of)))
		return e


class Str:
	format_validators = {
		"date-time": lambda e: datetime.strptime(e.strip(), "%Y-%m-%dT%H:%M:%S.%fZ"),
		"date": lambda e: datetime.strptime(e.strip(), "%Y-%m-%d"),
		"binary": lambda e: e,
		"byte": lambda e: b64decode(e)
	}

	def __init__(
		self, one_of=None, minlen=0, maxlen=4294967295,
		regex=None, default=_OBJ_END_, _name=None, **kwargs
	):
		self.minlen = minlen
		self.maxlen = maxlen
		self.one_of = set(one_of) if one_of else None
		self._default = default
		self.regex = regex and re.compile(regex)
		self._name = _name
		_fmt = kwargs.pop("format", None)
		self.fmt = _fmt and Str.format_validators[_fmt]

		# make schema
		self._schema_ = _schema = {"type": "string"}
		if(default != _OBJ_END_):
			_schema["default"] = default
		_schema = {"type": "string"}
		if(self.minlen):
			_schema["minimum"] = self.minlen
		if(self.maxlen and self.maxlen < 4294967295):
			_schema["maximum"] = self.maxlen
		if(self.one_of):
			_schema["enum"] = list(self.one_of)
		if(regex):
			_schema["pattern"] = regex
		if(_fmt):
			_schema["format"] = _fmt

	def validate(self, e):
		if(e == None):
			if(self._default != _OBJ_END_):
				return self._default
			raise TypeError("should be string")
		if(len(e) < self.minlen):
			raise TypeError("should be minlen {:d}".format(self.minlen))
		if(len(e) > self.maxlen):
			e = e[:self.maxlen]
		if(self.one_of and e not in self.one_of):
			raise TypeError("should be one of {}".format(self.one_of))
		if(self.regex and not self.regex.fullmatch(e)):
			raise TypeError("did not match the pattern {}".format(self.one_of))
		if(self.fmt):
			return self.fmt(e)
		return e


class Array:
	def __init__(self, _types, default=_OBJ_END_, _name=None):
		self._name = _name
		self._types = _types if isinstance(_types, (list, tuple)) else [_types]
		self._default = default

		# contents validation
		_s, _v = schema(self._types)

		self._schema_ = _schema = {"type": "array", "items": _s}
		if(default != _OBJ_END_):
			_schema["default"] = default
		# this is spefic validataion
		self.validate = partial(
			array_validation,
			self,
			mix=isinstance(self._types, tuple),
			**_v.keywords
		)

	def __getitem__(self, *keys):
		return Array(keys)


''' 
Type definitions:
- Object 
 {"a": b, 1: 2...}

- Object(Str, Str)
 {"a": "1", "b": "2"...}

- Object(Str, Optional[Int, Str])
 {"a": "1", "b": "2"...}

- Object({"names": List, "canPlay": Optional[Int], "attrs": Required[Object] }) 
 {"names" : ["a", None, "c"], "attrs": {} }

- Dict[Int, Str]

- Dict[Str, Optional[Str, Int]]
'''


class Object:
	def __init__(self, *val_type, default=_OBJ_END_, _required_=None, _name=None, **keys):
		self._default = default
		self._required = set(_required_) if _required_ else set()
		self._name = _name

		if(val_type):  # single type of value {"name": argType...}
			_key_val_type = val_type[0]\
				if isinstance(val_type[0], (list, tuple))\
				else val_type
			self._schema_ = _schema = {
				"type": "object",
				"additionalProperties": schema(_key_val_type[-1])[0]
			}
		else:  # specify property: valuetypes
			# instance specific schema
			self._properties = {}
			self._validations = {}
			self._property_types = {}
			for k, _type in keys.items():
				if(isinstance(_type, _Required)):
					_type = _type._types[0]
					# mark the key as required
					self._required.add(k)
				elif(isinstance(_type, _Optional)):
					_type = _type._types[0]
				_s, _v = schema(_type)
				self._properties[k] = _s
				self._validations[k] = _v
				self._property_types[k] = _type

			self._schema_ = _schema = {"type": "object", "properties": self._properties}
		if(default != _OBJ_END_):
			_schema["default"] = default

	# validates Object(a=str, b=int, h=Test)
	# just does inplace validation
	def validate(self, e=_OBJ_END_):
		is_validating_dict = True
		if(e == _OBJ_END_):
			e = self.__dict__
			is_validating_dict = False
		elif(isinstance(e, str)):
			e = json.loads(e)

		k = None
		attr_value = None
		cls = self.__class__ or self
		try:
			for k, attr_validation in cls._validations.items():
				attr_value = e.get(k, _OBJ_END_)
				if(attr_value != _OBJ_END_):
					_validated_attr = attr_validation(attr_value)
				else:
					# try getting class attribute default (declaration)
					_validated_attr = getattr(cls, k, None)

				if(_validated_attr == None and k in cls._required):
					raise TypeError("Field is required")

				if(_validated_attr != attr_value):
					e[k] = _validated_attr
			return e if is_validating_dict else self
		except Exception as ex:
			raise TypeError({
				"exception": (ex.args and ex.args[0]) or "Validation failed",
				"key": k,
				"value": attr_value,
				"schema": cls._properties[k]
			})

	def __getitem__(self, *keys):
		return Object(keys)

	def to_dict(self):
		ret = {}
		for k, attr_validation in self.__class__._validations.items():
			ret[k] = getattr(self, k, None)
		return ret


Dict = Object()


def to_int(e):
	return e and int(e)


def to_str(e):
	return e and str(e)


def to_float(e):
	return e and float(e)


def item_validation(e, simple_types=(), complex_validations=(), nullable=True):
	if(e == None and not nullable):
		raise TypeError("Cannot be none")

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

	raise TypeError("Cannot be none")


def array_validation(_type, arr, simple_types=(), complex_validations=(), mix=False, nullable=True):
	# sequece
	if(arr == None and not nullable):
		if(_type._default != _OBJ_END_):
			return _type._default
		raise TypeError("Cannot be none")
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
		# check types should not mixed
		if(not mix):  # single type, so check type matches with previous
			_cur_type = type(e)
			if(_prev_type == _OBJ_END_):
				_prev_type = _cur_type
			elif(_prev_type != _cur_type):
				valid = False

		if(not valid):
			arr[i] = None
			filter_nones = True

	if(filter_nones and not nullable):  # null not allowed
		n = len(arr)
		i = n - 1
		while(i >= 0):
			if(arr[i] == None):
				arr.pop(i)
			i -= 1

	return arr


# Array(str), Array((int, str), default=None), Array(Object), Array(Pet)
# List[str, int]-> anyOf int, str
# List[[str, int]] -> oneOf int, str

# Object(id=int, name=str)
# Dict[str, int]

# given any instance/class, returns schema, _validation function
def schema(x):
	if(isinstance(x, _Optional)):
		_schema, _validator = schema(x._types[0])
		return _schema, _validator
	if(isinstance(x, _Required)):
		_schema, _validator = schema(x._types[0])
		_schema["required"] = True
		return _schema, (
			lambda x: _validator(x) if x else TYPE_ERR("field is required")
		)

	elif(isinstance(x, type) and issubclass(x, Object) and x != Object):
		# Object types
		ret = schema.defs.get(x.__name__)
		if(ret):
			return {"schema": {"$ref": "#/definitions/" + x.__name__}}, x.validate

		x._validations = _validations = {}
		x._properties = _properties = {}
		x._property_types = {}
		x._required = _required = set()
		for k, _type in get_type_hints(x).items():
			is_required = True
			if(
				_type.__module__ == 'typing'
				and getattr(_type, "__origin__", None) == typing.Union
				and getattr(_type, "__args__", None)
			):
				# union
				_type = _type.__args__[0]
				is_required = False
			if(isinstance(_type, _Optional)):
				# optional
				_type = _type._types[0]
				is_required = False

			elif(isinstance(_type, _Required)):
				# required
				_type = _type._types[0]

			_schema_and_validation = schema(_type)
			if(_schema_and_validation):
				_properties[k], _validations[k] = _schema_and_validation
				_default = x.__dict__.get(k, _OBJ_END_)  # declaration default
				if(_default != _OBJ_END_):  # ? and not isinstance(_type, type)):
					_type._default = _default
					is_required = False
				is_required and _required.add(k)
				# keep track of propeties
				x._property_types[k] = _type
		# create schema
		x._schema_ = ret = schema.defs[x.__name__] = {
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

		return x._schema_, x.validate

	# special for tuples and list
	elif(isinstance(x, (list, tuple))):  # x = [int, str]->oneof, (int, str)->anyof/mixed
		is_nullable = False
		if(None in x):
			x.pop(x.index(None))
			is_nullable = True

		simple_types = []  # str, int, etc
		complex_validations = []  # array, object, SomeObject
		_schemas = []
		for _type in x:  #
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

		return ret, partial(
			item_validation,
			simple_types=tuple(simple_types),
			complex_validations=complex_validations,
			nullable=is_nullable
		)

	elif(x == int or x == Int):
		return {"type": "integer"}, to_int

	elif(x == float):
		return {"type": "number", "format": "float"}, to_float

	elif(x == str or x == Str):
		return {"type": "string"}, to_str

	elif(isinstance(x, Str)):
		return x._schema_, x.validate

	elif(isinstance(x, Int)):
		return x._schema_, x.validate

	elif(x == Array or x == list):  # genric
		return {"type": "array"}, (lambda e: e if isinstance(e, list) else None)

	elif(x == Object or x == dict):  # generic without any attributes
		return {"type": "object"}, (lambda e: e if isinstance(e, dict) else None)

	elif(isinstance(x, Array)):  # Array(_types)
		return x._schema_, x.validate

	elif(isinstance(x, Object)):  # Object(id=Array)
		return x._schema_, x.validate

	else:
		return None, None


# Defs, that require schema to be defined
List = Array(None)

schema.defs = {}


def all_subclasses(cls):
	return set(cls.__subclasses__()).union(
		[s for c in cls.__subclasses__() for s in all_subclasses(c)]
	)


schema.init = lambda: [schema(x) for x in all_subclasses(Object)]
