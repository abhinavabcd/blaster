import re
from base64 import b64decode
from datetime import datetime
import ujson as json
from typing import get_args, get_origin, types as Types, Optional
from functools import partial

from blaster.tools import _OBJ_END_, _16KB_, all_subclasses
# bare minimum validations and schema generators


class BlasterSchemaTypeError(TypeError):
	def __init__(self, data: object) -> None:
		self.data = self.sanitize_to_dict(data)
		super().__init__(data)

	def sanitize_to_dict(self, data: object) -> dict:
		if(isinstance(data, dict)):
			for k, v in data.items():
				data[k] = self.sanitize_to_dict(v)
		elif(isinstance(data, list)):
			for i, v in enumerate(data):
				data[i] = self.sanitize_to_dict(v)
		elif(isinstance(data, Object)):
			data = data.to_dict()

		return data


def RAISE_TYPE_ERROR(msg):
	raise BlasterSchemaTypeError(msg)


class Number:
	def __init__(self, one_of=None, _min=_OBJ_END_, _max=_OBJ_END_, default=_OBJ_END_, _type=int):
		self._min = _min
		self._max = _max
		self.one_of = set(one_of) if one_of else None
		self._default = default
		self._type = _type
		# make schema
		self._schema_ = _schema = {"type": "integer" if _type == int else "number"}
		if(default is not _OBJ_END_):
			_schema["default"] = default
		if(self._min is not _OBJ_END_):
			_schema["minimum"] = self._min
		if(self._max is not _OBJ_END_):
			_schema["maximum"] = self._max
		if(self.one_of):
			_schema["enum"] = list(self.one_of)

	def validate(self, e):
		if(e is None):
			if(self._default is not _OBJ_END_):
				return self._default
			raise TypeError("should be int")
		if(isinstance(e, str) and len(e) > 50):
			raise TypeError("should be valid int")
		e = self._type(e)
		if(self._min is not _OBJ_END_ and e < self._min):
			raise TypeError("should be min {:d}".format(self._min))
		if(self._max is not _OBJ_END_ and e > self._max):
			raise TypeError("more than max {:d}".format(self._max))
		if(self.one_of and e not in self.one_of):
			raise TypeError("should be one of {:d}".format(str(self.one_of)))
		return e


class Int(Number):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, _type=int, **kwargs)


class Float(Number):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, _type=float, **kwargs)


class Bool:
	def __init__(self, default=_OBJ_END_):
		self._schema_ = _schema = {"type": "boolean"}
		self._default = default
		if(default is not _OBJ_END_):
			_schema["default"] = default

	def validate(self, e):
		if(e is None):
			if(self._default is not _OBJ_END_):
				return self._default
			raise TypeError("should be boolean")
		return bool(e)


class Str:
	format_validators = {
		"date-time": lambda e: datetime.strptime(e.strip(), "%Y-%m-%dT%H:%M:%S.%fZ"),
		"date": lambda e: datetime.strptime(e.strip(), "%Y-%m-%d"),
		"binary": lambda e: e,
		"byte": lambda e: b64decode(e)
	}

	def __init__(
		self, one_of=None, minlen=1, maxlen=_OBJ_END_,
		regex=None, default=_OBJ_END_,
		before=None, format=None
	):
		self.minlen = minlen
		self.maxlen = maxlen if maxlen is not _OBJ_END_ else _16KB_
		self.one_of = set(one_of) if one_of else None
		self._default = default
		self.regex = regex and (re.compile(regex) if isinstance(regex, str) else regex)
		self.before = before
		self.fmt = format and Str.format_validators[format]

		# make schema
		self._schema_ = _schema = {"type": "string"}
		if(default is not _OBJ_END_):
			_schema["default"] = default

		if(minlen):
			_schema["minLength"] = minlen

		if(maxlen is not _OBJ_END_):
			_schema["maxLength"] = maxlen

		if(self.one_of):
			_schema["enum"] = list(self.one_of)
		if(regex):
			_schema["pattern"] = regex
		if(format):
			_schema["format"] = format

	def validate(self, e):
		if(self.before):
			e = (e is not None) and self.before(e)
		_default = self._default
		if(e is None):
			if(_default is not _OBJ_END_):
				return _default
			raise TypeError("should be string")
		if(not isinstance(e, str)):
			if(isinstance(e, (int, float))):
				e = str(e)
			else:
				raise TypeError("should be string")
		# e is a string now
		if(len(e) < self.minlen):
			if(not e):  # empty string allowed if minlen is 0 or default is set
				if(_default is not _OBJ_END_):
					return _default
			raise TypeError("should be minlen {:d}".format(self.minlen))
		if(len(e) > self.maxlen):
			e = e[:self.maxlen]
		if(self.one_of and e not in self.one_of):
			raise TypeError("should be one of {}".format(self.one_of))
		if(self.regex and not self.regex.fullmatch(e)):
			raise TypeError("did not match the pattern {}".format(self.regex.pattern))
		if(self.fmt):
			return self.fmt(e)
		return e


class Field:
	def __init__(self, _type, title=None, description=None, default=_OBJ_END_, json_name=None, **kwargs):
		self.title = title
		self.description = description
		self.default = default
		self.json_name = json_name
		# special type params for basics
		if(_type == str or _type == Str):
			_type = Str(**kwargs)
		elif(_type == int or _type == Int):
			_type = Int(**kwargs)
		elif(_type == float or _type == Float):
			_type = Float(**kwargs)
		self._type = _type


# _List([str, int]) -> oneOf str, int   -> list[str, int] -> list[str] | list[int] -> cannot mix object types
# _List((str, int)) -> anyOf str, int   -> list[str | int] -> can mix object types
class _List:
	def __init__(self, _types: list | tuple, default=_OBJ_END_):
		_types = self._types = _types if isinstance(_types, (list, tuple)) else [_types]
		self._default = default

		# contents validation
		_s, _item_validation = schema(_types)

		self._schema_ = _schema = {"type": "array", "items": _s}
		if(default is not _OBJ_END_):
			_schema["default"] = default
			self._default = default
		self._mix = isinstance(_types, tuple)
		# derieve these from the partial of item validation and use it for
		# array validation
		self._nullable = _item_validation.keywords.get("nullable", False)
		self._complex_validations = _item_validation.keywords.get("complex_validations") or ()
		self._simple_types = _item_validation.keywords.get("simple_types") or ()

	def validate(self, arr):
		# sequece
		default = self._default
		nullable = self._nullable
		simple_types = self._simple_types
		complex_validations = self._complex_validations
		mix = self._mix
		if(arr is None):
			if(default is not _OBJ_END_):
				# None or copy of default
				return list(default) if default is not None else None
			if(nullable):
				return None
			raise TypeError("Cannot be none")

		if(not isinstance(arr, list)):
			if(isinstance(arr, str) and arr.startswith("[")):
				arr = json.loads(arr)
			else:
				raise TypeError("Not an array type")

		_prev_mix_check = _OBJ_END_
		for i in range(len(arr)):
			val = arr[i]
			e = _validate(
				val, simple_types, complex_validations, nullable,
				matched_validators=(_mix_check := []) if not mix else None
			)
			if(e is not val):
				arr[i] = e
			# check types should not mixed
			if(not mix):  # single type, so check type matches with previous
				if(_prev_mix_check is _OBJ_END_):
					_prev_mix_check = _mix_check
				elif(_prev_mix_check != _mix_check):
					raise TypeError("Array values should not be mixed types")
				_prev_mix_check = _mix_check
		return arr


class _Set(_List):
	def __init__(self, _types, default=_OBJ_END_):
		super().__init__(_types, default)

	def validate(self, e):
		if((e := super().validate(e)) is not None):
			return set(e)

# Type definitions:
# - Object
#  {"a": b, 1: 2...}

# - Object(Str, Str)
#  {"a": "1", "b": "2"...}

# - Object(Str, Optional[Int, Str])
#  {"a": "1", "b": "2"...}

# - Object({"names": List, "canPlay": Optional[Int], "attrs": Required[Object] }) 
#  {"names" : ["a", None, "c"], "attrs": {} }

# - Dict[Int, Str]

# - Dict[Str, Optional[Str, Int]]


class _Dict:
	def __init__(self, k_type, val_type, default=_OBJ_END_):
		self.key_ype_validator = schema(k_type)[1]
		self.val_type_validator = schema(val_type)[1]
		self._default = default
		self._schema_ = {
			"type": "object",
			"additionalProperties": schema(val_type)[0]
		}

	def validate(self, e):
		if(e is None or not isinstance(e, dict)):
			if(self._default is not _OBJ_END_):
				return dict(self._default) if self._default is not None else None
			raise TypeError("should be a dict")

		for k in list(e.keys()):
			if(self.key_ype_validator(k) is None):
				e.pop(k)
				continue
			e[k] = self.val_type_validator(e[k])
		return e


class Object:
	_schema_def_name_ = None

	def __init__(self, default=_OBJ_END_, **keys):
		self._default = default

		# instance specific: Ex: Object(a=Int, b=Str)
		self._schema_properties = {}
		self._validations = {}
		self._property_types = {}
		for k, _type in keys.items():
			_s, _v = schema(_type)
			self._schema_properties[k] = _s
			self._validations[k] = _v
			self._property_types[k] = _type

		self._schema_ = _schema = {"type": "object", "properties": self._schema_properties}
		if(default is not _OBJ_END_):
			_schema["default"] = default

	def after_validate(self) -> (None | Exception):
		return

	def before_validate(self, _dict: dict) -> (None | Exception):
		return

	@classmethod
	def validate(cls, obj):  # obj -> instance of dict, cls, str
		# regular class instance
		# validate __dict__ of the instance inplace

		if(obj is None):
			return None

		if(isinstance(obj, dict)):
			_obj = cls()
			_obj.__dict__ = obj
			obj = _obj

		e = obj.__dict__

		obj.before_validate(e)

		k = None
		attr_value = None
		try:
			for k, attr_validation in cls._validations.items():
				attr_value = e.get(k, _OBJ_END_)
				if(attr_value is not _OBJ_END_):
					_validated_attr = attr_validation(attr_value)
					if(_validated_attr != attr_value):
						e[k] = _validated_attr
				else:
					# try getting class attribute default (declaration)
					_validated_attr = getattr(cls, k, _OBJ_END_)
					if(_validated_attr is _OBJ_END_):
						if(k in cls._required):  # required field
							raise TypeError("Field is required")
					elif(_validated_attr is not None):  # non-None default value
						e[k] = _validated_attr

			obj.after_validate()
			return obj
		except Exception as ex:
			raise BlasterSchemaTypeError({
				"key": k,
				"exception": (ex.args and ex.args[0]) or "Validation failed",
				"value": attr_value if attr_value is not _OBJ_END_ else None,
				"schema": cls._schema_properties[k]
			})

	@classmethod
	def from_dict(cls, _dict: dict, default=_OBJ_END_):
		try:
			for _k, k in cls._remap_dict_key_to_object_key.items():
				if(_k in _dict):
					_dict[k] = _dict.pop(_k)
			return cls.validate(_dict)

		except Exception as ex:
			if(default is not _OBJ_END_):
				return default
			raise ex

	def to_dict(self):
		ret = {}
		for k, attr_validation in self.__class__._validations.items():
			val = self.__dict__.get(k, _OBJ_END_)
			if(val is _OBJ_END_):
				continue
			if(isinstance(val, Object)):
				val = val.to_dict()
			elif(isinstance(val, list)):
				val = [v.to_dict() if isinstance(v, Object) else v for v in val]
			ret[k] = val
		return ret

	# does a deep copy of the object to dict
	@classmethod
	def deep_copy_to_dict(cls, obj):
		if(isinstance(obj, dict)):
			ret = {}
			for k, v in obj.items():
				ret[k] = cls.deep_copy_to_dict(v)
			return ret
		elif(isinstance(obj, list)):
			return [cls.deep_copy_to_dict(v) for v in obj]
		elif(isinstance(obj, Object)):
			ret = {}
			for k, attr_validation in obj.__class__._validations.items():
				val = obj.__dict__.get(k, _OBJ_END_)
				if(val is _OBJ_END_):
					continue
				ret[k] = cls.deep_copy_to_dict(val)
			return ret
		else:
			return obj


def to_int(e):
	return e and int(e)


def to_str(e):
	return e and str(e)


def to_float(e):
	return e and float(e)


# returns validated value or None, and what validators matched
def _validate(e, simple_types=(), complex_validations=(), nullable=True, matched_validators=None):
	valid = False
	for _simple_type in simple_types:
		if(isinstance(e, _simple_type)):
			valid = True
			if(matched_validators is not None):
				matched_validators.append(_simple_type)
			break
	ex = None
	if(not valid):
		for _complex_validation in complex_validations:
			try:
				e = _complex_validation(e)
				valid = True
				if(matched_validators is not None):
					matched_validators.append(_complex_validation)
				break
			except Exception as _ex:
				ex = _ex
	# if no validations are given are we good
	if(not valid and not simple_types and not complex_validations):
		if(e is None and not nullable):
			raise TypeError("Cannot be none")
		valid = True

	if(valid):
		return e

	if(nullable):
		return None

	raise (ex or TypeError("Invalid value"))


# Array(str), Array((int, str), default=None), Array(Object), Array(Pet)
# List[str, int] -> anyOf int, str
# List[(str, int)] -> oneOf int, str

# Object(id=int, name=str)
# Dict[str, int | str]

OptionalType = type(Optional[str])


# given any instance/class, returns (schema, _validation) function
# validation is inplace, if you want to validate to new, pass a copy
def schema(x, default=_OBJ_END_):

	if(type(x) is Types.UnionType):
		x = get_args(x)
	# if x is typing.Optional type

	if(type(x) is OptionalType):
		x = get_args(x)

	if(type(x) is Types.GenericAlias):
		_origin = get_origin(x)
		_args = list(get_args(x))
		if(_origin == list):
			return schema(_List(_args, default=default))
		elif(_origin == dict):
			_args = [str] * max(0, (2 - len(_args))) + _args
			return schema(_Dict(*_args, default=default))
		elif(_origin == set):
			return schema(_Set(_args, default=default))

	if(isinstance(x, type) and issubclass(x, Object) and x != Object):
		_schema_def_name_ = x.__module__ + "." + x.__name__
		if(_schema := schema.defs.get(_schema_def_name_)):  # already defined
			return _schema, x.validate

		x._validations = _validations = {}
		x._schema_properties = _schema_properties = {}
		x._fields_ = _fields_ = {}  # key: [type, default, {title, description, json_name}]
		x._property_types = {}
		x._required = _required = set()
		x._remap_dict_key_to_object_key = {}  # used when converting json/dict to object

		__x_mro = x.__mro__
		for cls_x in reversed(__x_mro[1:__x_mro.index(Object)]):
			_fields_.update(cls_x._fields_)
		for k, _type in x.__annotations__.items():
			_fields_[k] = {"type": _type, "default": x.__dict__.get(k, _OBJ_END_)}

		# capture the fields
		# val = Field(_type, title, description, default, json_name)
		for k, val in x.__dict__.items():
			if(isinstance(val, Field)):
				_default_value = val.default
				_fields_[k] = {
					"type": val._type,
					"default": _default_value,
					"json_name": val.json_name,
					"title": val.title,
					"description": val.description
				}
				setattr(x, k, _default_value)  # so when instance is created, it has default value copied

		for k, _field_data in _fields_.items():
			# pure type to instance of schema types
			_default_value = _field_data["default"]
			_type = _field_data["type"]

			_schema_and_validation = schema(_type, default=_default_value)
			if(_schema_and_validation):
				_schema_properties[k], _validations[k] = _schema_and_validation
				if(
					(_default_value is _OBJ_END_)
					and _schema_properties[k]
					and not _schema_properties[k].get("nullable")
				):
					_required.add(k)  # required if no default value

				_schema_properties[k]["title"] = _field_data.get("title")
				_schema_properties[k]["description"] = _field_data.get("description")
				if(dict_key := _field_data.get("json_name")):
					x._remap_dict_key_to_object_key[dict_key] = k

				# keep track of propeties
				x._property_types[k] = _type
		# create schema
		x._schema_ = _schema = schema.defs[_schema_def_name_] = {
			"type": "object",
		}
		_title = getattr(x, "_title_", None)
		_description = getattr(x, "_description_", None)
		if(_title):
			_schema["title"] = _title
		if(_description):
			_schema["description"] = getattr(x, "_description_", None)

		if(_schema_properties):
			_schema["properties"] = _schema_properties

		if(_required):
			_schema["required"] = list(_required)

		return x._schema_, x.validate

	# special for tuples and list
	elif(isinstance(x, (list, tuple))):  # x = [int, str]->oneof, (int, str) or int|str ->anyof/mixed
		simple_types = []  # str, int, etc
		complex_validations = []  # array, object, SomeObject
		_schemas = []
		is_nullable = False
		for _type in x:  #
			if(not _type or _type is type(None)):
				is_nullable = True
				continue

			_s, _v = schema(_type)
			_schemas.append(_s)
			if(_type in (int, str, float)):
				simple_types.append(_type)
			else:
				complex_validations.append(_v)

		ret = None
		if(len(_schemas) == 1):
			ret = _schemas[0]
		elif(isinstance(x, list)):
			ret = {"oneOf": _schemas}
		else:
			ret = {"anyOf": _schemas}

		if(is_nullable):
			ret["nullable"] = True

		return ret, partial(
			_validate,
			simple_types=tuple(simple_types),
			complex_validations=complex_validations,
			nullable=is_nullable
		)

	elif(isinstance(x, Str)):
		if(default is not _OBJ_END_):
			x._default = default
			x._schema_["default"] = default
		return x._schema_, x.validate

	elif(isinstance(x, Int)):
		if(default is not _OBJ_END_):
			x._default = default
			x._schema_["default"] = default
		return x._schema_, x.validate

	elif(isinstance(x, _Dict)):
		if(default is not _OBJ_END_):
			x._default = default
			x._schema_["default"] = default
		return x._schema_, x.validate

	elif(isinstance(x, _Set)):  # Array(_types)
		if(default is not _OBJ_END_):
			x._default = default
			x._schema_["default"] = default
		return (
			x._schema_,
			lambda s: set(y) if ((y := x.validate(s)) is not None) else None
		)

	elif(isinstance(x, _List)):  # Array(_types)
		if(default is not _OBJ_END_):
			x._default = default
			x._schema_["default"] = default
		return x._schema_, x.validate

	elif(isinstance(x, Bool)):
		if(default is not _OBJ_END_):
			x._default = default
			x._schema_["default"] = default
		return x._schema_, x.validate

	elif(isinstance(x, Object)):  # Object(id=Array)
		if(default is not _OBJ_END_):
			x._default = default
		return x._schema_, x.validate

	elif(x == int or x == Int):
		x = Int(default=default)
		return x._schema_, x.validate

	elif(x == float):
		x = Float(default=default)
		return x._schema_, x.validate

	elif(x == str or x == Str):
		x = Str(default=default)
		return x._schema_, x.validate

	elif(x == list):  # genric
		# make a copy if default exists
		x = _List(None, default=default)
		return x._schema_, x.validate

	elif(x == set):  # genric
		x = _List(None, default=default)
		return x._schema_, lambda s: set(y) if ((y := x.validate(s)) is not None) else None

	elif(x == dict):  # generic without any attributes
		x = _Dict(None, None, default=default)
		return x._schema_, x.validate

	elif(x == bool or x == Bool):  # generic without any attributes
		x = Bool(default=default)
		return x._schema_, x.validate

	else:
		return {"AnyValue": {}}, lambda x: x


# Defs, that require schema to be defined
class DictValidater:
	def __getitem__(self, *_types):
		n = len(_types)
		if(n == 1):
			return _Dict(str, _types[0])
		elif(n == 2):
			return _Dict(_types[0], _types[1])


class ListValidater:
	def __getitem__(self, _type):
		return _List(_type)


List = ListValidater()
Dict = DictValidater()

schema.defs = {}


def init():
	for cls in all_subclasses(Object):
		schema(cls)


schema.init = init
