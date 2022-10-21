import typing
import re
from base64 import b64decode
from datetime import datetime
import ujson as json
from typing import get_type_hints
from functools import partial

from blaster.tools import DummyObject
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


class Number:
    def __init__(self, one_of=None, _min=_OBJ_END_, _max=_OBJ_END_, default=_OBJ_END_, _name=None, _type=int):
        self._min = _min
        self._max = _max
        self.one_of = set(one_of) if one_of else None
        self._default = default
        self._name = _name
        self._type = _type
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
        return self._type(e)

class Int(Number):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, _type=int, **kwargs)

class Float(Number):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, _type=float, **kwargs)


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
        if(not isinstance(e, str)):
            e = str(e)
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
        _s, _item_validation = schema(self._types)

        self._schema_ = _schema = {"type": "array", "items": _s}
        if(default != _OBJ_END_):
            _schema["default"] = default
            self._default = default
        self._mix = isinstance(self._types, tuple)
        # derieve these from the partial of item validation and use it for
        # array validation
        self._nullable = _item_validation.keywords.get("nullable") or True
        self._complex_validations = _item_validation.keywords.get("complex_validations") or ()
        self._simple_types = _item_validation.keywords.get("simple_types") or ()
        self.validate = partial(
            array_validation, self, # arg here
            simple_types=self._simple_types,
            complex_validations=self._complex_validations,
            mix=self._mix,
            nullable=self._nullable
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

class _Dict:
    def __init__(self, k_type, val_type, default=_OBJ_END_, _name=None):
        self.key_ype_validator = schema(k_type)[1]
        self.val_type_validator = schema(val_type)[1]
        self._default = default
        self._name = _name
        self._schema_ = {
            "type": "object",
            "additionalProperties": schema(val_type)[0]
        }

    def validate(self, e):
        if(e == None):
            if(self._default != _OBJ_END_):
                return self._default

        if(not isinstance(e, dict)):
            return e
        for k in list(e.keys()):
            if(self.key_ype_validator(k) == None):
                e.pop(k)
                continue
            e[k] = self.val_type_validator(e[k])
        return e

    def __getitem__(self, *kv):
        return _Dict(
            str if len(kv) < 2 else kv[-2], 
            str if len(kv) < 1 else kv[-1]
        )
                
class Object:
    def __init__(self, default=_OBJ_END_, _required_=None, _name=None, **keys):
        self._default = default
        self._required = set(_required_) if _required_ else set()
        self._name = _name

        # instance specific: Ex: Object(a=Int, b=Str)
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
        

    # validates Object(a=str, b=int, h=Test).validate({} or obj or string)
    # validates ObjectClass.validate({} or )
    def validate(self):
        self.__class__.validate(self) # resets internal dict

    @classmethod
    def validate(cls, obj): # obj -> instance of dict, cls, str
        if(isinstance(obj, dict)):
            return cls.from_dict(obj)
        elif(isinstance(obj, str)):
            return cls.from_dict(json.loads(obj))

        # regular class instance
        # validate __dict__ of the instance inplace
        e = obj.__dict__
        k = None
        attr_value = None
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
            return obj
        except Exception as ex:
            raise TypeError({
                "exception": (ex.args and ex.args[0]) or "Validation failed",
                "key": k,
                "value": attr_value,
                "schema": cls._properties[k]
            })

    def from_dict(self, _dict: dict):
        return self.__class__.from_dict(_dict)

    @classmethod
    def from_dict(cls, _dict: dict):
        ret = cls()
        for _k, (k, _validator) in cls._dict_key_to_object_key.items():
            setattr(ret, k, _validator(_dict.get(_k)))
        return ret
        
    def to_dict(self):
        ret = {}
        for k, attr_validation in self.__class__._validations.items():
            ret[k] = getattr(self, k, None)
        return ret


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


def array_validation(_type, arr, simple_types=None, complex_validations=None, mix=False, nullable=True):
    # sequece
    if(arr == None):
        if(_type._default != _OBJ_END_):
            return _type._default
        if(nullable):
            return None
        raise TypeError("Cannot be none")
    if(not isinstance(arr, list)):
        print(arr)
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
        x._dict_key_to_object_key = {} # used when converting json/dict to object

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
            
            # pure type to instance of schema types
            _default = x.__dict__.get(k, _OBJ_END_)  # declaration default
            if(x == int or x == Int):
                _type = Int(default=_default)

            elif(x == float):
                _type = Float(default=_default)

            elif(x == str or x == Str):
                _type = Str(default=_default)

            elif(x == Array or x == list):  # genric
                _type = Array(default=list(_default))

            elif(x == dict or x == Dict):  # generic without any attributes
                _type = _Dict(default=dict(_default))

            _schema_and_validation = schema(_type)
            if(_schema_and_validation):
                _properties[k], _validations[k] = _schema_and_validation
                if(_default != _OBJ_END_):  # ? and not isinstance(_type, type)):
                    is_required = False
                is_required and _required.add(k)
                # keep track of propeties
                x._property_types[k] = _type
                x._dict_key_to_object_key[getattr(_type, "_name", None) or k] = (k, _validations[k])
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
                complex_validations.append(_v)

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

    elif(isinstance(x, Str)):
        return x._schema_, x.validate

    elif(isinstance(x, Int)):
        return x._schema_, x.validate

    elif(isinstance(x, _Dict)):
        return x._schema_, x.validate

    elif(isinstance(x, Array)):  # Array(_types)
        return x._schema_, x.validate

    elif(isinstance(x, Object)):  # Object(id=Array)
        return x._schema_, x.validate

    else:
        return None, lambda x: x


# Defs, that require schema to be defined
List = Array(None)
Dict = _Dict(str, None)

schema.defs = {}


def all_subclasses(cls):
    return set(cls.__subclasses__()).union(
        [s for c in cls.__subclasses__() for s in all_subclasses(c)]
    )


schema.init = lambda: [schema(x) for x in all_subclasses(Object)]
