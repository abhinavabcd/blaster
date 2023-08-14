import html
from functools import partial

# custom containers ##########
# SanitizedList and SanitizedDict are used for HTML safe operation
# the idea is to wrap them to sanitizeContainers, and escapt them while retrieving
# rather than during inserting/parsing stage
def _sanitize(val, escape_html=True, escape_quotes=False):
	if(isinstance(val, str)):
		if(escape_html):
			return html.escape(val, quote=escape_quotes)
	elif(isinstance(val, list)):
		return SanitizedList(val)
	elif(isinstance(val, dict)):
		return SanitizedDict(val)
	return val


class SanitizedObject:
	entries = None

	def __getitem__(self, key):
		return _sanitize(self.entries[key])

	def __setitem__(self, key, val):
		self.entries[key] = val

	def __delitem__(self, key):
		del self.entries[key]

	def __getattr__(self, attr):
		return getattr(self.entries, attr)

	def __contains__(self, key):
		return key in self.entries

	def __iter__(self):
		return map(_sanitize, self.entries.__iter__())

	def __len__(self):
		return len(self.entries)

	def __str__(self):
		return f"sanitized_{self.entries}"

	def __repr__(self):
		return repr(self.entries)

	def __eq__(self, other):
		if(isinstance(other, SanitizedObject)):
			return self.entries == other.entries
		return self.entries == other

	def __ne__(self, other):
		return not self.__eq__(other)

	def __add__(self, other):
		return self.entries + other

	def __sub__(self, other):
		return self.entries - other


class SanitizedDict(SanitizedObject):

	def __init__(self, entries=None, **kwargs):
		self.entries = entries if entries is not None else {}
		if(kwargs):
			self.entries.update(kwargs)

	@property
	def __class__(self):
		return dict

	def get(self, key, default=None, escape_html=True, escape_quotes=False):
		v = self.entries.get(key, _sanitize)  # just sentinel
		if(v is _sanitize):
			return default
		return _sanitize(v, escape_html=escape_html, escape_quotes=escape_quotes)

	def items(self):
		return map(lambda kv: (kv[0], _sanitize(kv[1])), self.entries.items())


class SanitizedList(SanitizedObject):
	# list
	def __init__(self, entries=None):
		if(isinstance(entries, SanitizedList)):
			self.entries = entries.entries
		else:
			self.entries = entries if entries is not None else []

	@property
	def __class__(self):
		return list

	def at(self, i, escape_html=True, escape_quotes=False):
		return _sanitize(self.entries[i], escape_html=escape_html, escape_quotes=escape_quotes)


class LowerKeyDict(dict):
	def __getitem__(self, k):
		return super().__getitem__(k.lower())

	def get(self, k, default=None):
		return super().get(k.lower(), default)


#  MONKEY PATCHING
def default_json_encoder(enc, obj):
	if(isinstance(obj, SanitizedObject)):
		return obj.entries
	return JSONEncoder.default(enc, obj)


try:
	from json import JSONEncoder
	JSONEncoder._olddefault = JSONEncoder.default
	JSONEncoder.default = default_json_encoder
except ImportError:
	pass


def default_ujson_encoder(obj):
	if(isinstance(obj, SanitizedObject)):
		return obj.entries
	return TypeError(f"Object of type {type(obj).__name__} is not JSON serializable" )


try:
	import ujson
	ujson.dumps = partial(ujson.dumps, default=default_ujson_encoder)
except ImportError:
	pass

try:
	import simplejson
	simplejson.dumps = partial(simplejson.dumps, default=default_ujson_encoder)
except ImportError:
	pass

# bson patching
try:
	from bson.codec_options import TypeEncoder, DEFAULT_CODEC_OPTIONS

	class SanitizedListBsonTypeEncoder(TypeEncoder):
		python_type = SanitizedList  # the Python type acted upon by this type codec
		bson_type = list  # the BSON type acted upon by this type codec

		def transform_python(self, value: SanitizedList):
			return value.entries

	class SanitizedDictBsonTypeEncoder(TypeEncoder):
		python_type = SanitizedDict
		bson_type = dict

		def transform_python(self, value: SanitizedDict):
			return value.entries

	DEFAULT_CODEC_OPTIONS.type_registry._encoder_map[SanitizedList] = SanitizedListBsonTypeEncoder()
	for enc in [SanitizedListBsonTypeEncoder(), SanitizedDictBsonTypeEncoder()]:
		DEFAULT_CODEC_OPTIONS.type_registry._encoder_map[enc.python_type] = enc.transform_python

except ImportError:
	pass
