import html


# custom containers ##########
# SanitizedList and SanitizedDict are used for HTML safe operation
# the idea is to wrap them to sanitizeContainers, and escapt them while retrieving
# rather than during inserting/parsing stage
class SanitizedSetterGetter(object):
	def __getitem__(self, k, escape_html=True, escape_quotes=False):
		val = super().__getitem__(k)
		if(escape_html and isinstance(val, str)):
			return html.escape(val, quote=escape_quotes)
		return val

	def __setitem__(self, key, val):
		if(isinstance(val, dict)):
			val = SanitizedDict(val)
		elif(isinstance(val, list)):
			val = SanitizedList(val)
		super().__setitem__(key, val)

	def __str__(self):
		return f"sanitized_{html.escape(super().__str__(), quote=False)}"


class SanitizedList(SanitizedSetterGetter, list):

	def __init__(self, entries=None):
		super().__init__()
		entries and self.extend(entries)

	def __iter__(self):
		# unoptimized but for this it's okay, always returns sanitized one
		def sanitized(val):
			if(isinstance(val, str)):
				return html.escape(val, quote=True)
			return val
		return map(sanitized, list.__iter__(self))

	def at(self, k, escape_html=True, escape_quotes=False):
		return self.__getitem__(
			k,
			escape_quotes=escape_quotes,
			escape_html=escape_html
		)

	def extend(self, _list):
		for val in _list:
			# calls __setitem__ again
			self.append(val)
		# allow chaining
		return self

	def append(self, val):
		if(isinstance(val, dict)):
			new_val = SanitizedDict()
			for k, v in val.items():
				new_val[k] = v  # calls __setitem__ nested way
			super().append(new_val)

		elif(isinstance(val, list)):
			new_val = SanitizedList()
			for i in val:
				new_val.append(i)
			super().append(new_val)
		else:
			super().append(val)
		# allow chaining
		return self


# intercepts all values setting and
class SanitizedDict(SanitizedSetterGetter, dict):

	def __init__(self, entries=None, **kwargs):
		super().__init__()
		if(entries):
			self.update(entries)
		if(kwargs):
			self.update(kwargs)

	# can pass escape_html=false if you want raw data
	def get(self, key, default=None, escape_html=True, escape_quotes=False):
		try:
			val = self.__getitem__(
				key,
				escape_html=escape_html,
				escape_quotes=escape_quotes
			)
			return val
		except KeyError:
			return default

	def items(self):
		# unoptimized but for this it's okay, always returns sanitized one
		def sanitized(key_val):
			key, val = key_val
			if(isinstance(val, str)):
				return (key, html.escape(val, quote=True))
			return key_val
		return map(sanitized, dict.items(self))

	def update(self, another):
		for k, v in another.items():
			# calls __setitem__ again
			self[k] = v
		# allow chaining
		return self


class LowerKeyDict(dict):
	def __getitem__(self, k):
		return super().__getitem__(k.lower())

	def get(self, k, default=None):
		return super().get(k.lower(), default)
