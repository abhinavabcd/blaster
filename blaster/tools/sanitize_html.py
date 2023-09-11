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
		elif(isinstance(val, dict)):
			if(isinstance(val, SanitizedDict)):
				return val
			val = SanitizedDict(val)
			self.__setitem__(k, val)
		elif(isinstance(val, list)):
			if(isinstance(val, SanitizedList)):
				return val
			val = SanitizedList(val)
			self.__setitem__(k, val)
		return val

	def __str__(self):
		return f"sanitized_{html.escape(super().__str__(), quote=False)}"


class SanitizedList(SanitizedSetterGetter, list):

	def __init__(self, entries=None):
		super().__init__()
		entries and self.extend(entries)

	def __iter__(self):
		# unoptimized but for this it's okay, always returns sanitized one
		return map(self.__getitem__, range(len(self)))

	def at(self, k, escape_html=True, escape_quotes=False):
		return self.__getitem__(
			k,
			escape_quotes=escape_quotes,
			escape_html=escape_html
		)


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
		return map(lambda k: (k, self.__getitem__(k)), dict.keys(self))

	# may or may not get all unsanitized results
	# if items are previously accessed, it will return sanitized ones
	def _items_unsanitized(self):
		return super().items()
