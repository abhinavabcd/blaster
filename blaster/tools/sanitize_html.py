def html_escape(s):
	s = s.replace("<", "&lt;")
	s = s.replace(">", "&gt;")
	return s


# custom containers ##########
# HtmlSanitizedList and HtmlSanitizedDict are used for HTML safe operation
# the idea is to wrap them to sanitizeContainers, and escapt them while retrieving
# rather than during inserting/parsing stage
class HtmlSanitizedSetterGetter(object):
	def __getitem__(self, k, escape_html=True):
		val = super().__getitem__(k)
		if(escape_html and isinstance(val, str)):
			return html_escape(val)
		elif(isinstance(val, dict)):
			if(isinstance(val, HtmlSanitizedDict)):
				return val
			val = HtmlSanitizedDict(val)
			self.__setitem__(k, val)
		elif(isinstance(val, list)):
			if(isinstance(val, HtmlSanitizedList)):
				return val
			val = HtmlSanitizedList(val)
			self.__setitem__(k, val)
		return val

	def __str__(self):
		return f"sanitized_{html_escape(super().__str__())}"


class HtmlSanitizedList(HtmlSanitizedSetterGetter, list):

	def __init__(self, entries=None):
		super().__init__()
		entries and self.extend(entries)

	def __iter__(self):
		# unoptimized but for this it's okay, always returns sanitized one
		return map(self.__getitem__, range(len(self)))

	def at(self, k, escape_html=True):
		return self.__getitem__(
			k,
			escape_html=escape_html
		)


# intercepts all values setting and
class HtmlSanitizedDict(HtmlSanitizedSetterGetter, dict):

	def __init__(self, entries=None, **kwargs):
		super().__init__()
		if(entries):
			self.update(entries)
		if(kwargs):
			self.update(kwargs)

	# can pass escape_html=false if you want raw data
	def get(self, key, default=None, escape_html=True):
		try:
			val = self.__getitem__(
				key,
				escape_html=escape_html,
			)
			return val
		except KeyError:
			return default

	def items(self):
		# unoptimized but for this it's okay, always returns sanitized one
		return map(lambda k: (k, self.__getitem__(k)), dict.keys(self))
