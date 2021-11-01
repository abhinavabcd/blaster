'''
Created on 22-Aug-2017

@author: abhinav
'''

# argparse==1.2.1
# gevent==1.2.1
# greenlet==0.4.12
# protobuf==3.2.0
# boto3==1.4.4

import os
import gevent
import signal
import ujson as json
import re
import urllib.parse
import time
import functools
import traceback
import inspect
from urllib.parse import urlencode

from gevent.threading import Lock
from gevent.server import StreamServer
from requests_toolbelt.multipart import decoder

from . import config as blaster_config
from .config import IS_DEV
from .common_funcs_and_datastructures import SanitizedDict, get_mime_type_from_filename, DummyObject,\
	set_socket_options
from .utils import events
from .logging import LOG_ERROR, LOG_SERVER
from .schema import Object, schema as schema_func
from .websocket.server import WebSocketServerHandler


_is_server_running = True
default_stream_headers = {
	'Content-Type': 'text/html; charset=utf-8',
	'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
	'X-Content-Type-Options': 'nosniff',
	'vary': 'Accept-Encoding',
	'X-Frame-Options': 'SAMEORIGIN',
	'X-UA-Compatible': 'IE=Edge',
	'X-XSS-Protection': '0'
}

I_AM_HANDLING_THE_SOCKET = object() # handler takes care of status line, headers and body


REUSE_SOCKET_FOR_HTTP = object()
_OBJ_END_ = object()

########some random constants
HTTP_MAX_REQUEST_BODY_SIZE = 1 * 1024 * 1024 # 1 mb
HTTP_MAX_HEADERS_DATA_SIZE = 16 * 1024 # 16kb


class BufferedSocket():
	
	is_eof = False
	sock = None
	store = None

	def __init__(self, sock):
		self.sock = sock
		self.store = bytearray()
		
	def close(self):
		self.sock.close()
		self.is_eof = True

	def send(self, *_data):
		total_sent = 0
		for data in _data:
			if(isinstance(data, str)):
				data = data.encode()
			n = 0
			data_len = len(data)
			data_mview = memoryview(data)
			while(n < data_len):
				sent = self.sock.send(data_mview[n:])
				if(sent < 0):
					return sent # return failed
				n += sent
			total_sent += n
		return total_sent
				
	def recv(self, n):
		if(self.is_eof):
			return None
		if(self.store):
			ret = self.store
			self.store = bytearray()
			return ret
		return self.sock.recv(n)

	def recvn(self, n):
		if(self.is_eof):
			return None
		while(len(self.store) < n):
			data = self.sock.recv(4096)
			if(not data):
				self.is_eof = True
				break
			self.store.extend(data)

		#return n bytes for now
		ret = self.store[:n]
		#set remaining to new store
		self.store = self.store[n:]
		return ret
		
	#fails if it couldn't find the delimiter until max_size
	def readuntil(self, delimiter, max_size, discard_delimiter):
		if(self.is_eof):
			return None
		#check in store
		if(isinstance(delimiter, str)):
			delimiter = delimiter.encode()
		
		delimiter_len = len(delimiter)
		#scan the store until end, if not found extend and continue until store > max_size
		to_scan_len = len(self.store)
		i = 0 # how much we scanned already
		
		_store = self.store # get a reference
		while(True):
			if(i > max_size):
				self.is_eof = True
				return None
			if(i >= delimiter_len):
				j = 0
				lookup_from = i - delimiter_len
				while(j < delimiter_len and _store[lookup_from + j] == delimiter[j]):
					j += 1

				if(j == delimiter_len):
					#found
					ret = None
					if(discard_delimiter):
						ret = _store[:i - delimiter_len]
					else:
						ret = _store[:i]
					self.store = _store[i:] # set store to unscanned/pending
					return ret
			if(i >= to_scan_len):
				#scanned all buffer
				data = self.sock.recv(4096)
				if(not data):
					self.is_eof = True
					return None

				_store.extend(data) # fetch more data
				to_scan_len = len(_store)
			i += 1

	def __getattr__(self, key):
		ret = getattr(self.sock, key, _OBJ_END_)
		if(ret == _OBJ_END_):
			raise AttributeError()
		return ret

def get_chunk_size_from_header(chunk_header):
	ret = 0
	i = 0
	l = len(chunk_header)
	A = ord('A')
	Z = ord('Z')
	_0 = ord('0')
	_9 = ord('9')
	while(i < l):
		c = ord(chunk_header[i])
		if(c >= A and c <= Z):
			ret = ret * 16 + (10 + c - A)
		elif(c >= _0 and c <= _9):
			ret = ret * 16 + (c - _0)
		else:
			return ret
		i += 1
	return ret


####parse query string
def parse_qs_modified(qs, keep_blank_values=True, strict_parsing=False):
	_dict = {}
	for name, value in urllib.parse.parse_qsl(qs, keep_blank_values, strict_parsing):
		if name.endswith("[]"):
			_values = _dict.get(name)
			if(_values == None):
				_dict[name] = _values = []
			elif(not isinstance(_values , list)): # make it a list of values
				_dict[name] = _values = [_values]
			_values.append(value)
		else:
			_dict[name] = value
	return _dict



class HeadersDict(dict):
	def __getitem__(self, k):
		return super().__getitem__(k.lower())

	def get(self, k, default=None):
		header_value = super().get(k.lower(), _OBJ_END_)
		if(header_value == _OBJ_END_):
			return default
		return header_value


class Query(Object):
	def __init__(self, **kwargs):
		super().__init__(**kwargs)

class Headers(Object):
	def __init__(self, **kwargs):
		super().__init__(**kwargs)

class Cookie(Object):
	def __init__(self, **kwargs):
		super().__init__(**kwargs)

class Body(Object):
	def __init__(self, **kwargs):
		super().__init__(**kwargs)

#Array(str), Array((int, str), default=None), Array(Object), Array(Pet)
#Object(id=int, name=str ), class Pet: id = int, name = str, friend = Pet
class Request:
	#class level
	_before_hooks = []
	_after_hooks = []

	after = []
	__argument_creator_hooks = {}

	#instance level
	sock = None
	_post = None
	_get = None
	_cookies = None
	_headers = None
	#cookies to send to client
	_cookies_to_set = None
	#url data, doesn't contain query string
	path = None

	@classmethod
	def before(cls, func=None):
		if(func and callable(func)):
			#decorating
			Request._before_hooks.append(func)
			return func
		return Request._before_hooks

	@classmethod
	def after(cls, func=None):
		if(func and callable(func)):
			#decorating
			Request._after_hooks.append(func)
			return func
		return Request._after_hooks


	def __init__(self, buffered_socket):
		self._get = SanitizedDict() # empty params by default
		self._headers = HeadersDict()
		self.sock = buffered_socket
		self.ctx = DummyObject()

	#searches in post and get
	def get(self, key, default=None, **kwargs):
		val = _OBJ_END_
		#try post params first
		if(self._post):
			val = self._post.get(key, default=_OBJ_END_, **kwargs)
		#try get param next
		if(val == _OBJ_END_ and self._get):
			val = self._get.get(key, default=_OBJ_END_, **kwargs)

		if(val == _OBJ_END_):
			return default
		return val

	def __setitem__(self, key, val):
		self._get[key] = val

	def __getitem__(self, key):
		ret = self.get(key, default=_OBJ_END_)
		if(ret == _OBJ_END_):
			raise LookupError()
		return ret

	def __getattr__(self, key):
		ret = self.get(key, default=_OBJ_END_)
		if(ret == _OBJ_END_):
			raise AttributeError()
		return ret

	def GET(self, key=None, **kwargs):
		if(key == None):
			return self._get
		return self._get.get(key, **kwargs)

	def POST(self, key=None, **kwargs):
		if(key == None):
			return self._post
		if(self._post):
			return self._post.get(key, **kwargs)
		return None

	def HEADERS(self, key=None, default=None):
		if(key == None):
			return self._headers
		return self._headers.get(key, default=default)

	def COOKIES(self, key, cookie_value=_OBJ_END_):
		ret = self._cookies.get(key)
		if(cookie_value != _OBJ_END_):
			if(self._cookies_to_set == None):
				self._cookies_to_set = {}
			if(isinstance(cookie_value, dict)):
				_cookie_val = str(cookie_value.pop("value", "1"))
				if(cookie_value):
					_cookie_val += ";"
					#other key=val atrributes of cookie
					for k, v in cookie_value.items():
						_cookie_val += (" " + k)
						if(v is not True): # if mentioned as True just the key is suffice like httpsOnly, secureOnly
							_cookie_val += "=%s;"%(str(v))
				cookie_value = _cookie_val
			if(isinstance(cookie_value, str)):
				self._cookies_to_set[key] = cookie_value
		return ret

	def parse_request_body(self, post_data_bytes, headers):
		if(not post_data_bytes):
			return None
		self._post = _post_params = SanitizedDict()
		content_type_header = headers.get("Content-Type", "")
		if(headers and content_type_header.startswith("multipart/form-data")):
			for part in decoder.MultipartDecoder(post_data_bytes, content_type_header).parts:
				content_disposition = part.headers.pop(b'Content-Disposition', None)
				if(not content_disposition):
					continue
				attrs = {}
				for i in content_disposition.split(b';'):
					key_val = i.split(b'=')
					if(len(key_val) == 2):
						key = key_val[0].strip()
						val = key_val[1].strip(b'"\'').decode()
						attrs[key] = val
				name = attrs.pop(b'name', _OBJ_END_)
				if(name != _OBJ_END_):
					#if no additional headers, and no other attributes,
					#it's just a plain input field
					if(not part.headers and not attrs):
						_post_params[name] = part.text
					else: # could be a file or other type of data, preserve all data
						_post_params[name] = {
							"attrs": attrs,
							"data": part.content,
							"headers": part.headers
						}
		else:
			post_data_str = post_data_bytes.decode('utf-8')
			if(post_data_str[0] == '{'
				or content_type_header == "application/json"
			):
				_post_params.update(json.loads(post_data_str))
			else: # urlencoded form
				_post_params.update(parse_qs_modified(post_data_str))

		return self

	#{type: hook...} function to call to create that argument

	@classmethod
	def set_arg_type_hook(cls, _type, hook=None):
		if(hook == None):
			#using as decorator  @Request.set_arg_type_hook(User)
			def wrapper(func):
				cls.__argument_creator_hooks[_type] = func
				return func
			return wrapper

		cls.__argument_creator_hooks[_type] = hook

	#create the value of the argument based on type, default
	def make_arg(self, name, _type, default, validator):
		if(not _type):
			return self.get(name, default)
		if(_type == Request):  # request_params: Request
			return self
		elif(_type == Query):  # query: Query
			return self._get
		elif(_type == Headers):# headers: Headers
			return self._headers
		elif(_type == Body):# headers: Headers
			return self._post

		elif(isinstance(_type, Query)): # Query(id=str, abc=str)
			if(not _type._validations):
				return self._get
			ret = {}
			for k, attr_validation in _type._validations.items():
				ret[k] = attr_validation(self._get.get(k, escape_quotes=False))
			return DummyObject(ret)

		elif(isinstance(_type, Headers)): # Headers('user-agent')
			if(not _type._validations):
				return self._get
			ret = {}
			for k, attr_validation in _type._validations.items():
				ret.k = attr_validation(self._headers.get(k))
			return DummyObject(ret)

		elif(isinstance(_type, Body)): # Headers('user-agent')
			if(not _type._post):
				return None
			ret = {}
			for k, attr_validation in _type._validations.items():
				ret.k = attr_validation(self._post.get(k, escape_quotes=False))
			return DummyObject(ret)

		#type should be class at this point
		has_arg_creator_hook = Request.__argument_creator_hooks.get(_type) # if has an arg injector, use it
		if(has_arg_creator_hook):
			return has_arg_creator_hook(self)

		elif(issubclass(_type, Query)): # :LoginRequestQuery
			ret = _type()
			for k, attr_validation in _type._validations.items():
				setattr(ret, k, self._get.get(k, escape_quotes=False))
			return ret

		elif(issubclass(_type, Body)):
			ret = _type()
			for k, attr_validation in _type._validations.items():
				setattr(ret, k, self._post.get(k, escape_quotes=False))
			return ret

		ret = self.get(name)
		return validator(ret) if validator else ret # get or post

	def to_dict(self):
		return {"get": self._get, "post": self._post}


#contains all running apps
_all_apps = set()
'''
	@app.route('/index.html')
'''
class App:
	route_handlers = None
	request_handlers = None
	name = None
	stream_server = None

	def __init__(self, **kwargs):
		self.info = {k: kwargs.get(k, "-") for k in {"title", "description", "version"}}
		self.route_handlers = []
		self.request_handlers = []

	def route(
		self,
		regex,
		methods=None,
		title='',
		description='',
		max_body_size=None,
		before=None,
		after=None
	):
		methods = methods or ("GET", "POST")
		if(isinstance(methods, str)):
			methods = (methods,)

		def wrapper(func):
			#arguments order
			#all untyped arguments are ignored should possiblly come from regex groups
			#typed args should be constructed, some may be constructed by hooks
			#named arguments not typed as passed on as with defaults

			self.route_handlers.append({
				"regex": regex,
				"func": func,
				"methods": methods,
				"title": title,
				"description": description,
				"max_body_size": max_body_size,
				"after": [after] if callable(after) else list(after or []),
				"before": [before] if callable(before) else list(before or [])
			})
			#return func as if nothing's decorated! (so you can call function as is)
			return func

		return wrapper


	def start(self, port=80, handlers=[], **ssl_args):
		#sort descending order of the path lengths
		self.route_handlers.sort(key=functools.cmp_to_key(lambda x, y: len(y["regex"]) - len(x["regex"]))) # reverse sort by length
		
		#all additional handlers go the bottom of the current list
		handlers_with_methods = []
		for handler in handlers:
			if(isinstance(handler, (list, tuple))):
				if(len(handler) < 2 or len(handler) > 3):
					continue
				handlers_with_methods.append({"regex": handler[0], "func": handler[1], "methods": ("GET",)})
			elif(isinstance(handler, dict)):
				_handler = {"methods": ("GET", "POST")} # defaults
				_handler.update(handler)
				handlers_with_methods.append(_handler)

		#add a openapi.json route handler
		self.openapi = {"components": {"schemas": dict(schema_func.defs)}, "paths": {}, "openapi": "3.0.1", "info": self.info}
		if(IS_DEV):
			self.route_handlers.append({
				"regex": "/openapi\.json",
				"methods": ("GET", ),
				"func": lambda: (["Content-type: application/json"], json.dumps(self.openapi))
			})

		#add additional handlers specified at start
		self.route_handlers.extend(handlers_with_methods)

		#create all schemas
		schema_func.init()

		regexes_map = {} # cache/update/overwrite

		#iterate smaller path to larger to fill the request_handlers map
		#build request_handlers map => {regex: {GET: handler,...}...}
		for handler in reversed(self.route_handlers):
			regex = handler["regex"]

			#check and identify path params if any from regex
			handler["path_params"] = _path_params = {}
			if(isinstance(regex, str)):
				#special case where path params can be {:varname} {:+varname}
				#replacing regex with regex haha!
				_path_params.update({x: False for x in re.findall(r'\{([a-zA-Z_][a-zA-Z0-9_]*)\}', regex)}) # optional
				_path_params.update({x: True for x in re.findall(r'\{\+([a-zA-Z_][a-zA-Z0-9_]*)\}', regex)}) # required
				_path_params.update({x: True for x in re.findall(r'\{\*([a-zA-Z_][a-zA-Z0-9_]*)\}', regex)}) # required

				regex = re.sub(r'\{([a-zA-Z_][a-zA-Z0-9_]*)\}', '(?P<\g<1>>[^/]*)', regex)
				regex = re.sub(r'\{\+([a-zA-Z_][a-zA-Z0-9_]*)\}', '(?P<\g<1>>[^/]+)', regex)
				regex = re.sub(r'\{\*([a-zA-Z_][a-zA-Z0-9_]*)\}', '(?P<\g<1>>.*)', regex)
				regex = re.sub(r'\{\*\+([a-zA-Z_][a-zA-Z0-9_]*)\}', '(?P<\g<1>>.+)', regex)

			existing_regex_method_handlers = regexes_map.get(regex)
			if(not existing_regex_method_handlers):
				regexes_map[regex] = existing_regex_method_handlers = {}

				#check for full path matches, if the given regex doesn't match or end
				if(not regex.startswith("^")):
					regex = "^" + regex
				if(regex.endswith("/")):
					regex += "?"
				if(not regex.endswith("$")):
					regex = regex + "$"

				self.request_handlers.append((re.compile(regex), existing_regex_method_handlers))

			for method in handler["methods"]:
				existing_regex_method_handlers[method.upper()] = handler

			#infer arguments from the typing
			func = handler["func"]
			original_func = getattr(func, "_original", func)
			func_signature = inspect.signature(original_func)
			args_spec = func_signature.parameters

			args = [] # typed or untyped
			kwargs = []
			_arg_already_exists = set()
			for arg_name, _def in args_spec.items():
				
				if(arg_name in _arg_already_exists):
					continue
				_arg_already_exists.add(arg_name)

				_type = _def.annotation if _def.annotation != inspect._empty else None
				if(_def.default == inspect._empty):
					if(not _type):
						if(arg_name not in _path_params): # check if it's a path param
							continue
						else:
							_type = str # default type for path params
					#name, type, default, validator
					args.append((arg_name, _type, None, schema_func(_type)[1]))
				else:
					#name, type, default, validator
					kwargs.append((arg_name, _type, _def.default, schema_func(_type)[1]))

			handler["args"] = args
			handler["kwargs"] = kwargs
			if(func_signature.return_annotation != inspect._empty):
				handler["return"] = func_signature.return_annotation

			#fix before and after functions at start, cannot be modified later
			handler["before"] = tuple(handler.get("before", []) + Request._before_hooks)
			handler["after"] = tuple(handler.get("after", []) + Request._after_hooks)

			#openapi docs
			self.generate_openapi_doc(handler)
		
		#longer paths match first, hence reversed
		self.request_handlers.reverse()

		class CustomStreamServer(StreamServer):
			def do_close(self, *args):
				return

			#lets handle closing ourself
			def wrap_socket_and_handle(self, client_socket, address):
				# used in case of ssl sockets
				return self.handle(self.wrap_socket(client_socket, **self.ssl_args), address)

		LOG_SERVER("server_start", port=port)
		self.stream_server = CustomStreamServer(('', port), handle=self.handle_connection, **ssl_args)
		#keep a track

		_all_apps.add(self)

	def generate_openapi_doc(self, handler):
		regex_path = handler["regex"]
		regex_path = regex_path.replace("\\.", ".").replace("{+", "{").replace("{*", "{") # openapi doesn't support yet
		self.openapi["paths"][regex_path] = _api = {}
		func = handler["func"]
		args = handler["args"]
		kwargs = handler["kwargs"]
		return_type = handler.get("return", None)

		for method in handler["methods"]:
			_parameters = []
			_body_classes = []
			_api[method.lower()] = _method_def = {
				"summary": handler.get("title", ""),
				"description": handler.get("description", "")
			}
			#add header, query, cookie params
			for arg in (args + kwargs):
				_type = arg[1]
				if(_type and isinstance(_type, type)):
					if(issubclass(_type, Query)):
						for _key, _schema in _type._schema_["properties"].items():
							_param = {
								"in": "query",
								"name": _key,
								"schema": _schema
							}
							if(_key in _type._required):
								_param["required"] = True

							_parameters.append(_param)

					if(issubclass(_type, Body)):
						_body_classes.append(_type)

				if(isinstance(_type, (Headers, Query, Cookie))):
					_in = "header" if isinstance(_type, Headers) else ("query" if isinstance(_type, Query) else "cookie")
					for _k, _s in _type._properties.items():
						_param = {
							"in": _in,
							"name": _k,
							"schema": _s
						}
						if(_key in _type._required):
							_param["required"] = True
							
						_parameters.append(_param)


			#add path params
			for _name, _required in handler.get("path_params", {}).items():
				_param = {
					"in": "path",
					"name": _name,
					"schema": {"type": "string"}
				}
				#this is tricky,
				_param["required"] = True
					
				_parameters.append(_param)

			if(_parameters):
				_method_def["parameters"] = _parameters

			if(_body_classes):
				_schema_name = ""
				if(len(_body_classes) == 1):
					_schema_name = getattr(func, "_original", func).__name__
				elif(len(_body_classes) > 1):
					#merge multiple body classes
					_schema_name = "And".join(sorted([getattr(func, "_original", func).__name__ for x in _body_classes]))
					_properties = {}
					for _key, _schema in _type._schema_["properties"].items():
						_properties[_key] = _schema

					#add schemas
					self.openapi["components"]["schemas"][_schema_name] = {"type": "object", "properties": _properties}

				_method_def["requestBody"] = {
					"content": {
						"application/json": {"schema": {"$ref": "#/components/schemas/" + _schema_name}},
						"application/x-www-form-urlencoded": {"schema": {"$ref": "#/components/schemas/" + _schema_name}}
					},
					"required": True
				}

			_method_def["responses"] = _reponse = {"200": {"description": "OK"}}

			if(return_type):
				_reponse["content"] = {
					"application/*": {
						"schema": {
							"$ref": "#/components/schemas/" + getattr(func, "_original", func).__name__
						}
					}
				}

	def serve(self):
		self.stream_server.serve_forever()

	def stop(self):
		self.stream_server.stop()
		_all_apps.remove(self)

	@classmethod
	def response_body_to_parts(cls, ret):
		status = response_headers = body = None
		if(isinstance(ret, tuple)):
			body_len = len(ret)
			if(body_len > 0):
				body = ret[-1]
			if(body_len > 1):
				response_headers = ret[-2]
			if(body_len > 2):
				status = ret[-3]
		else:
			body = ret

		return status, response_headers, body

	def process_http_request(self, buffered_socket):
		resuse_socket_for_next_http_request = True
		request_line = buffered_socket.readuntil('\r\n', 4096, True) # ignore request lines > 4096 bytes
		if(not request_line):
			return
		post_data = None
		request_params = Request(buffered_socket)
		cur_millis = int(1000 * time.time())
		request_type = None
		request_path = None
		headers = None
		try:
			request_line = request_line.decode("utf-8")
			request_type, _request_line = request_line.split(" ", 1)
			_http_protocol_index = _request_line.rfind(" ")
			request_path \
				= request_params.path \
				= _request_line[: _http_protocol_index]
			#http_version = _request_line[_http_protocol_index + 1:]

			query_start_index = request_path.find("?")
			if(query_start_index != -1):
				query_string = request_path[query_start_index + 1:]
				request_params._get.update(parse_qs_modified(query_string))

				#strip query string from path
				request_path \
					= request_params.path \
					= request_path[:query_start_index]

			#find the handler
			handler = None
			for regex, method_handlers in self.request_handlers:
				request_path_match = regex.match(request_path)
				
				if(request_path_match != None):
					handler = method_handlers.get(request_type)
					if(not handler):
						#perform GET call by default
						LOG_SERVER(
							"handler_method_not_found",
							request_type=request_type,
							msg="performing GET by default"
						)
						if(request_type != "GET"):
							handler = method_handlers.get("GET")
					if(handler):
						#found a handler
						break

			if(not handler):
				resuse_socket_for_next_http_request = False
				raise Exception("Method not found")

			#parse the headers
			headers = request_params._headers
			max_headers_data_size = handler.get("max_headers_data_size") or HTTP_MAX_HEADERS_DATA_SIZE
			_headers_data_size = 0
			while(_headers_data_size < max_headers_data_size): # keep reading headers
				data = buffered_socket.readuntil('\r\n', max_headers_data_size, True)
				if(data == b''):
					break
				elif(data == None):
					return # won't resuse socket

				header_name_value_pair = data.split(b':', 1)
				if(len(header_name_value_pair) == 2):
					header_name , header_value = header_name_value_pair
					headers[header_name.lower().decode()] = header_value.lstrip(b' ').decode()
				
				_headers_data_size += len(data)


			#check if there is a content length or transfer encoding chunked
			content_length = int(headers.get("Content-Length", 0))
			_max_body_size = handler.get("max_body_size") or HTTP_MAX_REQUEST_BODY_SIZE
			if(content_length > 0):
				if(content_length < _max_body_size):
					post_data = buffered_socket.recvn(content_length)

			else: # handle chuncked encoding
				transfer_encoding = headers.get("transfer-encoding")
				if(transfer_encoding and "chunked" in transfer_encoding):
					post_data = bytearray()
					chunk_size = get_chunk_size_from_header(buffered_socket.readuntil('\r\n', 512, True))# hex
					while(chunk_size > 0 and len(post_data) + chunk_size < _max_body_size):
						data = buffered_socket.recvn(chunk_size)
						post_data.extend(data)
						buffered_socket.readuntil('\r\n', 8, False) # remove the trailing \r\n
						#next chunk size
						chunk_size = get_chunk_size_from_header(buffered_socket.readuntil('\r\n', 512, True))# hex
					#as bytes


			func = handler.get("func")
			#process cookies
			cookie_header = headers.get("Cookie", None)
			decoded_cookies = {}
			if(cookie_header):
				_temp = cookie_header.strip().split(";")
				for i in _temp:
					decoded_cookies.update(parse_qs_modified(i.strip()))
			request_params._cookies = decoded_cookies
			#process cookies end
			request_params.parse_request_body(post_data, headers)

			handler_args = []
			handler_kwargs = {}

			#update any path matches if they exist
			path_params = request_path_match.groupdict()
			for path_param, path_param_value in path_params.items():
				if(path_param_value):
					request_params._get[path_param] = path_param_value

			#set a reference to handler
			request_params.handler = handler

			for _args in handler["args"]:
				handler_args.append(request_params.make_arg(*_args))

			for name, *_args in handler["kwargs"]:
				handler_kwargs[name] = request_params.make_arg(name, *_args)

			before_handling_hooks = handler.get("before")
			after_handling_hooks = handler.get("after")

			response_from_handler = None
			for before_handling_hook in before_handling_hooks: # pre processing
				response_from_handler = before_handling_hook(request_params)
				if(response_from_handler):
					break
			
			if(not response_from_handler):
				#if before handlers return already something
				response_from_handler = func(*handler_args, **handler_kwargs)
				
				for after_handling_hook in after_handling_hooks: # post processing
					response_from_handler = after_handling_hook(request_params, response_from_handler)

				#check and respond to keep alive
				resuse_socket_for_next_http_request = headers.get('Connection', "").lower() == "keep-alive"
				
				##app specific headers
				#handle various return values from handler functions

			status, response_headers, body = App.response_body_to_parts(response_from_handler)


			if(status or body != I_AM_HANDLING_THE_SOCKET):
				status = str(status) if status else '200 OK'
				buffered_socket.send(b'HTTP/1.1 ', status, b'\r\n')

			if(response_headers or body != I_AM_HANDLING_THE_SOCKET):
				if(isinstance(response_headers, list)):
					#send only the specified headers
					for header in response_headers:
						buffered_socket.send(header, b'\r\n')

				else:
					if(isinstance(response_headers, dict)):
						#mix default stream headers
						#add default stream headers
						for key, val in default_stream_headers.items():
							if(key not in response_headers):
								buffered_socket.send(key, b': ', val, b'\r\n')

					elif(response_headers == None): # no headers => use default stream headers
						response_headers = default_stream_headers

					#send all basic headers
					for key, val in response_headers.items():
						buffered_socket.send(key, b': ', val, b'\r\n')

				#send any new cookies set
				if(request_params._cookies_to_set):
					for cookie_name, cookie_val in request_params._cookies_to_set.items():
						buffered_socket.send(b'Set-Cookie: ', cookie_name, b'=', cookie_val, b'\r\n')

				#send back keep alive
				if(resuse_socket_for_next_http_request):
					buffered_socket.send(b'Connection: keep-alive\r\n')

				if(body == I_AM_HANDLING_THE_SOCKET):
					#close the headers
					buffered_socket.send(b'\r\n')
	
			if(body == I_AM_HANDLING_THE_SOCKET):
				return I_AM_HANDLING_THE_SOCKET

			else:
				#just a variable to track api type responses
				response_data = None
				if(isinstance(body, (dict, list))):
					response_data = body
					body = json.dumps(body)

				#encode body
				if(isinstance(body, str)):
					body = body.encode()

				#close headers and send the body data
				if(body):
					#finalize all the headers
					buffered_socket.send(b'Content-Length: ', str(len(body)), b'\r\n\r\n')
					buffered_socket.send(body)
				else:
					buffered_socket.send(b'Content-Length: 0', b'\r\n\r\n')

			LOG_SERVER("http", response_status=status, request_type=request_type , path=request_path, wallclockms=int(1000 * time.time()) - cur_millis)

		except Exception as ex:
			stacktrace_string = traceback.format_exc()
			body = None
			if(blaster_config.server_error_page):
				body = blaster_config.server_error_page(
					request_type,
					request_path,
					request_params,
					headers,
					stacktrace_string=stacktrace_string
				)
			if(not body):
				body = b'Internal server error'
			buffered_socket.send(
					b'HTTP/1.1 ', b'502 Server error', b'\r\n',
					b'Connection: close', b'\r\n',
					b'Content-Length: ', str(len(body)), b'\r\n\r\n',
					body
			)
			LOG_ERROR(
				"http_error",
				exception_str=str(ex),
				stacktrace_string=stacktrace_string,
				request_type=request_type,
				request_path=request_path,
				request_params_str=str(request_params.to_dict())
			)
			if(IS_DEV):
				traceback.print_exc()
			#BREAK THIS CONNECTION
		
		if(resuse_socket_for_next_http_request):
			return REUSE_SOCKET_FOR_HTTP

	'''
		all the connection handling magic happens here
	'''
	def handle_connection(self, socket, address):
		buffered_socket = BufferedSocket(socket)
		close_socket = True
		while(True):
			ret = self.process_http_request(buffered_socket)
			if(ret == REUSE_SOCKET_FOR_HTTP):
				continue # try again
			elif(ret == I_AM_HANDLING_THE_SOCKET):
				close_socket = False
			break

		if(close_socket):
			buffered_socket.close()


@events.register_as_listener("sigint")
def stop_all_apps():
	LOG_SERVER("server_info", data="exiting all servers")
	global _is_server_running
	
	_is_server_running = False
	
	events.broadcast_event("blaster_before_shutdown")

	for app in list(_all_apps):
		app.stop() # should handle all connections gracefully

	events.broadcast_event("blaster_after_shutdown")


# create a global app for generic single server use
_main_app = App(title="Blaster", description="Built for speed and rapid prototyping..", version="0.0.368")

#generic global route handler
route = _main_app.route

#generic glonal route handler
def start_server(*args, **kwargs):
	_main_app.start(*args, **kwargs)
	_main_app.serve()


def redirect_http_to_https():
	def redirect(path, request_params=None, user=None):
		host = request_params.HEADERS('host')
		if(not host):
			return "404", {}, "Not understood"
		return "302 Redirect", ["Location: https://%s%s?%s"%(host, request_params.path, request_params.query_string)], "Need Https"

	http_app = App("port=80")
	http_app.start(port=80, handlers=[("(.*)", redirect)])
	gevent.spawn(http_app.serve)
	return http_app


def is_server_running():
	return _is_server_running


#### additional utils


# File handler for blaster server
def static_file_handler(_base_folder_path_, default_file_path="index.html", file_not_found_page_cb=None):
	cached_file_data = {}
	gevent_lock = Lock()

	def file_handler(path, request_params: Request):
		if(not path):
			path = default_file_path
		#from given base_path
		path = os.path.abspath(_base_folder_path_ + str(path))
		if(not path.startswith(_base_folder_path_)):
			return "404 Not Found", [], "Invalid path submitted"
		file_data = cached_file_data.get(path, None)
		resp_headers = None

		if(not file_data or time.time() * 1000 - file_data[2] > 1000 if IS_DEV else 2 * 60 * 1000): # 1 millis
			gevent_lock.acquire()
			file_hash_key = path + urlencode(request_params._get)[:400]
			file_data = cached_file_data.get(file_hash_key, None)
			if(not file_data or time.time() * 1000 - file_data[2] > 1000): # 1 sec
				#put a lock here
				try:
					data = open(path, "rb").read()
					mime_type_headers = get_mime_type_from_filename(path)
					if(mime_type_headers):
						resp_headers = {
											'Content-Type': mime_type_headers.mime_type,
											'Cache-Control': 'max-age=31536000'
									}
					
					file_data = (data, resp_headers, time.time() * 1000)
					cached_file_data[file_hash_key] = file_data
				except Exception as ex:
					if(file_not_found_page_cb):
						file_data = (
										file_not_found_page_cb(path, request_params=None, headers=None, post_data=None),
										None,
										time.time() * 1000
									)
					else:
						file_data = ("--NO-FILE--", None, time.time() * 1000)
					
			gevent_lock.release()
					
		data, resp_headers, last_updated = file_data
		return resp_headers, data
	
	return file_handler

####################Auto api response utilities
class AutoApiResponse:
	data = None
	templates = None
	frame_template = None
	meta_tags = None
	return_type_param = None

	def __init__(self, data, template=None,
			frame_template=None, templates=None, meta_tags=None,
			return_type_param="return_type"
		):
		self.data = data
		self.frame_template = frame_template
		self.meta_tags = meta_tags
		self.templates = {"content": template}
		self.return_type_param = return_type_param
		if(templates):
			self.templates.update(templates)

def auto_api_response(request_params, response):
	(status, headers, body) = App.response_body_to_parts(response)
	if(isinstance(body, AutoApiResponse)):
		#just keep a reference as we overwrite body variable
		blaster_response_obj = body
		#set body as default to repose_obj.data
		body = blaster_response_obj.data
		return_type = request_params.get(blaster_response_obj.return_type_param)
		#standard return_types -> json, content, None(=>content inside frame)
		if(return_type != "json"):
			#if template file is given and body should be a dict for the template
			templates = blaster_response_obj.templates
			#if no template given or returns just the blaster_response_obj.data
			template_exists \
				= (return_type and templates.get(return_type))\
				or blaster_response_obj.templates.get("content")

			if(template_exists and isinstance(body, dict)):
				tmpl_rendered = template_exists.render(
					request_params=request_params,
					**body
				)

				frame_template = blaster_response_obj.frame_template
				#if there is a return type, just retutn the template
				#render the full frame
				if(return_type or not frame_template):
					body = tmpl_rendered # partial

				else: # return type is nothing -> return full frame
					body = frame_template.render(
						body_content=tmpl_rendered,
						meta_tags=blaster_response_obj.meta_tags
					)
	return status, headers, body



#add a default arg hook
def _get_web_socket_handler(request_params):
	set_socket_options(request_params.sock)
	ws = WebSocketServerHandler(request_params.sock) # sends handshake automatically
	ws.do_handshake(request_params.HEADERS())
	return ws


Request.set_arg_type_hook(WebSocketServerHandler, _get_web_socket_handler)

#sigint event broadcast
gevent.signal_handler(signal.SIGINT, lambda : events.broadcast_event("sigint"))
