'''
Created on 22-Aug-2017

@author: abhinav
'''
import os
import gevent
import ujson as json
import re
import urllib.parse
import time
import functools
import traceback
import inspect
import socket
from gevent.socket import socket as GeventSocket
from gevent.server import StreamServer
from requests_toolbelt.multipart import decoder

from . import req_ctx
from .tools import SanitizedDict, DummyObject,\
	set_socket_fast_close_options, BufferedSocket, ltrim
from .utils import events
from .utils.data_utils import FILE_EXTENSION_TO_MIME_TYPE
from .logging import LOG_ERROR, LOG_SERVER
from .schema import Int, Object, Required, Str, schema as schema_func
from .websocket.server import WebSocketServerHandler
from .config import IS_DEV, BLASTER_HTTP_TIMEOUT_WARN_THRESHOLD

if(IS_DEV):
	# dev specific config
	from .config import DEV_FORCE_ACCESS_CONTROL_ALLOW_ORIGIN,\
	DEV_FORCE_ACCESS_CONTROL_ALLOW_ORIGIN_ALL


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

# handler takes care of status line, headers and body
I_AM_HANDLING_THE_SOCKET = object()


REUSE_SOCKET_FOR_HTTP = object()
_OBJ_END_ = object()

# some random constants
_1_KB_ = 1024
_1_MB_ = 1024 * _1_KB_
HTTP_MAX_REQUEST_BODY_SIZE = 2 * _1_MB_  # 1 mb
HTTP_MAX_HEADERS_DATA_SIZE = 16 * _1_KB_  # 16kb


def get_chunk_size_from_header(chunk_header):
	ret = 0
	i = 0
	chunk_header_len = len(chunk_header)
	A = ord('A')
	Z = ord('Z')
	_0 = ord('0')
	_9 = ord('9')
	while(i < chunk_header_len):
		c = ord(chunk_header[i])
		if(c >= A and c <= Z):
			ret = ret * 16 + (10 + c - A)
		elif(c >= _0 and c <= _9):
			ret = ret * 16 + (c - _0)
		else:
			return ret
		i += 1
	return ret


# parse query string
def parse_qs_modified(qs, keep_blank_values=True, strict_parsing=False):
	_dict = {}
	for name, value in urllib.parse.parse_qsl(qs, keep_blank_values, strict_parsing):
		if name.endswith("[]"):
			_values = _dict.get(name)
			if(_values == None):
				_dict[name] = _values = []
			elif(not isinstance(_values, list)):  # make it a list of values
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
	def __init__(self, *key, **kwargs):
		if(key):
			kwargs[key] = Required[str]
		super().__init__(**kwargs)

class Headers(Object):
	def __init__(self, *key, **kwargs):
		if(key):
			kwargs[key] = Required[str]
		super().__init__(**kwargs)

class Cookie(Object):
	def __init__(self, *key, **kwargs):
		if(key):
			kwargs[key] = Required[str]
		super().__init__(**kwargs)

class Body(Object):
	def __init__(self, *key, **kwargs):
		if(key):
			kwargs[key] = Required[str]
		super().__init__(**kwargs)

# custom argument hooks
_argument_creator_hooks = {}

class Request:
	# class level
	_before_hooks = []
	_after_hooks = []

	after = []

	# instance level
	sock = None
	_params = None
	_body = None
	_body_raw = None
	_cookies = None
	_headers = None
	# cookies to send to client
	_cookies_to_set = None
	# url data, doesn't contain query string
	path = None

	@classmethod
	def before(cls, func=None):
		if(func and callable(func)):
			# decorating
			Request._before_hooks.append(func)
			return func
		return Request._before_hooks

	@classmethod
	def after(cls, func=None):
		if(func and callable(func)):
			# decorating
			Request._after_hooks.append(func)
			return func
		return Request._after_hooks

	def __init__(self, buffered_socket):
		self._params = SanitizedDict()  # empty params by default
		self._headers = HeadersDict()
		self.sock = buffered_socket
		self.ctx = DummyObject()

	# searches in post and get
	def get(self, key, default=None, **kwargs):
		val = _OBJ_END_
		# try post params first
		if(self._body):
			val = self._body.get(key, default=_OBJ_END_, **kwargs)
		# try get param next
		if(val == _OBJ_END_ and self._params):
			val = self._params.get(key, default=_OBJ_END_, **kwargs)

		if(val == _OBJ_END_):
			return default
		return val

	def __setitem__(self, key, val):
		self._params[key] = val

	def __getitem__(self, key):
		ret = self.get(key, default=_OBJ_END_)
		if(ret == _OBJ_END_):
			raise LookupError()
		return ret

	def __getattr__(self, key):
		ret = self.get(key, default=_OBJ_END_)
		if(ret == _OBJ_END_):
			raise AttributeError("{} not found".format(key))
		return ret

	def PARAMS(self, key=None, **kwargs):
		if(key == None):
			return self._params
		return self._params.get(key, **kwargs)

	def BODY(self, key=None, **kwargs):
		if(key == None):
			return self._body
		if(self._body):
			return self._body.get(key, **kwargs)
		return None

	def HEADERS(self, key=None, default=None):
		if(key == None):
			return self._headers
		return self._headers.get(key, default=default)

	def COOKIES(self, key=None, default=None):
		if(key == None):
			return self._cookies					
		return self._cookies.get(key, default)
		
	def SET_COOKIE(self, key, cookie_value):
		if(self._cookies_to_set == None):
			self._cookies_to_set = {}
		if(isinstance(cookie_value, dict)):
			_cookie_val = str(cookie_value.pop("value", "1"))
			if(cookie_value):
				_cookie_val += ";"
				# other key=val atrributes of cookie
				for k, v in cookie_value.items():
					_cookie_val += (" " + k)
					if(v is not True):
						# if mentioned as True just the key
						# is suffice like httpsOnly, secureOnly
						_cookie_val += "={};".format(v)
			cookie_value = _cookie_val
		if(isinstance(cookie_value, str)):
			self._cookies_to_set[key] = cookie_value
		

	def parse_request_body(self, post_data_bytes, headers):
		if(not post_data_bytes):
			return None
		self._body = _body = SanitizedDict()
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
					# if no additional headers, and no other attributes,
					# it's just a plain input field
					if(not part.headers and not attrs):
						_body[name] = part.text
					else:  # could be a file or other type of data, preserve all data
						_body[name] = {
							"attrs": attrs,
							"data": part.content,
							"headers": part.headers
						}
		else:
			post_data_str = post_data_bytes.decode('utf-8')
			if(
				post_data_str[0] == '{'
				or content_type_header == "application/json"
			):
				_body.update(json.loads(post_data_str))
			else:  # urlencoded form
				_body.update(parse_qs_modified(post_data_str))

		return self

	# {type: hook...} function to call to create that argument

	@staticmethod
	def set_arg_type_hook(_type, hook=None):
		if(hook == None):
			# using as decorator  @Request.set_arg_type_hook(User)
			def wrapper(func):
				_argument_creator_hooks[_type] = func
				return func
			return wrapper
		else:
			_argument_creator_hooks[_type] = hook

	@staticmethod
	def wrap_arg_hook_for_defaults(arg_type_hook, name, default):
		def wrapper(req):
			ret = arg_type_hook(req)
			if(ret != None):
				return ret
			elif(default != _OBJ_END_):
				return default
			raise TypeError("need {} with this request".format(name))
		return wrapper

	# create the value of the argument based on type, default
	@classmethod
	def arg_generator(cls, name, _type, default):
		if(_type in (str, int, float)):
			return lambda req: (_val:= req.get(name, default)) and _type(_val)
		elif(_type == Request):  # request_params: Request
			return lambda req: req
		elif(_type == Query):  # query: Query
			return lambda req: req._params
		elif(_type == Headers):  # headers: Headers
			return lambda req: req._headers
		elif(_type == Body):  # headers: Headers
			return lambda req: req._body
		elif(isinstance(_type, Query)):  # Query(id=str, abc=str)
			return lambda req: _type.from_dict(req._params)

		elif(isinstance(_type, Headers)):  # Headers('user-agent')
			return lambda req: _type.from_dict(req._headers)

		elif(isinstance(_type, Body)):  # Headers('user-agent')
			return lambda req: _type.from_dict(req._body)

		# type should be class at this point
		# if has an arg injector, use it
		has_arg_creator_hook = _argument_creator_hooks.get(_type)
		if(has_arg_creator_hook):
			return Request.wrap_arg_hook_for_defaults(has_arg_creator_hook, name, default)

		elif(isinstance(_type, type) and issubclass(_type, Body)):
			return lambda req: _type.from_dict(req._body)

		elif(isinstance(_type, type) and issubclass(_type, Query)):  # :LoginRequestQuery
			return lambda req: _type.from_dict(req._params)

		elif(
			isinstance(_type, Object)
			or (isinstance(_type, type) and issubclass(_type, Object))
		):
			return lambda req: _type.from_dict(req)

		else:
			def _no_type_arg(req):
				ret = req.get(name, default=_OBJ_END_)
				if(ret == _OBJ_END_):
					if(default == _OBJ_END_):
						# error
						raise TypeError("{:s} field is required".format(name))
					return default
				return ret
			return _no_type_arg

	def to_dict(self):
		return {"get": self._params, "post": self._body}


# contains all running apps
_all_apps = set()


# @app.route('/index.html')
class App:
	route_handlers = None
	request_handlers = None
	name = None
	stream_server = None
	server_error_page = None

	def __init__(self, server_error_page=None, **kwargs):
		self.info = {
			k: kwargs.get(k, "-") for k in {"title", "description", "version"}
		}
		self.route_handlers = []
		self.request_handlers = []
		# error
		self.server_error_page = server_error_page

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
			# arguments order
			# all untyped arguments are ignored should possiblly come from regex groups
			# typed args should be constructed, some may be constructed by hooks
			# named arguments not typed as passed on as with defaults

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
			# return func as if nothing's decorated! (so you can call function as is)
			return func

		return wrapper

	def start(self, port=80, handlers=[], **ssl_args):
		# sort descending order of the path lengths
		self.route_handlers.sort(
			key=functools.cmp_to_key(lambda x, y: len(y["regex"]) - len(x["regex"]))
		)  # reverse sort by length

		# all additional handlers go the bottom of the current list
		handlers_with_methods = []
		for handler in handlers:
			if(isinstance(handler, (list, tuple))):
				if(len(handler) < 2 or len(handler) > 3):
					continue
				handlers_with_methods.append({
					"regex": handler[0], "func": handler[1], "methods": ("GET",)
				})
			elif(isinstance(handler, dict)):
				_handler = {"methods": ("GET", "POST")}  # defaults
				_handler.update(handler)
				handlers_with_methods.append(_handler)

		# add a openapi.json route handler
		self.openapi = {
			"components": {
				"schemas": schema_func.defs
			},
			"paths": {},
			"openapi": "3.0.1",
			"info": self.info
		}
		if(IS_DEV):
			self.route_handlers.append({
				"regex": r"/openapi\.json",
				"methods": ("GET", ),
				"func": lambda: (["Content-type: application/json"], json.dumps(self.openapi))
			})

		# add additional handlers specified at start
		self.route_handlers.extend(handlers_with_methods)

		# create all schemas
		schema_func.init()

		regexes_map = {}  # cache/update/overwrite

		# iterate smaller path to larger to fill the request_handlers map
		# build request_handlers map => {regex: {GET: handler,...}...}
		for handler in reversed(self.route_handlers):
			regex = handler["regex"]

			# check and identify path params if any from regex
			handler["path_params"] = _path_params = {}
			if(isinstance(regex, str)):
				# special case where path params can be {:varname} {:+varname}
				# replacing regex with regex haha!
				_path_params.update({x: False for x in re.findall(r'\{([a-zA-Z_][a-zA-Z0-9_]*)\}', regex)})  # {varname} -> may exist
				_path_params.update({x: True for x in re.findall(r'\{\+([a-zA-Z_][a-zA-Z0-9_]*)\}', regex)})  # {+varname} -> should exist
				_path_params.update({x: True for x in re.findall(r'\{\*([a-zA-Z_][a-zA-Z0-9_]*)\}', regex)})  # {*varname} -> may exist including '/'

				regex = re.sub(r'\{([a-zA-Z_][a-zA-Z0-9_]*)\}', '(?P<\g<1>>[^/]*)', regex)
				regex = re.sub(r'\{\+([a-zA-Z_][a-zA-Z0-9_]*)\}', '(?P<\g<1>>[^/]+)', regex)
				regex = re.sub(r'\{\*([a-zA-Z_][a-zA-Z0-9_]*)\}', '(?P<\g<1>>.*)', regex)
				regex = re.sub(r'\{\*\+([a-zA-Z_][a-zA-Z0-9_]*)\}', '(?P<\g<1>>.+)', regex)

			existing_regex_method_handlers = regexes_map.get(regex)
			if(not existing_regex_method_handlers):
				regexes_map[regex] = existing_regex_method_handlers = {}

				# check for full path matches, if the given regex doesn't match or end
				if(not regex.startswith("^")):
					regex = "^" + regex
				if(regex.endswith("/")):
					regex += "?"
				if(not regex.endswith("$")):
					regex = regex + "$"

				self.request_handlers.append(
					(re.compile(regex), existing_regex_method_handlers)
				)

			for method in handler["methods"]:
				existing_regex_method_handlers[method.upper()] = handler

			# infer arguments from the typing
			func = handler["func"]
			original_func = getattr(func, "_original", func)
			func_signature = inspect.signature(original_func)
			args_spec = func_signature.parameters

			args = []  # typed or untyped
			kwargs = []
			arg_generators = []
			kwarg_generators = []
			_arg_already_exists = set()
			for arg_name, _def in args_spec.items():

				if(arg_name in _arg_already_exists):
					continue
				_arg_already_exists.add(arg_name)

				_type = _def.annotation if _def.annotation != inspect._empty else None
				_default = _OBJ_END_
				# check if it's pathparam, it's default is str
				if(not _type and arg_name in _path_params):
					_type = str  # default type for path params
					_default = ""

				if(_def.default == inspect._empty):
					args.append((arg_name, _type, _default))
					arg_generators.append(Request.arg_generator(arg_name, _type, _default))
				else:
					# name, type, default, validator
					kwargs.append((arg_name, _type, _def.default))
					if(_type):
						kwarg_generators.append((arg_name, Request.arg_generator(arg_name, _type, _def.default)))

			handler["args"] = args
			handler["kwargs"] = kwargs
			handler["arg_generators"] = arg_generators
			handler["kwarg_generators"] = kwarg_generators

			if(func_signature.return_annotation != inspect._empty):
				handler["return"] = func_signature.return_annotation

			# fix before and after functions at start, cannot be modified later
			handler["before"] = tuple(handler.get("before", []) + Request._before_hooks)
			handler["after"] = tuple(handler.get("after", []) + Request._after_hooks)

			# openapi docs
			self.generate_openapi_doc(handler)

		# longer paths match first, hence reversed
		self.request_handlers.reverse()

		class CustomStreamServer(StreamServer):
			def do_close(self, *args):
				return

			@classmethod
			def get_listener(cls, address, backlog=None, family=None):
				sock = GeventSocket(family=family)
				sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
				sock.bind(address)
				sock.listen(backlog or cls.backlog or 256)
				sock.setblocking(0)
				return sock

			# lets handle closing ourself
			def wrap_socket_and_handle(self, client_socket, address):
				# used in case of ssl sockets
				return self.handle(
					self.wrap_socket(client_socket, **self.ssl_args),
					address
				)

		LOG_SERVER("server_start", port=port)
		self.stream_server = CustomStreamServer(
			('', port),
			handle=self.handle_connection,
			**ssl_args
		)
		# keep a track

		_all_apps.add(self)

	def generate_openapi_doc(self, handler):
		regex_path = handler["regex"]
		regex_path = regex_path\
			.replace("\\.", ".")\
			.replace("{+", "{")\
			.replace("{*", "{")  # openapi doesn't support yet

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
			# add header, query, cookie params
			may_be_params = []
			for arg in (args + kwargs):
				_type = arg[1]
				if(_type and isinstance(_type, type)):
					# type
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

					elif(issubclass(_type, Body)):
						_body_classes.append(_type)

					else:
						may_be_params.append(arg)

				elif(isinstance(_type, (Headers, Query, Cookie))):
					# instnce
					_in = "header" \
						if isinstance(_type, Headers)\
						else ("query" if isinstance(_type, Query) else "cookie")

					for _k, _s in _type._properties.items():
						_param = {
							"in": _in,
							"name": _k,
							"schema": _s
						}
						if(_key in _type._required):
							_param["required"] = True

						_parameters.append(_param)
				else:
					may_be_params.append(arg)

			# add path params
			_path_params = handler.get("path_params", {})
			for _name, _required in _path_params.items():
				_param = {
					"in": "path",
					"name": _name,
					"schema": {"type": "string"}
				}
				# this is tricky,
				_param["required"] = True

				_parameters.append(_param)

			for _arg in may_be_params:
				_name, _type, *_ = _arg
				if(_name in _path_params):
					continue  # this is a path param
				_schema, _ = schema_func(_type)
				if(_schema):
					_parameters.append({
						"in": "query",
						"name": _name,
						"schema": _schema
					})

			if(_parameters):
				_method_def["parameters"] = _parameters

			if(_body_classes):
				_schema_name = ""
				if(len(_body_classes) == 1):
					_schema_name = getattr(func, "_original", func).__name__
				elif(len(_body_classes) > 1):
					# merge multiple body classes
					_schema_name = "And".join(
						sorted(
							[getattr(func, "_original", func).__name__ for x in _body_classes]
						)
					)
					_properties = {}
					for _type in _body_classes:
						for _key, _schema in _type._schema_["properties"].items():
							_properties[_key] = _schema

					# add schemas
					self.openapi["components"]["schemas"][_schema_name] = {
						"type": "object", "properties": _properties
					}

				_method_def["requestBody"] = {
					"content": {
						"application/json": {
							"schema": {"$ref": "#/components/schemas/" + _schema_name}
						},
						"application/x-www-form-urlencoded": {
							"schema": {"$ref": "#/components/schemas/" + _schema_name}
						}
					},
					"required": True
				}

			_method_def["responses"] = _reponse = {"200": {"description": "OK"}}

			if(return_type):
				_reponse["content"] = {
					"application/*": {
						"schema": {
							"$ref": "#/components/schemas/{:s}".format(
								getattr(func, "_original", func).__name__
							)
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
		# ignore request lines > 4096 bytes
		request_line = buffered_socket.readuntil('\r\n', 4096, True)
		if(not request_line):
			return
		post_data = None
		request_params = req_ctx.req = Request(buffered_socket)
		cur_millis = req_ctx.timestamp = int(1000 * time.time())
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
			# special issue, seeing request path will full domain name
			if(request_path.startswith("http")):
				if(
					request_path.startswith("https://")
					or request_path.startswith("http://")
				):
					_index = request_path.index("/", 8)
					if(_index != -1):
						request_path = request_path[_index:]

			# http_version = _request_line[_http_protocol_index + 1:]
			query_start_index = request_path.find("?")
			if(query_start_index != -1):
				query_string = request_path[query_start_index + 1:]
				request_params._params.update(parse_qs_modified(query_string))

				# strip query string from path
				request_path \
					= request_params.path \
					= request_path[:query_start_index]

			# find the handler
			handler = None
			for regex, method_handlers in self.request_handlers:
				request_path_match = regex.match(request_path)

				if(request_path_match != None):
					handler = method_handlers.get(request_type)
					if(not handler):
						# perform GET call by default
						LOG_SERVER(
							"handler_method_not_found",
							request_type=request_type,
							msg="performing GET by default"
						)
						if(request_type != "GET"):
							handler = method_handlers.get("GET")
					if(handler):
						# found a handler
						break

			if(not handler):
				resuse_socket_for_next_http_request = False
				raise Exception("Method not found")

			# parse the headers
			headers = request_params._headers
			max_headers_data_size \
				= handler.get("max_headers_data_size") or HTTP_MAX_HEADERS_DATA_SIZE
			_headers_data_size = 0
			while(_headers_data_size < max_headers_data_size):  # keep reading headers
				data = buffered_socket.readuntil('\r\n', max_headers_data_size, True)
				if(data == b''):
					break
				elif(data == None):
					return  # won't resuse socket

				header_name_value_pair = data.split(b':', 1)
				if(len(header_name_value_pair) == 2):
					header_name, header_value = header_name_value_pair
					headers[header_name.lower().decode()] = header_value.lstrip(b' ').decode()

				_headers_data_size += len(data)

			req_ctx.client_name = headers.get("x-client")
			# check if there is a content length or transfer encoding chunked
			content_length = int(headers.get("Content-Length", 0))
			_max_body_size = handler.get("max_body_size") or HTTP_MAX_REQUEST_BODY_SIZE
			if(content_length > 0):
				if(content_length < _max_body_size):
					post_data = buffered_socket.recvn(content_length)

			else:  # handle chuncked encoding
				transfer_encoding = headers.get("transfer-encoding")
				if(transfer_encoding and "chunked" in transfer_encoding):
					post_data = bytearray()
					chunk_size = get_chunk_size_from_header(
						buffered_socket.readuntil('\r\n', 512, True)
					)  # hex
					while(chunk_size > 0 and len(post_data) + chunk_size < _max_body_size):
						data = buffered_socket.recvn(chunk_size)
						post_data.extend(data)
						buffered_socket.readuntil('\r\n', 8, False)  # remove the trailing \r\n
						# next chunk size
						chunk_size = get_chunk_size_from_header(
							buffered_socket.readuntil('\r\n', 512, True)
						)  # hex
					# as bytes

			func = handler.get("func")
			# process cookies
			cookie_header = headers.get("Cookie", None)
			decoded_cookies = {}
			if(cookie_header):
				_temp = cookie_header.strip().split(";")
				for i in _temp:
					_kv = i.strip().split("=", 1)
					if(len(_kv) > 1):
						decoded_cookies[_kv[0]] = _kv[1]
			request_params._cookies = decoded_cookies
			# process cookies end
			request_params._body_raw = post_data
			request_params.parse_request_body(post_data, headers)

			handler_args = []
			handler_kwargs = {}

			# update any path matches if they exist
			path_params = request_path_match.groupdict()
			for path_param, path_param_value in path_params.items():
				if(path_param_value):
					request_params._params[path_param] = path_param_value

			# set a reference to handler
			request_params.handler = handler

			for _arg_generator in handler["arg_generators"]:
				handler_args.append(_arg_generator(request_params))

			for name, _kwarg_generator in handler["kwarg_generators"]:
				handler_kwargs[name] = _kwarg_generator(request_params)

			before_handling_hooks = handler.get("before")
			after_handling_hooks = handler.get("after")

			response_from_handler = None
			for before_handling_hook in before_handling_hooks:  # pre processing
				response_from_handler = before_handling_hook(request_params)
				if(response_from_handler):
					break

			if(not response_from_handler):
				# if before handlers return already something
				response_from_handler = func(*handler_args, **handler_kwargs)

				for after_handling_hook in after_handling_hooks:  # post processing
					response_from_handler = after_handling_hook(request_params, response_from_handler)

				# check and respond to keep alive
				resuse_socket_for_next_http_request \
					= headers.get('Connection', "").lower() == "keep-alive"

				# app specific headers
				# handle various return values from handler functions

			status, response_headers, body = App.response_body_to_parts(response_from_handler)

			if(status or (body != I_AM_HANDLING_THE_SOCKET)):
				# we will send the status, either default
				# or given from response
				status = str(status) if status else '200 OK'
				buffered_socket.send(b'HTTP/1.1 ', status, b'\r\n')

			if(response_headers or (body != I_AM_HANDLING_THE_SOCKET)):
				# we will be sending headers of this request
				# either default or given by response
				if(isinstance(response_headers, list)):
					# send only the specified headers
					for header in response_headers:
						buffered_socket.send(header, b'\r\n')

				else:
					if(isinstance(response_headers, dict)):
						# mix default stream headers
						# add default stream headers
						for key, val in default_stream_headers.items():
							if(key not in response_headers):
								buffered_socket.send(key, b': ', val, b'\r\n')

					elif(response_headers == None):  # no headers => use default stream headers
						response_headers = default_stream_headers

					# send all basic headers
					for key, val in response_headers.items():
						buffered_socket.send(key, b': ', val, b'\r\n')

				if(IS_DEV):
					if(DEV_FORCE_ACCESS_CONTROL_ALLOW_ORIGIN_ALL):
						buffered_socket.send(b'Access-Control-Allow-Origin: *\r\n')
					elif(DEV_FORCE_ACCESS_CONTROL_ALLOW_ORIGIN):
						buffered_socket.send(b'Access-Control-Allow-Origin: ', headers.get("origin", "*"), b'\r\n')

				# send any new cookies set
				if(request_params._cookies_to_set):
					for cookie_name, cookie_val in request_params._cookies_to_set.items():
						buffered_socket.send(b'Set-Cookie: ', cookie_name, b'=', cookie_val, b'\r\n')

				# send back keep alive
				if(resuse_socket_for_next_http_request):
					buffered_socket.send(b'Connection: keep-alive\r\n')

				if(body == I_AM_HANDLING_THE_SOCKET):
					# close the headers
					buffered_socket.send(b'\r\n')

			if(body == I_AM_HANDLING_THE_SOCKET):
				LOG_SERVER(
					"http_socket", request_type=request_type,
					path=request_path, wallclockms=int(1000 * time.time()) - cur_millis
				)

				return I_AM_HANDLING_THE_SOCKET

			else:
				# just a variable to track api type responses
				if(isinstance(body, (dict, list))):
					body = json.dumps(body)
					buffered_socket.send(b'Content-Type: application/json\r\n')

				# encode body
				if(isinstance(body, str)):
					body = body.encode()

				# close headers and send the body data
				content_length = 0
				if(body):
					# finalize all the headers
					buffered_socket.send(b'Content-Length: ', str(content_length:= len(body)), b'\r\n\r\n')
					buffered_socket.send(body)
				else:
					buffered_socket.send(b'Content-Length: 0', b'\r\n\r\n')

				_wallclock_ms = int(1000 * time.time()) - cur_millis
				LOG_SERVER(
					"http", response_status=status, request_type=request_type,
					path=request_path, content_length=content_length,
					wallclockms=_wallclock_ms
				)
				if(_wallclock_ms > BLASTER_HTTP_TIMEOUT_WARN_THRESHOLD):
					LOG_ERROR(
						"http_took_long", response_status=status, request_type=request_type,
						path=request_path, content_length=content_length,
						body_len=request_params._body_raw and len(request_params._body_raw),
						request_params_str=str(request_params.to_dict())[:16 * _1_KB_],
						wallclockms=_wallclock_ms
					)

		except Exception as ex:
			stacktrace_string = traceback.format_exc()
			body = None
			resp_headers = []
			# if it's a type error, send it
			if(isinstance(ex, TypeError)):
				body = ex.args and ex.args[0]

			# if given as error page handler
			if(not body and self.server_error_page):
				body = self.server_error_page(
					request_type,
					request_path,
					request_params,
					headers,
					stacktrace_string=stacktrace_string
				)

			if(isinstance(body, (dict, list))):
				body = json.dumps(body)
				resp_headers.append("Content-Type: application/json\r\n")

			body = body or b'Internal server error'
			# send the error response
			buffered_socket.send(
				b'HTTP/1.1 ', b'502 Server error', b'\r\n',
				*resp_headers,
				b'Connection: close', b'\r\n',
				b'Content-Length: ', str(len(body)), b'\r\n\r\n',
				body
			)
			LOG_ERROR(
				"http_error",
				exception_str=str(ex),
				stacktrace_string=stacktrace_string,
				request_type=request_type,
				request_line=request_line,
				request_params_str=str(request_params.to_dict())
			)
			if(IS_DEV):
				traceback.print_exc()
			# BREAK THIS CONNECTION

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
				continue  # try again
			elif(ret == I_AM_HANDLING_THE_SOCKET):
				close_socket = False
			break

		if(close_socket):
			buffered_socket.close()

@events.register_listener("blaster_exit0")
def stop_all_apps():
	LOG_SERVER("server_info", data="exiting all servers")
	global _is_server_running

	_is_server_running = False

	for app in list(_all_apps):
		app.stop()  # should handle all connections gracefully

# create a global app for generic single server use
_main_app = App(title="Blaster", description="Built for speed and rapid prototyping..", version="0.0.368")

# generic global route handler
route = _main_app.route


# generic glonal route handler
def start_server(*args, **kwargs):
	_main_app.start(*args, **kwargs)
	_main_app.serve()


def redirect_http_to_https():
	def redirect(path, request_params=None, user=None):
		host = request_params.HEADERS('host')
		if(not host):
			return "404", {}, "Not understood"
		return "302 Redirect", [
			"Location: https://{:s}{:s}?{:s}".format(
				host, request_params.path,
				request_params.query_string
			)
		], "Need Https"

	http_app = App("port=80")
	http_app.start(port=80, handlers=[("(.*)", redirect)])
	gevent.spawn(http_app.serve)
	return http_app


def is_server_running():
	return _is_server_running


# additional utils

DEFAULT_STATIC_FILE_CACHE = {}

def static_file_handler(
	url_path,
	_base_folder_path_,
	default_file="index.html",
	file_not_found_cb=None,
	file_cache=DEFAULT_STATIC_FILE_CACHE,
	dynamic_files=IS_DEV # always reload from filesystem if not in cache
):
	# both paths should start with /
	if(url_path[-1] != "/"):
		url_path += "/"  # it must be a folder

	if(_base_folder_path_[-1] != "/"):
		_base_folder_path_ += "/"  # it must be a folder

	def get_file_resp(path):
		file_path = os.path.abspath(_base_folder_path_ + str(path))

		if(not file_path.startswith(_base_folder_path_)):
			return "404 Not Found", [], "Invalid path submitted"
		data = open(file_path, "rb").read()
		resp_headers = None
		# add mime type header
		mime_type = FILE_EXTENSION_TO_MIME_TYPE.get(os.path.splitext(file_path)[1])
		if(mime_type):
			resp_headers = {
				'Content-Type': mime_type,
				'Cache-Control': 'max-age=31536000'
			}
		return ("200 OK", resp_headers, data)

	def file_handler(request_params: Request, path):
		if(not path):
			path = default_file

		file_resp = file_cache.get(url_path + path, None)
		if((not file_resp and dynamic_files) or IS_DEV): # always reload from filesystem in devmode
			try:
				file_resp = get_file_resp(path)
				file_cache[path] = file_resp # add to cache
			except Exception:
				pass

		return file_resp if file_resp else (
						file_not_found_cb 
						and file_not_found_cb(
							path, request_params=None, headers=None, post_data=None
						)
					) or ("404 Not Found", [], "-NO-FILE-")
	# headers , data
	# preload all files once on load
	file_names = [
		os.path.join(dp, f)
		for dp, dn, filenames in os.walk(_base_folder_path_) for f in filenames
	]
	for file_name in file_names:
		relative_file_path = ltrim(file_name, _base_folder_path_)
		_url_file_path = url_path + relative_file_path
		# strip / at the begininning in the cache
		# as we search matching path without leading slash in the cache 
		# in the file handler
		file_cache[_url_file_path] = get_file_resp(relative_file_path)

	return url_path + "{*path}", file_handler

def proxy_file_handler(url_path, proxy_url):
	import requests
	def file_handler(req:Request, path):
		ret = requests.get(proxy_url + path, headers=dict(req._headers))
		return {"Content-Type": ret.headers["Content-Type"]}, ret.text
	return url_path + "{*path}", file_handler

# Get args hook
def _get_web_socket_handler(request_params):
	set_socket_fast_close_options(request_params.sock)
	# sends handshake automatically
	ws = WebSocketServerHandler(request_params.sock)
	ws.do_handshake(request_params.HEADERS())
	return ws


Request.set_arg_type_hook(WebSocketServerHandler, _get_web_socket_handler)


