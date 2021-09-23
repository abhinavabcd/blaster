'''
Created on 22-Aug-2017

@author: abhinav
'''

# argparse==1.2.1
# gevent==1.2.1
# greenlet==0.4.12
# protobuf==3.2.0
# boto3==1.4.4

import ujson as json
import re
import urllib.parse
import time
import gevent
import functools
import traceback
from gevent.server import StreamServer
from gevent.pywsgi import WSGIServer
from requests_toolbelt.multipart import decoder

from .. import config as blaster_config
from ..config import IS_DEV, DEBUG_LEVEL
from ..common_funcs_and_datastructures import SanitizedDict
from ..utils import events
from ..logging import LOG_ERROR, LOG_SERVER
_is_server_running = True


default_wsgi_headers = [
	('Content-Type', 'text/html; charset=utf-8')
]
default_stream_headers = {
	'Content-Type': 'text/html; charset=utf-8',
	'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
	'X-Content-Type-Options': 'nosniff',
	'vary': 'Accept-Encoding',
	'X-Frame-Options': 'SAMEORIGIN',
	'X-UA-Compatible': 'IE=Edge',
	'X-XSS-Protection': '0'
}

I_AM_HANDLING_THE_SOCKET = object()
I_AM_HANDLING_THE_HEADERS = object()
I_AM_HANDLING_THE_STATUS = object()

REUSE_SOCKET_FOR_HTTP = object()

########some random constants
HTTP_MAX_REQUEST_BODY_SIZE = 1 * 1024 * 1024 # 1 mb
HTTP_MAX_HEADERS_DATA_SIZE = 16 * 1024 # 16kb

SLASH_N_ORDINAL = ord(b'\n')




def find_new_line(arr):
	n = len(arr)
	i = 0
	while(i < n):
		if(arr[i] == SLASH_N_ORDINAL):
			return i
		i += 1
	return -1


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
		for data in _data:
			if(isinstance(data, str)):
				data = data.encode()
			n = 0
			data_len = len(data)
			while(n < data_len):
				sent = self.sock.send(data[n:])
				n += sent
				if(sent < 0):
					return False
		return True
				
	def recv(self, n):
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
		
	def readline(self, max_size=HTTP_MAX_REQUEST_BODY_SIZE):
		if(self.is_eof):
			return None
		#indicates the index in store where we find the new line
		line_found = find_new_line(self.store)
		#check in store
		while(line_found == -1 and len(self.store) < max_size):
			data = self.sock.recv(4096)
			if(not data):
				self.is_eof = True
				break

			line_found = find_new_line(data)
			if(line_found != -1):
				line_found = len(self.store) + line_found

			self.store.extend(data) # fetch more data

		ret = None
		if(line_found != -1):
			ret = self.store[:line_found + 1]
			self.store = self.store[line_found + 1 :]
		else: # EOF, return everything in buffer
			ret = self.store
			self.store = bytearray()
			self.is_eof = True
		return ret

	def __getattr__(self, key):
		ret = getattr(self.sock, key, _OBJ_END_)
		if(ret == _OBJ_END_):
			raise AttributeError()
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


_OBJ_END_ = object()
class RequestParams:
	sock = None
	_post = None
	_get = None
	_cookies = None
	_headers = None
	#cookies to send to client
	_cookies_to_set = None
	#url data, doesn't contain query string
	path = None

	def __init__(self, buffered_socket):
		self._get = SanitizedDict() # empty params by default
		self._headers = HeadersDict()
		self.sock = buffered_socket

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


	def to_dict(self):
		return {"get": self._get, "post": self._post}

#type, required, in (query, body)
class Param(object):
	#ex:  "a": Param(in="query/body", required=False, type="List[str]")
	pass
#RequestObject


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

	def __init__(self, name=None):
		self.name = name
		self.route_handlers = []
		self.request_handlers = []

	def route(
		self,
		regex,
		methods=None,
		name='',
		description='',
		request_spec=None,
		response_spec=None, # -type: data
		max_body_size=None
	):
		methods = methods or ("GET", "POST")
		if(isinstance(methods, str)):
			methods = (methods,)

		def _decorator(func):
			_path_regex = regex
			if(isinstance(_path_regex, str)):
				#special case where path params can be {:varname} {:+varname}
				#replacing regex with regex haha!
				_path_regex = re.sub(r'\{:(\w+)\}', '(?P<\g<1>>[^/]*)', _path_regex)
				_path_regex = re.sub(r'\{:\+(\w+)\}', '(?P<\g<1>>[^/]+)', _path_regex)

			self.route_handlers.append({
				"regex": _path_regex,
				"func": func,
				"methods": methods,
				"name": name,
				"description": description,
				"request_spec": request_spec,# -type: data
				"response_spec": response_spec,# -type: data
				"max_body_size": max_body_size
			})
			#return func as if nothing's decorated! (so you can call function as is)
			return func

		return _decorator


	def handle_wsgi(self, environ, start_response):
		ret = None
		for regex, method_handler in self.request_handlers:
			args = regex.match(environ['PATH_INFO'])
			func = method_handler.get(environ["REQUEST_METHOD"].upper())
			if(args != None and func):
				kwargs = {}
				kwargs["request_params"] = urllib.parse.parse_qs(environ['QUERY_STRING'])

				fargs = args.groupdict()
				if(fargs):
					kwargs.update(fargs)
				ret = func(None, **kwargs)
				
				break
			
		status = response_headers = body = None
		if(ret != None):
			if(isinstance(ret, tuple)):
				l = len(ret)
				if(l > 0):
					body = ret[-1]
				if(l > 1):
					response_headers = ret[-2]
				if(l > 2):
					status = ret[-3]
			else:
				body = ret
			
			response_headers = response_headers or default_wsgi_headers
			status = status or "200 OK"
			start_response(status, response_headers)
			if(isinstance(body, list)):
				return body
			return [body]
		#close socket


	def start(self, port=80, handlers=[], use_wsgi=False, **ssl_args):
		self.route_handlers.sort(key=functools.cmp_to_key(lambda x, y: len(y["regex"]) - len(x["regex"]))) # reverse sort by length
		
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


		#add them to all route handlers
		self.route_handlers.extend(handlers_with_methods)

		regexes_map = {}
		#build {regex: {GET: handler,...}...}
		for handler in reversed(self.route_handlers):
			regex = handler["regex"]
			existing_regex_method_handlers = regexes_map.get(regex)
			if(not existing_regex_method_handlers):
				regexes_map[regex] = existing_regex_method_handlers = {}
				self.request_handlers.append((re.compile(regex), existing_regex_method_handlers))

			for method in handler["methods"]:
				existing_regex_method_handlers[method.upper()] = handler
		
		#sort ascending because we iterated in reverse earlier
		self.request_handlers.reverse()
		if(IS_DEV):
			for regex, method_handlers in self.request_handlers:
				print(regex.pattern, {method: handler["func"] for method, handler in method_handlers.items()})

		if(use_wsgi):
			self.stream_server = WSGIServer(('', port), self.handle_wsgi).serve_forever()
		else:
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
		request_line = buffered_socket.readline(4096) # ignore request lines > 4096 bytes
		if(not request_line):
			return
		post_data = None
		request_params = RequestParams(buffered_socket)
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
				raise Exception("Method not found")

			#parse the headers
			headers = request_params._headers
			max_headers_data_size = handler.get("max_headers_data_size") or HTTP_MAX_HEADERS_DATA_SIZE
			_headers_data_size = 0
			while(_headers_data_size < max_headers_data_size): # keep reading headers
				data = buffered_socket.readline(max_size=max_headers_data_size)
				if(data == b'\r\n'):
					break
				if(not data):
					return # won't resuse socket

				header_name_value_pair = data.split(b':', 1)
				if(len(header_name_value_pair) == 2):
					header_name , header_value = header_name_value_pair
					headers[header_name.lower().decode()] = header_value.lstrip(b' ').rstrip(b'\r\n').decode()
				
				_headers_data_size += len(data)


			#check if there is a content length or transfer encoding chunked
			content_length = int(headers.get("Content-Length", 0))
			_max_body_size = handler.get("max_body_size") or HTTP_MAX_REQUEST_BODY_SIZE
			if(content_length > 0):
				if(content_length < _max_body_size):
					post_data = buffered_socket.recv(content_length)

			else: # handle chuncked encoding
				transfer_encoding = headers.get("transfer-encoding")
				if(transfer_encoding == "chunked"):
					post_data = bytearray()
					chunk_size = int(buffered_socket.readline(), 16) # hex
					while(chunk_size > 0 and len(post_data) + chunk_size < _max_body_size):
						data = buffered_socket.recv(chunk_size + 2)
						post_data.extend(data)
						chunk_size = int(buffered_socket.readline(), 16) # hex
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

			_path_named_params = request_path_match.groupdict()
			#Note: cannot contain both names and unnamed
			if(_path_named_params):
				response_from_handler = func(request_params, **_path_named_params)
			else:
				response_from_handler = func(*request_path_match.groups(), request_params)

			
			##app specific headers
			#handle various return values from handler functions
			status, response_headers, body = App.response_body_to_parts(response_from_handler)

			if(status != I_AM_HANDLING_THE_STATUS):
				status = str(status) if status else '200 OK'
				buffered_socket.send(b'HTTP/1.1 ', status, b'\r\n')

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

			#check and respond to keep alive
			resuse_socket_for_next_http_request = headers.get('Connection', "").lower() == "keep-alive"
			#send back keep alive
			if(resuse_socket_for_next_http_request):
				buffered_socket.send(b'Connection: keep-alive\r\n')

			if(body == I_AM_HANDLING_THE_SOCKET):
				if(response_headers != I_AM_HANDLING_THE_HEADERS):
					buffered_socket.send(b'\r\n') # close the headers

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
				buffered_socket.send(b'\r\n')

			#just some debug for apis
			if(IS_DEV and DEBUG_LEVEL > 1 and response_data):
				print(
					"##API DEBUG",
					json.dumps({
						"PATH": request_path,
						"REQUEST_METHOD": request_type,
						"GET": request_params._get,
						"REQUEST_BODY": request_params._post,
						"REQUEST_CONTENT_TYPE": headers.get("content-type"),
						"RESPONSE_STATUS": status,
						"RESPONSE_HEADERS": response_headers,
						"REPONSE_BODY": response_data
					}, indent=4)
				)

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
				request_params=request_params.to_dict()
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
_main_app = App()

#generic global route handler
route_handler = _main_app.route

#generic glonal route handler
def start_stream_server(*args, **kwargs):
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
