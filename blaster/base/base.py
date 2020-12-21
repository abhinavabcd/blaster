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
import signal
import functools
import traceback
from gevent.server import StreamServer
from gevent.pywsgi import WSGIServer
from requests_toolbelt.multipart import decoder


from .. import config
from ..common_funcs_and_datastructures import cur_ms, LowerKeyDict, SanitizedDict

_is_server_running = True

exit_listeners = []

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



def server_log(log_type, cur_millis=None, **kwargs):
	if(not cur_millis):
		cur_millis = cur_ms()
	print(log_type , cur_millis, json.dumps(kwargs))



def write_data(socket, *_data):
	for data in _data:
		if(isinstance(data, str)):
			data = data.encode()
		n = 0
		data_len = len(data)
		while(n < data_len):
			sent = socket.send(data[n:])
			n += sent
			if(sent < 0):
				return False
	return True



class SocketDataReader():
	
	is_eof = False
	sock = None
	store = None
	_n = 0

	def __init__(self, sock):
		self.sock = sock
		self.store = bytearray()
		self._n = 0
	
	def read(self, n):
		if(self.is_eof):
			return None
		while(self._n + n > len(self.store)):
			data = self.sock.recv(1024)
			if(not data):
				self.is_eof = True
				break
			self.store.extend(data)
		ret = self.store[self._n:self._n + n]
		self._n += n
		return ret
		
			
			
	def read_line(self):
		if(self.is_eof):
			return None
		n = self._n
		line_found = False
		while(not line_found):
			if(n >= len(self.store)):
				data = self.sock.recv(1024)
				if(not data):
					self.is_eof = True
					break
				self.store.extend(data) # fetch more data
				
			if(self.store[n] == ord(b'\n')):
				line_found = True
			n += 1
		
		
		ret = self.store[self._n: n]
		self._n = n
		return ret

	
####parse query string
def parse_qs_modified(qs, keep_blank_values=True, strict_parsing=False):
	_dict = {}
	for name, value in urllib.parse.parse_qsl(qs, keep_blank_values, strict_parsing):
		if name in _dict or name.endswith("[]"):
			_values = _dict.get(name)
			if(_values == None):
				_dict[name] = _values = []
			elif(not isinstance(_values , list)): # make it a list of values
				_dict[name] = _values = [_values]
			_values.append(value)
		else:
			_dict[name] = value
	return _dict

#process data as json
def deserialize_post_data(post_data_bytes, headers):
	if(not post_data_bytes):
		return None
	_post_params = SanitizedDict()
	content_type_header = headers.get("Content-Type", b'')
	if(headers and content_type_header.startswith(b'multipart/form-data')):
		for part in decoder.MultipartDecoder(post_data_bytes, content_type_header.decode()).parts:
			content_disposition = part.headers.get(b'Content-Disposition')
			parsed_part_headers = {}
			for i in content_disposition.split(b';'):
				key_val = i.split(b'=')
				if(len(key_val) == 2):
					key = key_val[0].strip()
					val = key_val[1].strip(b'"\'').decode()
					parsed_part_headers[key] = val
			if(b'name' in parsed_part_headers):
				_post_params[parsed_part_headers[b'name']] = {
					"additional_part_headers": parsed_part_headers,
					"data": part.content or part.text,
					"part_headers": part.headers
				}

	else:
		post_data_str = post_data_bytes.decode('utf-8')
		if(post_data_str[0] == '{'
			or content_type_header == b'application/json'
		):
			_post_params.update(json.loads(post_data_str))
		else:
			_post_params.update(parse_qs_modified(post_data_str))

	return _post_params


SOME_OBJ = object()
class RequestParams:
	_post = None
	_get = None
	_cookies = None


	def __init__(self):
		self._get = SanitizedDict() # empty params by default

	#searches in post and get
	def get(self, key, default=None, **kwargs):
		val = SOME_OBJ
		#try post params first
		if(self._post):
			val = self._post.get(key, default=SOME_OBJ, **kwargs)
		#try get param next
		if(val == SOME_OBJ and self._get):
			val = self._get.get(key, default=SOME_OBJ, **kwargs)

		if(val == SOME_OBJ):
			return default
		return val

	def __setitem__(self, key, val):
		self._get[key] = val

	def __getitem__(self, key):
		ret = self.get(key, default=SOME_OBJ)
		if(ret == SOME_OBJ):
			raise LookupError()
		return ret

	def __getattr__(self, key):
		ret = self.get(key, default=SOME_OBJ)
		if(ret == SOME_OBJ):
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

	def COOKIES(self, key):
		return self._cookies.get(key)

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

	def __init__(self, name=None):
		self.name = name
		self.route_handlers = []
		self.request_handlers = []

	def route(self, regex):

		def _decorator(func):
			self.route_handlers.append((regex, func))

			def new_func(*args, **kwargs):
				return func(*args, **kwargs)

			new_func._original = getattr(func, "_original", func)
			return new_func
		
		return _decorator


	def handle_wsgi(self, environ, start_response):
		ret = None
		for handler in self.request_handlers:
			args = handler[0].match(environ['PATH_INFO'])
			func = handler[1]
			if(args != None):
				kwargs = {}
				kwargs["query_params"] = urllib.parse.parse_qs(environ['QUERY_STRING'])

				fargs = args.groups()
				if(fargs):
					ret = func(*fargs , **kwargs)
				else:
					ret = func(**kwargs)
				
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
		self.route_handlers.sort(key=functools.cmp_to_key(lambda x, y: len(y[0]) - len(x[0]))) # reverse sort by length
		self.route_handlers.extend(handlers)
		multiple_handler_filer = {}
		for regex, handler in reversed(self.route_handlers):
			if(regex in multiple_handler_filer):
				continue
			multiple_handler_filer[regex] = True

			if(isinstance(regex, str)):
				regex = re.compile(regex)
			self.request_handlers.append((regex, handler))
		
		#sort ascending because we iterated in reverse earlier
		self.request_handlers.reverse()
		if(config.IS_DEBUG):
			print(self.request_handlers)

		if(use_wsgi):
			self.stream_server = WSGIServer(('', port), self.handle_wsgi).serve_forever()
		else:
			#request_handlers.sort(key = lambda x:x[0] , reverse=True)
			self.stream_server = StreamServer(('', port), self.handle_connection, **ssl_args)
		#keep a track
		_all_apps.add(self)


	def serve(self):
		self.stream_server.serve_forever()

	def stop(self):
		self.stream_server.stop()
		_all_apps.remove(self)

	'''
		all the connection handling magic happens here
	'''
	def handle_connection(self, socket, address):
		socket_data_reader = SocketDataReader(socket)
		is_okay = True # reuse the connection to handle another
		while(is_okay):
			request_line = socket_data_reader.read_line()
			if(not request_line):
				return
			post_data = None
			request_params = RequestParams()
			cur_millis = int(1000 * time.time())
			request_type = None
			request_path = None
			headers = None
			try:
				request_line = request_line.decode("utf-8")
				request_type, _request_line = request_line.split(" ", 1)
				_http_protocol_index = _request_line.rfind(" ")
				request_path = _request_line[: _http_protocol_index]
				http_version = _request_line[_http_protocol_index + 1:]

				query_start_index = request_path.find("?")
				if(query_start_index != -1):
					request_params._get.update(
						parse_qs_modified(request_path[query_start_index + 1:])
					)
					request_path = request_path[:query_start_index]
			
				headers = LowerKeyDict()
				while(True): # keep reading headers
					data = socket_data_reader.read_line()
					if(data == b'\r\n'):
						break
					if(not data):
						return
					header_name , header_value = data.split(b': ', 1)
					headers[header_name.lower().decode()] = header_value.rstrip(b'\r\n')
				
				if(request_type == "POST" and headers.get("Content-Length", None)):
					n = int(headers.get("Content-Length", b'0'))
					if(n > 0):
						post_data = bytearray()
						while(len(post_data) < n):
							bts = socket_data_reader.read(n)
							if(not bts):
								break
							post_data.extend(bts)
				##app specific headers
				ret = status = response_headers = body = None
				for handler in self.request_handlers:
					args = handler[0].match(request_path)
					func = handler[1]
					
					if(args != None):
						kwargs = {}
						kwargs["request_params"] = request_params
						kwargs["headers"] = headers

						#process cookies
						cookie_header = headers.get("Cookie", None)
						decoded_cookies = {}
						if(cookie_header):
							_temp = cookie_header.strip().decode().split(";")
							for i in _temp:
								decoded_cookies.update(parse_qs_modified(i.strip()))
						request_params._cookies = decoded_cookies
						#process cookies end
						request_params._post = deserialize_post_data(post_data, headers)

						fargs = args.groups()
						if(fargs):
							ret = func(socket, *fargs , **kwargs)
							break
						else:
							ret = func(socket, **kwargs)
							break
				

				#handle various return values from handler functions
				if(ret != None):
					if(isinstance(ret, tuple)):
						body_len = len(ret)
						if(body_len > 0):
							body = ret[-1]
						if(body_len > 1):
							response_headers = ret[-2]
						if(body_len > 2):
							status = str(ret[-3]).encode()
					else:
						body = ret
		
					status = status or b'200 OK'
					is_okay = write_data(socket, b'HTTP/1.1 ', status, b'\r\n')

					if(response_headers):
						if(isinstance(response_headers, dict)):
							for key, val in response_headers.items():
								is_okay = is_okay and write_data(socket, key, b': ', val, b'\r\n')

							for key, val in default_stream_headers.items():
								if(key not in response_headers):
									is_okay = is_okay and write_data(socket, key, b': ', val, b'\r\n')

						elif(isinstance(response_headers, list)):
							for header in response_headers:
								is_okay = is_okay and write_data(socket, header, b'\r\n')

					else:
						for key, val in default_stream_headers.items():
							is_okay = is_okay and write_data(socket, key, b': ', val, b'\r\n')

					#check and respond to keep alive
					is_keep_alive = headers.get('Connection', b'').lower() == b'keep-alive'
					#send back keep alive
					if(is_keep_alive):
						is_okay = is_okay and write_data(socket, b'Connection: keep-alive\r\n')

					if(body != None): # if body doesn't exist , the handler function is holding the connection and handling it manually
						if(isinstance(body, (dict, list))):
							body = json.dumps(body)
						
						#encode body
						if(isinstance(body, str)):
							body = body.encode()
						is_okay = is_okay and write_data(socket, b'Content-Length: ', str(len(body)), b'\r\n\r\n')
						is_okay = is_okay and write_data(socket, body)
						
						#close if not keeping alive
						if(not is_keep_alive):
							socket.close()
							is_okay = False
					else: # close headers and dont rehandle this connection(probably socket if used for sending data in callbacks etc)
						write_data(socket, b'\r\n')
						is_okay = False
				else: # return value is None => manually handling
					is_okay = False

				server_log("http" , request_type=request_type , path=request_path, wallclockms=int(1000 * time.time()) - cur_millis)

			except Exception as ex:
				stacktrace_string = traceback.format_exc()
				body = None
				if(config.server_error_page):
					body = config.server_error_page(
						request_type,
						request_path,
						request_params,
						headers,
						stacktrace_string=stacktrace_string
					)
				if(not body):
					body = b'Internal server error'
				write_data(socket,
						b'HTTP/1.1 ', b'502 Server error', b'\r\n',
						b'Connection: close', b'\r\n',
						b'Content-Length: ', str(len(body)), b'\r\n\r\n',
						body
				)
				socket.close()
				server_log("http_error",
					cur_millis,
					exception_str=str(ex),
					stacktrace_string=stacktrace_string,
					query_params=request_params.to_dict()
				)
				if(config.IS_DEBUG):
					traceback.print_exc()
				return


def stop_all_apps():
	server_log("server_info", data="exiting all servers")
	global _is_server_running
	
	_is_server_running = False
	
	for i in exit_listeners:
		i()

	for app in list(_all_apps):
		app.stop() # should handle all connections gracefully

	#wait for queue to flush/process


gevent.signal_handler(signal.SIGINT, stop_all_apps)


# create a global app for generic single server use
_main_app = App()

#generic global route handler
route_handler = _main_app.route

#generic glonal route handler
def start_stream_server(*args, **kwargs):
	_main_app.start(*args, **kwargs)
	_main_app.serve()


def redirect_http_to_https():
	def redirect(sock, path, query_params=None, headers=None, post_data=None, user=None):
		host = headers.get('host')
		if(not host):
			return "404", {}, "Not understood"
		query_string = ("?" + urllib.parse.urlencode(query_params)) if query_params else ""
		return "302 Redirect", ["Location: https://%s%s%s"%(host.decode(), path, query_string)], "need https"

	http_app = App("port=80")
	http_app.start(port=80, handlers=[("(.*)", redirect)])
	gevent.spawn(http_app.serve)
	return http_app


def is_server_running():
	return _is_server_running
