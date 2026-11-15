"""HTTP routing through the path tree and the method-aware regex fallback."""
import blaster
import json
from pathlib import Path
import tempfile
import unittest
from contextlib import ExitStack
from unittest.mock import patch

from gevent import socket
from blaster import server
from blaster.schema import Str
from tests.test_http_adversarial import request, responses


class TestRoutingHTTP(unittest.TestCase):
	def setUp(self):
		stack = ExitStack()
		self.addCleanup(stack.close)
		for name in ('LOG_SERVER','LOG_ERROR','LOG_WARN'):
			stack.enter_context(patch.object(server,name,lambda *a,**k:None))
		stack.enter_context(patch.object(server,'IS_DEV',False))
		self.app = app = server.App()

		@app.route('/items/{id}', methods='GET')
		def optional(id: str='missing'):
			return {'id':id}

		@app.route('/users/me', methods='GET')
		def literal():
			return {'route':'literal'}

		@app.route('/users/{+name}', methods='POST')
		def required(name: str):
			return {'name':name}

		@app.route(r'/users/(?P<name>[a-z]+)', methods='DELETE')
		def fallback(name: str):
			return {'regex_name':name}

		@app.route(r'/ordered/.*', methods='GET')
		def broad():
			return {'route':'regex'}

		@app.route('/ordered/literal', methods='GET')
		def ordered():
			return {'route':'tree'}

		@app.route(r'/regex/(?P<code>[A-Z]{2}\d+)', methods='GET')
		def named(code: str):
			return {'code':code}

		@app.route('/mixed/file-{id}', methods='GET')
		def embedded(id):
			return {'id':id}

		@app.route('/legacy/{*rest}/end', methods='GET')
		@app.route('/legacy-required/{*+rest}', methods='GET')
		def regex_wildcard(rest: str):
			return {'rest':rest}

		@app.route('/catch/{*path}', methods='GET')
		def catch_all(path=''):
			return {'path':path}

		@app.route(r'/catch/.*', methods='GET')
		def catch_regex():
			return {'route':'regex'}

		@app.route('/same/{+id}', methods='GET')
		def get_id(id: str):
			return {'id':id}

		@app.route('/same/{+slug}', methods='POST')
		def post_slug(slug: str):
			return {'slug':slug}

		@app.route('/number/{+value}', methods='GET')
		def number(value: int):
			return {'value':value}

		@app.route('/duplicate', methods='GET')
		def first():
			return {'route':'first'}

		@app.route('/duplicate', methods='GET')
		def last():
			return {'route':'last'}

		@app.route('/escape/{+name}', methods='GET', escape_html=True)
		@app.route('/raw/{+name}', methods='GET', escape_html=False)
		def escaped(name: Str(maxlen=3)):
			return {'name':name}

		@app.route('/trailing/', methods='GET')
		def trailing():
			return {'route':'trailing'}

		@app.route('/double//slash', methods='GET')
		def double():
			return {'route':'double'}

		folder = stack.enter_context(tempfile.TemporaryDirectory())
		Path(folder,'index.html').write_text('static index')
		Path(folder,'nested').mkdir()
		Path(folder,'nested','file.txt').write_text('nested file')
		app.start(0,handlers=[server.static_file_handler('/static/',folder,file_cache={},dynamic_files=False)])
		self.addCleanup(app.stop)
		app.stream_server.address = ('127.0.0.1',0)
		app.stream_server.family = socket.AF_INET
		app.stream_server.start()
		self.address = app.stream_server.address
		self.addCleanup(app.stream_server.pool.kill)

	def exchange(self, path, method='GET'):
		with socket.create_connection(self.address,timeout=2) as sock:
			sock.sendall(request(b'Connection: close\r\n',method=method.encode(),path=path.encode()))
			wire=bytearray()
			while data := sock.recv(65536):
				wire.extend(data)
		return responses(bytes(wire))[0]

	def json(self,path,method='GET'):
		status,headers,body=self.exchange(path,method)
		self.assertEqual(status,200,body)
		self.assertEqual(headers[b'content-type'],b'application/json')
		return json.loads(body)

	def test_optional_segment_omission_and_query_defaults(self):
		for path in ('/items','/items/'):
			self.assertEqual(self.json(path),{'id':'missing'})
		self.assertEqual(self.json('/items/42'),{'id':'42'})
		self.assertEqual(self.json('/items?id=query'),{'id':'query'})
		self.assertEqual(self.json('/items/42?id=query'),{'id':'42'})

	def test_matching_path_without_method_uses_other_tree_then_regex(self):
		self.assertEqual(self.json('/users/me'),{'route':'literal'})
		self.assertEqual(self.json('/users/me','POST'),{'name':'me'})
		self.assertEqual(self.json('/users/me','DELETE'),{'regex_name':'me'})

	def test_tree_precedes_regex_including_catch_all(self):
		self.assertEqual(self.json('/ordered/literal'),{'route':'tree'})
		self.assertEqual(self.json('/ordered/other'),{'route':'regex'})
		self.assertEqual(self.json('/catch/a/b'),{'path':'a/b'})

	def test_regex_captures_embedded_parameters_and_wildcards(self):
		self.assertEqual(self.json('/regex/AB42'),{'code':'AB42'})
		self.assertEqual(self.json('/mixed/file-42'),{'id':'42'})
		self.assertEqual(self.json('/legacy/a/b/end'),{'rest':'a/b'})
		self.assertEqual(self.json('/legacy-required/a/b'),{'rest':'a/b'})

	def test_capture_names_and_validation_are_per_handler(self):
		self.assertEqual(self.json('/same/item'),{'id':'item'})
		self.assertEqual(self.json('/same/item','POST'),{'slug':'item'})
		self.assertEqual(self.json('/number/42?value=100'),{'value':42})

	def test_duplicate_handler_and_slash_rules(self):
		self.assertEqual(self.json('/duplicate'),{'route':'last'})
		self.assertEqual(self.json('/trailing'),{'route':'trailing'})
		self.assertEqual(self.json('/trailing/'),{'route':'trailing'})
		self.assertEqual(self.json('/double//slash'),{'route':'double'})

	def test_capture_encoding_and_escape_setting(self):
		self.assertEqual(self.json('/same/a%2Fb'),{'id':'a%2Fb'})
		self.assertEqual(self.json('/escape/<a>'),{'name':'&lt;a&gt;'})
		self.assertEqual(self.json('/raw/<a>'),{'name':'<a>'})

	def test_static_handler_uses_tree_catch_all(self):
		for path,expected in (('/static','static index'),('/static/','static index'),('/static/nested/file.txt','nested file')):
			status,_,body=self.exchange(path)
			self.assertEqual(status,200,body)
			self.assertEqual(body.decode(),expected)
