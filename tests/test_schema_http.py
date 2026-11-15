"""HTTP integration for schema attributes and reference-view JSON responses."""
import blaster
import json
import unittest
from contextlib import ExitStack
from unittest.mock import patch

from gevent import socket
from blaster import server
from blaster.schema import Object, Str
from tests.test_http_adversarial import request, responses


class HTTPItem(Object):
	name: Str(maxlen=3)


class HTTPOrder(server.Body):
	name: Str(maxlen=3)
	items: list[HTTPItem]
	options: dict[str, str]
	tags: list[str]


class TestSchemaHTTP(unittest.TestCase):
	def setUp(self):
		stack = ExitStack()
		self.addCleanup(stack.close)
		for name in ("LOG_SERVER", "LOG_ERROR", "LOG_WARN"):
			stack.enter_context(patch.object(server, name, lambda *a, **k: None))
		stack.enter_context(patch.object(server, "IS_DEV", False))
		app = server.App()

		@app.route('/model', methods='POST')
		def model(order: HTTPOrder):
			return order

		@app.route('/attributes', methods='POST')
		def attributes(order: HTTPOrder):
			return {"name": order.name, "item": order.items[0], "options": order.options, "tags": order.tags}

		@app.route('/view', methods='POST')
		def view(order: HTTPOrder):
			return order.options

		@app.route('/list', methods='POST')
		def sequence(order: HTTPOrder):
			return order.tags

		@app.route('/raw', methods='POST', escape_html=False)
		def raw(order: HTTPOrder):
			return order.to_dict()

		@app.route('/scalar', methods='POST')
		def scalar(name: Str(maxlen=3), tags: list[str]):
			return {"name": name, "tag": tags[0]}

		@app.route('/disabled/model', methods='POST', escape_html=False)
		def disabled_model(order: HTTPOrder):
			return order

		@app.route('/disabled/scalar', methods='POST', escape_html=False)
		def disabled_scalar(name: Str(maxlen=3), tags: list[str]):
			return {"name": name, "tag": tags[0]}

		@app.route('/enabled/model', methods='POST', escape_html=True)
		def enabled_model(order: HTTPOrder):
			return order

		@app.route('/request', methods='POST', escape_html=True)
		@app.route('/disabled/request', methods='POST', escape_html=False)
		def raw_request(req: server.Request):
			return {"body": req.BODY(), "params": req.PARAMS(), "name": req.get("name"),
				"query": req.PARAMS("q"), "body_name": req.BODY("name"),
				"fallback": req.BODY("missing", default="fallback")}

		@app.route('/request-view', methods='POST', escape_html=True)
		def request_view(req: server.Request):
			return req.to_dict()

		@app.route('/query', methods='POST', escape_html=True)
		@app.route('/disabled/query', methods='POST', escape_html=False)
		def query(q: Str(maxlen=3)):
			return {"q": q}

		@app.route('/slow', methods='POST', escape_html=True)
		def slow(order: HTTPOrder):
			import gevent
			gevent.sleep(0.01)
			return {"name": order.name, "nested": order.items[0].name}

		app.start(0)
		self.addCleanup(app.stop)
		app.stream_server.address = ('127.0.0.1', 0)
		app.stream_server.family = socket.AF_INET
		app.stream_server.start()
		self.address = app.stream_server.address
		self.addCleanup(app.stream_server.pool.kill)

	def post(self, path):
		body = json.dumps({"name": "<a>", "items": [{"name": "<b>"}],
			"options": {"label": "<c>"}, "tags": ["<d>"], "unknown": "private"}).encode()
		headers = (f'Content-Type: application/json\r\nContent-Length: {len(body)}\r\n'
			'Connection: close\r\n').encode()
		with socket.create_connection(self.address, timeout=2) as sock:
			sock.sendall(request(headers, body, path=path.encode()))
			wire = bytearray()
			while data := sock.recv(65536):
				wire.extend(data)
		status, headers, data = responses(bytes(wire))[0]
		self.assertEqual(status, 200, data)
		self.assertEqual(headers[b'content-type'], b'application/json')
		return json.loads(data)

	def test_model_response_filters_and_escapes(self):
		self.assertEqual(self.post('/model'), {"name": "&lt;a&gt;", "items": [{"name": "&lt;b&gt;"}],
			"options": {"label": "&lt;c&gt;"}, "tags": ["&lt;d&gt;"]})

	def test_views_and_models_inside_dict_response(self):
		self.assertEqual(self.post('/attributes'), {"name": "&lt;a&gt;", "item": {"name": "&lt;b&gt;"},
			"options": {"label": "&lt;c&gt;"}, "tags": ["&lt;d&gt;"]})

	def test_mapping_and_list_view_responses(self):
		self.assertEqual(self.post('/view'), {"label": "&lt;c&gt;"})
		self.assertEqual(self.post('/list'), ["&lt;d&gt;"])

	def test_explicit_raw_export(self):
		self.assertEqual(self.post('/raw')["items"], [{"name": "<b>"}])

	def test_scalar_request_binding_validates_before_escaping(self):
		self.assertEqual(self.post('/scalar'), {"name": "&lt;a&gt;", "tag": "&lt;d&gt;"})


	def test_route_override_disables_all_model_and_scalar_escaping(self):
		self.assertEqual(self.post('/disabled/model'), {"name": "<a>", "items": [{"name": "<b>"}],
			"options": {"label": "<c>"}, "tags": ["<d>"]})
		self.assertEqual(self.post('/disabled/scalar'), {"name": "<a>", "tag": "<d>"})
		self.assertEqual(self.post('/enabled/model')["name"], "&lt;a&gt;")

	def test_request_helpers_follow_route_policy(self):
		from urllib.parse import quote
		for path, escaped in (('/request', True), ('/disabled/request', False)):
			with self.subTest(path=path):
				result = self.post(path + '?q=' + quote('<q>'))
				name = '&lt;a&gt;' if escaped else '<a>'
				query = '&lt;q&gt;' if escaped else '<q>'
				self.assertEqual(result['body']['name'], name)
				self.assertEqual(result['name'], name)
				self.assertEqual(result['body_name'], name)
				self.assertEqual(result['params']['q'], query)
				self.assertEqual(result['query'], query)
				self.assertEqual(result['fallback'], 'fallback')
		self.assertEqual(self.post('/request-view')['post']['items'][0]['name'], '&lt;b&gt;')

	def test_query_validation_reads_raw_values_for_both_route_settings(self):
		self.assertEqual(self.post('/query?q=%3Cq%3E'), {'q': '&lt;q&gt;'})
		self.assertEqual(self.post('/disabled/query?q=%3Cq%3E'), {'q': '<q>'})

	def test_concurrent_routes_do_not_share_escape_policy(self):
		import gevent
		escaped = gevent.spawn(self.post, '/slow')
		raw = gevent.spawn(self.post, '/disabled/model')
		gevent.joinall([escaped, raw], raise_error=True)
		self.assertEqual(escaped.value, {'name': '&lt;a&gt;', 'nested': '&lt;b&gt;'})
		self.assertEqual(raw.value['name'], '<a>')
