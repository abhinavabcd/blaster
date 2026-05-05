
from blaster.schema import schema, Object, List, Str, Int, Optional, Dict, Field

import unittest
import json


class Test1(Object):
	a: List[int | str]
	b: Int
	c: Str(minlen=0)
	e: str


class Test2(Object):
	a: List[int]
	b: Int
	c: Str(minlen=0) = None
	e: str


class Test3(Object):
	a: List[int]
	b: Int
	c: Str(minlen=2) = None
	d: Optional[Str(format="date-time")]
	e: str
	f: Dict[str, int | str]


class Test4(Object):
	b: Str
	e: bool


class Test5(Object):
	a: list[int, str]
	b: dict[str, int | str] = None
	c: dict[str, int] | dict[int, str] = None
	d: list[list[int]] = None


class Test6(Object):
	__title__ = "test6"
	__description__ = "test6 description"
	a = Field(list[int] | list[str], "hello", "a description")


schema.init()


class TestTools(unittest.TestCase):
	def setUp(self):
		self.maxDiff = 2048
	def test_1(self):
		with self.assertRaises(Exception) as context:
			t = Test1.from_dict({"a": [1, 2, 3], "b": "1", "c": None, "e": "1"})

		t = Test2.from_dict({"a": [1, 2, 3], "b": "1", "c": None, "e": "1"})
		self.assertEqual(t.a, [1, 2, 3])
		self.assertEqual(t.b, 1)
		self.assertEqual(t.c, None)

		t = Test3.from_dict({"a": [1, 2, 3], "b": "1", "c": "", "e": "1", "f": {"a": 1, "b": "2"}})
		self.assertEqual(t.a, [1, 2, 3])
		self.assertEqual(t.b, 1)
		self.assertEqual(t.c, None)

		with self.assertRaises(Exception) as context:
			t = Test3.from_dict({"a": [1, 2, 3], "b": "1", "c": "a", "e": "1"})

		print(context.exception)

		with self.assertRaises(Exception) as context:
			t = Test3.from_dict({"a": [1, 2, 3], "b": "1", "c": "a", "e": "1"})
		print(context.exception)

		with self.assertRaises(Exception) as context:
			t = Test3.from_dict({"a": [1, 2, 3], "b": "1", "c": "", "e": "1", "f": {"a": 1, "b": []}})
		print(context.exception)

		t = Test4.from_dict({"b": "1", "e": "1"})
		self.assertEqual(t.e, True)
		with self.assertRaises(Exception) as context:
			t = Test4.from_dict({"b": "1", "e": None})
		print(context.exception)

		t = Test5.from_dict({"a": [1, 2, 3], "b": {"a": 1, "b": "2"}})
		with self.assertRaises(Exception) as context:
			t = Test5.from_dict({"a": [1, "2", 3]})
		print(context.exception)

		with self.assertRaises(Exception) as context:
			t = Test5.from_dict({"a": [1, 2, 3], "c": {"x": "x", "y": "y"}})
		print(context.exception)

		t = Test5.from_dict({"a": [1, 2, 3], "c": {1: "x", 2: "y"}})

		with self.assertRaises(Exception) as context:
			t = Test5.from_dict({"a": [1, 2, 3], "c": {"x": 1, "y": "y"}})
		print(context.exception)

		t = Test5.from_dict({"a": [1, 2, 3], "d": [[1, 2], [2, 3]]})
		with self.assertRaises(Exception) as context:
			t = Test5.from_dict({"a": [1, 2, 3], "d": [[1, "str"], [2, 3]]})
		print(context.exception)

		self.assertDictEqual(
			schema.defs["tests.test_schema.Test6"],
			{
				'type': 'object', 'title': 'test6', 'description': 'test6 description', 'properties': {'a': {'anyOf': [{'type': 'array', 'items': {'type': 'integer'}}, {'type': 'array', 'items': {'minLength': 1, 'type': 'string'}}], 'title': 'hello', 'description': 'a description'}}, 'required': ['a']
			}
		)

	def test_function_schema(self):
		def sample(a: int, b: str = "x"):
			return a, b

		_schema, validate = schema(sample)

		self.assertDictEqual(
			_schema,
			{
				"type": "function",
				"function": {
					"name": "sample",
					"description": None,
					"parameters": {
						"type": "object",
						"properties": {
							"a": {"type": "integer"},
							"b": {"type": "string", "default": "x", "minLength": 1}
						},
						"required": ["a"]
					}
				}
			}
		)
		validated = validate({"a": "2"})
		self.assertEqual(validated["args"], [2, "x"])
		self.assertDictEqual(validated["kwargs"], {"a": 2, "b": "x"})
		self.assertEqual(validated["_call_"](), (2, "x"))

	def test_function_schema_json_validation(self):
		def sample(a: int, b: Str(minlen=2)):
			return a, b

		_schema, validate = schema(sample)
		validated = validate(json.dumps({"a": "3", "b": "ok"}))
		self.assertEqual(validated["args"], [3, "ok"])
		self.assertDictEqual(validated["kwargs"], {"a": 3, "b": "ok"})
		self.assertEqual(validated["_call_"](), (3, "ok"))

		with self.assertRaises(Exception):
			validate({"a": "3", "b": "x"})

		with self.assertRaises(Exception):
			validate({"b": "ok"})

	def test_function_schema_object_validation(self):
		class FunctionArg(Object):
			name: str
			count: int

		def sample(arg: FunctionArg):
			return arg

		_schema, validate = schema(sample)
		validated = validate({"arg": {"name": "items", "count": "4"}})

		self.assertEqual(_schema["function"]["parameters"]["properties"]["arg"]["type"], "object")
		self.assertIsInstance(validated["args"][0], FunctionArg)
		self.assertIs(validated["args"][0], validated["kwargs"]["arg"])
		self.assertEqual(validated["args"][0].name, "items")
		self.assertEqual(validated["args"][0].count, 4)
		self.assertIs(validated["_call_"](), validated["args"][0])

	def test_function_schema_call_api(self):
		def call_api(url: str, method: str = "GET", headers: dict[str, str] = None, body: dict = None):
			return url, method, headers, body

		_schema, validate = schema(call_api)

		self.assertDictEqual(
			_schema,
			{
				"type": "function",
				"function": {
					"name": "call_api",
					"description": None,
					"parameters": {
						"type": "object",
						"properties": {
							"url": {"type": "string", "minLength": 1},
							"method": {"type": "string", "default": "GET", "minLength": 1},
							"headers": {
								"type": "object",
								"additionalProperties": {"type": "string", "minLength": 1}
							},
							"body": {
								"type": "object",
								"additionalProperties": True
							}
						},
						"required": ["url"]
					}
				}
			}
		)

		validated = validate({
			"url": "https://example.com",
			"headers": {"Accept": "application/json"},
			"body": {"limit": 10}
		})

		self.assertEqual(
			validated["args"],
			[
				"https://example.com",
				"GET",
				{"Accept": "application/json"},
				{"limit": 10}
			]
		)
		self.assertDictEqual(
			validated["kwargs"],
			{
				"url": "https://example.com",
				"method": "GET",
				"headers": {"Accept": "application/json"},
				"body": {"limit": 10}
			}
		)
		self.assertEqual(
			validated["_call_"](),
			(
				"https://example.com",
				"GET",
				{"Accept": "application/json"},
				{"limit": 10}
			)
		)
