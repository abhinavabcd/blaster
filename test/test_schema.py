
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
	_title_ = "test6"
	_description_ = "test6 description"

	a: List[int] | List[str] = Field("hello", "a description")


schema.init()


class TestTools(unittest.TestCase):
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
			schema.defs["test.test_schema.Test6"],
			{'type': 'object', 'title': 'test6', 'description': 'test6 description', 'properties': {'a': {'anyOf': [{'type': 'array', 'items': {'type': 'integer'}}, {'type': 'array', 'items': {'type': 'string'}}], 'title': 'hello', 'description': 'a description'}}, 'required': ['a']}
		)
