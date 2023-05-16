
from blaster.schema import schema, Object, List, Str, Int, Optional

import unittest
import json


class Test1(Object):
	a: List[int]
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


class Test4(Object):
	b: Str
	e: bool


schema.init()


class TestTools(unittest.TestCase):
	def test_1(self):
		try:
			t = Test1.from_dict({"a": [1, 2, 3], "b": "1", "c": None, "e": "1"})
			self.fail("Should have failed")
		except Exception as ex:
			print(ex)

		t = Test2.from_dict({"a": [1, 2, 3], "b": "1", "c": None, "e": "1"})
		self.assertEqual(t.a, [1, 2, 3])
		self.assertEqual(t.b, 1)
		self.assertEqual(t.c, None)

		t = Test3.from_dict({"a": [1, 2, 3], "b": "1", "c": "", "e": "1"})
		self.assertEqual(t.a, [1, 2, 3])
		self.assertEqual(t.b, 1)
		self.assertEqual(t.c, None)

		try:
			t = Test3.from_dict({"a": [1, 2, 3], "b": "1", "c": "a", "e": "1"})
			self.fail("Should have failed")
		except Exception as ex:
			print(ex)

		t = Test4.from_dict({"b": "1", "e": "1"})
		self.assertEqual(t.e, True)
		try:
			t = Test4.from_dict({"b": "1", "e": None})
			self.fail("Should have failed")
		except Exception as ex:
			print(ex)
	