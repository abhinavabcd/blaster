import unittest
import gevent
from blaster import tools, blaster_exit
import time
import random
from blaster.tools import get_time_overlaps, retry, \
	ExpiringCache, create_signed_value, decode_signed_value, \
	submit_background_task, background_task, ignore_exceptions, \
	ASSERT_RATE_PER_MINUTE, RateLimitingException, get_by_key_path, \
	xmltodict, sanitize_to_underscore_id, nsplit, get_by_key_list, create_operator_tree
from blaster.tools.sanitize_html import HtmlSanitizedDict, HtmlSanitizedList
from datetime import datetime, timedelta
from blaster.utils.data_utils import parse_string_to_units, parse_string_to_int, \
	parse_currency_string


class TestSanitization(unittest.TestCase):
	def test_sanitization(self):
		sd = HtmlSanitizedDict(a="<a>", b="<b>")
		sd["c"] = "<c>"
		sd["d"] = {"e": "<e>", "f": "<f>"}

		self.assertTrue(sd.get("a") == sd["a"] == "&lt;a&gt;")
		self.assertTrue(sd["d"]["e"] == "&lt;e&gt;")

		# test iterator
		for k, v in sd.items():
			if(isinstance(v, HtmlSanitizedDict)):
				for k1, v1 in v.items():
					self.assertTrue(">" not in v1)
			else:
				self.assertTrue(">" not in v)
		# sanitized Dict 2

		def is_sanitized(o):
			if(isinstance(o, dict)):
				for k, v in o.items():
					if(not is_sanitized(v)):
						print("not sanitized dict items", o, k, v)
						return False
				for k in o:
					if(not is_sanitized(o[k])):
						print("not sanitized dict", o, k, o[k])
						return False
			elif(isinstance(o, list)):
				for v in o:
					if(not is_sanitized(v)):
						print("not sanitized list item", o, v)
						return False
			elif(isinstance(o, str)):
				if(">" in o or "<" in o):
					print("not sanitized str", o)
					return False
				return True

			return is_sanitized(str(o))

		sd2 = HtmlSanitizedDict({"1": "<1>", "2": ["<b>", "<c>"], "3": {"a3": "<v></v>"}})
		self.assertTrue(is_sanitized(sd2))

		self.assertTrue(isinstance(sd, dict))
		sl = HtmlSanitizedList(["<a>", "<b>"])
		sl.append({"c": "<c>", "d": "<d>"})
		sl.extend(["<e>", "<f> <>><><><<<>>"])
		sl.append(["<g>", "<h>"])
		self.assertTrue(sl[0] == "&lt;a&gt;")
		self.assertTrue(sl[2]["c"] == "&lt;c&gt;")  # test dict in a list
		# test iterator
		for i in sl:
			if(isinstance(i, HtmlSanitizedList)):
				for j in i:
					self.assertTrue(">" not in j)
			else:
				self.assertTrue(">" not in i)

		self.assertTrue(is_sanitized(sl[:]))

		# test serialization deserialization
		self.assertTrue(isinstance(sl, list))


class TestAuth(unittest.TestCase):
	def test(self):
		secret = "ijkl"
		val = create_signed_value("abcd", "efgh", secret)
		self.assertEqual(decode_signed_value("abcd", val, secret).decode(), "efgh")

		val = create_signed_value("abcd", b"efgh2", secret)
		self.assertEqual(decode_signed_value("abcd", val, secret), b"efgh2")

		val = create_signed_value("abcd", "efgh2", secret, -1)
		self.assertEqual(decode_signed_value("abcd", val, secret), None)

		self.assertEqual(decode_signed_value("abcd", "asdasda", secret), None)
		print("Auth tests passed")


class TestTools(unittest.TestCase):
	def test_overlaps(self):
		self.assertEqual(
			get_time_overlaps(
				datetime(year=2021, month=10, day=1),
				datetime(year=2021, month=10, day=20),
				["Monday 10:30 - 12:30|100EUR", "Tuesday 10:30|200EUR"],
				exclude=["05/10/2021 10:30"]
			),
			[
				(datetime(2021, 10, 4, 10, 30), datetime(2021, 10, 4, 12, 30), ['100EUR']), 
				(datetime(2021, 10, 11, 10, 30), datetime(2021, 10, 11, 12, 30), ['100EUR']),
				(datetime(2021, 10, 12, 10, 30), datetime(2021, 10, 12, 11, 30), ['200EUR']),
				(datetime(2021, 10, 18, 10, 30), datetime(2021, 10, 18, 12, 30), ['100EUR']),
				(datetime(2021, 10, 19, 10, 30), datetime(2021, 10, 19, 11, 30), ['200EUR'])
			]
		)
		self.assertEqual(
			get_time_overlaps(
				datetime(year=2021, month=10, day=1),
				datetime(year=2021, month=10, day=20),
				["5/12/2021 10:30 - 12:30", "07/12/2021 10:30"]
			),
			[]
		)
		self.assertEqual(
			get_time_overlaps(
				datetime(year=2021, month=10, day=1),
				datetime(year=2021, month=10, day=20),
				["5/10/2021 10:30 - 6/10/2021 11:30"]
			),
			[(datetime(2021, 10, 5, 10, 30), datetime(2021, 10, 6, 11, 30), [])]
		)

		self.assertEqual(
			get_time_overlaps(
				datetime(year=2022, month=7, day=27, hour=23),
				datetime(year=2022, month=7, day=28, hour=2),
				["23:30 - 23:45", "00:00 - 00:15", "Wednesday - Thursday"],
				partial=True
			),
			[
				(datetime(2022, 7, 27, 23, 0), datetime(2022, 7, 28, 2, 0), []),  # web-thursday
				(datetime(2022, 7, 27, 23, 30), datetime(2022, 7, 27, 23, 45), []),  # 23:30 - 23:45
				(datetime(2022, 7, 28, 0, 0), datetime(2022, 7, 28, 0, 15), [])  # 00:00 - 00:15
			]
		)

		self.assertEqual(
			get_time_overlaps(
				datetime(year=2022, month=7, day=27, hour=23),
				datetime(year=2022, month=7, day=28, hour=2),
				["23:30 - 23:45", "00:00 - 00:15", "Wednesday 00:00 - Thursday 1:30"],
				partial=True
			),
			[
				(datetime(2022, 7, 27, 23, 0), datetime(2022, 7, 28, 1, 30), []),  # web-thursday
				(datetime(2022, 7, 27, 23, 30), datetime(2022, 7, 27, 23, 45), []),  # 23:30 - 23:45
				(datetime(2022, 7, 28, 0, 0), datetime(2022, 7, 28, 0, 15), [])  # 00:00 - 00:15
			]
		)

		self.assertEqual(
			get_time_overlaps(
				datetime(year=2021, month=10, day=1),
				datetime(year=2021, month=10, day=20),
				["5/10/2021 10:30 - 6/10/2021 11:30"],
				milliseconds=True,
				tz_delta=timedelta(hours=2)
			),
			[(1633422600000, 1633512600000, [])]
		)
		self.assertEqual(
			get_time_overlaps(
				datetime(year=2022, month=6, day=19),
				datetime(year=2022, month=6, day=27),
				"Monday 2:30 p.m - 6:30 p.m , Tuesday 9:30 a.m-12:30 p.m,  Tuesday 2:30 p.m-6:30 p.m, Wednesday 9:30 a.m - 12:30 p.m, Wednesday 2:30 p.m- 6:30 p.m, Thursday 9:30 a.m -12:30 p.m, Thursday 2:30 p.m-6:30 p.m, Friday 9:30 a.m- 12:30p.m, Friday 2:30 p.m-6:30 p.m, Saturday 9:30 a.m-12:30 p.m",
			),
			[
				(datetime(2022, 6, 20, 14, 30), datetime(2022, 6, 20, 18, 30), []),
				(datetime(2022, 6, 21, 9, 30), datetime(2022, 6, 21, 12, 30), []),
				(datetime(2022, 6, 21, 14, 30), datetime(2022, 6, 21, 18, 30), []),
				(datetime(2022, 6, 22, 9, 30), datetime(2022, 6, 22, 12, 30), []),
				(datetime(2022, 6, 22, 14, 30), datetime(2022, 6, 22, 18, 30), []),
				(datetime(2022, 6, 23, 9, 30), datetime(2022, 6, 23, 12, 30), []),
				(datetime(2022, 6, 23, 14, 30), datetime(2022, 6, 23, 18, 30), []),
				(datetime(2022, 6, 24, 9, 30), datetime(2022, 6, 24, 12, 30), []), 
				(datetime(2022, 6, 24, 14, 30), datetime(2022, 6, 24, 18, 30), []),
				(datetime(2022, 6, 25, 9, 30), datetime(2022, 6, 25, 12, 30), [])
			]
		)

		self.assertEqual(
			get_time_overlaps(
				datetime(year=2022, month=6, day=19),
				datetime(year=2022, month=6, day=25),
				"Monday - wednesday 2:30 p.m - 6:30 p.m , 21/06/2022 - 24/06/2022 9:30 a.m to 12:30 p.m",
				exclude=["22/06/2022 9:30 a.m"]
			),
			[
				(datetime(2022, 6, 20, 14, 30), datetime(2022, 6, 20, 18, 30), []),
				(datetime(2022, 6, 21, 9, 30), datetime(2022, 6, 21, 12, 30), []),
				(datetime(2022, 6, 21, 14, 30), datetime(2022, 6, 21, 18, 30), []),
				# (datetime(2022, 6, 22, 9, 30), datetime(2022, 6, 22, 12, 30), []),
				(datetime(2022, 6, 22, 14, 30), datetime(2022, 6, 22, 18, 30), []),
				(datetime(2022, 6, 23, 9, 30), datetime(2022, 6, 23, 12, 30), []),
				(datetime(2022, 6, 24, 9, 30), datetime(2022, 6, 24, 12, 30), []),
			]
		)

		self.assertEqual(
			get_time_overlaps(
				datetime(year=2022, month=6, day=19),
				datetime(year=2022, month=6, day=25),
				"Monday - wednesday 2:30 p.m - 6:30 p.m , 21/06/2022 - 24/06/2022 9:30 a.m to 12:30 p.m",
				exclude=["22/06/2022 9:30 a.m"],
				interval="2h"
			),
			[
				(datetime(2022, 6, 20, 14, 30), datetime(2022, 6, 20, 16, 30), []),
				(datetime(2022, 6, 20, 16, 30), datetime(2022, 6, 20, 18, 30), []),
				(datetime(2022, 6, 21, 9, 30), datetime(2022, 6, 21, 11, 30), []),
				(datetime(2022, 6, 21, 14, 30), datetime(2022, 6, 21, 16, 30), []),
				(datetime(2022, 6, 21, 16, 30), datetime(2022, 6, 21, 18, 30), []),
				# (datetime(2022, 6, 22, 9, 30), datetime(2022, 6, 22, 12, 30), []),
				(datetime(2022, 6, 22, 14, 30), datetime(2022, 6, 22, 16, 30), []),
				(datetime(2022, 6, 22, 16, 30), datetime(2022, 6, 22, 18, 30), []),
				(datetime(2022, 6, 23, 9, 30), datetime(2022, 6, 23, 11, 30), []),
				(datetime(2022, 6, 24, 9, 30), datetime(2022, 6, 24, 11, 30), []),
			]
		)

		dt_from = datetime(year=2024, month=9, day=17)
		self.assertEqual(
			get_time_overlaps(
				dt_from, dt_from + timedelta(days=365),
				"18-09", partial=True, limit=1
			),
			[
				(datetime(2024, 9, 18, 0, 0), datetime(2024, 9, 19, 0, 0), [])
			]
		)

		self.assertEqual(
			get_time_overlaps(
				dt_from, dt_from + timedelta(days=365),
				"monday 09:00-17:30, tuesday 09:00-17:30, wednesday 09:00-17:30, thursday 09:00-17:30, friday 09:00-17:30, sunday 09:00-17:30",
				limit=1
			),
			[
				(datetime(2024, 9, 17, 9, 0), datetime(2024, 9, 17, 17, 30), []),
			]
		)

		self.assertEqual(
			get_time_overlaps(
				dt_from, dt_from + timedelta(days=365),
				"Monday 8:00 AM -  10:00 AM, 10:01 AM - 12:00 PM, 12:01 PM - 2:00 PM, 2:01 PM - 3:00 PM, Tuesday 8:00 AM -  10:00 AM, 10:01 AM - 12:00 PM, 12:01 PM - 2:00 PM, 2:01 PM - 3:00 PM, Wednesday 8:00 AM -  10:00 AM, 10:01 AM - 12:00 PM, 12:01 PM - 2:00 PM, 2:01 PM - 3:00 PM, Thursday 8:00 AM -  10:00 AM, 10:01 AM - 12:00 PM, 12:01 PM - 2:00 PM, 2:01 PM - 3:00 PM, Friday 8:00 AM -  10:00 AM, 10:01 AM - 12:00 PM, 12:01 PM - 2:00 PM, 2:01 PM - 3:00 PM, Saturday 8:00 AM -  10:00 AM, 10:01 AM - 12:00 PM",
				limit=1
			),
			[(datetime(2024, 9, 17, 8, 0), datetime(2024, 9, 17, 10, 0), [])]
		)

		self.assertEqual(
			get_time_overlaps(
				dt_from, dt_from + timedelta(days=365),
				"next weekend",
				limit=1
			),
			[(datetime(2024, 9, 21, 0, 0), datetime(2024, 9, 23, 0, 0), [])]
		)

		self.assertEqual(
			get_time_overlaps(
				dt_from, dt_from + timedelta(days=365),
				"next month weekend",
				limit=1
			),
			[(datetime(2024, 10, 5, 0, 0), datetime(2024, 10, 7, 0, 0), [])]
		)

		self.assertEqual(
			get_time_overlaps(
				dt_from, dt_from + timedelta(days=365),
				"Fri 14th, April 2025 6:00 PM",
				limit=1, delim="\n"
			),
			[(datetime(2025, 4, 14, 18, 0), datetime(2025, 4, 14, 19, 0), [])]
		)

		self.assertEqual(
			get_time_overlaps(
				dt_from, dt_from + timedelta(days=365),
				"Saturday 2:00 PM - Monday 8:00 AM",
				limit=1, delim="\n"
			),
			[(datetime(2024, 9, 21, 14, 0), datetime(2024, 9, 23, 8, 0), [])]
		)

	def test_thread_pool(self):
		from blaster.tools import ThreadPool
		tp = ThreadPool(5)
		runs = []
		for i in range(100):
			tp.add_task(lambda i: runs.append(i), i)
		tp.stop()
		self.assertEqual(len(runs), 100)

	def test_rate_limiting(self):
		for i in range(60):
			ASSERT_RATE_PER_MINUTE("test")
		try:
			ASSERT_RATE_PER_MINUTE("test")
			self.assertTrue(False)  # should not happen
		except Exception as ex:
			self.assertTrue(isinstance(ex, RateLimitingException))

	def test_find(self):
		from blaster.tools import find_nth
		self.assertEqual(find_nth("abcabcabcx", "abc", 2), 3)
		self.assertEqual(find_nth("abcabcabcx", "abc", 3), 6)

	def test_can_retry(self):
		runs = []

		@retry(2)
		def _retry_func():
			runs.append(1)
			raise Exception

		try:
			_retry_func()
		except Exception:
			self.assertEqual(len(runs), 2)

	def test_ignore_exceptions(self):
		runs = []

		@ignore_exceptions
		def _retry_func():
			runs.append(1)
			raise Exception
		self.assertIsNone(_retry_func())
		self.assertEqual(len(runs), 1)

		@ignore_exceptions(IndexError)
		def _retry_func2():
			runs.append(1)
			raise Exception

		try:
			_retry_func2()
		except Exception as ex:
			self.assertEqual(len(runs), 2)

	def test_run_shell(self):
		from blaster.tools import run_shell
		s = run_shell("sh ./test/test_run_shell.sh")
		self.assertEqual(s.total_output, "This is a message to stdout\n")
		self.assertEqual(s.total_err, "This is an error message to stderr\n")

	def test_string_to_units(self):
		self.assertEqual(parse_string_to_units(".9 units"), (0.9, "units"))
		self.assertEqual(parse_string_to_units("0.9 units"), (0.9, "units"))
		self.assertEqual(parse_string_to_units("rs -1.9"), (-1.9, "rs"))
		self.assertEqual(parse_currency_string("INR 2000"), (2000000, "INR", 2000))
		self.assertEqual(parse_currency_string("2000INR D"), (2000000, "INR", 2000))
		self.assertEqual(parse_currency_string("D INR2000"), (2000000, "INR", 2000))
		self.assertEqual(parse_currency_string("2000", default_currency_code="INR"), (2000000, "INR", 2000))	
		self.assertEqual(parse_string_to_int("20,00"), 2000)
		self.assertEqual(parse_string_to_int("20.00"), 20)
		self.assertIsNone(parse_string_to_int("20a", full_match=True))
		self.assertIsNone(parse_string_to_int("20 ", full_match=True))

	def remove_duplicates(self):
		l2 = tools.remove_duplicates([1, 2, 3, 5, 3])
		self.assertEqual(l2, [1, 2, 3, 5])

		l2 = tools.remove_duplicates(["a", "a", "b", "c", "a", "b"])
		self.assertEqual(l2, ["a", "b", "c"])

		l2 = tools.remove_duplicates([{"a": 1}, {"a": 1}, {"a": 2}, {"a": 2}], key=lambda x: x.get("a"))
		self.assertEqual(l2, [{"a": 1}, {"a": 2}])

	def test_expiring_cache(self):
		c = ExpiringCache(10, ttl=1000)
		c.set(1, 2)
		c.set(2, 2)
		time.sleep(1)
		c.set(3, 3)
		self.assertIsNone(c.get(1))
		self.assertIsNone(c.get(2))
		self.assertEqual(c.get(3), 3)
		time.sleep(1)  # all items expired
		self.assertIsNone(c.get(3))
		for i in range(9):
			time.sleep(0.2)
			c.set(i, i)
		# when last item is added, it expired first 4 items
		self.assertEqual(len(c.to_son()), 5)

	def test_get_by_key_path(self):
		self.assertEqual(
			get_by_key_path({"a": {"b": 1}}, "a.b"), 1
		)
		self.assertEqual(
			get_by_key_path({"a": [{"b": 1}, {"b": 2}]}, "a[].b"),
			[1, 2]
		)
		self.assertEqual(
			get_by_key_path({"a": [{"b": 1}, {"b": 2}]}, "a[0].b"),
			1
		)
		self.assertEqual(
			get_by_key_path({"a": [{"b": 1}, {"b": 2}]}, "a[0,1].b"),
			[1, 2]
		)

		self.assertEqual(
			get_by_key_path({"a": [{"b": {"d": "d1", "c": "c1", "e": "e1"}}, {"b": {"d": "d2", "c": "c2", "e": "e2"}}]}, "a[].b.c,e"),
			[{"c": "c1", "e": "e1"}, {"c": "c2", "e": "e2"}]
		)
		self.assertEqual(get_by_key_path({"a": 1}, ""), {"a": 1})

		self.assertEqual(
			get_by_key_path({"a": [{"b": [{"c": 1, "d": {"e": 1}}, {"c": 2}]}, {"b": [{"c": 3}, {"c": 4}]}]}, "a[].b[].c,d:e"),
			[[{"c": 1, "e": 1}, {"c": 2}], [{"c": 3}, {"c": 4}]]
		)

	def test_get_by_key_list(self):
		self.assertEqual(
			get_by_key_list({"a": {"b": 1}}, ["a", "b"]), 1
		)
		self.assertEqual(
			get_by_key_list({"a": [{"b": 1}, {"b": 2}]}, ["a", 0, "b"]),
			1
		)
		# NONE
		self.assertIsNone(
			get_by_key_list({"a": [{"b": 1}, {"b": 2}]}, ["a", 0, "c"])
		)
		self.assertIsNone(
			get_by_key_list({"a": [{"b": None}, {"b": 2}]}, ["a", 0, "b", "c"])
		)

	def test_sanitize_to_underscore_id(self):
		self.assertEqual(sanitize_to_underscore_id("a b c"), "a_b_c")
		self.assertEqual(sanitize_to_underscore_id("a b    __ c"), "a_b_c")
		self.assertEqual(sanitize_to_underscore_id("a b    __ c_ ?"), "a_b_c")
		self.assertEqual(sanitize_to_underscore_id("___a b    __ c_ ?"), "_a_b_c")

	def test_xml_to_dict(self):
		xml_string = "\n<note>\n  <to>Tove</to>\n  <from>Jani</from>\n  <heading>Reminder & Reminder</heading>\n  <body>Don't forget me this weekend!</body>\n</note>\n"
		xmltodict(xml_string)
		xml_string = "<note><to>Tove</to><from>Jani</from><heading>Reminder &amp; Reminder</heading><body>Don't forget me this weekend!</body></note>"
		print(xmltodict(xml_string))

	def test_operator_tree(self):
		# tree -> {"left", "right", "operator"}
		tree1 = create_operator_tree("a + b - c * d / e", operators=["+", "-", "*", "/"])
		self.assertEqual(
			tree1,
			{'op': '+', 'left': 'a', 'right': {'op': '-', 'left': 'b', 'right': {'op': '*', 'left': 'c', 'right': {'op': '/', 'left': 'd', 'right': 'e'}}}}
		)
		# with parantheses
		tree2 = create_operator_tree("(a + b) - (c * d) / e", operators=["+", "-", "*", "/"])
		self.assertDictEqual(tree2, {'op': '-', 'left': {'op': '+', 'left': 'a', 'right': 'b'}, 'right': {'op': '/', 'left': {'op': '*', 'left': 'c', 'right': 'd'}, 'right': 'e'}})

		tree2_5 = create_operator_tree("(('a c' + b) - (c * d)) / e", operators=["+", "-", "*", "/"])
		self.assertDictEqual(tree2_5, {'op': '/', 'left': {'op': '-', 'left': {'op': '+', 'left': "'a c'", 'right': 'b'}, 'right': {'op': '*', 'left': 'c', 'right': 'd'}}, 'right': 'e'})

		tree3 = create_operator_tree("", operators=["+", "-", "*", "/"])
		self.assertIsNone(tree3)

		tree4 = create_operator_tree("ordered:gte:2025-07-01 18:49,lte:2025-07-09 18:49", operators=["AND", "OR", "NOT"])
		print(tree4)

	def test_nsplit(self):
		# "", 3  -> None, None, None
		self.assertListEqual(
			nsplit("", ":", 2),
			["", None, None]
		)
		# "a:b:c", 3 -> "a", "b", "c"
		self.assertListEqual(
			nsplit("a:b:c", ":", 2),
			["a", "b", "c"]
		)
		# "a::b:c" -> "a", "", "b", "c"
		self.assertListEqual(
			nsplit("a::b:c", ":", 2),
			["a", "", "b:c"]
		)
		# "a:::c", 3 -> "a", "", "", "c"
		self.assertListEqual(
			nsplit("a:::c", ":", 2),
			["a", "", ":c"]
		)

		self.assertListEqual(
			nsplit(":", ":", 2),
			["", "", None]
		)

		self.assertListEqual(
			nsplit("dpoint::Localpickup : ikea", ":", 2),
			["dpoint", "", "Localpickup : ikea"]
		)


class TestBackgroundTasks(unittest.TestCase):
	def test_background_threads_and_order(self):
		NUM_BUCKETS = 1000
		NUM_BG_TASKS_PER_BUCKET = 100
		partitions = {i: [] for i in range(NUM_BUCKETS)}

		def run(x, y):
			time.sleep(random.random() * 0.1)
			partitions[x].append(y)

		def spin_at_random_times(i):
			for j in range(NUM_BG_TASKS_PER_BUCKET):
				submit_background_task(i, run, i, j)

		join_threads = []
		for i in range(NUM_BUCKETS):
			join_threads.append(
				gevent.spawn(spin_at_random_times, i)
			)

		gevent.joinall(join_threads)
		# after blaster exit
		blaster_exit()  # this should flush the queues and wait
		# All background threads submitted
		for i, j in partitions.items():
			self.assertEqual(j, [i for i in range(NUM_BG_TASKS_PER_BUCKET)])

	def test_class_method_background_task(self):
		from blaster.tools import partitioned_tasks_runner
		partitioned_tasks_runner.is_processing = True  # fix for tests

		class ABT:
			def __init__(self):
				self.a = 100

			@background_task
			def bg_task(self):
				self.a = 200

		abt = ABT()
		abt.bg_task()
		# exit blaster and wait for it to finish
		blaster_exit()
		self.assertEqual(abt.a, 200)


if __name__ == "__main__":
	unittest.main()
