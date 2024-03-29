import unittest
from blaster import tools
import time
import ujson as json
from blaster.tools import get_time_overlaps, retry,\
	ExpiringCache, create_signed_value, decode_signed_value
from blaster.tools.sanitize_html import SanitizedDict, SanitizedList
from datetime import datetime, timedelta
from blaster.utils.data_utils import parse_string_to_units,\
	parse_currency_string


class TestSanitization(unittest.TestCase):
	def test_sanitization(self):
		sd = SanitizedDict(a="<a>", b="<b>")
		sd["c"] = "<c>"
		sd["d"] = {"e": "<e>", "f": "<f>"}

		self.assertTrue(sd.get("a") == sd["a"] == "&lt;a&gt;")
		self.assertTrue(sd["d"]["e"] == "&lt;e&gt;")

		# test iterator
		for k, v in sd.items():
			if(isinstance(v, SanitizedDict)):
				for k1, v1 in v.items():
					self.assertTrue(">" not in v1)
			else:
				self.assertTrue(">" not in v)
		# sanitized Dict 2

		def is_sanitized(o):
			if(isinstance(o, dict)):
				for k, v in o.items():
					if(not is_sanitized(v)):
						return False
				for k in o:
					if(not is_sanitized(o[k])):
						return False
			elif(isinstance(o, list)):
				for v in o:
					if(not is_sanitized(v)):
						return v
			elif(isinstance(o, str)):
				if(">" in o or "<" in o):
					return False
				return True

			return is_sanitized(str(o))

		sd2 = SanitizedDict({"1": "<1>", "2": ["<b>", "<c>"], "3": {"a3": "<v></v>"}})
		self.assertTrue(is_sanitized(sd2))

		self.assertTrue(isinstance(sd, dict))
		sl = SanitizedList(["<a>", "<b>"])
		sl.append({"c": "<c>", "d": "<d>"})
		sl.extend(["<e>", "<f> <>><><><<<>>"])
		sl.append(["<g>", "<h>"])
		self.assertTrue(sl[0] == "&lt;a&gt;")
		self.assertTrue(sl[2]["c"] == "&lt;c&gt;")  # test dict in a list
		# test iterator
		for i in sl:
			if(isinstance(i, SanitizedList)):
				for j in i:
					self.assertTrue(">" not in j)
			else:
				self.assertTrue(">" not in i)

		self.assertTrue(is_sanitized(sl[:]))

		# test serialization deserialization
		self.assertTrue(isinstance(sl, list))

		print(json.dumps(sl))


class TestAuth(unittest.TestCase):
	def test(self):
		secret = "ijkl"
		val = create_signed_value("abcd", "efgh", secret).decode('utf-8')
		self.assertEqual(decode_signed_value("abcd", val, secret), b"efgh")

		val = create_signed_value("abcd", "efgh2", secret, -1).decode('utf-8')
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

	def test_find(self):
		from blaster.tools import find_nth
		self.assertEqual(find_nth("abcabcabcx", "abc", 2), 3)
		self.assertEqual(find_nth("abcabcabcx", "abc", 3), 6)

	@retry(2)
	def test_can_retry(self):
		raise Exception

	def test_run_shell(self):
		from blaster.tools import run_shell
		s = run_shell("sh ./test/test_run_shell.sh")

	def test_string_to_units(self):
		print(parse_string_to_units(".9 units"))
		print(parse_string_to_units("0.9 units"))
		print(parse_string_to_units("rs -1.9"))
		print(parse_currency_string("INR 2000"))

	def remove_duplicates(self):
		l2 = tools.remove_duplicates([1, 2, 3, 5, 3])
		self.assertEqual(l2, [1, 2, 3, 5])

		l2 = tools.remove_duplicates(["a", "a", "b", "c", "a", "b"])
		self.assertEqual(l2, ["a", "b", "c"])

		l2 = tools.remove_duplicates([{"a": 1}, {"a":1}, {"a": 2}, {"a": 2}], key=lambda x:x.get("a"))
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
		time.sleep(1) # all items expired
		self.assertIsNone(c.get(3))
		for i in range(9):
			time.sleep(0.2)
			c.set(i, i) 
		# when last item is added, it expired first 4 items
		self.assertEqual(len(c.to_son()), 5)


if __name__ == "__main__":
	unittest.main()
