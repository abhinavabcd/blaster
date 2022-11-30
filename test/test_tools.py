import unittest
from blaster import tools
from blaster.tools import get_time_overlaps, retry,\
	SanitizedDict, SanitizedList
from datetime import datetime
from blaster.utils.data_utils import parse_string_to_units,\
	parse_currency_string


class TestSanitization(unittest.TestCase):
	def test_sanitization(self):
		sd = SanitizedDict(a="<a>", b="<b>")
		sd["c"] = "<c>"
		sd["d"] = {"e": "<e>", "f": "<f>"}

		self.assertTrue(sd.get("a") == sd["a"] == "&lt;a&gt;")
		self.assertTrue(sd["d"]["e"] == "&lt;e&gt;")

		sl = SanitizedList(["<a>", "<b>"])
		sl.append({"c": "<c>", "d": "<d>"})
		sl.extend(["<e>", "<f>"])

		self.assertTrue(sl[0] == "&lt;a&gt;")
		self.assertTrue(sl[2]["c"] == "&lt;c&gt;")
		self.assertTrue(sl[3] == "&lt;e&gt;")


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
				milliseconds=True
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

	@retry(2)
	def test_can_retry(self):
		raise Exception

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
		
		

if __name__ == "__main__":
	unittest.main()
