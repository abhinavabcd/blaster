import blaster
import unittest
from blaster.utils.data_utils import convert_currency, parse_string_to_units,\
	parse_currency_string


class TestDataUtils(unittest.TestCase):
	def test_convert_currency(self):
		self.assertGreater(convert_currency(100, "USD"), 7500)
		self.assertEqual(parse_currency_string("100", country_code="KE")[1], "KES")
		v, c = parse_string_to_units("1")
		self.assertAlmostEqual(v, 1) and self.assertEqual(c, "EUR")
		v, c = parse_string_to_units("KSH 91a")
		self.assertAlmostEqual(v, 91) and self.assertEqual(c, "KSH")

		v, c = parse_string_to_units("KSH 91,10,1000")
		self.assertAlmostEqual(v, 91101000) and self.assertEqual(c, "KSH")

		v, c = parse_string_to_units("KSH 91,10,1000.90")
		self.assertAlmostEqual(v, 91101000.90) and self.assertEqual(c, "KSH")
