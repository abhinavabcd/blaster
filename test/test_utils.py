import blaster
import unittest
from blaster.utils.data_utils import convert_currency, parse_currency_string


class TestDataUtils(unittest.TestCase):
    def test_convert_currency(self):
        self.assertGreater(convert_currency(100, "USD"), 7500)
        self.assertEqual(parse_currency_string("100", country_code="KE")[1], "KES")
        


