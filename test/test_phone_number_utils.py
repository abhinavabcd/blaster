import unittest
from blaster.utils.phone_number_utils import PhoneNumberObj


class Test(unittest.TestCase):
	def test_parsing(self):
		print(PhoneNumberObj.parse_phone_number('+917680971071').__dict__)
		print(PhoneNumberObj.parse_phone_number('+917680971071').__dict__)
		print(PhoneNumberObj.parse_phone_number("229270555", country_phone_code="31").__dict__)
		self.assertEqual(PhoneNumberObj.parse_phone_number('+917680971071', '+91').phone_number, "917680971071")
		self.assertEqual(PhoneNumberObj.parse_phone_number('917680971071', '+91').phone_number, "917680971071")
		self.assertEqual(PhoneNumberObj.parse_phone_number('07680971071', '+91').phone_number, "917680971071")
		self.assertEqual(PhoneNumberObj.parse_phone_number('7680971071', '+91').phone_number, "917680971071")
		self.assertEqual(PhoneNumberObj.parse_phone_number('7680971071', '+64').phone_number, "647680971071")
		self.assertEqual(PhoneNumberObj.parse_phone_number('76809710', '+64').phone_number, "6476809710")
