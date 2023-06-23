import re
from .data_utils import COUNTRY_DATA


NON_DIGITS = re.compile(r"[^0-9]")

country_code_num_digits_map = {"93": [9], "358": [10], "355": [9], "213": [9], "1": [10], "374": [6], "297": [7], "61": [9], "672": [6], "43": [11], "994": [9], "973": [8], "880": [10], "375": [9], "32": [9], "501": [7], "229": [6, 7, 8, 9], "387": [8], "55": [11], "246": [7], "359": [9], "226": [8], "855": [9], "235": [8], "56": [9], "86": [11], "57": [10], "682": [5], "506": [8], "385": [9], "357": [8], "420": [9], "45": [8], "670": [8], "593": [9], "20": [10], "503": [7], "44": [10], "268": [8], "500": [5], "298": [5], "691": [7], "33": [9], "594": [9], "689": [6], "241": [7], "995": [9], "49": [10], "233": [9], "30": [10], "299": [6], "590": [12], "852": [8], "36": [9], "91": [10], "62": [10], "98": [10], "353": [9], "972": [9], "39": [10], "81": [11], "7": [10], "686": [5], "383": [8], "965": [8], "371": [8], "961": [8], "231": [8], "218": [10], "370": [8], "352": [9], "265": [7, 8, 9], "60": [7], "960": [7], "223": [8], "692": [7], "596": [12], "230": [8], "52": [10], "373": [8], "976": [8], "382": [8], "95": [10], "977": [10], "31": [9], "687": [6], "64": [8, 9, 10], "227": [8], "234": [8, 10], "254": [9], "255": [9], "256": [9], "683": [4], "90": [11], "47": [8], "968": [8], "92": [10], "680": [7], "970": [9], "507": [8], "51": [9], "63": [10], "48": [9], "351": [9], "974": [8], "262": [12], "290": [4], "966": [9], "381": [8], "65": [8], "421": [9], "677": [7], "252": [9], "27": [9], "34": [9], "94": [7], "46": [7], "41": [9], "963": [9], "886": [9], "66": [9], "228": [8], "216": [8], "380": [9], "971": [9], "58": [7], "84": [9], "967": [9]}


class SimpleTrie(dict):
	end_obj = None


country_code_num_digits_trie = SimpleTrie()


for i, j in country_code_num_digits_map.items():
	_root = country_code_num_digits_trie
	for k in i:
		if k not in _root:
			_root[k] = SimpleTrie()
		_root = _root[k]
	_root.end_obj = j


class PhoneNumberObj:
	phone_number = None
	country_phone_code = None
	national_number = None

	def __init__(self, country_phone_code, national_number, phone_number):
		self.phone_number = phone_number
		self.national_number = national_number
		if(country_phone_code):
			self.country_phone_code = country_phone_code.lstrip("+")

	@classmethod
	def validate(cls, phone_number):  # with country code
		phone_number = phone_number.lstrip("+0")
		_root = country_code_num_digits_trie
		for i in range(0, len(phone_number)):
			_d = phone_number[i]
			if(_root.end_obj):
				_remaining_len = len(phone_number) - i
				for _r in _root.end_obj:
					if(_r == _remaining_len):
						return PhoneNumberObj(phone_number[: i], phone_number[i:], phone_number)

			if(_d not in _root):
				break
			_root = _root[_d]
		return None

	# mobile numbers only
	@classmethod
	def parse_phone_number(
		cls, phone_number,
		country_phone_code=None, iso2_country_code=None
	):
		given_phone_number = NON_DIGITS.sub("", phone_number or "")
		phone_number = given_phone_number.lstrip("0")
		# Assume it's with country code

		# Add given country code
		if(not country_phone_code and iso2_country_code):
			if(_country_data := COUNTRY_DATA.get(iso2_country_code)):
				country_phone_code = _country_data["phone_code"]

		# try with given country code
		if(country_phone_code):
			if(ret := PhoneNumberObj.validate(country_phone_code + phone_number)):
				return ret
			if(len(given_phone_number) != len(phone_number)):  # 0 strippe
				if(ret := PhoneNumberObj.validate(country_phone_code + given_phone_number)):
					return ret
		# Assuming full number is given
		return PhoneNumberObj.validate(phone_number)


def sanitize_phone_number(
	phone_number, country_phone_code=None,
	iso2_country_code=None
):
	try:
		phone_number = PhoneNumberObj.parse_phone_number(
			phone_number,
			country_phone_code=country_phone_code,
			iso2_country_code=iso2_country_code
		).phone_number
	except Exception:
		return None

	return phone_number
