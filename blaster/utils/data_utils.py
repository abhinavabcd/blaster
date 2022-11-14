# -*- coding: utf-8 -*-

'''
Created on 18-Sep-2017

@author: abhinav
'''
import os
import re
import requests
import json
from ..config import DEFAULT_CURRENCY_CODE

# https://www.iban.com/country-codes
# https://www.iban.com/currency-codes
# https://www.iban.com/exchange-rates

COUNTRY_DATA = json.loads(open(os.path.join(os.path.dirname(__file__), "data/countries.json")).read())

# currencies data
_currency_aliases = {"RS" : "INR", "KSH": "KES"}
INR_EXCHANGE_RATE = {
	'USD': 0.012183438191392, 'EUR': 0.012422414249388, 'GBP': 0.010834394013173, 'JPY': 1.7987256393607, 'AUD': 0.018918695391527, 'CHF': 0.012264650860763, 'CAD': 0.016475260651803, 'ERN': 0.18138325525675, 'WST': 0.033606866950932, 'BRL': 0.061575865015267, 'NPR': 1.6002818984603, 'ZAR': 0.21904097072182, 'AZN': 0.020548978744206, 'PYG': 87.698105901714, 'GYD': 2.5174813763509, 'RWF': 12.847640911669, 'MOP': 0.097330612967188, 'BAM': 0.023829610727296, 'DKK': 0.091520558147964, 'LKR': 4.4253369648097, 'TND': 0.039426152317367, 'TWD': 0.3904224223879, 'IQD': 17.67608659105, 'AFN': 1.0633090696497, 'NAD': 0.21804729998828, 'SYP': 30.090527398382, 'LAK': 208.6569418136, 'GTQ': 0.094094802595334, 'PKR': 2.6891007125432, 'BGN': 0.024025334618917, 'PEN': 0.048222592843438, 'TMT': 0.042347807393353, 'SVC': 0.10534734117731, 'XCD': 0.032572927334165, 'AOA': 5.8338777609108, 'MVR': 0.18604621560991, 'SAR': 0.045747954133748, 'PLN': 0.057555274399256, 'GIP': 0.010615125834899, 'GEL': 0.033448488915627, 'MKD': 0.74888335612859, 'AWG': 0.021780640046069, 'HNL': 0.29746098884978, 'KES': 1.4619751970779, 'BHD': 0.0045697582094003, 'EGP': 0.29597397827042, 'HRK': 0.092572586150074, 'MRO': 0.46205975580038, 'COP': 61.045485173549, 'BBD': 0.024295428377073, 'DJF': 2.142075760844, 'MZN': 0.77155198457389, 'UGX': 45.737601645542, 'HKD': 0.095637705566837, 'MAD': 0.13281872006941, 'MYR': 0.057808842145032, 'MDL': 0.231466653616, 'PAB': 0.012111725630068, 'FJD': 0.027507699219076, 'CDF': 24.717683552498, 'TOP': 0.028683525013509, 'VUV': 1.4613772827802, 'KWD': 0.0037577233608513, 'THB': 0.45571957269225, 'XOF': 8.0978306511739, 'IRR': 508.82594230842, 'BOB': 0.083453674891916, 'LRD': 1.8502266037841, 'SDG': 6.9721953727962, 'PGK': 0.042396738640658, 'BWP': 0.16086663493789, 'OMR': 0.0046684705199891, 'ILS': 0.0432693372504, 'NGN': 5.3171962841669, 'UZS': 135.05352448274, 'ETB': 0.64238204558346, 'TTD': 0.081557777541978, 'ZMW': 0.19505971360262, 'KHR': 49.8448143478, 'SEK': 0.13334849790674, 'SGD': 0.01714093842782, 'HUF': 4.9317152140234, 'BYN': 0.036140191195114, 'TJS': 0.12333235182038, 'GMD': 0.75524441290526, 'CVE': 1.34522357781, 'ALL': 1.4339963895992, 'SCR': 0.15912051783169, 'DOP': 0.65633142833622, 'CNY': 0.087777573416397, 'ISK': 1.7941283265918, 'LYD': 0.060637696688727, 'CLP': 11.29324809433, 'BSD': 0.012040013068743, 'XPF': 1.4503666266756, 'LSL': 0.21788110539988, 'TZS': 28.066765860052, 'ANG': 0.021747410910516, 'LBP': 18.359279806714, 'MXN': 0.23771601585986, 'KZT': 5.6065613522336, 'HTG': 1.5472110658854, 'BND': 0.016981614654388, 'KMF': 5.967227017736, 'MRU': 0.45701040812891, 'MNT': 41.071840558137, 'JOD': 0.008611275201891, 'PHP': 0.71215375291958, 'XAF': 8.0977363153449, 'AMD': 4.8273157990942, 'UYU': 0.48746899128935, 'JMD': 1.8442581308686, 'SSP': 7.3400952698581, 'CUP': 0.012040013068743, 'NZD': 0.020664124312008, 'TRY': 0.2266941970772, 'VND': 302.6775993976, 'KGS': 1.0110804368492, 'MGA': 51.459947846019, 'SRD': 0.35599005016766, 'GHS': 0.1684502123068, 'MWK': 12.358841776249, 'YER': 3.0106372857781, 'NOK': 0.12560943375401, 'QAR': 0.044393720562668, 'CZK': 0.29969962628925, 'RUB': 0.78333676549317, 'RSD': 1.4310703139363, 'NIO': 0.433121227704, 'SBD': 0.099050592614216, 'MMK': 25.263810011899, 'MUR': 0.53332091471014, 'VES': 0.10313041241555, 'BDT': 1.2562816787422, 'RON': 0.06004767821541, 'DZD': 1.7031267545841, 'ARS': 1.9109965053783, 'STN': 0.30074698609641, 'BIF': 24.889857229834, 'SZL': 0.21804729998828, 'SOS': 6.8403926844849, 'AED': 0.044710920816339, 'IDR': 191.24754830074, 'KRW': 17.128153864145, 'UAH': 0.44986965813791, 'CRC': 7.5351414066391, 'BZD': 0.024254200770799, 'GNF': 103.76043930477
}

INR_EXCHANGE_RATE["INR"] = 1  # this is the base
INR_EXCHANGE_RATE["APP_CURRENCY"] = 1  # this is the base
INR_EXCHANGE_RATE["ROUTE"] = 1  # legacy
INR_EXCHANGE_RATE["POINTS"] = 1000  # legacy

# update currency aliases
for i,j in _currency_aliases.items():
	INR_EXCHANGE_RATE[i] = INR_EXCHANGE_RATE[j]

def reload_exchange_rates():
	# can use : 
	for k, v in requests.get("http://www.floatrates.com/daily/inr.json").json().items():
		INR_EXCHANGE_RATE[v["code"]] = v["rate"]
	# update with aliases
	for i,j in _currency_aliases.items():
		INR_EXCHANGE_RATE[i] = INR_EXCHANGE_RATE[j]
	
# currencies END


#### MIME_TYPES
__mime_types = json.loads(open(os.path.join(os.path.dirname(__file__), "data/mime_types.json")).read())
FILE_EXTENSION_TO_MIME_TYPE = {k: v["mime_type"] for k, v in __mime_types.items()}
MIME_TYPE_TO_EXTENSION = {v["mime_type"]: k for k, v in __mime_types.items()}

#points  =
def make_nearrest_currency(val, f):
	val = int(val)
	if val == 0:
		return f
	if(val%f == 0):
		return val
	return f * (val / f + 1)


'''
	return UNITS (current absolute app units value), CURRENCY_CODE, VALUE_IN_CURRENCY_CODE
'''
NUM_REGEX = re.compile(r'[+-.]?[0-9]+(?:\.[0-9]+)?')
def parse_currency_string(currency_str, default_currency_code=DEFAULT_CURRENCY_CODE, country_code=None):
	val = NUM_REGEX.findall(currency_str)
	if(not val):
		return 0, default_currency_code, 0

	val = val[0]
	currency_code = currency_str.replace(val, "").strip().upper()
	if(not currency_code):
		if(country_code):
			currency_code = COUNTRY_DATA[country_code]["currency_code"]

	if(not currency_code or currency_code not in INR_EXCHANGE_RATE): # if invalid currency set to default
		currency_code = default_currency_code

	if(val[0] == "."):
		val = "0." + val[1:].replace(".", "", 1)

	val = float(val) # convert str to float
	return int(val * 1000 * INR_EXCHANGE_RATE[currency_code]), currency_code, val


def convert_currency(val, from_currency='APP_CURRENCY', to_currency='INR'):
	# val / from_currency_inr_exchange -> inr  then multiply to_exchange rate 
	return (val *  INR_EXCHANGE_RATE[to_currency.upper()]) / INR_EXCHANGE_RATE[from_currency.upper()]


# simply parse number , units
def parse_string_to_units(full_str, default_unit="EUR"):
	val = NUM_REGEX.findall(full_str)
	if(not val):
		return 0, default_unit

	units_value = val[0]
	units_code = full_str.replace(units_value, "").strip() or default_unit

	if(units_value[0] == "."):
		units_value = "0." + units_value[1:].replace(".", "", 1)

	return float(units_value), units_code
