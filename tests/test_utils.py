import blaster
import unittest
from blaster.utils.data_utils import convert_currency, parse_string_to_units, \
	parse_currency_string
from blaster.utils import events
from blaster.utils.events import NOT_HANDLING_EVENT


@events.register_listener("test:event_1")
def test_event_1_func():
	return "event_1_func"


@events.register_listener("test:event_1")
def test_event_1a_func():
	return None


@events.register_listener("test:event_1")
def test_event_1b_func():
	return NOT_HANDLING_EVENT


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


class TestEvents(unittest.TestCase):
	@events.register_listener("test:event_1")
	@staticmethod
	def handle_event():
		return "event_1_TestDataUtils.handle_event"

	def test_events(self):
		self.assertTupleEqual(
			events.broadcast_event("test:event_1"),
			("event_1_func2", "event_1_TestDataUtils.handle_event", None, "event_1_func")
		)
		# test broadcast_event_iter
		iter = events.broadcast_event_iter("test:event_1")
		self.assertEqual(next(iter), "event_1_func2")
		self.assertEqual(next(iter), "event_1_TestDataUtils.handle_event")
		self.assertEqual(next(iter), None)
		self.assertEqual(next(iter), "event_1_func")

		self.assertEqual(next(events.broadcast_event_iter("test:event_1")), "event_1_func2")


@events.register_listener("test:event_1")
def test_event_1_func2():
	return "event_1_func2"
