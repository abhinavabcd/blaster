"""Schema storage stays raw; explicit server/container views escape on read."""
import unittest

from blaster.schema import Object, Field, Str, List, schema, BlasterSchemaTypeError
from blaster.tools.sanitize_html import HtmlSanitizedDict, HtmlSanitizedList, raw_value


class LazyItem(Object):
	name: Str(maxlen=3)
	count: int


class LazyOrder(Object):
	name = Field(Str(maxlen=3), json_name="displayName")
	child: LazyItem
	items: list[LazyItem]
	options: dict[str, str]
	tags: list[str]
	active: bool = True


schema(LazyOrder)


def order_data():
	return {
		"displayName": "<a>", "child": {"name": "<b>", "count": "2"},
		"items": [{"name": "<c>", "count": "3"}],
		"options": {"label": "<d>"}, "tags": ["<e>"],
		"unknown": "<f>",
	}


class TestRawSchema(unittest.TestCase):
	def test_raw_validation_and_inplace_identity(self):
		data = order_data()
		child, items, options = data["child"], data["items"], data["options"]
		obj = LazyOrder.from_dict(data)
		self.assertIs(obj.__dict__, data)
		self.assertIs(obj.child.__dict__, child)
		self.assertIs(raw_value(obj.items), items)
		self.assertIs(raw_value(obj.options), options)
		self.assertEqual(child["name"], "<b>")
		self.assertEqual(child["count"], 2)
		self.assertEqual(data["name"], "<a>")
		self.assertEqual(data["unknown"], "<f>")
		self.assertNotIn("displayName", data)
		self.assertTrue(obj.active)
		self.assertNotIn("unknown", obj.to_dict())

	def test_schema_attributes_and_exports_stay_raw(self):
		obj = LazyOrder.from_dict(order_data())
		self.assertEqual(obj.name, "<a>")
		self.assertEqual(obj.child.name, "<b>")
		self.assertEqual(obj.items[0].name, "<c>")
		self.assertEqual(obj.options["label"], "<d>")
		self.assertEqual(list(obj.tags), ["<e>"])
		self.assertEqual(obj.unknown, "<f>")
		self.assertEqual(obj.to_dict()["items"][0]["name"], "<c>")
		self.assertEqual(obj.to_dict()["items"][0]["name"], "<c>")
		self.assertEqual(obj.to_dict(), obj.to_dict())
		self.assertIs(type(obj.items), list)
		self.assertIs(type(obj.options), dict)

	def test_shared_mutations_and_replaced_containers(self):
		data = order_data()
		obj = LazyOrder.from_dict(data)
		view = obj.options
		data["options"]["label"] = "<x>"
		self.assertEqual(view["label"], "<x>")
		view["label"] = "<y>"
		self.assertEqual(data["options"]["label"], "<y>")
		obj.tags.append("<z>")
		self.assertEqual(data["tags"], ["<e>", "<z>"])
		data["options"] = {"label": "<q>"}
		self.assertEqual(obj.options["label"], "<q>")
		self.assertIsNot(view, obj.options)
		obj.options = view
		self.assertIs(data["options"], raw_value(view))

	def test_copy_does_not_escape_or_modify_original(self):
		data = order_data()
		obj = LazyOrder.from_dict(data, inplace=False)
		self.assertIsNot(obj.__dict__, data)
		self.assertEqual(data, order_data())
		self.assertEqual(obj.child.__dict__["name"], "<b>")
		obj.options["label"] = "changed"
		self.assertEqual(data["options"]["label"], "<d>")

	def test_sanitized_input_is_read_raw_for_constraints(self):
		data = HtmlSanitizedDict(order_data())
		obj = LazyOrder.from_dict(data)
		self.assertIs(obj.__dict__, data)
		self.assertEqual(obj.name, "<a>")
		self.assertEqual(dict.__getitem__(data, "name"), "<a>")
		self.assertIs(type(dict.__getitem__(data, "items")), list)

	def test_loading_skips_init_and_preserves_normal_construction(self):
		calls = []
		class ConstructorModel(Object):
			count: int
			def __init__(self):
				calls.append(True)
				super().__init__()
		schema(ConstructorModel)
		self.assertEqual(ConstructorModel.from_dict({"count": "2"}).count, 2)
		self.assertEqual(calls, [])
		ConstructorModel()
		self.assertEqual(calls, [True])

	def test_hooks_and_standalone_models_always_see_raw_values(self):
		seen = []
		class HookModel(Object):
			name: Str(maxlen=3)
			child: LazyItem
			def before_validate(self, data):
				seen.append((self.name, data["child"]["name"]))
			def after_validate(self):
				seen.append((self.name, self.child.name))
		schema(HookModel)
		obj = HookModel.from_dict({"name": "<a>", "child": {"name": "<b>", "count": 1}})
		self.assertEqual(seen, [("<a>", "<b>"), ("<a>", "<b>")])
		self.assertEqual(obj.child.name, "<b>")
		with self.assertRaises(BlasterSchemaTypeError):
			HookModel.from_dict({"name": "too long", "child": {"name": "<b>", "count": 1}})
		self.assertEqual(obj.name, "<a>")

	def test_conversion_is_not_skipped_for_equal_values(self):
		obj = LazyItem.from_dict({"name": "<a>", "count": True})
		self.assertIs(type(obj.count), int)

	def test_nullable_homogeneous_array_keeps_existing_mixing_rule(self):
		_, validate = schema(List[[int, None]])
		with self.assertRaises(TypeError):
			validate([1, None])
		self.assertEqual(validate([None, None]), [None, None])

	def test_required_and_default_return_behavior(self):
		with self.assertRaises(BlasterSchemaTypeError):
			LazyItem.from_dict({"name": "<a>"})
		self.assertIsNone(LazyItem.from_dict({"name": "<a>"}, default=None))

	def test_validation_does_not_change_other_models(self):
		import gevent
		from gevent.event import Event
		entered, resume = Event(), Event()
		class ConcurrentHookModel(Object):
			name: str
			def before_validate(self, data):
				entered.set()
				resume.wait(timeout=1)
				self.asserted_raw = self.name
		schema(ConcurrentHookModel)
		other = LazyItem.from_dict({"name": "<a>", "count": 1})
		worker = gevent.spawn(ConcurrentHookModel.from_dict, {"name": "<b>"})
		try:
			self.assertTrue(entered.wait(timeout=1))
			self.assertEqual(other.name, "<a>")
		finally:
			resume.set()
			worker.join(timeout=1)
		self.assertTrue(worker.successful())
		self.assertEqual(worker.value.__dict__["asserted_raw"], "<b>")
		self.assertEqual(worker.value.name, "<b>")


class TestSanitizedViews(unittest.TestCase):
	def test_lazy_nested_views_do_not_copy_or_replace_storage(self):
		data = {"child": {"name": "<a>"}, "values": ["<b>"]}
		container = HtmlSanitizedDict(data)
		child, values = container["child"], container["values"]
		self.assertIs(dict.__getitem__(container, "child"), data["child"])
		self.assertIs(raw_value(child), data["child"])
		self.assertIs(container["child"], child)
		data["child"]["name"] = "<c>"
		self.assertEqual(child["name"], "&lt;c&gt;")
		values.append("<d>")
		self.assertEqual(data["values"], ["<b>", "<d>"])
		self.assertEqual(values[:], ["&lt;b&gt;", "&lt;d&gt;"])
		self.assertEqual(values.get(0, escape_html=False), "<b>")

	def test_mapping_reads_and_replacement(self):
		container = HtmlSanitizedDict(name="<a>")
		self.assertEqual(container["name"], "&lt;a&gt;")
		self.assertEqual(container.get("name"), "&lt;a&gt;")
		self.assertEqual(list(container.values()), ["&lt;a&gt;"])
		self.assertEqual(list(container.items()), [("name", "&lt;a&gt;")])
		self.assertEqual(container.get("name", escape_html=False), "<a>")
		self.assertEqual(container.raw(), {"name": "<a>"})
		container["child"] = {"name": "<b>"}
		old = container["child"]
		container["child"] = {"name": "<c>"}
		self.assertEqual(container["child"]["name"], "&lt;c&gt;")
		self.assertIsNot(old, container["child"])
		with self.assertRaises(KeyError):
			container["missing"]
		self.assertEqual(container.get("missing", "fallback"), "fallback")

	def test_list_reads_and_mutation(self):
		values = HtmlSanitizedList(["<a>", {"name": "<b>"}])
		self.assertEqual(values[0], "&lt;a&gt;")
		self.assertEqual(values.raw()[0], "<a>")
		self.assertEqual(list(values[:1]), ["&lt;a&gt;"])
		self.assertEqual(list(values[:1]), list(values)[:1])
		self.assertEqual(values.get(100, default="fallback"), "fallback")
		child = values[-1]
		values.insert(0, "new")
		self.assertIs(raw_value(values[-1]), raw_value(child))
		values[-1] = {"name": "<c>"}
		self.assertEqual(values[-1]["name"], "&lt;c&gt;")


class TestServerObjectViews(unittest.TestCase):
	def test_server_view_keeps_schema_objects_raw(self):
		from blaster.server import Request
		obj = LazyOrder.from_dict(order_data())
		req = Request(None)
		req.escape_html = True
		view = req._expose(obj)
		self.assertIsInstance(view, LazyOrder)
		self.assertIs(view.__dict__, obj.__dict__)
		self.assertEqual(view.name, "&lt;a&gt;")
		self.assertEqual(view.child.name, "&lt;b&gt;")
		self.assertEqual(view.items[0].name, "&lt;c&gt;")
		self.assertIs(view.child, view.child)
		self.assertEqual(view.options["label"], "&lt;d&gt;")
		self.assertEqual(view.to_dict()["name"], "&lt;a&gt;")
		self.assertEqual(view.to_dict(escape_html=False)["name"], "<a>")
		self.assertEqual(obj.name, "<a>")
		self.assertEqual(obj.to_dict()["name"], "<a>")
		view.child.name = "<x>"
		view.options["label"] = "<y>"
		self.assertEqual(obj.child.name, "<x>")
		self.assertEqual(obj.options["label"], "<y>")
		self.assertEqual(view.child.name, "&lt;x&gt;")
		view.options = view.options
		self.assertIs(type(obj.options), dict)
		del view.options["label"]
		self.assertNotIn("label", obj.options)

	def test_escape_policy_is_request_local(self):
		from blaster.server import Request
		obj = LazyOrder.from_dict(order_data())
		escaped, raw = Request(None), Request(None)
		escaped.escape_html, raw.escape_html = True, False
		view = escaped._expose(obj)
		self.assertIs(raw._expose(obj), obj)
		self.assertEqual(view.name, "&lt;a&gt;")
		self.assertEqual(raw._expose(obj).name, "<a>")
		self.assertEqual(view.items[0].name, "&lt;c&gt;")
