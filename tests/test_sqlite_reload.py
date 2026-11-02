import blaster  # gevent monkey-patch must happen first

import unittest
from unittest.mock import patch

from blaster.sqlite_orm import Attribute, Model


class ReloadModel(Model):
	id = Attribute(str)
	name = Attribute(str)
	age = Attribute(int, default=0)
	meta = Attribute(dict)
	tags = Attribute(list)
	_pk_attrs_ = ['id']


ReloadModel._attrs_ = {
	name: value for name, value in vars(ReloadModel).items()
	if isinstance(value, Attribute)
}
ReloadModel._col_attrs_ = {
	name: ReloadModel._attrs_[name] for name in ('name', 'age')
}


class TestReload(unittest.TestCase):
	def setUp(self):
		self.obj = ReloadModel(_is_create_new_=False, id='1', name='old', age=0)
		self.row = {'_': 20, 'id': '1', 'name': 'new', 'age': 42,
			'__': {'meta': {'nested': {'x': 1}}, 'tags': ['new']}}

	def fetch(self, *pk, projections=None):
		self.assertEqual(pk, ('1',))
		row = self.row
		if projections is not None:
			row = {key: value for key, value in self.row.items()
				if key in ['_', 'id'] + projections}
			row['__'] = {key: value for key, value in self.row['__'].items()
				if key in projections}
		return ReloadModel.__new__(ReloadModel)._from_doc(row)

	def test_full_reload(self):
		self.obj.name = 'local'
		with patch.object(ReloadModel, 'get', side_effect=self.fetch):
			self.assertIs(self.obj.reload(), self.obj)
		self.assertEqual(self.obj.name, 'new')
		self.assertEqual(self.obj.age, 42)
		self.assertFalse(self.obj._set_updates)
		self.obj.meta['nested']['x'] = 2
		self.assertIn(('meta', 'nested', 'x'), self.obj._set_updates)

	def test_projection_preserves_other_changes(self):
		self.obj.name = 'local'
		self.obj.meta['stale'] = 1
		with patch.object(ReloadModel, 'get', side_effect=self.fetch):
			self.assertIs(self.obj.reload([ReloadModel.meta]), self.obj)
		self.assertEqual(self.obj.name, 'local')
		self.assertEqual(self.obj._set_updates, {('name',): 'local'})
		self.obj.meta['nested']['x'] = 2
		self.assertIn(('meta', 'nested', 'x'), self.obj._set_updates)

	def test_if_none(self):
		self.obj.name = ''
		with patch.object(ReloadModel, 'get', side_effect=self.fetch) as get:
			self.obj.reload(if_none=True)
			self.assertEqual(get.call_args.kwargs['projections'], ['name', 'meta', 'tags'])
			self.assertEqual(self.obj.age, 0)
			get.reset_mock()
			self.obj.reload(['name', 'age'], if_none=True)
			get.assert_not_called()

	def test_validation_and_missing_row(self):
		with self.assertRaises(ValueError):
			self.obj.reload(['unknown'], if_none=True)
		with patch.object(ReloadModel, 'get', return_value=None):
			with self.assertRaises(LookupError):
				self.obj.reload()
		self.assertEqual(self.obj.name, 'old')

	def test_missing_json_field_clears_old_value(self):
		self.obj.meta['stale'] = 1
		self.row['__'] = {}
		with patch.object(ReloadModel, 'get', side_effect=self.fetch):
			self.obj.reload('meta')
		self.assertEqual(self.obj.meta, {})
		self.assertFalse(self.obj._set_updates)
