"""SQLite-specific transaction, persistence and routing checks."""
import blaster  # gevent patching first
import sqlite3
import tempfile
from contextlib import closing
import unittest

import gevent

from blaster.sqlite_orm import (
    Attribute, DatabaseNode, INDEX, Model, initialize_model, initialize_sqlite,
)


class TestDatabaseNode(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.node = DatabaseNode(self.tmp.name + '/main.sqlite3')
        self.addCleanup(self.node.close)
        node = self.node

        class Item(Model):
            _table_name_ = 'items'
            _db_node_ = node
            _id = Attribute(str)
            value = Attribute(int, default=0)
            data = Attribute(dict)

        self.Item = Item
        initialize_model(Item)

    def test_wal_normal_and_persisted_after_reopen(self):
        with self.node.use_conn() as conn:
            self.assertEqual(conn.execute('PRAGMA journal_mode').fetchone()[0], 'wal')
            self.assertEqual(conn.execute('PRAGMA synchronous').fetchone()[0], 1)
        self.Item(_id='saved', value=42).commit()
        # An independent connection sees the acknowledged commit immediately.
        with closing(sqlite3.connect(self.node.path)) as conn:
            self.assertEqual(conn.execute('SELECT value FROM items').fetchone()[0], 42)
        self.node.close()
        reopened = DatabaseNode(self.node.path)
        self.addCleanup(reopened.close)
        self.Item._db_node_ = reopened
        self.assertEqual(self.Item.get('saved').value, 42)

    def test_concurrent_writes_share_connection_and_commit_batches(self):
        statements, connections = [], []
        with self.node.use_conn() as conn:
            conn.set_trace_callback(statements.append)
        def insert(i):
            self.node.write(lambda conn: connections.append(id(conn)))
            return self.Item(_id=str(i), value=i).commit()
        jobs = [gevent.spawn(insert, i) for i in range(30)]
        gevent.joinall(jobs, raise_error=True)
        self.assertEqual(len(list(self.Item.query())), 30)
        self.assertEqual(len(set(connections)), 1)
        commits = sum(sql == 'COMMIT' for sql in statements)
        self.assertGreater(commits, 0)
        self.assertLess(commits, 30)
        self.assertTrue(all(job.value._is_create_new_ is False for job in jobs))

    def test_failed_write_does_not_discard_batch_peers(self):
        self.Item(_id='duplicate').commit()
        def try_insert(key):
            try:
                self.Item(_id=key).commit()
            except sqlite3.IntegrityError:
                return 'duplicate'
            return 'ok'
        jobs = [gevent.spawn(try_insert, key) for key in ['a', 'duplicate', 'b']]
        gevent.joinall(jobs, raise_error=True)
        self.assertEqual([job.value for job in jobs], ['ok', 'duplicate', 'ok'])
        self.assertIsNotNone(self.Item.get('a'))
        self.assertIsNotNone(self.Item.get('b'))
        self.Item(_id='after').commit()

    def test_callback_failure_rolls_back_nested_writes_and_models(self):
        first = self.Item(_id='first', value=1).commit()
        second = self.Item(_id='second', value=2).commit()
        def fail(obj, row):
            second.update({'$inc': {'value': 10}})
            self.assertEqual(second.value, 12)
            raise ValueError('abort callback')
        with self.assertRaisesRegex(ValueError, 'abort callback'):
            first.update({'$set': {'value': 99}}, callback=fail)
        self.assertEqual(first.value, 1)
        self.assertEqual(second.value, 2)
        self.assertEqual(self.Item.get('first').value, 1)
        self.assertEqual(self.Item.get('second').value, 2)

    def test_caught_nested_failure_uses_its_own_savepoint(self):
        self.node.write(lambda c: c.execute('CREATE TABLE raw_values (value INTEGER)'))
        def outer(conn):
            def inner(conn):
                conn.execute('INSERT INTO raw_values VALUES (1)')
                raise ValueError('nested')
            try:
                self.node.write(inner)
            except ValueError:
                pass
            conn.execute('INSERT INTO raw_values VALUES (2)')
        self.node.write(outer)
        self.assertEqual([r['value'] for r in self.node.fetch_rows('SELECT * FROM raw_values')], [2])

    def test_concurrent_increments_use_latest_row(self):
        self.Item(_id='counter').commit()
        copies = [self.Item.get('counter') for _ in range(25)]
        jobs = [gevent.spawn(obj.update, {'$inc': {'value': 1, 'data.count': 1}}) for obj in copies]
        gevent.joinall(jobs, raise_error=True)
        fresh = self.Item.get('counter')
        self.assertEqual(fresh.value, 25)
        self.assertEqual(fresh.data['count'], 25)

    def test_dbnode_named_and_instance_routing(self):
        other = DatabaseNode(self.tmp.name + '/other.sqlite3', name='sqlite-test-other')
        self.addCleanup(other.close)
        class Elsewhere(Model):
            _table_name_ = 'elsewhere'
            _db_node_ = 'sqlite-test-other'
            _id = Attribute(str)
        initialize_model(Elsewhere)
        self.assertIs(Elsewhere._db_node_, other)
        self.assertIs(self.Item._db_node_, self.node)
        Elsewhere(_id='remote').commit()
        self.assertEqual(list(self.node.fetch_rows("SELECT name FROM sqlite_master WHERE name='elsewhere'")), [])
        self.assertEqual(Elsewhere.get('remote')._id, 'remote')

    def test_generated_id_and_bytes_bool(self):
        node = self.node
        class Generated(Model):
            _table_name_ = 'generated'
            _db_node_ = node
            enabled = Attribute(bool)
            blob = Attribute(bytes)
        initialize_model(Generated)
        first = Generated(enabled=True, blob=b'\x00\xff').commit()
        second = Generated(enabled=False).commit()
        self.assertIsInstance(first._id, int)
        self.assertGreater(second._id, first._id)
        fresh = Generated.get(first._id)
        self.assertIs(fresh.enabled, True)
        self.assertEqual(fresh.blob, b'\x00\xff')
        initialize_model(Generated)  # generated Attribute survives initialization
        self.assertEqual(Generated.get(second._id)._id, second._id)

    def test_composite_key_and_expression_partial_indexes(self):
        node = self.node
        class Pair(Model):
            _table_name_ = 'pairs'
            _db_node_ = node
            left = Attribute(str)
            right = Attribute(int)
            data = Attribute(dict)
            INDEX(left, right, {'unique': True})
            INDEX('data.rank', {'unique': False, 'condition': 'right > 0'})
        initialize_model(Pair)
        Pair(left='a', right=1, data={'rank': 10}).commit()
        Pair(left='a', right=2).commit()
        self.assertEqual(Pair.get('a', 1).data, {'rank': 10})
        self.assertEqual(len(Pair.get([{'left': 'a', 'right': 1}, {'left': 'a', 'right': 2}])), 2)
        self.assertEqual(list(Pair.query({'data.rank': {'$gte': 2}}))[0].right, 1)

    def test_close_drains_pending_writes(self):
        jobs = [gevent.spawn(self.Item(_id=str(i)).commit) for i in range(10)]
        gevent.sleep(0)  # enqueue the jobs before closing
        self.node.close()
        gevent.joinall(jobs, raise_error=True)
        with closing(sqlite3.connect(self.node.path)) as conn:
            self.assertEqual(conn.execute('SELECT count(*) FROM items').fetchone()[0], 10)
        with self.assertRaises(RuntimeError):
            self.node.write(lambda conn: None)

    def test_commit_failure_rolls_back_whole_batch_and_writer_recovers(self):
        def schema(conn):
            conn.execute('CREATE TABLE parent (id INTEGER PRIMARY KEY)')
            conn.execute('CREATE TABLE child (parent_id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)')
        self.node.write(schema)
        def insert(sql):
            try:
                self.node.write(lambda c: c.execute(sql).rowcount)
            except sqlite3.IntegrityError:
                return 'rolled back'
        jobs = [gevent.spawn(insert, sql) for sql in (
            'INSERT INTO parent VALUES (1)',
            'INSERT INTO child VALUES (999)',
        )]
        gevent.joinall(jobs, raise_error=True)
        self.assertEqual([j.value for j in jobs], ['rolled back', 'rolled back'])
        self.assertEqual(list(self.node.fetch_rows('SELECT * FROM parent')), [])
        self.assertEqual(list(self.node.fetch_rows('SELECT * FROM child')), [])
        self.Item(_id='recovered').commit()
        self.assertIsNotNone(self.Item.get('recovered'))

    def test_external_reader_sees_old_value_until_callback_commits(self):
        item = self.Item(_id='visible', value=1).commit()
        def callback(obj, row):
            self.assertEqual(self.Item.get('visible').value, 2)
            with closing(sqlite3.connect(self.node.path)) as external:
                self.assertEqual(external.execute('SELECT value FROM items').fetchone()[0], 1)
        item.update({'$set': {'value': 2}}, callback=callback)
        self.assertEqual(self.Item.get('visible').value, 2)

    def test_batch_size_limits_transactions(self):
        self.node.batch_size = 3
        statements = []
        with self.node.use_conn() as conn:
            conn.set_trace_callback(statements.append)
        jobs = [gevent.spawn(self.Item(_id=str(i)).commit) for i in range(8)]
        gevent.joinall(jobs, raise_error=True)
        self.assertEqual(sum(sql == 'COMMIT' for sql in statements), 3)

    def test_raw_connection_release_rolls_back_unfinished_transaction(self):
        conn = self.node.get_conn()
        try:
            conn.execute('BEGIN')
            conn.execute("INSERT INTO items (_id) VALUES ('uncommitted')")
        finally:
            self.node.put_conn(conn)
        self.assertIsNone(self.Item.get('uncommitted'))
        self.Item(_id='committed').commit()

    def test_force_insert_and_offset_only_query(self):
        self.Item(_id='one', value=1).commit()
        duplicate = self.Item(_id='one', value=7).commit(force=True)
        self.assertEqual(duplicate.value, 7)
        self.Item(_id='two').commit()
        self.assertEqual(len(list(self.Item.query(offset=1))), 1)


if __name__ == '__main__':
    unittest.main()
