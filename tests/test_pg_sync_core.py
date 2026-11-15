"""Driver-isolated pool/transaction tests; no PostgreSQL server is required.

Real PostgreSQL SQL behavior is covered separately by ./test_with_pg.sh.
"""
import asyncio
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import subprocess
import sys
import threading
import time
import unittest
from unittest.mock import patch

from orms import pg
from orms.async_wrapper import AsyncAdapter as GenericAdapter
from orms.pg_async import AsyncAdapter as PgAdapter
from orms.sqlite_async import AsyncAdapter as SqliteAdapter


class Cursor:
    def __init__(self, conn):
        self.conn = conn
        self.description = True

    def execute(self, sql, params=None):
        self.conn.statements.append((sql, params))

    def fetchone(self):
        return self.conn.pool.rows.popleft() if self.conn.pool.rows else None

    def fetchall(self):
        return self.conn.pool.batches.popleft() if self.conn.pool.batches else []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        pass


class Connection:
    def __init__(self, pool):
        self.pool = pool
        self.statements = []
        self.commits = self.rollbacks = 0
        self.fail_commit = False

    def cursor(self):
        return Cursor(self)

    def get_backend_pid(self):
        return id(self)

    def commit(self):
        if self.fail_commit:
            self.fail_commit = False
            raise RuntimeError('deferred constraint failed')
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class Pool:
    def __init__(self, max_conn=2):
        self.max_conn = max_conn
        self.free, self.connections = [], []
        self.used = set()
        self.peak = self.closes = 0
        self.rows, self.batches = deque(), deque()

    def getconn(self):
        if len(self.used) >= self.max_conn:
            raise AssertionError('pool capacity exceeded')
        conn = self.free.pop() if self.free else Connection(self)
        if conn not in self.connections:
            self.connections.append(conn)
        self.used.add(conn)
        self.peak = max(self.peak, len(self.used))
        return conn

    def putconn(self, conn):
        self.used.remove(conn)
        self.free.append(conn)

    def closeall(self):
        if self.used:
            raise AssertionError('closed an in-use connection')
        self.closes += 1


def record_class(node):
    class Record(pg.Model):
        _table_name_ = 'unit_records'
        _db_node_ = node
        _id = pg.Attribute(str)
        value = pg.Attribute(int, default=0)
        data = pg.Attribute(dict)
    # Schema introspection belongs to the real DB suite. Here the driver supplies
    # rows so we can exercise the genuine Model and transaction implementations.
    Record._attrs_ = {name: value for name, value in vars(Record).items() if isinstance(value, pg.Attribute)}
    Record._pk_attrs_ = ['_id']
    Record._col_attrs_ = {'value': Record.value}
    return Record


def loaded(model, key='one', value=1):
    return model.__new__(model)._from_doc({'_': 1, '_id': key, 'value': value, '__': {}})


class SyncCoreTests(unittest.TestCase):
    def setUp(self):
        self.pool = Pool()
        self.factory = patch.object(pg.psycopg2.pool, 'SimpleConnectionPool', return_value=self.pool)
        self.factory.start()
        self.addCleanup(self.factory.stop)
        self.node = pg.DatabaseNode('unused', 5432, 'user', 'password', 'test', max_conn=2)
        self.addCleanup(self.node.close)
        self.Record = record_class(self.node)

    def test_standalone_core_has_no_async_or_gevent_runtime(self):
        script = """import sys
from orms.pg import DatabaseNode, Model
import psycopg2.extensions
assert 'blaster' not in sys.modules
assert 'gevent' not in sys.modules
assert 'asyncio' not in sys.modules
assert not hasattr(sys.modules[Model.__module__], 'AsyncModel')
assert not hasattr(sys.modules[Model.__module__], 'AsyncDatabaseNode')
assert psycopg2.extensions.get_wait_callback() is None
node = DatabaseNode('unused', 5432, 'user', 'password', 'db')
assert node._pool is None
node.close()
"""
        subprocess.run([sys.executable, '-B', '-c', script], check=True, timeout=10)

    def test_generic_wrapper_imports_no_backend_or_driver(self):
        script = """import sys
from orms.async_wrapper import AsyncAdapter
assert 'orms.pg' not in sys.modules
assert 'orms.sqlite' not in sys.modules
assert 'psycopg2' not in sys.modules
assert 'gevent' not in sys.modules
assert 'blaster' not in sys.modules
"""
        subprocess.run([sys.executable, '-B', '-c', script], check=True, timeout=10)

    def test_missing_schema_initialization_raises_directly(self):
        with self.assertRaises(pg.MissingTableError):
            pg.initialize_model(self.Record, create_tables=False)

    def test_constructor_does_not_install_driver_hooks(self):
        with patch.object(pg.psycopg2.extensions, 'set_wait_callback') as setter:
            node = pg.DatabaseNode('unused', 5432, 'user', 'password', 'test')
            node.close()
        setter.assert_not_called()

    def test_nested_scopes_share_connection_and_commit_once(self):
        with self.node.use_conn() as outer:
            pid = outer.get_backend_pid()
            with self.node.transaction():
                self.assertEqual(self.node.write(lambda conn: conn.get_backend_pid()), pid)
                outer.commit()
            self.assertEqual(self.pool.connections[0].commits, 0)
        self.assertEqual(self.pool.connections[0].commits, 1)
        self.assertEqual(len(self.pool.connections), 1)

    def test_failed_nested_savepoint_leaves_outer_scope_usable(self):
        with self.node.transaction():
            with self.assertRaisesRegex(ValueError, 'inner failure'):
                with self.node.transaction():
                    raise ValueError('inner failure')
            self.node.write(lambda conn: conn.cursor().execute('SELECT after_failure'))
        conn = self.pool.connections[0]
        self.assertEqual(conn.commits, 1)
        self.assertTrue(any(sql.startswith('ROLLBACK TO SAVEPOINT') for sql, _ in conn.statements))

    def test_rollback_restores_genuine_model_update(self):
        obj = loaded(self.Record)
        self.pool.rows.append({'_': 2, 'value': 9})
        with self.assertRaisesRegex(ValueError, 'abort'):
            with self.node.transaction():
                obj.update({'$set': {'value': 9}})
                self.assertEqual(obj.value, 9)
                raise ValueError('abort')
        self.assertEqual(obj.value, 1)
        self.assertEqual(obj.updated_at, 1)
        self.assertEqual(self.pool.connections[0].rollbacks, 1)

    def test_commit_failure_restores_model_and_returns_connection(self):
        obj = loaded(self.Record)
        self.pool.rows.append({'_': 2, 'value': 9})
        with self.assertRaisesRegex(RuntimeError, 'deferred constraint'):
            with self.node.use_conn() as conn:
                conn.session.conn.fail_commit = True
                obj.update({'$set': {'value': 9}})
        self.assertEqual(obj.value, 1)
        self.assertFalse(self.pool.used)
        self.node.write(lambda conn: None)

    def test_native_threads_wait_for_bounded_pool_capacity(self):
        def operation():
            with self.node.transaction():
                time.sleep(.01)
        with ThreadPoolExecutor(max_workers=8) as executor:
            list(executor.map(lambda _: operation(), range(12)))
        self.assertEqual(self.pool.peak, 2)
        self.assertFalse(self.pool.used)

    def test_close_drains_borrowers_and_is_idempotent(self):
        entered, leave = threading.Event(), threading.Event()
        def borrow():
            with self.node.transaction():
                entered.set()
                if not leave.wait(2):
                    raise TimeoutError('test synchronization')
        with ThreadPoolExecutor(max_workers=2) as executor:
            active = executor.submit(borrow)
            self.assertTrue(entered.wait(2))
            closing = executor.submit(self.node.close)
            try:
                time.sleep(.01)
                self.assertFalse(closing.done())
            finally:
                leave.set()
            active.result(timeout=2)
            closing.result(timeout=2)
        self.node.close()
        self.assertEqual(self.pool.closes, 1)
        with self.assertRaises(RuntimeError):
            self.node.get_conn()

    def test_async_callbacks_rejected_and_connection_recovered(self):
        obj = loaded(self.Record)
        self.pool.rows.append({'_': 2, 'value': 9})
        async def callback(*args):
            pass
        with self.assertRaisesRegex(TypeError, 'synchronous'):
            obj.update({'$set': {'value': 9}}, callback=callback)
        self.assertEqual(obj.value, 1)
        self.assertFalse(self.pool.used)


class AsyncBackendTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.pool = Pool()
        self.factory = patch.object(pg.psycopg2.pool, 'SimpleConnectionPool', return_value=self.pool)
        self.factory.start()
        self.addCleanup(self.factory.stop)
        self.node = pg.DatabaseNode('unused', 5432, 'user', 'password', 'test', max_conn=2)
        self.SyncRecord = record_class(self.node)
        self.adapter = PgAdapter(initializer=lambda model: None)
        self.Record = await self.adapter.initialize(self.SyncRecord)

    async def asyncTearDown(self):
        await self.adapter.close()
        self.node.close()

    async def test_both_backend_loaders_share_one_wrapper_implementation(self):
        self.assertIs(PgAdapter.run, GenericAdapter.run)
        self.assertIs(SqliteAdapter.run, GenericAdapter.run)
        self.assertIs(PgAdapter.model, SqliteAdapter.model)
        self.assertIs(PgAdapter.transaction, SqliteAdapter.transaction)

    async def test_pg_loader_initialization_passes_schema_option_once(self):
        with patch.object(pg, 'initialize_model') as loader:
            adapter = PgAdapter(create_tables=True)
            try:
                first, second = await asyncio.gather(adapter.initialize(self.SyncRecord), adapter.initialize(self.SyncRecord))
                self.assertIs(first, second)
                loader.assert_called_once_with(self.SyncRecord, create_tables=True)
            finally:
                await adapter.close()

    async def test_pg_schema_failure_can_be_retried_with_creation_enabled(self):
        adapter = PgAdapter(create_tables=False)
        try:
            with self.assertRaises(pg.MissingTableError):
                await adapter.initialize(self.SyncRecord)
            from functools import partial
            Record = await adapter.initialize(self.SyncRecord,
                initializer=partial(pg.initialize_model, create_tables=True))
            self.assertIs(Record, adapter.model(self.SyncRecord))
        finally:
            await adapter.close()

    async def test_generic_adapter_requires_loader_only_for_first_load(self):
        adapter = GenericAdapter()
        try:
            with self.assertRaises(ValueError):
                await adapter.initialize(self.SyncRecord)
            Record = await adapter.initialize(self.SyncRecord, initializer=lambda model: None)
            self.assertIs(Record, await adapter.initialize(self.SyncRecord))
        finally:
            await adapter.close()

    async def test_facade_get_and_query_use_real_pg_model(self):
        self.pool.rows.append({'_': 1, '_id': 'one', 'value': 3, '__': {'x': 1}})
        obj = await self.Record.get('one')
        self.assertIsInstance(obj, self.Record)
        self.assertIsInstance(obj.sync, self.SyncRecord)
        self.assertEqual(obj.value, 3)
        self.assertEqual(obj.data, {})
        self.pool.batches.append([{'_': 1, '_id': 'one', 'value': 3, '__': {}}])
        rows = [row async for row in self.Record.query()]
        self.assertEqual([row._id for row in rows], ['one'])

    async def test_facade_update_and_transaction_rollback(self):
        self.pool.rows.append({'_': 1, '_id': 'one', 'value': 1, '__': {}})
        obj = await self.Record.get('one')
        self.pool.rows.append({'_': 2, 'value': 4})
        self.assertTrue(await obj.update({'$set': {'value': 4}}))
        self.assertEqual(obj.value, 4)
        self.pool.rows.append({'_': 3, 'value': 10})
        def fail(record):
            record.update({'$set': {'value': 10}})
            raise ValueError('abort adapter transaction')
        with self.assertRaisesRegex(ValueError, 'abort adapter transaction'):
            await self.adapter.transaction(self.node, fail, obj)
        self.assertEqual(obj.value, 4)

    async def test_generic_adapter_can_register_both_backend_models(self):
        from orms.sqlite import Attribute, DatabaseNode, Model, initialize_model
        db = DatabaseNode()
        try:
            class Local(Model):
                _table_name_ = 'local'
                _db_node_ = db
                _id = Attribute(str)
            User = await self.adapter.initialize(Local, initializer=initialize_model)
            self.assertEqual((await User(_id='sqlite').commit())._id, 'sqlite')
            self.pool.rows.append({'_': 1, '_id': 'pg', 'value': 2, '__': {}})
            self.assertEqual((await self.Record.get('pg')).value, 2)
        finally:
            db.close()


if __name__ == '__main__':
    unittest.main()
