"""The sync SQLite core and its small, independent asyncio adapter."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import contextvars
import importlib.util
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
import unittest

from orms.sqlite import Attribute, DatabaseNode, Model, initialize_model, with_lock
from orms.sqlite_async import AsyncAdapter


def define_item(db):
    class Item(Model):
        _table_name_ = 'items'
        _db_node_ = db
        _id = Attribute(str)
        value = Attribute(int, default=0)
        data = Attribute(dict)
        tags = Attribute(list)
    initialize_model(Item)
    return Item


class SyncTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = DatabaseNode(Path(self.tmp.name) / 'sync.db')
        self.addCleanup(self.db.close)
        self.Item = define_item(self.db)

    def test_core_import_is_standalone_and_synchronous(self):
        script = """
            import sys
            from orms.sqlite import DatabaseNode, Model
            assert 'blaster' not in sys.modules
            assert 'gevent' not in sys.modules
            assert 'asyncio' not in sys.modules
            assert not hasattr(sys.modules[Model.__module__], 'AsyncModel')
            assert not hasattr(sys.modules[Model.__module__], 'AsyncDatabaseNode')
            DatabaseNode().close()
        """
        subprocess.run([sys.executable, '-B', '-c', textwrap.dedent(script)], check=True, timeout=10)

    def test_native_threads_batch_atomic_increments(self):
        self.Item(_id='counter').commit()
        statements = []
        with self.db.use_conn() as conn:
            conn.set_trace_callback(statements.append)
        barrier = threading.Barrier(12)
        def increment(i):
            item = self.Item.get('counter')
            barrier.wait()
            item.update({'$inc': {'value': 1, 'data.count': 1}})
        with ThreadPoolExecutor(max_workers=12) as pool:
            list(pool.map(increment, range(12)))
        item = self.Item.get('counter')
        self.assertEqual(item.value, 12)
        self.assertEqual(item.data['count'], 12)
        self.assertLess(sum(sql == 'COMMIT' for sql in statements), 12)

    def test_explicit_transaction_commits_once(self):
        statements = []
        with self.db.use_conn() as conn:
            conn.set_trace_callback(statements.append)
        with self.db.transaction():
            for i in range(5):
                self.Item(_id=str(i)).commit()
            self.assertEqual(len(list(self.Item.query())), 5)
        self.assertEqual(sum(sql == 'COMMIT' for sql in statements), 1)
        with closing(sqlite3.connect(self.db.path)) as other:
            self.assertEqual(other.execute('SELECT count(*) FROM items').fetchone()[0], 5)

    def test_transaction_rollback_restores_models_and_dirty_state(self):
        item = self.Item(_id='existing', value=1).commit()
        item.value = 7
        created = self.Item(_id='new')
        with self.assertRaisesRegex(ValueError, 'abort'):
            with self.db.transaction():
                item.commit()
                created.commit()
                raise ValueError('abort')
        self.assertEqual(item.value, 7)
        self.assertEqual(item._set_updates, {('value',): 7})
        self.assertTrue(created._is_create_new_)
        self.assertEqual(self.Item.get('existing').value, 1)
        self.assertIsNone(self.Item.get('new'))

    def test_nested_transaction_savepoint_isolates_failure(self):
        with self.db.transaction():
            self.Item(_id='before').commit()
            with self.assertRaises(ValueError):
                with self.db.transaction():
                    self.Item(_id='rolled-back').commit()
                    raise ValueError('inner')
            self.Item(_id='after').commit()
        self.assertEqual({item._id for item in self.Item.query()}, {'before', 'after'})

    def test_transaction_blocks_unrelated_threads(self):
        entered, leave = threading.Event(), threading.Event()
        def transaction():
            with self.db.transaction():
                self.Item(_id='inside').commit()
                entered.set()
                if not leave.wait(2):
                    raise TimeoutError('test synchronization')
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(transaction)
            self.assertTrue(entered.wait(2))
            second = pool.submit(self.Item(_id='outside').commit)
            try:
                time.sleep(.01)
                self.assertFalse(second.done())
            finally:
                leave.set()
            first.result(timeout=2)
            second.result(timeout=2)
        self.assertEqual(len(list(self.Item.query())), 2)

    def test_stdlib_thread_functions_remain_patchable(self):
        if importlib.util.find_spec('gevent') is None:
            self.skipTest('optional patching demonstration requires gevent')
        script = """
            from orms.sqlite import *
            from gevent import monkey
            monkey.patch_all()
            import gevent
            db = DatabaseNode()
            class Item(Model):
                _table_name_ = 'patched'
                _db_node_ = db
                _id = Attribute(str)
            initialize_model(Item)
            jobs = [gevent.spawn(Item(_id=str(i)).commit) for i in range(8)]
            gevent.joinall(jobs, raise_error=True)
            assert len(list(Item.query())) == 8
            db.close()
        """
        subprocess.run([sys.executable, '-B', '-c', textwrap.dedent(script)], check=True, timeout=10)

    def test_core_file_can_be_symlinked_into_consumer(self):
        Path(self.tmp.name, 'consumer_sqlite.py').symlink_to(Path(sys.modules[Model.__module__].__file__).resolve())
        script = """
            import sys
            sys.path.insert(0, sys.argv[1])
            from consumer_sqlite import *
            assert 'blaster' not in sys.modules and 'gevent' not in sys.modules
            db = DatabaseNode()
            class Item(Model):
                _table_name_ = 'linked'
                _db_node_ = db
                _id = Attribute(str)
            initialize_model(Item)
            Item(_id='ok').commit()
            assert Item.get('ok')._id == 'ok'
            db.close()
        """
        subprocess.run([sys.executable, '-B', '-I', '-c', textwrap.dedent(script), self.tmp.name], check=True, timeout=10)


class AsyncTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.adapter = AsyncAdapter(max_workers=4)
        self.db = await self.adapter.run(DatabaseNode, Path(self.tmp.name) / 'async.db')
        self.Item = await self.adapter.run(define_item, self.db)

    async def asyncTearDown(self):
        await self.adapter.close()
        self.db.close()

    async def test_same_sync_model_supports_crud_and_queries(self):
        item = await self.adapter.run(self.Item(_id='one', value=10).commit)
        self.assertIsInstance(item, Model)
        item.data['nested'] = {'n': 1}
        item.tags.append('a')
        await self.adapter.run(item.commit)
        fresh = await self.adapter.run(self.Item.get, 'one', projections=['data'])
        self.assertEqual(fresh.data['nested']['n'], 1)
        self.assertEqual(fresh.value, 0)
        rows = await self.adapter.run(self.Item.query, {'value': {'$gte': 10}})
        self.assertIsInstance(rows, list)
        self.assertEqual([row._id for row in rows], ['one'])
        await self.adapter.run(item.update, {'$inc': {'value': 2}, '$push': {'tags': 'b'}})
        await self.adapter.run(item.reload)
        self.assertEqual(item.tags, ['a', 'b'])
        self.assertEqual(item.value, 12)
        await self.adapter.run(item.delete)
        self.assertIsNone(await self.adapter.run(self.Item.get, 'one'))

    async def test_whole_transaction_runs_on_one_worker_and_commit(self):
        ids = []
        statements = []
        await self.adapter.run(self.db.write, lambda conn: conn.set_trace_callback(statements.append))
        statements.clear()  # exclude the completed trace-installation write
        def save():
            ids.append(threading.get_ident())
            first = self.Item(_id='first').commit()
            first.update({'$inc': {'value': 1}}, callback=lambda *args: ids.append(threading.get_ident()))
            self.Item(_id='second').commit()
            return first
        item = await self.adapter.transaction(self.db, save)
        self.assertEqual(item.value, 1)
        self.assertEqual(len(set(ids)), 1)
        self.assertNotEqual(ids[0], threading.get_ident())
        self.assertEqual(sum(sql == 'COMMIT' for sql in statements), 1)

    async def test_transaction_rollback_restores_all_models(self):
        item = await self.adapter.run(self.Item(_id='existing', value=1).commit)
        created = self.Item(_id='new')
        def fail():
            item.update({'$inc': {'value': 10}})
            created.commit()
            raise ValueError('abort transaction')
        with self.assertRaisesRegex(ValueError, 'abort transaction'):
            await self.adapter.transaction(self.db, fail)
        self.assertEqual(item.value, 1)
        self.assertTrue(created._is_create_new_)
        self.assertIsNone(await self.adapter.run(self.Item.get, 'new'))
        self.assertEqual((await self.adapter.run(self.Item.get, 'existing')).value, 1)

    async def test_sync_callbacks_join_transaction_without_async_bridge(self):
        first = await self.adapter.run(self.Item(_id='first', value=1).commit)
        second = await self.adapter.run(self.Item(_id='second', value=2).commit)
        def fail(*args):
            second.update({'$inc': {'value': 10}})
            raise ValueError('abort callback')
        with self.assertRaisesRegex(ValueError, 'abort callback'):
            await self.adapter.run(first.update, {'$inc': {'value': 1}}, callback=fail)
        self.assertEqual(second.value, 2)
        self.assertEqual((await self.adapter.run(self.Item.get, 'second')).value, 2)

    async def test_async_callbacks_are_rejected_and_rolled_back(self):
        item = await self.adapter.run(self.Item(_id='callback').commit)
        async def callback(*args):
            return None
        with self.assertRaisesRegex(TypeError, 'synchronous'):
            await self.adapter.run(item.update, {'$inc': {'value': 1}}, callback=callback)
        self.assertEqual((await self.adapter.run(self.Item.get, 'callback')).value, 0)
        with self.assertRaisesRegex(TypeError, 'synchronous'):
            await self.adapter.transaction(self.db, callback)

    async def test_adapter_rejects_async_operations(self):
        async def operation():
            return 3
        with self.assertRaisesRegex(TypeError, 'synchronous'):
            await self.adapter.run(operation)
        with self.assertRaisesRegex(TypeError, 'synchronous'):
            await self.adapter.run(lambda: operation())

    async def test_event_loop_stays_responsive(self):
        ticks = 0
        async def heartbeat():
            nonlocal ticks
            for _ in range(12):
                await asyncio.sleep(.005)
                ticks += 1
        def slow():
            time.sleep(.1)
        await asyncio.gather(self.adapter.run(slow), heartbeat())
        self.assertEqual(ticks, 12)

    async def test_executor_bounds_native_workers(self):
        threads = set()
        active = maximum = 0
        lock = threading.Lock()
        def operation():
            nonlocal active, maximum
            with lock:
                threads.add(threading.get_ident())
                active += 1
                maximum = max(maximum, active)
            time.sleep(.01)
            with lock:
                active -= 1
        await asyncio.gather(*(self.adapter.run(operation) for _ in range(20)))
        self.assertLessEqual(maximum, 4)
        self.assertLessEqual(len(threads), 4)
        self.assertGreater(maximum, 1)

    async def test_iterator_materializes_before_return_and_inside_transaction(self):
        caller = threading.get_ident()
        def query():
            yield threading.get_ident()
        self.assertNotEqual((await self.adapter.run(query))[0], caller)
        def transaction_query():
            self.Item(_id='lazy').commit()
            yield self.Item.get('lazy')
        result = await self.adapter.transaction(self.db, transaction_query)
        self.assertEqual(result[0]._id, 'lazy')

    async def test_context_variables_follow_worker_call_and_transaction(self):
        variable = contextvars.ContextVar('request', default='missing')
        variable.set('request-123')
        self.assertEqual(await self.adapter.run(variable.get), 'request-123')
        self.assertEqual(await self.adapter.transaction(self.db, variable.get), 'request-123')

    async def test_cancellation_settles_work_before_propagating(self):
        item = await self.adapter.run(self.Item(_id='cancel').commit)
        entered = asyncio.Event()
        loop = asyncio.get_running_loop()
        def callback(*args):
            loop.call_soon_threadsafe(entered.set)
            time.sleep(.04)
        task = asyncio.create_task(self.adapter.run(item.update, {'$inc': {'value': 1}}, callback=callback))
        await entered.wait()
        task.cancel()
        await asyncio.sleep(.005)
        task.cancel()  # repeated cancellation must not orphan the worker
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(item.value, 1)
        self.assertEqual((await self.adapter.run(self.Item.get, 'cancel')).value, 1)

    async def test_close_drains_accepted_work_without_closing_database(self):
        tasks = [asyncio.create_task(self.adapter.run(self.Item(_id=str(i)).commit)) for i in range(12)]
        await asyncio.sleep(0)
        await self.adapter.close()
        await asyncio.gather(*tasks)
        await self.adapter.close()
        self.assertEqual(len(list(self.Item.query())), 12)
        with self.assertRaises(RuntimeError):
            await self.adapter.run(self.Item.get, '0')

    async def test_concurrent_increments_and_failed_write_isolation(self):
        await self.adapter.run(self.Item(_id='counter').commit)
        def increment():
            self.Item.get('counter').update({'$inc': {'value': 1, 'data.count': 1}})
        await asyncio.gather(*(self.adapter.run(increment) for _ in range(20)))
        fresh = await self.adapter.run(self.Item.get, 'counter')
        self.assertEqual((fresh.value, fresh.data['count']), (20, 20))
        with self.assertRaises(sqlite3.IntegrityError):
            await self.adapter.run(self.Item(_id='counter').commit)
        await self.adapter.run(self.Item(_id='after').commit)

    async def test_sync_lease_decorator_runs_inside_worker(self):
        item = await self.adapter.run(self.Item(_id='locked').commit)
        @with_lock
        def increment(obj):
            obj.update({'$inc': {'value': 1}})
            return obj.value
        self.assertEqual(await self.adapter.run(increment, item), 1)

    async def test_commit_failure_rolls_back_explicit_transaction(self):
        def schema(conn):
            conn.execute('CREATE TABLE parent (id INTEGER PRIMARY KEY)')
            conn.execute('CREATE TABLE child (id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)')
        await self.adapter.run(self.db.write, schema)
        item = await self.adapter.run(self.Item(_id='fk').commit)
        def fail_at_commit():
            item.update({'$inc': {'value': 1}})
            self.db.write(lambda conn: conn.execute('INSERT INTO child VALUES (999)').rowcount)
        with self.assertRaises(sqlite3.IntegrityError):
            await self.adapter.transaction(self.db, fail_at_commit)
        self.assertEqual(item.value, 0)
        self.assertEqual((await self.adapter.run(self.Item.get, 'fk')).value, 0)

    async def test_transaction_reload_rollback_preserves_dirty_changes(self):
        item = await self.adapter.run(self.Item(_id='reload', value=1).commit)
        stale = await self.adapter.run(self.Item.get, 'reload')
        stale.value = 7
        def fail():
            item.update({'$set': {'value': 99}})
            stale.reload(['value'])
            raise ValueError('abort reload')
        with self.assertRaisesRegex(ValueError, 'abort reload'):
            await self.adapter.transaction(self.db, fail)
        self.assertEqual(stale.value, 7)
        self.assertEqual(stale._set_updates, {('value',): 7})
        self.assertEqual((await self.adapter.run(self.Item.get, 'reload')).value, 1)

    async def test_facade_generated_once_and_sync_class_unchanged(self):
        calls = []
        original_get = self.Item.get.__func__
        def loader(model):
            calls.append(model)
            time.sleep(.005)
            initialize_model(model)
        first, second = await asyncio.gather(
            self.adapter.initialize(self.Item, initializer=loader),
            self.adapter.initialize(self.Item, initializer=loader))
        self.assertIs(first, second)
        self.assertIs(first, self.adapter.model(self.Item))
        self.assertIs(first, await self.adapter.initialize(self.Item))
        self.assertEqual(calls, [self.Item])
        self.assertIs(self.Item.get.__func__, original_get)
        self.assertIs(first.sync_model, self.Item)

    async def test_facade_loader_initializes_fresh_models(self):
        db = self.db
        class Fresh(Model):
            _table_name_ = 'facade_fresh'
            _db_node_ = db
            _id = Attribute(str)
            value = Attribute(int, default=5)
        User = await self.adapter.initialize(Fresh)
        user = await User(_id='created').commit()
        self.assertIsInstance(user, User)
        self.assertIsInstance(user.sync, Fresh)
        self.assertEqual((await User.get('created')).value, 5)
        await User.create_table()

    async def test_facade_crud_identity_and_local_attributes(self):
        User = await self.adapter.initialize(self.Item)
        user = User(_id='one', value=10)
        self.assertIs(await user.commit(), user)
        user.value = 20
        user.data['nested'] = {'n': 1}
        user.tags.append('a')
        self.assertIs(await user.commit(), user)
        fresh = await User.get('one', projections=[User.value, User.data])
        self.assertIsInstance(fresh, User)
        self.assertEqual(fresh.value, 20)
        self.assertEqual(fresh.data, {'nested': {'n': 1}})
        user.data['nested']['n'] = 2
        await user.commit()
        self.assertIs(await fresh.reload(), fresh)
        self.assertEqual(fresh.data['nested']['n'], 2)
        self.assertEqual(fresh.to_dict()['value'], 20)
        self.assertTrue(await user.update({'$inc': {'value': 1}}))
        await user.delete()
        self.assertIsNone(await User.get('one'))

    async def test_facade_many_results_query_iteration_and_projection(self):
        User = await self.adapter.initialize(self.Item)
        await asyncio.gather(*(User(_id=str(i), value=i).commit() for i in range(5)))
        many = await User.get(['0', '2', 'missing'])
        self.assertEqual({row._id for row in many}, {'0', '2'})
        self.assertTrue(all(isinstance(row, User) for row in many))
        self.assertEqual(await User.get([]), [])
        rows = await User.query({'value': {'$gte': 2}}, sort=[('value', -1)])
        self.assertEqual([row.value for row in rows], [4, 3, 2])
        self.assertTrue(all(isinstance(row, User) for row in rows))
        self.assertEqual([row._id async for row in User.query({'value': 3})], ['3'])
        projected = await User.get('4', projections=[User.data])
        self.assertEqual(projected.value, 0)

    async def test_facade_multiple_models_and_result_containers(self):
        db = self.db
        class Other(Model):
            _table_name_ = 'facade_other'
            _db_node_ = db
            _id = Attribute(str)
        User, Address = await self.adapter.initialize(self.Item, Other)
        user = await User(_id='user').commit()
        address = await Address(_id='address').commit()
        result = await self.adapter.transaction(db, lambda a, b: {'items': [a], 'other': (b,)}, user, address)
        self.assertIs(result['items'][0], user)
        self.assertIs(result['other'][0], address)

    async def test_facade_arguments_unwrap_tracked_containers(self):
        User = await self.adapter.initialize(self.Item)
        source = await User(_id='source', tags=['a'], data={'n': 1}).commit()
        target = await User(_id='target').commit()
        target.tags = source.tags
        self.assertIs(target.tags._parent, target.sync)
        await target.update({'$set': {'tags': source.tags, 'data': source.data}})
        fresh = await User.get('target')
        self.assertEqual(fresh.tags, ['a'])
        self.assertEqual(fresh.data, {'n': 1})

    async def test_facade_sync_callbacks_and_transactions_use_raw_records(self):
        User = await self.adapter.initialize(self.Item)
        user = await User(_id='callback').commit()
        seen = []
        def callback(record, row):
            self.assertIsInstance(record, self.Item)
            seen.append(User.sync_model.get('callback').value)
            record.update({'$inc': {'value': 2}})
        self.assertTrue(await user.update({'$inc': {'value': 1}}, callback=callback))
        self.assertEqual(seen, [1])
        self.assertEqual(user.value, 3)
        result = await self.adapter.transaction(self.db, lambda record: record.update({'$inc': {'value': 1}}) and record, user)
        self.assertIs(result, user)
        self.assertEqual(user.value, 4)

    async def test_facade_rollback_restores_proxied_state(self):
        User = await self.adapter.initialize(self.Item)
        user = await User(_id='rollback', value=1).commit()
        def fail(record):
            record.update({'$inc': {'value': 10}})
            raise ValueError('rollback facade')
        with self.assertRaisesRegex(ValueError, 'rollback facade'):
            await self.adapter.transaction(self.db, fail, user)
        self.assertEqual(user.value, 1)
        self.assertEqual((await User.get('rollback')).value, 1)

    async def test_facade_loader_retry_after_failure(self):
        def fail(model):
            raise ValueError('load failed')
        with self.assertRaisesRegex(ValueError, 'load failed'):
            await self.adapter.initialize(self.Item, initializer=fail)
        User = await self.adapter.initialize(self.Item)
        self.assertEqual((await User(_id='retry').commit())._id, 'retry')
        with self.assertRaises(ValueError):
            await self.adapter.initialize()
        with self.assertRaises(TypeError):
            self.adapter.model(object)

    async def test_facade_closed_and_cross_adapter_usage(self):
        User = await self.adapter.initialize(self.Item)
        other = AsyncAdapter()
        try:
            with self.assertRaises(ValueError):
                other.model(User)
        finally:
            await other.close()
        await self.adapter.close()
        with self.assertRaises(RuntimeError):
            await User.get('closed')
        with self.assertRaises(RuntimeError):
            User(_id='closed')

    async def test_facade_locks_do_not_starve_one_worker_adapter(self):
        single = AsyncAdapter(max_workers=1)
        try:
            User = single.model(self.Item)
            await User(_id='lease').commit()
            order = []
            async def work(i):
                user = await User.get('lease')
                async with user.lock(timeout=1000):
                    order.append(('start', i))
                    await asyncio.sleep(.01)
                    order.append(('end', i))
            await asyncio.wait_for(asyncio.gather(work(1), work(2)), timeout=3)
            self.assertEqual(order[0][1], order[1][1])
            self.assertEqual(order[2][1], order[3][1])
        finally:
            await single.close()

    async def test_facade_awaited_lock_silent_timeout_and_unlock(self):
        User = await self.adapter.initialize(self.Item)
        user = await User(_id='lease').commit()
        lease = await user.lock()
        self.assertTrue(lease)
        other = await User.get('lease')
        failed = await other.lock(timeout=10, silent=True)
        self.assertFalse(failed)
        with self.assertRaises(TimeoutError):
            await other.lock(timeout=10)
        await lease.unlock()
        self.assertFalse(lease)
        async with other.lock(timeout=100):
            await other.update({'$inc': {'value': 1}})
        self.assertEqual((await User.get('lease')).value, 1)

    async def test_facade_cancelled_lock_acquisition_releases_accepted_lease(self):
        db = self.db
        entered = asyncio.Event()
        loop = asyncio.get_running_loop()
        class Slow(self.Item):
            _table_name_ = 'facade_slow_lock'
            _db_node_ = db
            def lock(self, *args, **kwargs):
                handle = super().lock(*args, **kwargs)
                loop.call_soon_threadsafe(entered.set)
                time.sleep(.03)
                return handle
        User = await self.adapter.initialize(Slow)
        user = await User(_id='cancel-lock').commit()
        lease = user.lock()
        task = asyncio.create_task(lease.acquire())
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertFalse(lease)
        row = await self.adapter.run(self.db.fetch_rows, 'SELECT __ FROM facade_slow_lock')
        self.assertNotIn('_lock_', row[0]['__'])

    async def test_facade_cancelled_unlock_settles_lease_state(self):
        db = self.db
        entered = asyncio.Event()
        loop = asyncio.get_running_loop()
        class Slow(self.Item):
            _table_name_ = 'facade_slow_unlock'
            _db_node_ = db
            def unlock(self, *args, **kwargs):
                loop.call_soon_threadsafe(entered.set)
                time.sleep(.03)
                return super().unlock(*args, **kwargs)
        User = await self.adapter.initialize(Slow)
        user = await User(_id='cancel-unlock').commit()
        lease = await user.lock()
        task = asyncio.create_task(lease.unlock())
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertFalse(lease)
        row = await self.adapter.run(self.db.fetch_rows, 'SELECT __ FROM facade_slow_unlock')
        self.assertNotIn('_lock_', row[0]['__'])

    async def test_facade_concurrent_failed_load_does_not_discard_retry(self):
        successful_loads = []
        def fail(model):
            time.sleep(.005)
            raise ValueError('initial failure')
        def succeed(model):
            successful_loads.append(model)
            time.sleep(.01)
            initialize_model(model)
        async def retry():
            try:
                return await self.adapter.initialize(self.Item, initializer=fail)
            except ValueError:
                return await self.adapter.initialize(self.Item, initializer=succeed)
        first, second = await asyncio.gather(retry(),
            self.adapter.initialize(self.Item, initializer=fail), return_exceptions=True)
        self.assertIsInstance(second, ValueError)
        self.assertIs(first, await self.adapter.initialize(self.Item, initializer=succeed))
        self.assertEqual(successful_loads, [self.Item])


if __name__ == '__main__':
    unittest.main()
