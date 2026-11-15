"""Real PostgreSQL asyncio integration tests; run separately from gevent."""
import asyncio
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
import unittest
import uuid

import psycopg2
from orms.pg import (
    Attribute, INDEX, Model, DatabaseNode,
    EVENT_AFTER_CREATE, EVENT_AFTER_UPDATE, OptimisticLockError,
    with_lock,
)
from orms.pg_async import AsyncAdapter


def connection_options():
    return dict(
        host=os.environ.get('PG_TEST_HOST', 'localhost'),
        port=int(os.environ.get('PG_TEST_PORT', '5499')),
        user=os.environ.get('PG_TEST_USER', 'postgres'),
        password=os.environ.get('PG_TEST_PASSWORD', 'postgres'),
        db_name=os.environ.get('PG_TEST_DB', 'postgres'),
    )


@unittest.skipUnless(os.environ.get('PG_TEST_PORT'), 'Run ./test_with_pg.sh for a temporary PostgreSQL database')
class AsyncTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.adapter = AsyncAdapter(max_workers=4, create_tables=True)
        self.addAsyncCleanup(self.adapter.close)
        self.db = DatabaseNode(**connection_options(), max_conn=4)
        async def close_db():
            await self.adapter.run(self.db.close)
        self.addAsyncCleanup(close_db)
        db = self.db
        table = 'pg_async_' + uuid.uuid4().hex[:12]
        class Item(Model):
            _table_name_ = table
            _db_node_ = db
            _id = Attribute(str)
            value = Attribute(int, default=0)
            name = Attribute(str)
            data = Attribute(dict)
            tags = Attribute(list)
            INDEX(_id, {'unique': True})
            INDEX(name, {'unique': False})
        self.SyncItem = Item
        self.Item = await self.adapter.initialize(Item)
        async def drop():
            await self.adapter.run(self.db.write, lambda conn: conn.cursor().execute('DROP TABLE IF EXISTS ' + table))
        self.addAsyncCleanup(drop)

    async def test_standalone_import_no_gevent_or_server(self):
        self.assertNotIn('gevent', sys.modules)
        self.assertNotIn('blaster', sys.modules)
        self.assertEqual(psycopg2.extensions.get_wait_callback(), None)

    async def test_crud_tracked_json_projection_and_reload(self):
        item = await self.Item(_id='one', value=10, name='Alice', data={'nested': {'n': 1}}, tags=['a']).commit()
        item.data['nested']['n'] = 2
        item.tags.append('b')
        await item.commit()
        fresh = await self.Item.get('one')
        self.assertEqual(fresh.data['nested']['n'], 2)
        self.assertEqual(fresh.tags, ['a', 'b'])
        self.assertEqual((await self.Item.get('one', projections=['data'])).value, 0)
        self.assertEqual((await self.Item.get('one', projections=[self.Item.name])).name, 'Alice')
        await fresh.update({'$inc': {'value': 1}, '$push': {'data.events': {'type': 'ok'}}})
        await item.reload(['value', 'data'])
        self.assertEqual(item.value, 11)
        self.assertEqual(item.data['events'], [{'type': 'ok'}])
        del item.data['nested']
        await item.commit()
        self.assertNotIn('nested', (await self.Item.get('one')).data)
        await item.delete()
        self.assertIsNone(await self.Item.get('one'))

    async def test_query_many_sort_offset_and_json_filter(self):
        await asyncio.gather(*(self.Item(_id=str(i), value=i, data={'rank': i}).commit() for i in range(8)))
        rows = await self.Item.query({'data.rank': {'$gte': 3}}, sort=[('value', -1)], limit=2, offset=1)
        self.assertEqual([item.value for item in rows], [6, 5])
        self.assertEqual([item._id async for item in self.Item.query({'value': {'$in': [1, 2]}})], ['1', '2'])
        self.assertEqual(len(await self.Item.get(['1', '2', 'missing'])), 2)
        self.assertEqual(await self.Item.get([]), [])
        self.assertEqual([item.value for item in await self.Item.query({'$or': [{'value': 100}, {'value': 7}]})], [7])

    async def test_postgres_io_does_not_block_loop_and_pool_is_bounded(self):
        ticks = 0
        active = set()
        def slow(conn):
            active.add(threading.get_ident())
            with conn.cursor() as cur:
                cur.execute('SELECT pg_sleep(0.06)')
                cur.fetchone()
        async def heartbeat():
            nonlocal ticks
            for _ in range(12):
                await asyncio.sleep(.005)
                ticks += 1
        await asyncio.gather(*(self.adapter.run(self.db.write, slow) for _ in range(12)), heartbeat())
        self.assertEqual(ticks, 12)
        self.assertLessEqual(len(active), 4)
        self.assertNotIn(threading.get_ident(), active)

    async def test_duplicate_does_not_poison_pool(self):
        await self.Item(_id='duplicate').commit()
        with self.assertRaises(psycopg2.errors.UniqueViolation):
            await self.Item(_id='duplicate').commit()
        await self.Item(_id='after').commit()
        self.assertIsNotNone(await self.Item.get('after'))

    async def test_force_insert_and_atomic_updates(self):
        await self.Item(_id='one', value=1).commit()
        duplicate = await self.Item(_id='one', value=9).commit(force=True)
        self.assertEqual(duplicate.value, 9)
        await duplicate.update({'$set': {'data.a.b': 1}, '$inc': {'data.count': 2}, '$push': {'tags': 'x'}})
        self.assertEqual(duplicate.data, {'a': {'b': 1}, 'count': 2})
        await duplicate.update({'$unset': {'data.a': 1}})
        self.assertEqual(duplicate.data, {'count': 2})
        self.assertFalse(await duplicate.update({'$inc': {'value': 99}}, conditions={'value': 100}))
        self.assertEqual(duplicate.value, 9)

    async def test_stale_copy_retries_and_deleted_row_raises(self):
        await self.Item(_id='one').commit()
        first, second = await asyncio.gather(self.Item.get('one'), self.Item.get('one'))
        await first.update({'$inc': {'value': 1}})
        await second.update({'$inc': {'value': 1}})
        self.assertEqual(second.value, 2)
        await first.delete()
        with self.assertRaises(OptimisticLockError):
            await second.update({'$inc': {'value': 1}})

    async def test_concurrent_updates_preserve_json_siblings(self):
        await self.Item(_id='counter').commit()
        copies = await asyncio.gather(*(self.Item.get('counter') for _ in range(3)))
        await asyncio.gather(*(obj.update({'$set': {'data.k' + str(i): i}, '$inc': {'value': 1}})
            for i, obj in enumerate(copies)))
        fresh = await self.Item.get('counter')
        self.assertEqual(fresh.value, 3)
        self.assertEqual(fresh.data, {'k0': 0, 'k1': 1, 'k2': 2})

    async def test_cancelled_callback_rolls_back_and_returns_connection(self):
        item = await self.Item(_id='cancel-callback').commit()
        def callback(*args):
            raise asyncio.CancelledError()
        with self.assertRaises(asyncio.CancelledError):
            await item.update({'$inc': {'value': 1}}, callback=callback)
        self.assertEqual(item.value, 0)
        self.assertEqual((await self.Item.get('cancel-callback')).value, 0)
        await self.Item(_id='after-cancel-callback').commit()

    async def test_callback_nested_success_and_final_same_row_state(self):
        first = await self.Item(_id='first').commit()
        second = await self.Item(_id='second').commit()
        def callback(obj, row):
            time.sleep(.005)
            second.sync.update({'$inc': {'value': 2}})
            obj.update({'$inc': {'value': 3}})
        await first.update({'$inc': {'value': 1}}, callback=callback)
        self.assertEqual(first.value, 4)
        self.assertEqual(second.value, 2)
        self.assertEqual((await self.Item.get('first')).value, 4)

    async def test_callback_rollback_restores_database_and_models(self):
        first = await self.Item(_id='first', value=1).commit()
        second = await self.Item(_id='second', value=2).commit()
        created = self.Item(_id='nested')
        def fail(*args):
            second.sync.update({'$inc': {'value': 10}})
            created.sync.commit()
            raise ValueError('abort callback')
        with self.assertRaisesRegex(ValueError, 'abort callback'):
            await first.update({'$set': {'value': 99}}, callback=fail)
        self.assertEqual((first.value, second.value), (1, 2))
        self.assertTrue(created._is_create_new_)
        self.assertIsNone(await self.Item.get('nested'))
        self.assertEqual((await self.Item.get('second')).value, 2)
        self.assertEqual((await self.Item.get('first')).value, 1)

    async def test_rollback_restores_reloaded_fields_and_pending_changes(self):
        item = await self.Item(_id='reload', value=1).commit()
        stale = await self.Item.get('reload')
        stale.value = 7
        def fail(*args):
            stale.sync.reload(['value'])
            self.assertEqual(stale.value, 99)
            raise ValueError('abort reload')
        with self.assertRaisesRegex(ValueError, 'abort reload'):
            await item.update({'$set': {'value': 99}}, callback=fail)
        self.assertEqual(stale.value, 7)
        self.assertEqual(stale._set_updates, {('value',): 7})
        self.assertEqual((await self.Item.get('reload')).value, 1)

    async def test_nested_failure_preserves_successful_sibling(self):
        first = await self.Item(_id='first').commit()
        second = await self.Item(_id='second').commit()
        third = await self.Item(_id='third').commit()
        def inner(*args):
            raise ValueError('inner failure')
        def outer(*args):
            with self.assertRaises(ValueError):
                second.sync.update({'$set': {'value': 20}}, callback=inner)
            third.sync.update({'$set': {'value': 30}})
        await first.update({'$inc': {'value': 1}}, callback=outer)
        self.assertEqual((await self.Item.get('second')).value, 0)
        self.assertEqual((await self.Item.get('third')).value, 30)

    async def test_unrelated_task_uses_independent_transaction(self):
        item = await self.Item(_id='first', value=1).commit()
        entered, proceed = asyncio.Event(), threading.Event()
        loop = asyncio.get_running_loop()
        def fail(*args):
            loop.call_soon_threadsafe(entered.set)
            if not proceed.wait(3):
                raise TimeoutError('test synchronization')
            raise ValueError('outer rollback')
        task = asyncio.create_task(item.update({'$inc': {'value': 99}}, callback=fail))
        await entered.wait()
        try:
            await self.Item(_id='independent').commit()
            self.assertEqual((await self.Item.get('first')).value, 1)
        finally:
            proceed.set()
        with self.assertRaisesRegex(ValueError, 'outer rollback'):
            await task
        self.assertIsNotNone(await self.Item.get('independent'))

    async def test_nested_callback_works_with_single_connection_pool(self):
        node = DatabaseNode(**connection_options(), max_conn=1)
        self.addCleanup(node.close)
        original = self.SyncItem._db_node_
        self.SyncItem._db_node_ = node
        try:
            item = await self.Item(_id='single').commit()
            def callback(obj, row):
                obj.update({'$inc': {'value': 2}})
            await asyncio.wait_for(item.update({'$inc': {'value': 1}}, callback=callback), 5)
            self.assertEqual(item.value, 3)
        finally:
            self.SyncItem._db_node_ = original

    async def test_async_leases_and_with_lock_decorator(self):
        await self.Item(_id='locked').commit()
        order = []
        async def work(i):
            obj = await self.Item.get('locked')
            async with obj.lock(timeout=3000):
                order.append(('start', i))
                await asyncio.sleep(.01)
                order.append(('end', i))
        await asyncio.gather(work(1), work(2))
        self.assertEqual(order[0][1], order[1][1])
        self.assertEqual(order[2][1], order[3][1])
        @with_lock
        def increment(obj):
            obj.update({'$inc': {'value': 1}})
            return obj.value
        obj = await self.Item.get('locked')
        self.assertEqual(await self.adapter.run(increment, obj.sync), 1)

    async def test_cancelled_caller_leaves_state_consistent(self):
        item = await self.Item(_id='one').commit()
        entered = asyncio.Event()
        loop = asyncio.get_running_loop()
        def callback(*args):
            loop.call_soon_threadsafe(entered.set)
            time.sleep(.03)
        task = asyncio.create_task(item.update({'$inc': {'value': 1}}, callback=callback))
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(item.value, 1)
        self.assertEqual((await self.Item.get('one')).value, 1)

    async def test_named_nodes_and_replica_routing(self):
        replica = DatabaseNode(**connection_options(), name='pg-async-replica')
        self.addCleanup(replica.close)
        self.db.replica = 'pg-async-replica'
        await self.Item(_id='read').commit()
        self.assertEqual((await self.Item.get('read', _replica=True))._id, 'read')
        self.assertIsNotNone(replica._pool)
        self.assertEqual(len(await self.Item.query(_replica=True)), 1)

    async def test_commit_failure_rolls_back_and_pool_recovers(self):
        item = await self.Item(_id='first').commit()
        def transaction(record):
            record.update({'$inc': {'value': 1}})
            def sql(conn):
                with conn.cursor() as cur:
                    cur.execute('CREATE TEMP TABLE deferred_parent (id integer PRIMARY KEY) ON COMMIT DROP')
                    cur.execute('CREATE TEMP TABLE deferred_child (id integer REFERENCES deferred_parent(id) DEFERRABLE INITIALLY DEFERRED) ON COMMIT DROP')
                    cur.execute('INSERT INTO deferred_child VALUES (999)')
            self.db.write(sql)
        with self.assertRaises(psycopg2.errors.ForeignKeyViolation):
            await self.adapter.transaction(self.db, transaction, item)
        self.assertEqual(item.value, 0)
        self.assertEqual((await self.Item.get('first')).value, 0)
        await self.Item(_id='after').commit()

    async def test_raw_queries_and_sync_callback(self):
        item = await self.Item(_id='one').commit()
        seen = []
        def callback(obj, row):
            seen.append(self.SyncItem.get('one').value)
        await item.update({'$inc': {'value': 1}}, callback=callback)
        self.assertEqual(seen, [1])
        rows = await self.adapter.run(self.db.fetch_rows, 'SELECT 42 AS answer')
        self.assertEqual(rows[0]['answer'], 42)

    async def test_schema_migration_and_generated_id(self):
        db, table = self.db, self.Item._table_name_ + '_generated'
        class Generated(Model):
            _table_name_ = table
            _db_node_ = db
            name = Attribute(str)
        Generated = await self.adapter.initialize(Generated)
        async def drop():
            await self.adapter.run(self.db.write, lambda conn: conn.cursor().execute('DROP TABLE IF EXISTS ' + table))
        self.addAsyncCleanup(drop)
        item = await Generated(name='generated').commit()
        self.assertIsInstance(item._id, int)
        self.assertEqual((await Generated.get(item._id)).name, 'generated')
        await self.adapter.run(self.db.write, lambda conn: conn.cursor().execute('ALTER TABLE ' + table + ' DROP COLUMN name'))
        await Generated.create_table()
        self.assertEqual((await Generated(name='restored').commit()).name, 'restored')

    async def test_shutdown_drains_callback_descendants(self):
        adapter = AsyncAdapter(create_tables=True)
        self.addAsyncCleanup(adapter.close)
        User = adapter.model(self.SyncItem)
        item = await User(_id='close').commit()
        entered = asyncio.Event()
        loop = asyncio.get_running_loop()
        def callback(*args):
            loop.call_soon_threadsafe(entered.set)
            time.sleep(.02)
            self.SyncItem(_id='nested-close').commit()
        task = asyncio.create_task(item.update({'$inc': {'value': 1}}, callback=callback))
        await entered.wait()
        await adapter.close()
        await task
        self.assertIsNotNone(await self.Item.get('nested-close'))
        with self.assertRaises(RuntimeError):
            await User.get('closed')

    async def test_list_dict_mutations_and_event_listeners(self):
        seen = []
        self.Item.on([EVENT_AFTER_CREATE, EVENT_AFTER_UPDATE], lambda obj: seen.append(obj._id))
        item = await self.Item(_id='channels').commit()
        for i in range(100):
            item.data[str(i)] = {}
        await item.commit()
        self.assertEqual(len((await self.Item.get(item._id)).data), 100)
        item.data.clear()
        item.data['new'] = {'keep': True}
        item.tags.extend(['a', 'b', 'c'])
        await item.commit()
        item.tags.pop()
        item.tags.insert(0, 'x')
        await item.commit()
        self.assertEqual((await self.Item.get(item._id)).data, {'new': {'keep': True}})
        self.assertEqual(item.tags, ['x', 'a', 'b'])
        self.assertEqual(len(seen), 4)

    async def test_consumer_file_symlink_import(self):
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, 'consumer_pg.py').symlink_to(Path(sys.modules[Model.__module__].__file__).resolve())
            script = """
                import sys
                sys.path.insert(0, sys.argv[1])
                from consumer_pg import Model, DatabaseNode
                assert 'blaster' not in sys.modules and 'gevent' not in sys.modules
                assert 'asyncio' not in sys.modules
            """
            await asyncio.to_thread(subprocess.run, [sys.executable, '-I', '-c', textwrap.dedent(script), tmp], check=True, timeout=10)


if __name__ == '__main__':
    unittest.main()
