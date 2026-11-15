"""Standalone gevent PostgreSQL tests, without importing the Blaster server."""
import os
import sys
import unittest
import uuid

import gevent
import psycopg2
from orms.pg import Attribute, DatabaseNode, Model, initialize_model, with_lock


def connection_options():
    return dict(
        host=os.environ.get('PG_TEST_HOST', 'localhost'),
        port=int(os.environ.get('PG_TEST_PORT', '5499')),
        user=os.environ.get('PG_TEST_USER', 'postgres'),
        password=os.environ.get('PG_TEST_PASSWORD', 'postgres'),
        db_name=os.environ.get('PG_TEST_DB', 'postgres'),
    )


@unittest.skipUnless(os.environ.get('PG_TEST_PORT'), 'Run ./test_with_pg.sh for a temporary PostgreSQL database')
class GeventTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from gevent import monkey
        monkey.patch_all()
        from psycopg2.extras import wait_select
        psycopg2.extensions.set_wait_callback(wait_select)

    def setUp(self):
        self.db = DatabaseNode(**connection_options(), max_conn=3)
        self.addCleanup(self.db.close)
        db, table = self.db, 'pg_gevent_' + uuid.uuid4().hex[:12]
        class Item(Model):
            _table_name_ = table
            _db_node_ = db
            _id = Attribute(str)
            value = Attribute(int, default=0)
            data = Attribute(dict)
            tags = Attribute(list)
        self.Item = Item
        initialize_model(Item, create_tables=True)
        self.addCleanup(self.drop)

    def drop(self):
        with self.db.use_conn() as conn:
            with conn.cursor() as cur:
                cur.execute('DROP TABLE IF EXISTS ' + self.Item._table_name_)

    def test_standalone_import_does_not_start_server(self):
        self.assertNotIn('blaster', sys.modules)
        self.assertIsNotNone(psycopg2.extensions.get_wait_callback())

    def test_queries_yield_to_greenlets_and_pool_waits_at_capacity(self):
        ticks = []
        overlapping = []
        def query(i):
            rows = list(self.db.fetch_rows('SELECT pg_sleep(0.04), %s AS value', [i]))
            return rows[0]['value']
        def heartbeat():
            for i in range(12):
                gevent.sleep(.005)
                ticks.append(i)
                overlapping.append(any(not job.ready() for job in jobs))
        jobs = [gevent.spawn(query, i) for i in range(12)]
        beat = gevent.spawn(heartbeat)
        gevent.joinall(jobs + [beat], raise_error=True)
        self.assertEqual([job.value for job in jobs], list(range(12)))
        self.assertEqual(len(ticks), 12)
        self.assertTrue(any(overlapping))

    def test_concurrent_crud_and_tracked_json(self):
        def create(i):
            item = self.Item(_id=str(i), data={'nested': {'i': i}}).commit()
            item.data['nested']['i'] += 1
            item.tags.append(i)
            item.commit()
            return item
        jobs = [gevent.spawn(create, i) for i in range(20)]
        gevent.joinall(jobs, raise_error=True)
        self.assertEqual(len(list(self.Item.query())), 20)
        for i in range(20):
            fresh = self.Item.get(str(i))
            self.assertEqual(fresh.data['nested']['i'], i + 1)
            self.assertEqual(fresh.tags, [i])

    def test_callback_rollback_restores_all_nested_writes(self):
        first = self.Item(_id='first', value=1).commit()
        second = self.Item(_id='second', value=2).commit()
        created = self.Item(_id='nested')
        def callback(*args):
            second.update({'$inc': {'value': 10}})
            created.commit()
            raise ValueError('rollback nested')
        with self.assertRaisesRegex(ValueError, 'rollback nested'):
            first.update({'$inc': {'value': 99}}, callback=callback)
        self.assertEqual(first.value, 1)
        self.assertEqual(second.value, 2)
        self.assertTrue(created._is_create_new_)
        self.assertEqual(self.Item.get('first').value, 1)
        self.assertEqual(self.Item.get('second').value, 2)
        self.assertIsNone(self.Item.get('nested'))

    def test_callback_success_uses_same_connection(self):
        first = self.Item(_id='first').commit()
        second = self.Item(_id='second').commit()
        pids = []
        def callback(obj, row):
            pids.append(next(self.db.fetch_rows('SELECT pg_backend_pid() AS pid'))['pid'])
            second.update({'$inc': {'value': 2}})
            obj.update({'$inc': {'value': 3}})
            pids.append(next(self.db.fetch_rows('SELECT pg_backend_pid() AS pid'))['pid'])
        first.update({'$inc': {'value': 1}}, callback=callback)
        self.assertEqual(first.value, 4)
        self.assertEqual(second.value, 2)
        self.assertEqual(pids[0], pids[1])

    def test_failed_nested_sql_savepoint_does_not_poison_outer(self):
        item = self.Item(_id='one').commit()
        def callback(*args):
            try:
                self.Item(_id='one').commit()
            except psycopg2.errors.UniqueViolation:
                pass
            self.Item(_id='after').commit()
        item.update({'$inc': {'value': 1}}, callback=callback)
        self.assertIsNotNone(self.Item.get('after'))
        self.assertEqual(item.value, 1)

    def test_record_leases_and_decorator(self):
        self.Item(_id='locked').commit()
        order = []
        def work(i):
            obj = self.Item.get('locked')
            with obj.lock(timeout=3000):
                order.append(('start', i))
                gevent.sleep(.01)
                order.append(('end', i))
        gevent.joinall([gevent.spawn(work, i) for i in range(2)], raise_error=True)
        self.assertEqual(order[0][1], order[1][1])
        self.assertEqual(order[2][1], order[3][1])
        @with_lock
        def increment(obj):
            obj.update({'$inc': {'value': 1}})
            return obj.value
        self.assertEqual(increment(self.Item.get('locked')), 1)

    def test_pool_recovers_after_error_and_closes_idempotently(self):
        self.Item(_id='one').commit()
        with self.assertRaises(psycopg2.errors.UniqueViolation):
            self.Item(_id='one').commit()
        self.Item(_id='after').commit()
        node = DatabaseNode(**connection_options())
        list(node.fetch_rows('SELECT 1'))
        node.close()
        node.close()
        with self.assertRaises(RuntimeError):
            list(node.fetch_rows('SELECT 1'))


if __name__ == '__main__':
    unittest.main()
