# mongo orm
# setup: 2 replica sets
# intialize table with primary and multiple secondary shards

# insert 10000 documents
# 1. verify all 10000 are split to shards
# 2. verify secondary shards are also updated for all 10000 documents

# update 100 documents
# 1. verify  updates are propagated to the secondary shards and are available for query

import blaster
blaster.config.load("test.yaml")

import unittest
import time
import gevent
import random
from blaster.mongo_orm import Model, Attribute, INDEX, SHARD_BY, initialize_mongo
from blaster.tools import get_random_id, get_str_id


class AModel(Model):
    _collection_name_ = "a"

    a = Attribute(int)
    b = Attribute(str)
    c = Attribute(str)
    d = Attribute(str)
    e = Attribute(str)
    f = Attribute(str)
    g = Attribute(list)

    INDEX(
        (a, b),
        (b, c),
        (c, d),
        (c, d, e),
        (c, e)
    )
    SHARD_BY(primary=a, secondary=[b, c])


class BModel(Model):
    _collection_name_ = "b"

    a = Attribute(int)
    b = Attribute(str)
    c = Attribute(str)
    d = Attribute(str)
    e = Attribute(str)
    f = Attribute(str)

    INDEX(
        (a,),
        (b, c),
        (c, d),
        (c, d, e),
        (c, e)
    )
    SHARD_BY(primary=a, secondary=[b, c])

    def before_update(self):
        self.b = (self.b or "") + "0"


class CModel(Model):
    _collection_name_ = "c"

    _id = Attribute(str)
    a = Attribute(list)
    b = Attribute(dict)
    c = Attribute(str)


initialize_mongo(
    [
        {"host": "mongo1-1:9042", "replicaset": "rs1"},
        # {"host": "localhost:27017"}
    ],
    "test_" + str(int(time.time()))
)


class TestConcurrentThreadUpdates(unittest.TestCase):
    def make_table_changes(self, _by):
        item = AModel.get(a=0)
        item.b = (item.b or "") + _by
        item.f = (item.f or "") + _by
        # this should propagate b to secondary shard correctly
        item.commit()

        for i in range(10):
            _i = random.randint(0, 9)
            item = AModel.get(a=_i)
            item.b = (item.b or "") + _by
            item.c = (item.c or "") + _by
            item.d = (item.d or "") + _by
            item.e = (item.e or "") + _by
            item.f = (item.f or "") + _by
            item.commit()
            print(item.to_dict())

    def test_concurrent_thread_updates(self):
        for i in range(10):
            AModel(a=i).commit()
        gevent.joinall([gevent.spawn(self.make_table_changes, str(i)) for i in range(10)])


class TestValidators(unittest.TestCase):
    def test_validators(self):
        a = AModel(a=11, b="-ab-" * 1000).commit(force=True)
        self.assertEqual(len(a.b), 2048)


class TestBasics(unittest.TestCase):
    def test_update_propagation(self):
        a = AModel(a=11, b="23", c="33", d="43", e="53").commit()
        a.c = None
        a.commit()

        self.assertIsNone(
            AModel._secondary_shards_["c"]._Model_.get_collection(None).find_one({"c": None})
        )

    def test_attrs_belongs_to_model(self):
        print(AModel._attrs_["c"]._models)
        self.assertEqual(len(AModel._attrs_["c"]._models), 3)
        self.assertEqual(len(AModel._attrs_["d"]._models), 2)
        self.assertEqual(len(AModel._attrs_["f"]._models), 1)

    def test_commit_with_transaction(self):
        a = AModel(a=11, b="22", c="33").commit()
        try:
            b = AModel(a=12, b="22", c="33").commit()  # should crash but should create entry in a shard

        except Exception:
            pass
        self.assertIsNone(
            AModel.get_collection(11).find_one({"a": 12})
        )

    def test_transaction_chain(self):
        a = AModel(a=11, b="21", c="31", d="41", e="51").commit()
        a.f = "100"
        a.c = "32"

        def after_mongo_update(self, old_doc, new_doc, _transaction=None):
            raise Exception("some ex")
        try:
            a.update({}, after_mongo_update=after_mongo_update)
        except Exception as ex:
            print(str(ex))

        # test update doesn't commit
        self.assertEqual(
            AModel.get_collection(11).find_one({"a": 11})["c"],
            "31"
        )

    def test_max_rate_exception(self):
        for i in range(101, 300, 1):
            CModel(_id=str(i), c=str(i)).commit()

        CModel.query({}, limit=15)  # this should warn
        raised_exception = False
        try:
            l = list(CModel.query({}))  # this should warn
        except Exception as ex:
            raised_exception = True
            print(str(ex))

        self.assertTrue(raised_exception)


class TestListAndDict(unittest.TestCase):
    def test_mongo_list(self):
        a = AModel(a=11).commit()
        a.g.append(1)
        a.commit()
        a.g.append(2)
        a.g.append(3)
        a.commit()
        self.assertListEqual(a.g, [1, 2, 3])
        a.g.insert(0, 0)
        a.commit()
        self.assertListEqual(a.g, [0, 1, 2, 3])
        a.g.append(4)
        a.commit()
        self.assertListEqual(a.g, [0, 1, 2, 3, 4])
        try:
            a.g.append(4)
            a.g.insert(0, -1)            
        except Exception:
            print("Exception raised - OK")
        a = AModel.get(a=11, use_cache=False)
        self.assertListEqual(a.g, [0, 1, 2, 3, 4])

        a.g.append(5)
        a.g.append(6)
        a.g.append(7)
        a.g.append(8)
        a.commit()
        self.assertListEqual(a.g, [0, 1, 2, 3, 4, 5, 6, 7, 8])
        a = AModel.get(a=11, use_cache=False)


class TestSnippets(unittest.TestCase):
    def test_1(self):
        a = AModel(a=int(time.time()), b="00904441").commit()


class TestUpdates(unittest.TestCase):
    def test_update_with_change(self):
        b = BModel(a=0).commit()
        b = BModel.get(a=0)
        b.update({"$set": {"c": 100}})

        b = list(b.query({"a": 0}))[0]
        self.assertTrue(b.b.startswith("0"))
        self.assertTrue(b.c == "100")

    def test_list_insert(self):
        c = CModel(_id="3").commit()
        c.a.insert(0, 100)
        c.commit()
        self.assertTrue(c.a == [100])

        c_from_db = CModel.get("3", use_cache=False)
        self.assertTrue(c_from_db.a == [100])
        c.a.insert(1, 150)
        c.commit()

        c_from_db = CModel.get("3", use_cache=False)
        self.assertTrue(c_from_db.a == [100, 150])


class TestBugs(unittest.TestCase):
    def test_top_level_or_query_fix(self):
        for i in range(100):
            CModel(_id=str(i), c=str(i)).commit()
        CModel(_id="100").commit()
        l = list(CModel.query({"_id": {"$in": ["1", "3", "5"]}, "$or": [{"c": "1"}, {"c": "5"}]}))
        self.assertEqual(len(l), 2)



# test remove multiple values from MongoList
# test remove multiple values from MongoDict
# BLASTER_MONGO_RUNNING_IN_TEST_MODE=1 python -m unittest test.test_mongo_orm.TestBugs
