from blaster.aws import dynamix
from blaster.aws.dynamix.model import Model
from blaster.aws.dynamix.fields import CharField, IntegerField, SetField, ListField,\
    DictField
import time


class ForumThread(Model):

    __table_name__ = 'forum_thread'

    __secondary_indexes__ = {
                             "user_threads_index": dict(hash_key="user_id", range_key="updated_at", projections=["title"]), # secondary index
                            }
    
    forum_id = CharField(name='forum_id', hash_key=True) # range key
    thread_id = CharField(name='thread_id', range_key=True) # range key
    user_id = CharField(name="user_id")
    title = CharField(name="title")
    description = CharField(name="description")
    updated_at = IntegerField(name='updated_at', default=lambda : int(time.time() * 1000), indexed=True) # local index
    expiry_timestamp = IntegerField(name='expiry_timestamp', indexed=True, projections=["updated_at"]) # local index with selected attributes projected to index
    tags = SetField(name="tags")
    user_reviews = ListField(name="user_reviews", default=[])
    key_value_data = DictField(name="key_value_data")


dynamix.create_table(ForumThread, force=True)


forum_thread = ForumThread.create(forum_id="science", thread_id="physics_1", title="gravitational field of a blackhole", description="equations and more", tags=set(["physics", "gravity"]) , user_id="me", key_value_data={"a": 1, "b" : 2})

forum_thread = ForumThread.get(forum_id="science", thread_id="physics_1")

print(forum_thread.to_son())

threads, cursor = ForumThread.query().where(ForumThread.forum_id.eq("science"), ForumThread.thread_id.begins_with("physics_")).get_items()
for i in threads:
    print(i.to_son())
    
threads, cursor = ForumThread.query().start_key(cursor).use_index(ForumThread.updated_at , asc=True).where(ForumThread.forum_id.eq("science"), ForumThread.updated_at.lte(int(time.time()*1000))).get_items(requery_for_all_projections=True)
for i in threads:
    print(i.to_son())

print(ForumThread.batch_write([{"forum_id": "science" , "thread_id": "physics_" + str(num), "title": "Physics", "description" : "some description ...."} for num in range(100)]))
#Updates
forum_thread = ForumThread.get(forum_id="science", thread_id="physics_1")
forum_thread.set_field(ForumThread.updated_at, int(time.time() * 1000))

forum_thread.append_to_listfield(ForumThread.user_reviews, ["100", "101"])
forum_thread.add_to_setfield(ForumThread.tags, set(["black_hole", "white_hole"]))
forum_thread.update_to_dict_field(ForumThread.key_value_data, {"a": 100, "d": 10})

forum_thread.commit_changes()# need commit changes, as adding and remove from a same field overlaps document paths
#but you get the idea of updating multiple fields and commiting changes at the end

#removing from fields
forum_thread.remove_by_index_from_listfield(ForumThread.user_reviews, 1) # removes first index
forum_thread.delete_from_setfield(ForumThread.tags, set(["black_hole"]))
forum_thread.remove_from_dict_field(ForumThread.key_value_data, ["a"])


forum_thread.commit_changes()# need commit changes, as cannot add and remove at same time


print(forum_thread.to_son())