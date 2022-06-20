'''
Created on 29-Dec-2017

@author: abhinav
'''
from .model import Model
from .fields import CharField, IntegerField
from ..tools import cur_ms


class InboxMessage(Model):
    
    __table_name__ = 'inbox_messages'

    ReadCapacityUnits = 2
    WriteCapacityUnits = 2
    
    inbox_id = CharField(name="inbox_id", hash_key=True)
    created_at = IntegerField(name="created_at", range_key=True)
    
    #### Note dest_id => the owner who owns this inbox_id
    
    dest_session_id =  CharField(name="dest_session_id")
    src_id =  CharField(name="src_id", indexed=True)
    
    _type = IntegerField(name="_type")
    payload = CharField(name="payload")
    payload1 = CharField(name="payload1")
    payload2 = CharField(name="payload2")
    

    @classmethod
    def add_to_inbox(cls, inbox_id,  src_id, _type , payload, payload1=None, payload2 = None, created_at=None):
        if(created_at == None):
            created_at = cur_ms()
        payload = None if not payload else str(payload)
        payload1 = None if not payload1 else str(payload1)
        payload2 = None if not payload2 else str(payload2)

        return InboxMessage.create(inbox_id=inbox_id, src_id=src_id, _type=_type, payload=payload,  payload1=payload1, payload2=payload2, created_at=created_at  )
    
    @classmethod
    def add_to_inbox_multiple(cls, list_of_dicts ):
        for i in list_of_dicts:
            if(not i.get("created_at", None)):
                i["created_at"] = cur_ms()
        return InboxMessage.batch_write(list_of_dicts)
    
    
    
        

    @classmethod
    def get_inbox_messages(cls, inbox_id ,less_than_timestamp=None, greater_than_timestamp=None):
        if(greater_than_timestamp==None or greater_than_timestamp<0):
            greater_than_timestamp=0
        
        if(less_than_timestamp==None or less_than_timestamp<0):
            less_than_timestamp= cur_ms()
        items, cursor = InboxMessage.query().use_index(None, asc=False).where(InboxMessage.inbox_id.eq(inbox_id) ,  InboxMessage.created_at.between(int(greater_than_timestamp), int(less_than_timestamp))).get_items()
        if(items):
            less_than_timestamp = items[-1].created_at
            
        
        return items, less_than_timestamp, cursor!=None
    
