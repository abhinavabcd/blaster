'''
Created on 22-Aug-2017

@author: abhinav
'''
from .gevent_aws_base.base import start_wsgi_server, write_data,\
    start_stream_server
import argparse 
import ujson as json
import collections
from .common_funcs_and_datastructures import LRUCache, create_signed_value,\
    get_random_id, decode_signed_value,\
    set_socket_options,\
    Connection
from .urllib_utils import get_data
import random
from .ws_handler import WebSocketServerHandler
import time

#basic session descriptions   
#session = {"id": , "addr": , "description": ,"image": , "type": , "last_few_chats": [] }
public_sessions = LRUCache(100000)
private_sessions = LRUCache(100000)


servers = {"localhost": { "load": 0.0}} # {server_id : address , load: }



def get_random_name():
    random.choice(["Pandugadu","Rgv", "Apple", "atalo ariti pandu", "erri pushpam", "naku koncham mental"])




##app decorators

def need_user(func):
    def new_func(sock, *args , **kwargs):        
        if(kwargs("user",None)):
            return func(sock , *args, **kwargs)
        return "404", None, "no user ? darn !."
        
    return new_func
    

def decode_user(func):
    def new_func(sock, *args , **kwargs):        
        query_params = kwargs.get("query_params",None)
        auth_key = query_params.get("auth_key", None)
        if(auth_key):
            user = json.loads(decode_signed_value("auth_key", auth_key))
            kwargs["user"] = user
            
        return func(sock , *args, **kwargs)
        
    return new_func

def get_internal_auth_key():
    return create_signed_value("auth_key", {"user_id": "internal", "name": "don"})





#master server 
#show all recently active sessions #lru based
def get_sessions(sock ,query_params=None, headers=None, post_data=None):
    recent_sessions = public_sessions.cache.items()[:-100]
    return json.dumps( { "recent_sessions":  recent_sessions  } )


#get individual session data
def get_session(sock, query_params=None, headers=None, post_data=None):
    session_id = query_params.get("session_id", None)
    session = None
    if(session_id and session_id.startswith("private_")):
        session = private_sessions.cache.get(session_id,None)#returns empty string
    else:
        session = public_sessions.cache.get(session_id, "")#returns empty string
    
    if(session):
        return json.dumps(session)
    
    return "" #empty body

#probably called internally between servers to main server
@need_user
@decode_user
def update_session_data(sock, session_id, query_params=None, headers=None, post_data=None, user=None):
    if(session_id):
        if(user["user_id"] == "internal"):
            public_sessions.set("session_id", json.loads(post_data)) # will update this blindly
            return "OK"
    
    return "Nope"


#update load etc
@need_user
@decode_user
def update_server_stats(sock, query_params=None, headers=None, post_data=None, user=None):
    server_addr = query_params.get("server_addr", None)
    if(user["user_id"] == "internal"):
        servers[server_addr] = json.loads(post_data)
        return "OK"
    
    return "Nope"
    
@need_user
@decode_user
def create_new_session(sock, query_params=None, headers=None, post_data=None, user=None):
    #create on some random server a
    description = query_params.get("description", None)
    image = query_params.get("image", None)
    is_public = query_params.get("is_public", None)
    available_servers = []
    for i in servers:
        if(servers[i]["load"]< 1):
            available_servers.append(i)
    
    addr = random.choice(available_servers)
    
    if(not addr):
        return "" #########could not create
    
    if(is_public=="true"):
        session_id = get_random_id(10)
    else:
        session_id = "private_"+get_random_id(10)
        
  
    session_obj = {"session_id": session_id, "description" : description, "image": image ,"type": 0, "addr": addr, "user": user}
    
    #update on that server
    req = get_data("http://"+addr+"/after_new_session_created?auth_key="+get_internal_auth_key(), post=json.dumps(session_obj))
    if(not req or req.read()!="okay"):
        return "401 error",None, ""
        
    
    if(is_public=="true"):
        public_sessions.set(session_id ,  session_obj )
    else:
        private_sessions.set(session_id ,  session_obj )
        
    return json.dumps(session_obj)
            

#all other mini servers
session_data = {}

@need_user
@decode_user
def after_new_session_created(sock,query_params=None, headers=None, post_data=None, user=None):
    if(user["user_id"]!="internal"):
        return "404 not found",None, ""
    
    session_obj = json.loads(post_data)
    if(not session_obj["session_id"] in session_data):
        session_data[session_obj["session_id"]] = session_obj
        session_obj["connections"] = LRUCache(500)
        session_obj["viewer_connections"] = LRUCache(500)
        
        session_data["messages"] = collections.deque(maxlen=100)
    return "okay"
    


@decode_user
def create_or_update_user(sock,query_params=None, headers=None, post_data=None, user=None):
    user_name = query_params.get("name", None)
    user_id = None
    if(user):
        user_id = user["user_id"]
    return create_signed_value("auth_key", json.dumps({"name":user_name or get_random_name(), "user_id" : user_id or get_random_id(10)}))

#web socket based connection
@need_user
@decode_user
def join_session(sock, session_id, query_params=None, headers=None, post_data=None, user=None):
    session_id = session_id or query_params.get("session_id", None)
    if(not session_id in session_data):
        return "404 not found", None, ""
    
    user_id = user["user_id"]
    session_obj = session_data[session_id]
    session_connections = session_obj["connections"]
    session_connections_temp = session_obj["viewer_connections"]
    
    session_messages = session_obj["messages"]

    ws = WebSocketServerHandler(sock, headers)#sends handshake automatically 
    set_socket_options(sock)
    
    def delete_connection(ex):
        del session_connections[ws]
    
    def on_connected():
        ws.conn_obj = conn = Connection(ws, user_id)
        conn.is_viewer_list = True
        popped_connections = session_connections_temp.set(conn, True)
        for c , b in popped_connections:
            c.ws.close()
        
        #send last 100 messages
        initial_messages = json.dumps(session_messages[:])
        msg_to_send = {"type": 101, "payload": initial_messages}
        conn.send(json.dumps(msg_to_send), add_to_assumend_sent=False)
        
        
        
    def on_timeout():
        if(ws.conn_obj):
            ws.conn_obj.send(json.dumps({"type":0, "timestamp": int(time.time()*1000)}))
            return True
        return False
    
    def on_message():
        try:
            message_data = json.loads(ws.data)
            if(message_data.get("type", None)==1):
                if(ws.conn_obj.is_viewer_list):
                    session_connections_temp.delete(ws.conn_obj)
                    ws.conn_obj.is_viewer_list = False
                    popped_connections = session_connections.set(ws.conn_obj, True)
                    for c , b in popped_connections:
                            c.ws.close()
                                           
                message_data["src_id"] = user_id #user_id
                message_data["timestamp"] = int(time.time()*1000) #user_id
                
                session_messages.append( message_data )
                #broadcast to all other connections in this session
                message_data_str = json.dumps(message_data)
                for conn in session_connections:
                    if(conn!=ws.conn_obj):
                        conn.send(message_data_str)
                for conn in session_connections_temp:
                    if(conn!=ws.conn_obj):
                        conn.send(message_data_str)
            
        except:
            pass
    
    ws.handleClose = delete_connection
    ws.handleConnected = on_connected
    ws.handleMessage = on_message
    ws.handleMessage = on_timeout
     
    ws.do_handshake(headers)
    ws.start_handling()
    return





if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description='process arguments')
    args = parser.parse_args()

    start_stream_server(8081, handlers=[
                            ('^/get_sessions', get_sessions), 
                            ('^/get_session/(.*)', get_session), 
                            ('^/create_or_update_user', create_or_update_user),
                            ('^/after_new_session_created', after_new_session_created), 
                            ('^/create_new_session', create_new_session), 
                            ('^/update_session_data/(.*)', update_session_data),  
                            ('^/join_session/(.*)', join_session),
                            ('~/update_server_stats',update_server_stats)
                          ])
    
