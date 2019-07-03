'''
Created on 02-Jul-2019

@author: abhinav
'''
from blaster.urllib_utils import get_data
import json
import gevent
from blaster.websocket._core import create_connection
import time


auth_key1 = json.loads(get_data("http://localhost:8081/create_or_update_user").read())["auth_key"]
auth_key2 = json.loads(get_data("http://localhost:8081/create_or_update_user").read())["auth_key"]

print(auth_key1, auth_key2)
session_id = json.loads(get_data("http://localhost:8081/get_session/abhinav?auth_key="+auth_key1).read())["session"]["session_id"]
print(session_id)
   
ws1 = create_connection("ws://localhost:8081/join_session/"+session_id+"?auth_key="+auth_key1)

ws2 = create_connection("ws://localhost:8081/join_session/"+session_id+"?auth_key="+auth_key2)

ws3 = create_connection("ws://localhost:8081/join_session/"+session_id+"?auth_key="+auth_key2)

#initial messages
print(ws2.recv())
print(ws1.recv())
print(ws3.recv())


time.sleep(1)


ws1.send(json.dumps({'type':1, "payload":"Hello world" }))
time.sleep(0.1)
print(ws2.recv())
print(ws3.recv())

ws1.send_close()
ws2.send_close()