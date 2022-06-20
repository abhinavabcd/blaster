'''
Created on 22-Aug-2017

@author: abhinav
'''
import blaster
from blaster.config import IS_DEV

## set the configs
from blaster.base import start_server, parse_qs_modified
import argparse
import ujson as json
import collections
from blaster.tools import LRUCache, create_signed_value,\
	get_random_id, decode_signed_value,\
	set_socket_options,\
	WebsocketConnection, cur_ms
from blaster.urllib_utils import get_data
import random
from blaster.websocket.server import WebSocketServerHandler
import time
from blaster.constants import TYPE_ERROR, TYPE_OK, LOG_TYPE_EXCEPTION
from blaster.base import route_handler, is_server_running, LOG_WARN
import socket
import gevent
from gevent.time import sleep
import urllib

blaster.config.secrets = {"v0": "asdsadkjhsakjdhjksadasdasd"}



'''
main server has all the sessions info
when other server starts , update_server_status  onto main server
create_session on main server will create on a minion server or on self
connect to the minion server for a particular session.
'''


#basic session descriptions
#session = {"id": , "addr": , "description": ,"image": , "type": , "last_few_chats": [] }
public_sessions = LRUCache(100000)
private_sessions = LRUCache(100000)


#TODO: get the public ip address somehow
self_server_addr = None

main_server_addr = None

#relevant on main server
servers = None


TYPE_SESSION = 10
TYPE_SESSIONS = 11
TYPE_USER = 12


random_user_names = ["Pandugadu", "Rgv", "Apple", "atalo ariti pandu", "erri pushpam", "naku koncham mental"]
def get_random_name(hash_val=None):
	if(hash_val):
		return random_user_names[hash(hash_val)%len(random_user_names)]
	return random.choice(random_user_names)




##app decorators

def need_user(func):
	def new_func(*args , **kwargs):
		if(kwargs.get("user", None)): # atleast one has to exist
			return func(*args, **kwargs)
		return {"type": TYPE_ERROR, "error_message": "no user ? darn !."}
		
	new_func._original = getattr(func, "_original", func)
	return new_func
	
def decode_user(func):
	def new_func(*args , **kwargs):
		request_params = kwargs.get("request_params", None)
		auth_key = request_params.get("auth_key", None)
		if(not auth_key):
			#try reading from headers
			cookie = request_params.HEADERS("Cookie", None)
			if(cookie):
				cookie = cookie.decode()
				_temp = cookie.strip('\r\n').split("; ")
				vals = {}
				for i in _temp:
					vals.update(parse_qs_modified(str(i)))
					
				auth_key = vals.get("auth_key", None)
		
		if(auth_key):
			user_data = decode_signed_value("user_data", auth_key, max_age_days=10)
			#_type = user_data.get("type",None)
			if(user_data):
				kwargs["user"] = json.loads(user_data)
			
			else: # simple user_id
				user_id = decode_signed_value("user_key", auth_key, max_age_days=10)
				if(user_id):
					kwargs["user"] = {"user_id": user_id, "name": get_random_name(user_id)}
			
				
			
			
		return func(*args, **kwargs)
	
	new_func._original = getattr(func, "_original", func)
	return new_func


def get_internal_auth_key():
	return create_signed_value("user_data", json.dumps({"user_id": "internal", "name": "don"}))


def update_stats_on_main():
	errors = 0
	sleep(60) # initial sleep
	while(is_server_running()):
		try:
			req = get_data("http://" + main_server_addr + "/update_server_stats?auth_key=" + get_internal_auth_key().decode() + "&server_addr=" + self_server_addr, post=json.dumps({"load": 0.0}))
			if(not req or json.loads(req.read())["type"] != TYPE_OK):
				errors += 1
			sleep(5 * 60) # 5 minutes sleep
		except Exception as ex:
			LOG_WARN(LOG_TYPE_EXCEPTION, ex="could not update on main server")
			sleep(1 * 60) # 1 minutes sleep


#master server
#show all recently active sessions #lru based
@route_handler('^/get_sessions')
@decode_user
@need_user
def get_sessions(request_params, user=None):
	recent_sessions = public_sessions.cache.items()[:-100]
	return {"type": TYPE_SESSIONS, "sessions": [session_obj for session_id, session_obj in recent_sessions]}


#get individual session data
@route_handler('^/get_session/(.*)')
@decode_user
@need_user
def get_session(session_id, request_params, user=None):
	session_id = request_params.get("session_id", session_id)
	session = None
	if(session_id and session_id.startswith("private__")):
		session = private_sessions.cache.get(session_id, None) # returns empty string
	else:
		session = public_sessions.cache.get(session_id, "") # returns empty string
	
	if(session):
		return {"type": TYPE_SESSION , "session" : session}
	else:
		return create_new_session._original(request_params, user, session_id=session_id)
		
	return {"type": TYPE_ERROR, "error_message": "not found"} # empty body

#probably called internally between servers to main server
@route_handler('^/update_session_data')
@decode_user
@need_user
def update_session_data(session_id, request_params, user=None):
	if(user["user_id"] != "internal"):
		return {"type": TYPE_ERROR, "error_message": "not authorized"} # empty body
	if(session_id):
		public_sessions.set("session_id", json.loads(request_params.POST())) # will update this blindly
		return {"type": TYPE_OK, "message": "session updated"} # empty body
	
	return {"type": TYPE_ERROR, "error_message": "not found"} # empty body

@route_handler('^/send_session_message/(.*)')
@decode_user
@need_user
def send_session_message(session_id, request_params, user=None):
	if(user["user_id"] != "internal"):
		return {"type": TYPE_ERROR, "error_message": "not authorized"} # empty body
	
	session_obj = session_data.exists(session_id)
	message_data = json.loads(request_params.POST())
	if(session_obj):
		session_connections = session_obj["connections"]
		session_connections_temp = session_obj["viewer_connections"]
		
		session_messages = session_obj["messages"]
		session_messages.append(message_data)
		# broadcast to all other connections in this session
		message_data_str = json.dumps(message_data)
		for conn, temp in session_connections.cache.items():
			try:
				conn.send(message_data_str)
			except Exception as ex:
				pass
		for conn, temp in session_connections_temp.cache.items():
			try:
				conn.send(message_data_str)
			except Exception as ex:
				pass

	
		return {"type": TYPE_OK, "message": "sent"} # empty body
	
	else:
		#main server
		if(session_id and session_id.startswith("private__")):
			session = private_sessions.cache.get(session_id, None) # returns empty string
		else:
			session = public_sessions.cache.get(session_id, "") # returns empty string
		
		#call on the server and return the response
		return get_data("http://" + session["addr"] + "/send_message/" + session_id + "?auth_key=" + get_internal_auth_key().decode(), post=request_params.POST()).read()
		



#update load etc
@route_handler('^/update_server_stats')
@decode_user
@need_user
def update_server_stats(request_params, user=None):
	server_addr = urllib.request.unquote(request_params.get("server_addr", None))
	if(user["user_id"] == "internal"):
		server_info = json.loads(request_params.POST().decode())
		server_info["last_updated"] = cur_ms()
		servers.set(server_addr, server_info)
		return {"type": TYPE_OK, "message": "updated"} # empty body
	
	return {"type": TYPE_ERROR, "error_message": "not authorized"} # empty body


'''on the main server'''  
@route_handler('^/create_new_session')
@process_post_params
@decode_user
@need_user
def create_new_session(request_params, user=None, session_id=None):
	#create on some random server a
	description = request_params.get("description", None)
	image = request_params.get("image", None)
	is_public = request_params.get("is_public", "true")
	available_servers = []
	for i, server in servers.cache.items():
		if(server["load"] < 1 and server["last_updated"] > cur_ms() - 10 * 60 * 1000): # updates less than 10 minutes
			available_servers.append(i)
	
	addr = random.choice(available_servers)
	
	if(not addr):
		return {"type": TYPE_ERROR, "error_message": "no address"} # empty body
	
	if(session_id):
		pass
	elif(is_public == "true"):
		session_id = get_random_id(10)
	else:
		session_id = "private__" + get_random_id(10)
		
	session_obj = {"session_id": session_id, "description" : description, "image": image, "type": 0, "addr": addr, "user": user}
	
	#update on that server
	req = get_data("http://" + addr + "/after_new_session_created?auth_key=" + get_internal_auth_key().decode(), post=json.dumps(session_obj))
	if(not req or json.loads(req.read())["type"] != TYPE_OK):
		return {"type": TYPE_ERROR, "error_message": "not found"} # empty body
		
	
	if(is_public == "true"):
		public_sessions.set(session_id, session_obj)
	else:
		private_sessions.set(session_id, session_obj)
		
	return {"type": TYPE_SESSION , "session" : session_obj}
			

#all sessions that exists on this server
session_data = LRUCache(10000)

@route_handler('^/after_new_session_created')
@decode_user
@need_user
def after_new_session_created(request_params, user=None):
	if(user["user_id"] != "internal"):
		return {"type": TYPE_ERROR, "error_message": "invalid user"} # empty body
	
	session_obj = json.loads(request_params.POST().decode())
	session_id = session_obj["session_id"] 
	existing_session_obj = session_data.exists(session_id)
	if(not existing_session_obj):
		session_data.set(session_id , session_obj)
		#create session fields
		session_obj["connections"] = LRUCache(500)
		session_obj["viewer_connections"] = LRUCache(500)
		session_obj["messages"] = collections.deque(maxlen=100)
	return {"type": TYPE_OK, "message": "updated"} # empty body
	

@route_handler('^/create_or_update_user')
@decode_user
def create_or_update_user(request_params, user=None):
	user_name = request_params.get("name", None) or get_random_name()
	user_id = None
	if(user):
		user_id = user["user_id"]
	return {"type": TYPE_USER, "user_name": user_name , "auth_key": create_signed_value("user_data", json.dumps({"name": user_name, "user_id" : user_id or get_random_id(10)}))}

#web socket based connection
@route_handler('^/join_session/(.*)')
@decode_user
@need_user
def join_session(session_id, request_params, user=None):
	session_id = session_id or request_params.get("session_id", None)
	session_obj = session_data.exists(session_id)
	if(not session_obj):
		return {"type": TYPE_ERROR, "error_message": "not found"} # empty body

	user_id = user["user_id"]
	session_connections = session_obj["connections"]
	session_connections_temp = session_obj["viewer_connections"]
	
	session_messages = session_obj["messages"]

	ws = WebSocketServerHandler(request_params.HEADERS()) # sends handshake automatically 
	set_socket_options(request_params.sock)
	
	def delete_connection(ex):
		session_connections.delete(ws)
	
	def on_connected():
		ws.conn_obj = conn = WebsocketConnection(ws, user_id)
		conn.is_viewer_list = True
		popped_connections = session_connections_temp.set(conn, True)
		for c , b in popped_connections:
			c.ws.close()
		
		#send last 100 messages
		initial_messages = json.dumps(session_messages)
		msg_to_send = {"type": 101, "payload": initial_messages}
		conn.send(json.dumps(msg_to_send), add_to_assumend_sent=False)
		
		
		
	def on_timeout():
		if(ws.conn_obj):
			ws.conn_obj.send(json.dumps({"type": 0, "timestamp": int(time.time() * 1000)}))
			return True
		return False
	
	def on_message():
		try:
			message_data = json.loads(ws.data)
			if(message_data.get("type", None) == 1):
				if(ws.conn_obj.is_viewer_list):
					session_connections_temp.delete(ws.conn_obj)
					ws.conn_obj.is_viewer_list = False
					popped_connections = session_connections.set(ws.conn_obj, True)
					for c , b in popped_connections:
							c.ws.close()
				
				
				message_data["src_id"] = user_id # user_id
				message_data["timestamp"] = int(time.time() * 1000) # user_id
				
				session_messages.append(message_data)
				#broadcast to all other connections in this session
				message_data_str = json.dumps(message_data)
				for conn, temp in session_connections.cache.items():
					if(conn != ws.conn_obj):
						try:
							conn.send(message_data_str)
						except Exception as ex:
							pass
				for conn, temp in session_connections_temp.cache.items():
					if(conn!=ws.conn_obj):
						try:
							conn.send(message_data_str)
						except Exception as ex:
							pass
		except Exception as ex:
			LOG_WARN(LOG_TYPE_EXCEPTION, exception=str(ex))
	
	ws.on_close = delete_connection
	ws.on_connected = on_connected
	ws.on_message = on_message
	ws.on_timeout = on_timeout
	ws.do_handshake(request_params.HEADERS())
	ws.start_handling()
	return





if __name__ == "__main__":
	
	parser = argparse.ArgumentParser(description='process arguments')
	parser.add_argument('--port',
		help='host port specifically')
	
	parser.add_argument('--addr',
		help='host name')
	
	parser.add_argument('--main_addr',
		help='main server host name')
	
	
	
	args = parser.parse_args()
	
	port = 8081
	if(args.port):
		port = int(args.port)
	
	#TODO: get the public ip address somehow
	self_server_addr = (args.addr or socket.gethostbyname(socket.gethostname())) + ":" + str(port)
	
	main_server_addr = args.main_addr or self_server_addr
	
	#relevant on main server
	servers = LRUCache(100, [(self_server_addr, {"load": 0.0, "last_updated": cur_ms()})]) # {server_id : address , load: }
	

	
	gevent.spawn(update_stats_on_main)
	
	start_server(port, handlers=[])
