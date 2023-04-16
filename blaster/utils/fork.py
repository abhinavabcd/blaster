import os
import pickle
import sys
import time
import socket
from . import events
from gevent.threading import Thread
from ..tools import BufferedSocket

# MULTIPROCESS SERVER
_has_forked = False
_is_listening = True
BLASTER_FORK_ID = 0


# forks current process x num_process
# creates a connection between process using local unix socket
# can broadcast events to all process
# can call top level functions on master process
def blaster_fork(num_procs):
	# post load imports, because of config variables
	# GLOBALS
	global BLASTER_FORK_ID, _has_forked

	num_procs = min(num_procs, os.cpu_count())

	if(_has_forked):
		raise Exception("Cannot blaster_fork again")
	_has_forked = True

	BROADCASTER_PID = os.getpid()
	multiproc_broadcaster_sock_address = "/tmp/blaster-" + str(BROADCASTER_PID) + ".s"

	sock = None
	tracked_connections = []
	joinables = []

	# message types
	EVENT = 0
	FUNCTION_CALL = 1
	FUNCTION_CALL_RESPONSE = 1

	def create_message_to_send(data):
		data_to_send = pickle.dumps(data)
		data_len_bytes = len(data_to_send).to_bytes(4, byteorder=sys.byteorder)
		return data_len_bytes, data_to_send

	# starts reading for events
	def read_data_from_other_process(sock):
		CUR_PID = os.getpid()
		if(BROADCASTER_PID != CUR_PID):
			print("cloned process:reading from broadcaster process:", CUR_PID)
		else:
			print("broadcaster:reading from cloned process:", CUR_PID)

		while(_is_listening and (data_size_bytes:= sock.recvn(4))):
			data_size = int.from_bytes(data_size_bytes, byteorder=sys.byteorder)
			data_bytes = sock.recvn(data_size)
			data = pickle.loads(data_bytes)
			if(data["type"] == EVENT):
				events.broadcast_event(data["event"], *data["args"], **data["kwargs"])
				# rebroadcast to others
				if(BROADCASTER_PID == CUR_PID):
					# we are the central broadcaster process
					# broadcast to all clients
					for _sock in tracked_connections:
						_sock.sendl(data_bytes)  # locked send
			if(data["type"] == FUNCTION_CALL):
				if(BROADCASTER_PID == CUR_PID):
					# execute only on master
					ret = data["func"](*data["args"], **data["kwargs"])
					# send this to the sock
					sock.sendl(
						*create_message_to_send({
							"type": FUNCTION_CALL_RESPONSE, "hash": data["hash"], "ret": ret
						})
					)

		sock.close()
		(sock in tracked_connections) and tracked_connections.remove(sock)

	# create multiproc broadcaster
	def broadcast_event_multiproc(_id, *args, **kwargs):
		# send it to master process, which broadcasts to every
		# process including the current one
		msg_header, msg_payload = create_message_to_send(
			{"type": EVENT, "event": _id, "args": args, "kwargs": kwargs}
		)

		if(BROADCASTER_PID == os.getpid()):
			# we are the central broadcaster process
			# broadcast to all clients
			for _sock in tracked_connections:
				_sock.sendl(msg_header, msg_payload)
			# broadcast to current process
			events.broadcast_event(_id, *args, **kwargs)
		else:
			# send it to master process which
			# broadcast to everywhere else
			sock.sendl(msg_header, msg_payload)

	events.broadcast_event_multiproc = broadcast_event_multiproc

	@events.register_listener("blaster_exit1")
	def stop_broadcaster():
		global _is_listening
		_is_listening = False
		# send empty messages to break connections
		if(os.getpid() == BROADCASTER_PID):
			# this will close all pending read connections
			broadcast_event_multiproc(None, None)

		for _thread in joinables:
			print("Waiting on multiproc broadcaster thread to shutdown..")
			_thread.join()

	# start forking
	BLASTER_FORK_ID = 0
	for i in range(1, num_procs):
		pid = os.fork()
		if(pid == 0):
			# cloned process
			BLASTER_FORK_ID = i
			break
		# main process, contuing forking more
		print("Forked/Cloned...", pid)

	if(os.getpid() == BROADCASTER_PID):
		# create a socket and listen for events to broadcast
		sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
		sock.bind(multiproc_broadcaster_sock_address)
		sock.listen(2)

		# wait for other forked processes to connect to this broadcaster
		for i in range(num_procs - 1):
			client_sock, client_addr = sock.accept()
			client_sock = BufferedSocket(client_sock)
			# keep track
			tracked_connections.append(client_sock)
			joinables.append(_thread := Thread(target=read_data_from_other_process, args=(client_sock,)))
			_thread.start()

		sock.close()  # all child processes connected

	else:
		for i in range(3):
			try:
				# cloned process, connect to the BROADCASTER SOCKET
				sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
				sock.connect(multiproc_broadcaster_sock_address)
				joinables.append(_thread := Thread(target=read_data_from_other_process, args=(BufferedSocket(sock),)))
				_thread.start()
				break
			except Exception as ex:
				print("waiting to connect to broadcaster :", ex)
				time.sleep(0.1 * (i + 1))
