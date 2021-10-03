'''
Created on Jul 7, 2016

@author: abhinav
'''

import sys
import base64
import hashlib
import socket
import errno

import codecs
import struct

VER = sys.version_info[0]


_VALID_STATUS_CODES = [1000, 1001, 1002, 1003, 1007, 1008,
												1009, 1010, 1011, 3000, 3999, 4000, 4999]

HANDSHAKE_STR = (
	"HTTP/1.1 101 Switching Protocols\r\n"
	"Upgrade: WebSocket\r\n"
	"Connection: Upgrade\r\n"
	"Sec-WebSocket-Accept: %(acceptstr)s\r\n\r\n"
)

GUID_STR = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'

STREAM = 0x0
TEXT = 0x1
BINARY = 0x2
CLOSE = 0x8
PING = 0x9
PONG = 0xA

HEADERB1 = 1
HEADERB2 = 3
LENGTHSHORT = 4
LENGTHLONG = 5
MASK = 6
PAYLOAD = 7

MAXHEADER = 65536
MAXPAYLOAD = 33554432



class WebSocketServerHandler(object):

	def __init__(self, sock):
		self.client = sock
		self.handshaked = False

		self.fin = 0
		self.data = bytearray()
		self.opcode = 0
		self.hasmask = 0
		self.maskarray = None
		self.length = 0
		self.lengtharray = None
		self.index = 0
		self.request = None
		self.usingssl = False

		self.frag_start = False
		self.frag_type = BINARY
		self.frag_buffer = None
		self.frag_decoder = codecs.getincrementaldecoder('utf-8')(errors='strict')
		self.closed = False
		self.state = HEADERB1

		# restrict the size of header and payload for security reasons
		self.maxheader = MAXHEADER
		self.maxpayload = MAXPAYLOAD


		#additional fields convinience
		self.conn_obj = None
		
	def start_handling(self):
		while(True):
			try:
				self._handle_data()
			except (socket.error, Exception) as ex:
				self.on_close(ex)
				self.client.close()
				break
			except socket.timeout as ex:
					if(not self.on_timeout()):
						self.on_close(ex)
						self.client.close()
						break


	def on_timeout(self):
		return False

	def on_message(self):
		"""
				Called when websocket frame is received.
				To access the frame data call self.data.
				If the frame is Text then self.data is a unicode object.
				If the frame is Binary then self.data is a bytearray object.
		"""
		pass

	def on_connected(self):
		"""
				Called when a websocket client connects to the server.
		"""
		pass

	def on_close(self , ex):
		"""
				Called when a websocket server gets a Close frame from a client.
		"""
		pass

	def _handle_packet(self):
		if self.opcode == CLOSE:
			pass
		elif self.opcode == STREAM:
			pass
		elif self.opcode == TEXT:
			pass
		elif self.opcode == BINARY:
			pass
		elif self.opcode == PONG or self.opcode == PING:
			if len(self.data) > 125:
					raise Exception('control frame length can not be > 125')
		else:
				# unknown or reserved opcode so just close
			raise Exception('unknown opcode')

		if self.opcode == CLOSE:
			status = 1000
			reason = u''
			length = len(self.data)

			if length == 0:
					pass
			elif length >= 2:
					status = struct.unpack_from('!H', self.data[:2])[0]
					reason = self.data[2:]

					if status not in _VALID_STATUS_CODES:
							status = 1002

					if len(reason) > 0:
							try:
									reason = reason.decode('utf8', errors='strict')
							except Exception:
									status = 1002
			else:
					status = 1002

			self.close(status, reason)
			return

		elif self.fin == 0:
				if self.opcode != STREAM:
						if self.opcode == PING or self.opcode == PONG:
								raise Exception('control messages can not be fragmented')

						self.frag_type = self.opcode
						self.frag_start = True
						self.frag_decoder.reset()

						if self.frag_type == TEXT:
								self.frag_buffer = []
								utf_str = self.frag_decoder.decode(self.data, final=False)
								if utf_str:
										self.frag_buffer.append(utf_str)
						else:
								self.frag_buffer = bytearray()
								self.frag_buffer.extend(self.data)

				else:
						if self.frag_start is False:
								raise Exception('fragmentation protocol error')

						if self.frag_type == TEXT:
								utf_str = self.frag_decoder.decode(self.data, final=False)
								if utf_str:
										self.frag_buffer.append(utf_str)
						else:
								self.frag_buffer.extend(self.data)

		else:
				if self.opcode == STREAM:
						if self.frag_start is False:
								raise Exception('fragmentation protocol error')

						if self.frag_type == TEXT:
								utf_str = self.frag_decoder.decode(self.data, final=True)
								self.frag_buffer.append(utf_str)
								self.data = u''.join(self.frag_buffer)
						else:
								self.frag_buffer.extend(self.data)
								self.data = self.frag_buffer

						self.on_message()

						self.frag_decoder.reset()
						self.frag_type = BINARY
						self.frag_start = False
						self.frag_buffer = None

				elif self.opcode == PING:
						self._send_message(False, PONG, self.data)

				elif self.opcode == PONG:
						pass

				else:
						if self.frag_start is True:
								raise Exception('fragmentation protocol error')

						if self.opcode == TEXT:
								try:
										self.data = self.data.decode('utf8', errors='strict')
								except Exception as exp:
										raise Exception('invalid utf-8 payload')

						self.on_message()


	def do_handshake(self, headers):
		
		try:
			key = headers['Sec-WebSocket-Key'].strip()
			k = key + GUID_STR
			k_s = base64.b64encode(hashlib.sha1(k.encode()).digest()).decode('utf-8')
			hStr = HANDSHAKE_STR % {'acceptstr': k_s}
			self._send_buffer(hStr.encode('utf-8'))
			self.handshaked = True
			self.on_connected()
			
#          base64.b64encode(
#                 hashlib.sha1(key + self.GUID).digest())
		except Exception as e:
			raise Exception('handshake failed: %s', str(e))
	

	def _handle_data(self):
			# do the HTTP header and handshake
			data = self.client.recv(8192)
			if not data:
					raise Exception("remote socket closed")

			if VER >= 3:
					for d in data:
							self._parse_message(d)
			else:
					for d in data:
							self._parse_message(ord(d))

	def close(self, status=1000, reason=u''):
		"""
				Send Close frame to the client. The underlying socket is only closed
				when the client acknowledges the Close frame.
				status is the closing identifier.
				reason is the reason for the close.
			"""
		try:
				if self.closed is False:
					close_msg = bytearray()
					close_msg.extend(struct.pack("!H", status))
					if isinstance(reason, str):
							close_msg.extend(reason.encode('utf-8'))
					else:
							close_msg.extend(reason)

					self._send_message(False, CLOSE, close_msg)

		finally:
					self.closed = True


	def _send_buffer(self, buff):
		size = len(buff)
		tosend = size
		already_sent = 0

		while tosend > 0:
			try:
					# i should be able to send a bytearray
					sent = self.client.send(buff[already_sent:])
					if sent == 0:
						raise RuntimeError('socket connection broken')

					already_sent += sent
					tosend -= sent

			except socket.error as e:
					# if we have full buffers then wait for them to drain and try again
					if e.errno in [errno.EAGAIN, errno.EWOULDBLOCK]:
						self._send_buffer(buff[already_sent:])
					else:
						raise e

		return None

	def send_fragment_start(self, data):
		"""
				Send the start of a data fragment stream to a websocket client.
				Subsequent data should be sent using send_fragment().
				A fragment stream is completed when send_fragment_end() is called.
				If data is a unicode object then the frame is sent as Text.
				If the data is a bytearray object then the frame is sent as Binary.
		"""
		opcode = BINARY
		if isinstance(data, str):
			opcode = TEXT
		self._send_message(True, opcode, data)

	def send_fragment(self, data):
		"""
				see send_fragment_start()
				If data is a unicode object then the frame is sent as Text.
				If the data is a bytearray object then the frame is sent as Binary.
		"""
		self._send_message(True, STREAM, data)

	def send_fragment_end(self, data):
		"""
				see send_fragment_end()
				If data is a unicode object then the frame is sent as Text.
				If the data is a bytearray object then the frame is sent as Binary.
		"""
		self._send_message(False, STREAM, data)

	def send(self, data, is_text=True):
		"""
				Send websocket data frame to the client.
				If data is a unicode object then the frame is sent as Text.
				If the data is a bytearray object then the frame is sent as Binary.
		"""
		opcode = BINARY
		if is_text or isinstance(data, str):
			opcode = TEXT
		self._send_message(False, opcode, data)


	def _send_message(self, fin, opcode, data):

		payload = bytearray()

		b1 = 0
		b2 = 0
		if fin is False:
			b1 |= 0x80
		b1 |= opcode

		if isinstance(data, str):
			data = data.encode('utf-8')

		length = len(data)
		payload.append(b1)

		if length <= 125:
			b2 |= length
			payload.append(b2)

		elif length >= 126 and length <= 65535:
			b2 |= 126
			payload.append(b2)
			payload.extend(struct.pack("!H", length))

		else:
			b2 |= 127
			payload.append(b2)
			payload.extend(struct.pack("!Q", length))

		if length > 0:
			payload.extend(data)

		self._send_buffer(payload)


	def _parse_message(self, byte):
		# read in the header
		if self.state == HEADERB1:

			self.fin = byte & 0x80
			self.opcode = byte & 0x0F
			self.state = HEADERB2

			self.index = 0
			self.length = 0
			self.lengtharray = bytearray()
			self.data = bytearray()

			rsv = byte & 0x70
			if rsv != 0:
					raise Exception('RSV bit must be 0')

		elif self.state == HEADERB2:
			mask = byte & 0x80
			length = byte & 0x7F

			if self.opcode == PING and length > 125:
					raise Exception('ping packet is too large')

			if mask == 128:
					self.hasmask = True
			else:
					self.hasmask = False

			if length <= 125:
					self.length = length

					# if we have a mask we must read it
					if self.hasmask is True:
						self.maskarray = bytearray()
						self.state = MASK
					else:
						# if there is no mask and no payload we are done
						if self.length <= 0:
								try:
									self._handle_packet()
								finally:
									self.state = self.HEADERB1
									self.data = bytearray()

						# we have no mask and some payload
						else:
								# self.index = 0
								self.data = bytearray()
								self.state = PAYLOAD

			elif length == 126:
					self.lengtharray = bytearray()
					self.state = LENGTHSHORT

			elif length == 127:
					self.lengtharray = bytearray()
					self.state = LENGTHLONG


		elif self.state == LENGTHSHORT:
			self.lengtharray.append(byte)

			if len(self.lengtharray) > 2:
					raise Exception('short length exceeded allowable size')

			if len(self.lengtharray) == 2:
					self.length = struct.unpack_from('!H', self.lengtharray)[0]

					if self.hasmask is True:
						self.maskarray = bytearray()
						self.state = MASK
					else:
						# if there is no mask and no payload we are done
						if self.length <= 0:
								try:
									self._handle_packet()
								finally:
									self.state = HEADERB1
									self.data = bytearray()

						# we have no mask and some payload
						else:
								# self.index = 0
								self.data = bytearray()
								self.state = PAYLOAD

		elif self.state == LENGTHLONG:

			self.lengtharray.append(byte)

			if len(self.lengtharray) > 8:
					raise Exception('long length exceeded allowable size')

			if len(self.lengtharray) == 8:
					self.length = struct.unpack_from('!Q', self.lengtharray)[0]

					if self.hasmask is True:
						self.maskarray = bytearray()
						self.state = MASK
					else:
						# if there is no mask and no payload we are done
						if self.length <= 0:
								try:
									self._handle_packet()
								finally:
									self.state = HEADERB1
									self.data = bytearray()

						# we have no mask and some payload
						else:
								# self.index = 0
								self.data = bytearray()
								self.state = PAYLOAD

		# MASK STATE
		elif self.state == MASK:
			self.maskarray.append(byte)

			if len(self.maskarray) > 4:
					raise Exception('mask exceeded allowable size')

			if len(self.maskarray) == 4:
					# if there is no mask and no payload we are done
					if self.length <= 0:
						try:
								self._handle_packet()
						finally:
								self.state = HEADERB1
								self.data = bytearray()

					# we have no mask and some payload
					else:
						# self.index = 0
						self.data = bytearray()
						self.state = PAYLOAD

		# PAYLOAD STATE
		elif self.state == PAYLOAD:
			if self.hasmask is True:
					self.data.append(byte ^ self.maskarray[self.index % 4])
			else:
					self.data.append(byte)

			# if length exceeds allowable size then we except and remove the connection
			if len(self.data) >= self.maxpayload:
					raise Exception('payload exceeded allowable size')

			# check if we have processed length bytes; if so we are done
			if (self.index + 1) == self.length:
					try:
						self._handle_packet()
					finally:
						# self.index = 0
						self.state = HEADERB1
						self.data = bytearray()
			else:
					self.index += 1
