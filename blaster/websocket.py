"""Minimal synchronous WebSocket transport using cooperative gevent I/O."""

import base64
import hashlib
import os
import struct
from urllib.parse import urlsplit

from gevent import socket, ssl
from gevent.lock import Semaphore


class Websocket:
    """Use connect(url) as a client, or pass a socket and call do_handshake(headers).

    send() accepts str or bytes; recv() returns str, bytes, or None on close.
    Socket timeouts propagate as TimeoutError. One greenlet may receive while
    others send; concurrent receives are serialized. No compression or proxies.
    """

    def __init__(self, sock=None, *, max_size=32 * 1024 * 1024):
        self.sock = sock
        self.closed = sock is None
        self._server = sock is not None
        self._buffer = bytearray()
        self._fragments = bytearray()
        self._opcode = None
        self._send_lock = Semaphore()
        self._recv_lock = Semaphore()
        self.max_size = max_size
        self._data = None  # arbitrary user data

    def connect(self, url, timeout=None, headers=None):
        """Connect to ws:// or wss://, validating the HTTP upgrade response."""
        if not self.closed:
            raise RuntimeError('Already connected')
        parts = urlsplit(url)
        if parts.scheme not in ('ws', 'wss') or not parts.hostname or parts.fragment:
            raise ValueError('Expected a ws:// or wss:// URL without a fragment')
        self.sock = socket.create_connection(
            (parts.hostname, parts.port or (443 if parts.scheme == 'wss' else 80)), timeout)
        self.closed = False
        self._server = False
        self._buffer.clear()
        self._fragments.clear()
        self._opcode = None
        try:
            if parts.scheme == 'wss':
                self.sock = ssl.create_default_context().wrap_socket(
                    self.sock, server_hostname=parts.hostname)
            key = base64.b64encode(os.urandom(16)).decode('ascii')
            target = (parts.path or '/') + ('?' + parts.query if parts.query else '')
            host = parts.hostname
            if ':' in host:
                host = '[' + host + ']'
            if parts.port:
                host += ':' + str(parts.port)
            fields = {'Host': host, 'Upgrade': 'websocket', 'Connection': 'Upgrade',
                      'Sec-WebSocket-Key': key, 'Sec-WebSocket-Version': '13'}
            fields.update(headers or {})
            lines = [f'GET {target} HTTP/1.1'] + [f'{k}: {v}' for k, v in fields.items()]
            if any('\r' in line or '\n' in line for line in lines):
                raise ValueError('Invalid HTTP header')
            self._write(('\r\n'.join(lines) + '\r\n\r\n').encode('ascii'))
            while b'\r\n\r\n' not in self._buffer:
                if len(self._buffer) >= 65536:
                    raise ValueError('Upgrade headers too large')
                self._read(len(self._buffer) + 1)
            head, tail = self._buffer.split(b'\r\n\r\n', 1)
            self._buffer = bytearray(tail)
            lines = head.decode('iso-8859-1').split('\r\n')
            response = {k.lower(): v.strip() for k, v in
                        (line.split(':', 1) for line in lines[1:])}
            expected = self._accept(key)
            if (lines[0].split()[:2] != ['HTTP/1.1', '101']
                    or response.get('upgrade', '').lower() != 'websocket'
                    or 'upgrade' not in response.get('connection', '').lower().replace(' ', '').split(',')
                    or response.get('sec-websocket-accept', '').strip() != expected
                    or 'sec-websocket-extensions' in response
                    or 'sec-websocket-protocol' in response):
                raise ValueError('Invalid WebSocket upgrade response')
        except BaseException:
            self.sock.close()
            self.closed = True
            raise
        return self

    @staticmethod
    def _accept(key):
        return base64.b64encode(hashlib.sha1(
            (key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode('ascii')).digest()).decode('ascii')

    def do_handshake(self, headers):
        """Accept an HTTP upgrade whose headers have already been read."""
        headers = {k.lower(): v.strip() for k, v in headers.items()}
        key = headers.get('sec-websocket-key', '')
        if (headers.get('upgrade', '').lower() != 'websocket'
                or 'upgrade' not in headers.get('connection', '').lower().replace(' ', '').split(',')
                or headers.get('sec-websocket-version') != '13'
                or len(base64.b64decode(key, validate=True)) != 16):
            raise ValueError('Invalid WebSocket upgrade request')
        self._write(('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n'
                     'Connection: Upgrade\r\nSec-WebSocket-Accept: '
                     + self._accept(key) + '\r\n\r\n').encode('ascii'))

    def settimeout(self, timeout):
        getattr(self.sock, 'sock', self.sock).settimeout(timeout)

    def _write(self, data):
        while data:
            sent = self.sock.send(data)
            if not sent:
                raise ConnectionError('Socket closed while sending')
            data = data[sent:]

    def _read(self, size):
        # Keep partial frames buffered so recv() can resume after a timeout.
        while len(self._buffer) < size:
            data = self.sock.recv(max(4096, size - len(self._buffer)))
            if not data:
                raise ConnectionError('Socket closed without a close frame')
            self._buffer.extend(data)

    def _send(self, opcode, data):
        if self.closed:
            raise ConnectionError('WebSocket is closed')
        length = len(data)
        header = bytes([0x80 | opcode, (0 if self._server else 0x80) | min(length, 126)])
        if length >= 65536:
            header = header[:1] + bytes([127 | (0 if self._server else 0x80)]) + struct.pack('!Q', length)
        elif length >= 126:
            header += struct.pack('!H', length)
        if not self._server:
            mask = os.urandom(4)
            header += mask
            data = bytes(value ^ mask[i % 4] for i, value in enumerate(data))
        with self._send_lock:
            self._write(header + data)

    def send(self, data, is_text=None):
        """Send one complete message; strings use UTF-8 text frames."""
        opcode = 1 if isinstance(data, str) or is_text else 2
        self._send(opcode, data.encode('utf-8') if isinstance(data, str) else bytes(data))

    def ping(self, data=b''):
        data = data.encode('utf-8') if isinstance(data, str) else bytes(data)
        if len(data) > 125:
            raise ValueError('Ping payload exceeds 125 bytes')
        self._send(9, data)

    @staticmethod
    def _validate_close(data):
        if len(data) == 1:
            raise ValueError('Invalid close payload')
        if data:
            code = struct.unpack('!H', data[:2])[0]
            if code not in (1000, 1001, 1002, 1003, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014) and not 3000 <= code < 5000:
                raise ValueError('Invalid close code')
            data[2:].decode('utf-8')

    def recv(self):
        """Receive a complete message, answering ping and close frames internally."""
        with self._recv_lock:
            try:
                return self._recv()
            except TimeoutError:
                raise
            except (ValueError, ConnectionError, OSError):
                self.close(1002)
                raise

    def _recv(self):
        while not self.closed:
            self._read(2)
            first, second = self._buffer[:2]
            final, opcode, masked = bool(first & 128), first & 15, bool(second & 128)
            length, offset = second & 127, 2
            if first & 112 or opcode not in (0, 1, 2, 8, 9, 10) or masked != self._server:
                raise ValueError('Invalid WebSocket frame')
            if opcode >= 8 and (not final or length > 125):
                raise ValueError('Invalid control frame')
            if length >= 126:
                count = 2 if length == 126 else 8
                self._read(offset + count)
                length = int.from_bytes(self._buffer[offset:offset + count], 'big')
                if length < (126 if count == 2 else 65536) or length >= 1 << 63:
                    raise ValueError('Invalid frame length')
                offset += count
            if opcode < 8 and length + len(self._fragments) > self.max_size:
                raise ValueError('Message too large')
            payload_offset = offset + (4 if masked else 0)
            self._read(payload_offset + length)
            data = bytes(self._buffer[payload_offset:payload_offset + length])
            if masked:
                mask = self._buffer[offset:offset + 4]
                data = bytes(value ^ mask[i % 4] for i, value in enumerate(data))
            del self._buffer[:payload_offset + length]
            if opcode == 8:
                self._validate_close(data)
                try:
                    self._send(8, data)
                except OSError:
                    pass
                finally:
                    self.closed = True
                    self.sock.close()
                return None
            if opcode == 9:
                self._send(10, data)
            elif opcode == 10:
                continue
            else:
                if opcode:
                    if self._opcode is not None:
                        raise ValueError('Expected continuation frame')
                    self._opcode = opcode
                elif self._opcode is None:
                    raise ValueError('Unexpected continuation frame')
                self._fragments.extend(data)
                if final:
                    data = bytes(self._fragments)
                    opcode = self._opcode
                    self._fragments.clear()
                    self._opcode = None
                    return data.decode('utf-8') if opcode == 1 else data
        return None

    def close(self, status=1000, reason=''):
        """Send a close frame and release the socket; safe to call repeatedly."""
        if self.closed:
            return
        reason = reason.encode('utf-8') if isinstance(reason, str) else bytes(reason)
        data = struct.pack('!H', status) + reason
        self._validate_close(data)
        if len(data) > 125:
            raise ValueError('Close reason exceeds 123 bytes')
        try:
            self._send(8, data)
        except OSError:
            pass
        finally:
            self.closed = True
            self.sock.close()

    def start_handling(self):
        """Run the server callback loop; on_message reads the message from data."""
        error = None
        try:
            while not self.closed:
                try:
                    self.data = self.recv()
                except TimeoutError:
                    if self.on_timeout():
                        continue
                    raise
                if self.data is None:
                    break
                self.on_message()
        except Exception as exc:
            error = exc
        finally:
            self.close()
            self.on_close(error)

    def on_message(self):
        pass

    def on_close(self, error):
        pass

    def on_timeout(self):
        return False
