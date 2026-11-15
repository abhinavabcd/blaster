"""Wire-level and cooperative I/O checks for the minimal WebSocket transport."""
import base64
import unittest

import gevent
from gevent import socket

from blaster.websocket import Websocket


class TestWebsocket(unittest.TestCase):
    def setUp(self):
        self.left, self.right = socket.socketpair()
        self.addCleanup(self.left.close)
        self.addCleanup(self.right.close)
        self.client = Websocket(self.left)
        self.client._server = False
        self.server = Websocket(self.right)
        self.left.settimeout(1)
        self.right.settimeout(1)

    def test_text_binary_and_lengths(self):
        for data in ('hello ☃', '', b'', b'\x00\xff', b'a' * 126, b'b' * 65536):
            sender = gevent.spawn(self.client.send, data)
            self.assertEqual(self.server.recv(), data)
            sender.get(timeout=2)
            sender = gevent.spawn(self.server.send, data)
            self.assertEqual(self.client.recv(), data)
            sender.get(timeout=2)

    def test_fragmentation_and_ping(self):
        self.right.sendall(b'\x01\x02\xe2\x98\x89\x01p\x80\x01\x83')
        self.assertEqual(self.client.recv(), '☃')
        self.server.send('after pong')
        self.assertEqual(self.client.recv(), 'after pong')
        # A masked pong was sent back with the ping payload.
        frame = self.right.recv(100)
        self.assertEqual(frame[:2], b'\x8a\x81')
        self.assertEqual(frame[6] ^ frame[2], ord('p'))

    def test_timeout_preserves_partial_frame_and_fragments(self):
        self.left.settimeout(0.01)
        self.right.sendall(b'\x01\x01a\x80\x03b')
        with self.assertRaises(TimeoutError):
            self.client.recv()
        self.right.sendall(b'cd')
        self.assertEqual(self.client.recv(), 'abcd')

    def test_receive_yields(self):
        receiver = gevent.spawn(self.client.recv)
        gevent.sleep(0)
        self.assertFalse(receiver.ready())
        self.server.send('awake')
        self.assertEqual(receiver.get(timeout=1), 'awake')

    def test_send_backpressure_yields(self):
        self.left.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        payload = b'x' * (256 * 1024)
        sender = gevent.spawn(self.client.send, payload)
        self.addCleanup(sender.kill)
        gevent.sleep(0.02)
        # The peer has not read yet: send must suspend without blocking this greenlet.
        self.assertFalse(sender.ready(), 'send did not wait for socket backpressure')
        self.assertEqual(self.server.recv(), payload)
        sender.get(timeout=2)

    def test_concurrent_senders_serialize_frames(self):
        self.left.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        messages = [str(i) * 65536 for i in range(4)]
        senders = [gevent.spawn(self.client.send, message) for message in messages]
        for sender in senders:
            self.addCleanup(sender.kill)
        received = [self.server.recv() for _ in messages]
        for sender in senders:
            sender.get(timeout=2)
        self.assertCountEqual(received, messages)

    def test_receive_timeout_yields_and_connection_remains_usable(self):
        self.left.settimeout(0.05)
        def receive():
            try:
                self.client.recv()
            except TimeoutError as exc:
                return exc

        receiver = gevent.spawn(receive)
        self.addCleanup(receiver.kill)
        gevent.sleep(0.01)
        self.assertFalse(receiver.ready(), 'recv timeout blocked the gevent hub')
        self.assertIsInstance(receiver.get(timeout=1), TimeoutError)
        self.server.send('after timeout')
        self.assertEqual(self.client.recv(), 'after timeout')

    def test_close(self):
        self.client.close(reason='done')
        self.assertIsNone(self.server.recv())
        self.assertTrue(self.server.closed)
        self.client.close()
        with self.assertRaises(ConnectionError):
            self.client.send('closed')

    def test_invalid_frames(self):
        for frame in (b'\xc1\x00', b'\x80\x00', b'\x09\x00', b'\x81\x80' + b'1234',
                      b'\x82\x7e\x00\x01a', b'\x88\x01a', b'\x81\x01\xff'):
            with self.subTest(frame=frame):
                left, right = socket.socketpair()
                ws = Websocket(left)
                ws._server = False
                try:
                    right.sendall(frame)
                    with self.assertRaises(ValueError):
                        ws.recv()
                    self.assertTrue(ws.closed)
                finally:
                    left.close()
                    right.close()

    def test_message_size_limit(self):
        self.client.max_size = 2
        self.right.sendall(b'\x82\x03')
        with self.assertRaises(ValueError):
            self.client.recv()

    def test_client_and_server_upgrade(self):
        listener = socket.socket()
        listener.bind(('127.0.0.1', 0))
        listener.listen(1)
        self.addCleanup(listener.close)

        def serve():
            conn, _ = listener.accept()
            conn.settimeout(1)
            ws = Websocket(conn)
            try:
                request = bytearray()
                while b'\r\n\r\n' not in request:
                    request.extend(conn.recv(4096))
                lines = request.decode().split('\r\n')
                self.assertEqual(lines[0], 'GET /echo?q=1 HTTP/1.1')
                headers = dict(line.split(': ', 1) for line in lines[1:] if line)
                ws.do_handshake(headers)
                ws.send(ws.recv())
                ws.recv()
            finally:
                ws.close()

        worker = gevent.spawn(serve)
        ws = Websocket().connect(f'ws://127.0.0.1:{listener.getsockname()[1]}/echo?q=1', timeout=1)
        try:
            ws.send('echo')
            self.assertEqual(ws.recv(), 'echo')
        finally:
            ws.close()
        worker.get(timeout=2)

    def test_invalid_upgrade_request(self):
        with self.assertRaises(ValueError):
            self.server.do_handshake({'Sec-WebSocket-Key': base64.b64encode(b'a' * 16).decode()})


if __name__ == '__main__':
    unittest.main()
