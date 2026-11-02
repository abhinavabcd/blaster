"""Bounded localhost HTTP/1.1 security regressions; failures expose server bugs.

Run: python -m unittest tests.test_http_adversarial -v
No expectedFailure markers: assertions describe the desired secure behavior.

Framing reference: https://www.rfc-editor.org/rfc/rfc9112.html#section-6.3
Ambiguous framing/obsolete folding use a strict rejection policy. The total
header deadline and inclusive body-size limit are explicit hardening/API
expectations, not RFC-mandated durations or sizes. Limits are reduced only to
keep resource probes small. Chunk rejection tests must be interpreted together
with valid chunk controls: rejecting every chunked body is not correctness.
"""
import blaster  # Apply gevent monkey patches before importing networking modules.
import json
import unittest
from contextlib import ExitStack
from unittest.mock import patch

import gevent
from gevent import socket
from blaster import server


TIMEOUT = 0.15
BODY_LIMIT = 256
HEADER_LIMIT = 1024


def request(headers=b'', body=b'', method=b'POST', path=b'/probe'):
    return method + b' ' + path + b' HTTP/1.1\r\nHost: localhost\r\n' + headers + b'\r\n' + body


SENTINEL = request(method=b'GET', path=b'/sentinel')


def responses(wire, methods=()):
    """Parse every response, checking lengths instead of searching for status text."""
    result = []
    while wire:
        head, sep, rest = wire.partition(b'\r\n\r\n')
        if not sep:
            raise AssertionError(f'Truncated response headers: {wire[:160]!r}')
        lines = head.split(b'\r\n')
        protocol, status, *_ = lines[0].split(b' ')
        if protocol != b'HTTP/1.1':
            raise AssertionError(f'Invalid status line: {lines[0]!r}')
        headers = {}
        for line in lines[1:]:
            key, value = line.split(b':', 1)
            key = key.lower()
            if key == b'content-length' and key in headers:
                raise AssertionError('Duplicate response Content-Length')
            headers[key] = value.strip()
        code = int(status)
        method = methods[len(result)] if len(result) < len(methods) else None
        size = 0 if method == 'HEAD' or code in (204, 304) or code < 200 else int(headers[b'content-length'])
        if len(rest) < size:
            raise AssertionError('Truncated response body')
        result.append((int(status), headers, rest[:size]))
        wire = rest[size:]
    return result


class HTTPFixture(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for name, value in [('HTTP_SOCKET_MAX_TIMEOUT', TIMEOUT),
                            ('HTTP_MAX_HEADERS_DATA_SIZE', HEADER_LIMIT),
                            ('IS_DEV', False), ('LOG_SERVER', lambda *a, **k: None),
                            ('LOG_ERROR', lambda *a, **k: None),
                            ('LOG_WARN', lambda *a, **k: None)]:
            self.stack.enter_context(patch.object(server, name, value))
        self.calls = []
        self.received_headers = []
        self.peer_ports = []
        self.app = server.App()

        @self.app.route('/probe', methods=['GET', 'POST', 'HEAD'], max_body_size=BODY_LIMIT)
        @self.app.route('/sentinel', methods=['GET'], max_body_size=BODY_LIMIT)
        def probe(req: server.Request):
            self.received_headers.append(dict(req._headers))
            self.peer_ports.append(req.sock.getpeername()[1])
            body = bytes(req._body_raw or b'')
            self.calls.append((req.path, body))
            return {'path': req.path, 'body': body.decode('ascii')}

        @self.app.route('/status/{code}')
        def status(code: int, req: server.Request):
            self.peer_ports.append(req.sock.getpeername()[1])
            return code, {}, 'representation'

        @self.app.route('/cookies')
        def cookies(req: server.Request):
            return req.COOKIES()

        @self.app.route('/framing/{kind}')
        def framing(kind: str):
            headers = {'content-length': '999', 'transfer-encoding': 'chunked'}
            if kind == 'list':
                headers = [f'{k}: {v}' for k, v in headers.items()]
            elif kind == 'bytes':
                headers = [f'{k}: {v}'.encode() for k, v in headers.items()]
            return headers, 'correct'

        self.app.start(0)
        self.addCleanup(self.app.stop)
        # App.start configures routes; StreamServer.start actually binds/listens.
        self.app.stream_server.address = ('127.0.0.1', 0)
        self.app.stream_server.family = socket.AF_INET
        self.app.stream_server.start()
        self.address = self.app.stream_server.address
        self.addCleanup(self.app.stream_server.pool.kill)

    def connect(self):
        sock = socket.create_connection(self.address, timeout=1)
        self.addCleanup(sock.close)
        sock.settimeout(1)
        return sock

    def drain(self, sock, methods=()):
        wire = bytearray()
        with gevent.Timeout(2, AssertionError('Server did not close within deadline')):
            while True:
                try:
                    data = sock.recv(4096)
                except ConnectionResetError:
                    break
                if not data:
                    break
                wire.extend(data)
                self.assertLess(len(wire), 128 * 1024, 'Unbounded response')
        return responses(bytes(wire), methods)

    def exchange(self, packet, fragments=None, methods=()):
        sock = self.connect()
        try:
            if fragments is None:
                sock.sendall(packet)
            else:
                for fragment in fragments:
                    sock.sendall(fragment)
                    gevent.sleep(0.001)
            sock.shutdown(socket.SHUT_WR)
            return self.drain(sock, methods)
        finally:
            sock.close()

    def assertHealthy(self):
        replies = self.exchange(request(method=b'GET'))
        self.assertEqual([r[0] for r in replies], [200])
        self.assertEqual(json.loads(replies[0][2])['path'], '/probe')

    def assertRejected(self, packet):
        replies = self.exchange(packet)
        observed = list(self.calls)
        self.assertHealthy()  # Also verify an independent connection after every attack.
        self.assertEqual(observed, [], f'Malformed request reached application: {observed!r}')
        self.assertTrue(all(r[0] >= 400 for r in replies), replies)


# Each attack includes a valid subsequent request: rejection must close the
# connection without dispatching that suffix. No proxy is assumed in this suite.
REJECT_CASES = {
    'conflicting_lengths': request(b'Content-Length: 5\r\nContent-Length: 0\r\n'),
    'conflicting_lengths_reverse': request(b'Content-Length: 0\r\nContent-Length: 5\r\n', b'x=123'),
    'mixed_case_duplicate_length': request(b'Content-Length: 5\r\ncOnTeNt-LeNgTh: 0\r\n'),
    'comma_conflicting_lengths': request(b'Content-Length: 1, 0\r\n'),
    'cl_te': request(b'Content-Length: 5\r\nTransfer-Encoding: chunked\r\n', b'0\r\n\r\n'),
    'te_cl': request(b'Transfer-Encoding: chunked\r\nContent-Length: 5\r\n', b'0\r\n\r\n'),
    'duplicate_te': request(b'Transfer-Encoding: chunked\r\nTransfer-Encoding: identity\r\n'),
    'te_not_final_chunked': request(b'Transfer-Encoding: chunked, gzip\r\n', b'0\r\n\r\n'),
    'te_unknown': request(b'Transfer-Encoding: identity\r\n'),
    'te_substring': request(b'Transfer-Encoding: xchunked\r\n', b'0\r\n\r\n'),
    'te_empty': request(b'Transfer-Encoding:\r\n'),
    'negative_length': request(b'Content-Length: -1\r\n'),
    'signed_length': request(b'Content-Length: +0\r\n'),
    'underscored_length': request(b'Content-Length: 0_0\r\n'),
    'hex_length': request(b'Content-Length: 0x0\r\n'),
    'empty_length': request(b'Content-Length:\r\n'),
    'huge_integer_length': request(b'Content-Length: ' + b'9' * 500 + b'\r\n'),
    'unicode_length': request('Content-Length: ٠\r\n'.encode()),
    'space_before_colon': request(b'Content-Length : 0\r\n'),
    'tab_before_colon': request(b'Content-Length\t: 0\r\n'),
    'folded_framing_header': request(b'X-Test: harmless\r\n Content-Length: 0\r\n'),
    'colonless_header': request(b'Content-Length 0\r\n'),
    'nul_header_name': request(b'Content\x00Length: 0\r\n'),
    'nul_header_value': request(b'X-Test: a\x00b\r\n'),
    'bare_cr_header_value': request(b'X-Test: a\rb\r\n'),
    'bare_lf_header_value': request(b'X-Test: a\nb\r\n'),
    'missing_host': b'GET /probe HTTP/1.1\r\n\r\n',
    'duplicate_host': request(b'Host: attacker.invalid\r\n'),
    'invalid_version': b'GET /probe NOTHTTP\r\nHost: localhost\r\n\r\n',
    'extra_request_token': b'GET /probe HTTP/1.1 EXTRA\r\nHost: localhost\r\n\r\n',
    'nul_request_target': request(path=b'/probe\x00'),
    'invalid_utf8_request': request(path=b'/probe\xff'),
    'oversized_request_line': request(path=b'/' + b'a' * 4096),
    'oversized_single_header': request(b'X-Large: ' + b'a' * HEADER_LIMIT + b'\r\n'),
    'oversized_aggregate_headers': request((b'X-A: ' + b'a' * 95 + b'\r\n') * 12),
    'oversized_body': request(b'Content-Length: 257\r\n', b'x' * 257),
}
for name, chunk in {
    'invalid_hex': b'G\r\nx\r\n0\r\n\r\n',
    'negative': b'-1\r\nx\r\n0\r\n\r\n',
    'signed': b'+1\r\nx\r\n0\r\n\r\n',
    'hex_prefix': b'0x1\r\nx\r\n0\r\n\r\n',
    'missing_data_crlf': b'1\r\nxZZ\r\n0\r\n\r\n',
    'too_large': b'101\r\n' + b'x' * 257 + b'\r\n0\r\n\r\n',
    'aggregate_too_large': (b'80\r\n' + b'x' * 128 + b'\r\n') * 3 + b'0\r\n\r\n',
    'overlong_extension': b'1;' + b'x' * 600 + b'\r\nx\r\n0\r\n\r\n',
    'aggregate_extensions': (b'1;k=' + b'a' * 60 + b'\r\nx\r\n') * 20 + b'0\r\n\r\n',
    'invalid_extension': b'1;foo="unterminated\r\nx\r\n0\r\n\r\n',
    'trailer_framing': b'0\r\nContent-Length: 0\r\n\r\n',
    'oversized_trailers': b'0\r\n' + (b'X-T: ' + b'a' * 95 + b'\r\n') * 12 + b'\r\n',
}.items():
    REJECT_CASES['chunk_' + name] = request(b'Transfer-Encoding: chunked\r\n', chunk)


class TestMalformedHTTP(HTTPFixture):
    pass


def rejection_test(packet):
    def test(self):
        self.assertRejected(packet + SENTINEL)
    return test


for name, packet in REJECT_CASES.items():
    setattr(TestMalformedHTTP, 'test_' + name, rejection_test(packet))


class TestHTTPControls(HTTPFixture):
    def test_get_and_pipeline(self):
        replies = self.exchange(request(method=b'GET') + SENTINEL)
        self.assertEqual([r[0] for r in replies], [200, 200])
        self.assertEqual(self.calls, [('/probe', b''), ('/sentinel', b'')])

    def test_content_length_pipeline(self):
        self.exchange(request(b'Content-Length: 5\r\n', b'x=123') + SENTINEL)
        self.assertEqual(self.calls, [('/probe', b'x=123'), ('/sentinel', b'')])

    def test_request_looking_body_stays_in_body(self):
        body = b'x=' + SENTINEL
        replies = self.exchange(request(
            f'Content-Length: {len(body)}\r\n'.encode(), body) + SENTINEL)
        self.assertEqual([r[0] for r in replies], [200, 200])
        self.assertEqual(self.calls, [('/probe', body), ('/sentinel', b'')])

    def test_leading_zero_length_and_optional_whitespace(self):
        replies = self.exchange(request(b'Content-Length:\t005 \t\r\n', b'x=123'))
        self.assertEqual([r[0] for r in replies], [200])
        self.assertEqual(self.calls, [('/probe', b'x=123')])

    def test_persistent_connection_without_client_eof(self):
        sock = self.connect()
        for path in (b'/probe', b'/sentinel'):
            sock.sendall(request(method=b'GET', path=path))
            wire = bytearray()
            # The client deliberately sends no EOF and waits for each response.
            while b'\r\n\r\n' not in wire:
                byte = sock.recv(1)
                self.assertTrue(byte, 'Unexpected EOF in persistent headers')
                wire.extend(byte)
            head, _, _ = wire.partition(b'\r\n\r\n')
            size = next(int(line.split(b':', 1)[1]) for line in head.split(b'\r\n')
                        if line.lower().startswith(b'content-length:'))
            for _ in range(size):
                byte = sock.recv(1)
                self.assertTrue(byte, 'Unexpected EOF in persistent response')
                wire.extend(byte)
            self.assertEqual(responses(bytes(wire))[0][0], 200)
        self.assertEqual(self.calls, [('/probe', b''), ('/sentinel', b'')])

    def test_every_two_fragment_boundary(self):
        packet = request(b'Content-Length: 5\r\n', b'x=123') + SENTINEL
        for split in range(1, len(packet)):
            with self.subTest(split=split):
                self.calls.clear()
                replies = self.exchange(packet, [packet[:split], packet[split:]])
                self.assertEqual([r[0] for r in replies], [200, 200])
                self.assertEqual(self.calls, [('/probe', b'x=123'), ('/sentinel', b'')])

    def test_fragmented_request(self):
        packet = request(b'Content-Length: 5\r\n', b'x=123')
        replies = self.exchange(packet, [bytes([b]) for b in packet])
        self.assertEqual([r[0] for r in replies], [200])
        self.assertEqual(self.calls, [('/probe', b'x=123')])

    def test_body_below_limit(self):
        body = b'x' * (BODY_LIMIT - 1)
        replies = self.exchange(request(b'Content-Length: 255\r\n', body))
        self.assertEqual([r[0] for r in replies], [200])
        self.assertEqual(self.calls, [('/probe', body)])

    def test_body_at_limit(self):
        replies = self.exchange(request(b'Content-Length: 256\r\n', b'x' * BODY_LIMIT))
        self.assertEqual([r[0] for r in replies], [200])

    def test_connection_close(self):
        replies = self.exchange(request(b'Connection: close\r\n') + SENTINEL)
        self.assertEqual([r[0] for r in replies], [200])
        self.assertEqual(self.calls, [('/probe', b'')])

    def test_connection_close_token_list(self):
        replies = self.exchange(request(b'Connection: keep-alive, Close\r\n') + SENTINEL)
        self.assertEqual([r[0] for r in replies], [200])
        self.assertEqual(self.calls, [('/probe', b'')])

    def test_bad_length_returns_client_error(self):
        replies = self.exchange(request(b'Content-Length: invalid\r\n'))
        self.assertEqual(len(replies), 1)
        self.assertEqual(replies[0][0], 400)


def chunk_control(header, body, expected):
    def test(self):
        replies = self.exchange(request(header, body) + SENTINEL)
        self.assertEqual([r[0] for r in replies], [200, 200])
        self.assertEqual(self.calls, [('/probe', expected), ('/sentinel', b'')])
    return test


for name, encoding, body, expected in [
    ('empty', b'chunked', b'0\r\n\r\n', b''),
    ('basic', b'chunked', b'3\r\nx=1\r\n0\r\n\r\n', b'x=1'),
    ('lower_hex', b'chunked', b'a\r\n0123456789\r\n0\r\n\r\n', b'0123456789'),
    ('upper_hex', b'chunked', b'A\r\n0123456789\r\n0\r\n\r\n', b'0123456789'),
    ('extension', b'chunked', b'3;foo=bar\r\nx=1\r\n0\r\n\r\n', b'x=1'),
    ('trailers', b'chunked', b'3\r\nx=1\r\n0\r\nX-Checksum: abc\r\n\r\n', b'x=1'),
    ('case_insensitive', b'Chunked', b'3\r\nx=1\r\n0\r\n\r\n', b'x=1'),
]:
    setattr(TestHTTPControls, 'test_chunk_' + name,
            chunk_control(b'Transfer-Encoding: ' + encoding + b'\r\n', body, expected))


class TestIncompleteAndSlowClients(HTTPFixture):
    def test_missing_chunk_terminator(self):
        self.assertRejected(request(b'Transfer-Encoding: chunked\r\n', b'1\r\nx\r\n'))

    def test_missing_trailer_terminator(self):
        self.assertRejected(request(b'Transfer-Encoding: chunked\r\n', b'0\r\nX-T: a\r\n'))

    def test_truncated_fixed_body(self):
        self.assertRejected(request(b'Content-Length: 20\r\n', b'x=1'))

    def test_missing_fixed_body(self):
        self.assertRejected(request(b'Content-Length: 20\r\n'))

    def test_unterminated_headers(self):
        self.assertRejected(b'GET /probe HTTP/1.1\r\nHost: localhost\r\n')

    def test_truncated_chunk(self):
        self.assertRejected(request(b'Transfer-Encoding: chunked\r\n', b'10\r\nx'))

    def test_idle_header_timeout(self):
        sock = self.connect()
        sock.sendall(b'GET /probe HTTP/1.1\r\nHost:')
        self.drain(sock)
        self.assertEqual(self.calls, [])
        self.assertHealthy()

    def test_idle_body_timeout(self):
        sock = self.connect()
        sock.sendall(request(b'Content-Length: 20\r\n', b'x'))
        self.drain(sock)
        self.assertEqual(self.calls, [])
        self.assertHealthy()

    def test_concurrent_slow_headers_allow_healthy_client(self):
        sockets = [self.connect() for _ in range(24)]
        for sock in sockets:
            sock.sendall(b'GET /probe HTTP/1.1\r\nHost:')
        self.assertHealthy()
        for sock in sockets:
            self.drain(sock)
        self.assertEqual(self.calls, [('/probe', b'')])

    def test_slow_drip_has_total_header_deadline(self):
        # Hardening policy, not an RFC-mandated duration: a request should have
        # a total deadline as well as a per-recv idle timeout. Bound the probe.
        sock = self.connect()
        sock.sendall(b'GET /probe HTTP/1.1\r\nX-Slow: ')
        for _ in range(8):
            gevent.sleep(TIMEOUT / 3)
            try:
                sock.sendall(b'a')
            except (BrokenPipeError, ConnectionResetError):
                break
        gevent.sleep(0.01)
        sock.settimeout(0.02)
        try:
            data = sock.recv(4096)
        except socket.timeout:
            self.fail('Header drip kept connection alive beyond 2x idle timeout')
        except ConnectionResetError:
            data = b''
        self.assertFalse(data.startswith(b'HTTP/1.1 200'))
        self.assertEqual(self.calls, [])


class TestProxyHTTPCompatibility(HTTPFixture):
    def test_header_wire_limit_includes_crlfs(self):
        # Host line = 17 bytes; X-Pad prefix+CRLF = 9; final CRLF = 2.
        for excess in (0, 1):
            with self.subTest(excess=excess):
                self.calls.clear()
                packet = request(b'X-Pad: ' + b'a' * (HEADER_LIMIT - 28 + excess) + b'\r\n')
                if excess:
                    replies = self.exchange(packet + SENTINEL)
                    self.assertEqual([r[0] for r in replies], [431])
                    self.assertEqual(self.calls, [])
                else:
                    self.assertEqual([r[0] for r in self.exchange(packet)], [200])

    def test_chunk_body_at_limit(self):
        body = b'x' * BODY_LIMIT
        packet = request(b'Transfer-Encoding: chunked\r\n', b'100\r\n' + body + b'\r\n0\r\n\r\n')
        self.assertEqual([r[0] for r in self.exchange(packet + SENTINEL)], [200, 200])
        self.assertEqual(self.calls, [('/probe', body), ('/sentinel', b'')])

    def test_fragmented_chunks_extensions_and_trailers(self):
        packet = request(b'transfer-encoding: Chunked\r\nte: trailers\r\n',
                         b'1 ; a = "quoted;\\\"value"\r\nx\r\n2\r\n=1\r\n0\r\nX-T: a\r\n\r\n') + SENTINEL
        replies = self.exchange(packet, [bytes([b]) for b in packet])
        self.assertEqual([r[0] for r in replies], [200, 200])
        self.assertEqual(self.calls, [('/probe', b'x=1'), ('/sentinel', b'')])

    def test_identical_duplicate_lengths(self):
        replies = self.exchange(request(b'Content-Length: 5\r\ncontent-length: 5\r\n', b'x=123') + SENTINEL)
        self.assertEqual([r[0] for r in replies], [200, 200])

    def test_split_cookies(self):
        replies = self.exchange(request(b'cookie: a=1\r\nCookie: b=2\r\n', path=b'/cookies'))
        self.assertEqual(json.loads(replies[0][2]), {'a': '1', 'b': '2'})

    def test_head_then_get(self):
        replies = self.exchange(request(method=b'HEAD') + SENTINEL, methods=('HEAD', 'GET'))
        self.assertEqual([r[0] for r in replies], [200, 200])
        self.assertEqual(replies[0][2], b'')
        get_reply = self.exchange(request(method=b'GET'))[0]
        self.assertEqual(replies[0][1][b'content-length'], get_reply[1][b'content-length'])

    def test_head_error_has_no_body(self):
        replies = self.exchange(request(b'Content-Length: invalid\r\n', method=b'HEAD'), methods=('HEAD',))
        self.assertEqual([r[0] for r in replies], [400])
        self.assertEqual(replies[0][2], b'')

    def test_bodyless_status_then_get(self):
        for status in (204, 304):
            with self.subTest(status=status):
                replies = self.exchange(request(path=f'/status/{status}'.encode()) + SENTINEL)
                self.assertEqual([r[0] for r in replies], [status, 200])
                self.assertEqual(replies[0][2], b'')
                self.assertNotIn(b'content-length', replies[0][1])

    def test_server_owns_buffered_response_framing(self):
        for kind in ('dict', 'list', 'bytes'):
            with self.subTest(kind=kind):
                replies = self.exchange(request(path=f'/framing/{kind}'.encode()) + SENTINEL)
                self.assertEqual([r[0] for r in replies], [200, 200])
                self.assertEqual(replies[0][2], b'correct')
                self.assertNotIn(b'transfer-encoding', replies[0][1])

    def test_http10_upstream_closes_by_default(self):
        packet = request(method=b'GET').replace(b'HTTP/1.1', b'HTTP/1.0')
        replies = self.exchange(packet + SENTINEL)
        self.assertEqual([r[0] for r in replies], [200])

    def test_expect_continue_before_upload(self):
        for header, body in [(b'Content-Length: 3\r\n', b'x=1'),
                             (b'Transfer-Encoding: chunked\r\n', b'3\r\nx=1\r\n0\r\n\r\n')]:
            with self.subTest(header=header):
                sock = self.connect()
                sock.sendall(request(header + b'Expect: 100-continue\r\n'))
                wire = bytearray()
                while not wire.endswith(b'\r\n\r\n'):
                    byte = sock.recv(1)
                    self.assertTrue(byte)
                    wire.extend(byte)
                self.assertEqual(wire, b'HTTP/1.1 100 Continue\r\n\r\n')
                sock.sendall(body + SENTINEL)
                sock.shutdown(socket.SHUT_WR)
                self.assertEqual([r[0] for r in self.drain(sock)], [200, 200])

    def test_unsupported_expectation(self):
        replies = self.exchange(request(b'Expect: something-else\r\n'))
        self.assertEqual([r[0] for r in replies], [417])
        self.assertEqual(self.calls, [])


if __name__ == '__main__':
    unittest.main()
