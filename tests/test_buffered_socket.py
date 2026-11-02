"""Delimiter boundaries, fragmentation, EOF, and read-buffer interoperability."""
import blaster
import random
import unittest
from collections import deque

from blaster.tools import BufferedSocket


class FragmentSocket:
    def __init__(self, fragments):
        self.fragments = deque(fragments)
        self.reads = 0

    def recv(self, size):
        self.reads += 1
        return self.fragments.popleft() if self.fragments else b''


class TestBufferedSocket(unittest.TestCase):
    def test_all_delimiter_boundaries(self):
        for delimiter in (b'\r\n', b'abab', b'!', b'END-END'):
            wire = b'payload' + delimiter + b'tail'
            for split in range(1, len(wire)):
                for discard in (False, True):
                    with self.subTest(delimiter=delimiter, split=split, discard=discard):
                        sock = BufferedSocket(FragmentSocket([wire[:split], wire[split:]]))
                        self.assertEqual(sock.readuntil(delimiter, 100, discard),
                                         b'payload' if discard else b'payload' + delimiter)
                        self.assertEqual(sock.recvn(4), b'tail')

    def test_exact_limit_and_one_over(self):
        for prebuffered in (False, True):
            for limit in (-1, 0, 1, 4, 5, 6):
                with self.subTest(prebuffered=prebuffered, limit=limit):
                    wire = b'abc\r\ntail'
                    transport = FragmentSocket([] if prebuffered else [wire])
                    sock = BufferedSocket(transport)
                    if prebuffered:
                        sock.readbuf.extend(wire)
                    result = sock.readuntil('\r\n', limit, True)
                    if limit >= 5:
                        self.assertEqual(result, b'abc')
                        self.assertEqual(sock.recvn(4), b'tail')
                    else:
                        self.assertIsNone(result)
                        self.assertTrue(sock.is_eof)

    def test_eof_never_returns_partial_line(self):
        for wire in (b'', b'abc', b'abc\r'):
            with self.subTest(wire=wire):
                sock = BufferedSocket(FragmentSocket([wire] if wire else []))
                self.assertIsNone(sock.readuntil(b'\r\n', 100, True))
                self.assertTrue(sock.is_eof)

    def test_buffered_delimiter_needs_no_recv(self):
        transport = FragmentSocket([])
        sock = BufferedSocket(transport)
        sock.readbuf.extend(b'one\r\ntwo\r\n')
        self.assertEqual(sock.readuntil(b'\r\n', 5, True), b'one')
        self.assertEqual(sock.readuntil(b'\r\n', 5, False), b'two\r\n')
        self.assertEqual(transport.reads, 0)

    def test_limit_stops_reading(self):
        transport = FragmentSocket([b'x' * 16] * 100)
        sock = BufferedSocket(transport)
        self.assertIsNone(sock.readuntil(b'\r\n', 32, True))
        self.assertEqual(transport.reads, 2)

    def test_empty_delimiter_is_invalid(self):
        with self.assertRaises(ValueError):
            BufferedSocket(FragmentSocket([])).readuntil(b'', 100, True)

    def test_seeded_fragmented_sequences(self):
        rng = random.Random(1842)
        for trial in range(200):
            parts = [bytes(rng.choices(b'abcdef', k=rng.randrange(40))) for _ in range(8)]
            wire = b'\r\n'.join(parts) + b'\r\n'
            fragments = []
            while wire:
                size = rng.randrange(1, 16)
                fragments.append(wire[:size])
                wire = wire[size:]
            sock = BufferedSocket(FragmentSocket(fragments))
            for part in parts:
                with self.subTest(trial=trial, part=part):
                    self.assertEqual(sock.readuntil(b'\r\n', len(part) + 2, True), part)
            self.assertIsNone(sock.readuntil(b'\r\n', 100, True))


if __name__ == '__main__':
    unittest.main()
