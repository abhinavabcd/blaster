"""Reproducible CPU-only HTTP parser/dispatcher profile (not a throughput test).

Run from the repository root:
    python -m examples.profile_http --requests 3000 --routes 1
    python -m examples.profile_http --requests 3000 --routes 1000

No listener is opened. Application setup and route compilation are not timed.
Logging is disabled to isolate parsing/dispatch; transport is an in-memory stream.
"""
import blaster
import argparse
import cProfile
import io
import pstats
import time
from contextlib import ExitStack
from unittest.mock import patch

from blaster import server
from blaster.tools import BufferedSocket


class MemorySocket:
    def __init__(self, packet):
        self.stream = io.BytesIO(packet)
        self.sent = 0

    def recv(self, size):
        return self.stream.read(size)

    def sendall(self, data):
        self.sent += len(data)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--requests', type=int, default=3000)
    parser.add_argument('--routes', type=int, default=1)
    parser.add_argument('--output', help='Optional cProfile .pstats destination')
    args = parser.parse_args()
    if args.requests < 1 or args.routes < 1:
        parser.error('requests and routes must be positive')

    with ExitStack() as stack:
        for name in ('LOG_SERVER', 'LOG_WARN', 'LOG_ERROR'):
            stack.enter_context(patch.object(server, name, lambda *a, **k: None))
        stack.enter_context(patch.object(server, 'IS_DEV', False))
        app = server.App()
        for i in range(args.routes):
            app.route(f'/r{i:06d}')(lambda: 'ok')
        app.start(0)  # Configure real routes without binding a socket.
        stack.callback(app.stop)
        target = f'/r{args.routes - 1:06d}'
        print(f'{args.routes} routes, last-route match, {args.requests} requests/workload')
        profiler = cProfile.Profile()
        for label, headers, body in [
            ('small GET', b'', b''),
            ('32 headers', (b'X-Header: ' + b'a' * 60 + b'\r\n') * 32, b''),
            ('8 KiB form POST', b'Content-Length: 8192\r\n', b'x=' + b'a' * 8190),
        ]:
            method = 'POST' if body else 'GET'
            packet = (f'{method} {target} HTTP/1.1\r\nHost: localhost\r\n'.encode()
                      + headers + b'\r\n' + body)

            def run():
                for _ in range(args.requests):
                    sock = MemorySocket(packet)
                    result = app.process_http_request(BufferedSocket(sock))
                    if result is not server.REUSE_SOCKET_FOR_HTTP or not sock.sent:
                        raise AssertionError('Workload did not produce a successful reusable response')

            start = time.perf_counter()
            run()
            elapsed = time.perf_counter() - start
            print(f'{label}: {elapsed:.3f}s unprofiled, {elapsed / args.requests * 1e6:.1f} us/request')
            profiler.runcall(run)
        if args.output:
            profiler.dump_stats(args.output)
        pstats.Stats(profiler).strip_dirs().sort_stats('tottime').print_stats(25)


if __name__ == '__main__':
    main()
