"""CDP session routing, event ordering, and tab cleanup regression tests."""
import json
import unittest
from collections import deque
from unittest.mock import Mock, patch
from pathlib import Path
import base64
import shutil
import sys

import gevent
from gevent.event import AsyncResult, Event
from gevent.queue import Queue
from gevent.server import StreamServer

from blaster import tools


class ChromeSocket:
    def __init__(self):
        self.closed = False
        self.messages = deque()
        self.commands = []
        self.tabs = 0
        self.navigation_error = None
        self.emit_load = True

    def connect(self, url, timeout):
        self.url = url

    def settimeout(self, timeout):
        pass

    def close(self):
        self.closed = True

    def send(self, raw):
        message = json.loads(raw)
        self.commands.append(message)
        method = message['method']
        result = {}
        if method == 'Target.createTarget':
            self.tabs += 1
            result = {'targetId': f'tab-{self.tabs}'}
        elif method == 'Target.attachToTarget':
            result = {'sessionId': f'session-{self.tabs}'}
        elif method == 'Page.navigate':
            # Old documents and other sessions must not satisfy the load wait.
            for session, loader in ((message['sessionId'], 'old'), ('other', 'new')):
                self.messages.append({'method': 'Page.lifecycleEvent', 'sessionId': session,
                                      'params': {'name': 'load', 'loaderId': loader}})
            result = {'loaderId': 'new'}
            if self.navigation_error:
                result['errorText'] = self.navigation_error
            # The real load event can precede the navigation command response.
            if self.emit_load:
                self.messages.append({'method': 'Page.lifecycleEvent',
                                      'sessionId': message['sessionId'],
                                      'params': {'name': 'load', 'loaderId': 'new'}})
        elif method == 'Page.getFrameTree':
            result = {'frameTree': {'frame': {'id': 'frame', 'loaderId': 'document'}}}
        elif method == 'Page.setDocumentContent':
            self.messages.append({'method': 'Page.lifecycleEvent',
                                  'sessionId': message['sessionId'],
                                  'params': {'name': 'load', 'loaderId': 'document'}})
        elif method == 'Runtime.evaluate':
            result = {'result': {'value': '<html>rendered</html>'}}
        elif method == 'Page.captureScreenshot':
            result = {'data': 'png-base64'}
        self.messages.append({'id': message['id'], 'result': result})

    def recv(self):
        if not self.messages:
            raise TimeoutError("No more Chrome messages")
        return json.dumps(self.messages.popleft())


class TestChromeWorker(unittest.TestCase):
    def setUp(self):
        self.socket = ChromeSocket()
        self.queue = Queue()
        self.keys = {}
        self.process = Mock()
        self.process.poll.return_value = None
        self.worker = None
        self.patches = [
            patch.object(tools, 'browser_url_queue', self.queue),
            patch.object(tools, 'chrome_browser_processes_started', 1),
            patch.object(tools, 'LOG_WARN'),
            patch.object(tools, 'Websocket', return_value=self.socket),
            patch.object(tools.subprocess, 'Popen', side_effect=self.start_process),
        ]
        self.mocks = [p.start() for p in self.patches]
        self.addCleanup(self.cleanup)

    def cleanup(self):
        if self.worker is not None:
            self.worker.kill()
        for p in reversed(self.patches):
            p.stop()

    def start_process(self, args, **kwargs):
        profile = next(a.split('=', 1)[1] for a in args if a.startswith('--user-data-dir='))
        Path(profile, 'DevToolsActivePort').write_text('12345\n/devtools/browser/test\n')
        return self.process

    def run_jobs(self, *jobs):
        for url, ret_type in jobs:
            key = AsyncResult()
            self.keys[(url, ret_type)] = key
            self.queue.put((key, url, ret_type))
        self.queue.put(None)
        self.worker = gevent.spawn(tools.run_chrome_in_separate_cpu_process)
        self.worker.get(timeout=2)

    def test_reuses_socket_with_fresh_sessions_for_both_outputs(self):
        self.run_jobs(('https://html-url', 'html'), ('https://screenshot-url', 'screenshot'))
        self.assertEqual(self.keys[('https://html-url', 'html')].get(timeout=0), '<html>rendered</html>')
        self.assertEqual(self.keys[('https://screenshot-url', 'screenshot')].get(timeout=0), 'png-base64')
        self.mocks[3].assert_called_once()
        commands = self.socket.commands
        self.assertEqual([m['params']['targetId'] for m in commands
                          if m['method'] == 'Target.closeTarget'], ['tab-1', 'tab-2'])
        self.assertEqual([m['sessionId'] for m in commands
                          if m['method'] == 'Page.navigate'], ['session-1', 'session-2'])
        self.assertEqual([m['id'] for m in commands], list(range(1, len(commands) + 1)))
        self.assertTrue(self.socket.closed)
        self.process.wait.assert_called_once_with(timeout=5)
        self.assertEqual(tools.chrome_browser_processes_started, 0)

    def test_screenshot_formats_and_quality(self):
        cases = [
            ('screenshot:jpeg:100', {'format': 'jpeg', 'quality': 100}),
            ('screenshot:jpeg:70', {'format': 'jpeg', 'quality': 70}),
            ('screenshot:jpeg:0', {'format': 'jpeg', 'quality': 0}),
            ('screenshot:jpeg', {'format': 'jpeg', 'quality': 100}),
            ('screenshot', {'format': 'jpeg', 'quality': 100}),
            ('screenshot::70', {'format': 'jpeg', 'quality': 70}),
            ('screenshot:jpeg:', {'format': 'jpeg', 'quality': 100}),
            ('screenshot:png', {'format': 'png', 'quality': 100}),
        ]
        self.run_jobs(*( ('https://example.test', ret_type) for ret_type, _ in cases))
        screenshots = [m for m in self.socket.commands if m['method'] == 'Page.captureScreenshot']
        self.assertEqual(len(screenshots), len(cases))
        for (ret_type, expected), screenshot in zip(cases, screenshots):
            with self.subTest(ret_type=ret_type):
                self.assertEqual(screenshot['params'], expected)
                self.assertEqual(self.keys[('https://example.test', ret_type)].get(timeout=0), 'png-base64')

    def test_invalid_quality_returns_none_cleans_up_and_keeps_processing(self):
        self.run_jobs(('https://example.test', 'screenshot:jpeg:bad'),
                      ('https://example.test', 'screenshot:jpeg:70'))
        self.assertIsNone(self.keys[('https://example.test', 'screenshot:jpeg:bad')].get(timeout=0))
        self.assertEqual(self.keys[('https://example.test', 'screenshot:jpeg:70')].get(timeout=0), 'png-base64')
        closed = [m for m in self.socket.commands if m['method'] == 'Target.closeTarget']
        self.assertEqual(len(closed), 2)
        self.mocks[3].assert_called_once()

    def test_html_content_is_injected_instead_of_navigated(self):
        for html in ('  <html><body>雪</body></html>', 'plain text',
                     '<html><!--' + 'x' * (5 * 1024 * 1024) + '--><body>large</body></html>'):
            with self.subTest(size=len(html)):
                self.socket = ChromeSocket()
                self.mocks[3].return_value = self.socket
                self.run_jobs((html, 'screenshot:jpeg:100'))
                content = next(m for m in self.socket.commands if m['method'] == 'Page.setDocumentContent')
                self.assertEqual(content['params']['html'], html)
                self.assertFalse(any(m['method'] == 'Page.navigate' for m in self.socket.commands))

    def test_long_https_url_is_still_navigated(self):
        url = 'https://example.test/?query=' + 'a' * 300
        self.run_jobs((url, 'html'))
        navigation = next(m for m in self.socket.commands if m['method'] == 'Page.navigate')
        self.assertEqual(navigation['params']['url'], url)

    def test_navigation_failure_closes_tab_and_processes_next_job(self):
        original_send = self.socket.send

        def send(raw):
            message = json.loads(raw)
            self.socket.navigation_error = ('net::ERR_FAILED'
                if message['params'].get('url') == 'https://bad-url' else None)
            original_send(raw)

        self.socket.send = send
        self.run_jobs(('https://bad-url', 'html'), ('https://good-url', 'html'))
        self.assertIsNone(self.keys[('https://bad-url', 'html')].get(timeout=0))
        self.assertEqual(self.keys[('https://good-url', 'html')].get(timeout=0), '<html>rendered</html>')
        self.assertEqual(sum(m['method'] == 'Target.closeTarget'
                             for m in self.socket.commands), 2)

    def test_transport_failure_reconnects_for_next_job(self):
        replacement = ChromeSocket()
        self.mocks[3].side_effect = [self.socket, replacement]
        original_send = self.socket.send

        def send(raw):
            if json.loads(raw)['method'] == 'Page.navigate':
                raise BrokenPipeError('peer disconnected')
            original_send(raw)

        self.socket.send = send
        self.run_jobs(('https://lost-url', 'html'), ('https://next-url', 'html'))
        self.assertIsNone(self.keys[('https://lost-url', 'html')].get(timeout=0))
        self.assertEqual(self.keys[('https://next-url', 'html')].get(timeout=0), '<html>rendered</html>')
        self.assertEqual(self.mocks[3].call_count, 2)
        self.assertTrue(self.socket.closed)
        self.assertTrue(replacement.closed)

    def test_stale_load_events_do_not_complete_navigation(self):
        self.socket.emit_load = False
        self.run_jobs(('https://url', 'html'))
        self.assertIsNone(self.keys[('https://url', 'html')].get(timeout=0))
        self.assertTrue(any(m['method'] == 'Target.closeTarget' for m in self.socket.commands))
        self.assertFalse(any(m['method'] == 'Runtime.evaluate' for m in self.socket.commands))

    def test_waiting_for_work_yields_and_sentinel_stops_worker(self):
        self.worker = gevent.spawn(tools.run_chrome_in_separate_cpu_process)
        gevent.sleep(0.02)
        self.assertFalse(self.worker.ready())
        self.keys[('https://url', 'html')] = key = AsyncResult()
        self.queue.put((key, 'https://url', 'html'))
        self.queue.put(None)
        self.worker.get(timeout=2)
        self.assertEqual(self.keys[('https://url', 'html')].get(timeout=0), '<html>rendered</html>')

    def test_startup_polling_yields(self):
        def delayed_start(args, **kwargs):
            profile = next(a.split('=', 1)[1] for a in args if a.startswith('--user-data-dir='))
            writer = gevent.spawn_later(0.02, Path(profile, 'DevToolsActivePort').write_text,
                                        '12345\n/devtools/browser/test\n')
            self.addCleanup(writer.kill)
            return self.process

        self.mocks[4].side_effect = delayed_start
        self.run_jobs(('https://url', 'html'))
        self.assertEqual(self.keys[('https://url', 'html')].get(timeout=0), '<html>rendered</html>')


class TestChromeResults(unittest.TestCase):
    def test_async_results_do_not_hash_html_and_keep_identical_requests_separate(self):
        class UnhashableHtml(str):
            def __hash__(self):
                raise AssertionError('HTML was hashed')

        html = UnhashableHtml('<html>' + 'x' * (5 * 1024 * 1024) + '</html>')
        queue = Queue()
        replies = []

        def complete_requests():
            for value in ('first', 'second'):
                reply, content, ret_type = queue.get()
                replies.append(reply)
                self.assertIs(content, html)
                self.assertEqual(ret_type, 'screenshot:jpeg:100')
                reply.set(value)

        with patch.object(tools, 'browser_url_queue', queue), \
             patch.object(tools, 'chrome_browser_processes_started', 1), \
             patch.object(tools, 'sleep', side_effect=AssertionError('Caller must not poll')):
            worker = gevent.spawn(complete_requests)
            callers = [gevent.spawn(tools.load_page_with_chrome, html, 'screenshot:jpeg:100') for _ in range(2)]
            try:
                self.assertEqual([caller.get(timeout=1) for caller in callers], ['first', 'second'])
                worker.get(timeout=1)
            finally:
                gevent.killall([worker, *callers])
        self.assertIsNot(replies[0], replies[1])
        self.assertTrue(all(isinstance(reply, AsyncResult) for reply in replies))

    def test_timeout_yields_and_late_completion_does_not_affect_next_request(self):
        queue = Queue()
        with patch.object(tools, 'browser_url_queue', queue), \
             patch.object(tools, 'chrome_browser_processes_started', 1):
            caller = gevent.spawn(tools.load_page_with_chrome, 'https://slow', timeout=0.03)
            self.addCleanup(caller.kill)
            reply, _, _ = queue.get(timeout=1)
            gevent.sleep(0)
            self.assertFalse(caller.ready())
            self.assertIsNone(caller.get(timeout=1))
            reply.set('late result')
            next_caller = gevent.spawn(tools.load_page_with_chrome, 'https://slow')
            self.addCleanup(next_caller.kill)
            next_reply, _, _ = queue.get(timeout=1)
            self.assertIsNot(next_reply, reply)
            self.assertFalse(next_caller.ready())
            next_reply.set('fresh result')
            self.assertEqual(next_caller.get(timeout=1), 'fresh result')

    def test_failed_capture_returns_none_without_waiting_for_timeout(self):
        queue = Queue()
        with patch.object(tools, 'browser_url_queue', queue), \
             patch.object(tools, 'chrome_browser_processes_started', 1):
            caller = gevent.spawn(tools.load_page_with_chrome, 'https://failed')
            self.addCleanup(caller.kill)
            reply, _, _ = queue.get(timeout=1)
            reply.set(None)
            self.assertIsNone(caller.get(timeout=0.1))

class TestChromeCooperation(unittest.TestCase):
    def test_subprocess_wait_yields(self):
        # Use the same subprocess module as the worker, with an actual OS child.
        process = tools.subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(0.15)'])
        self.addCleanup(process.wait)
        self.addCleanup(lambda: process.kill() if process.poll() is None else None)
        waiter = gevent.spawn(process.wait, timeout=2)
        self.addCleanup(waiter.kill)
        gevent.sleep(0.02)
        self.assertFalse(waiter.ready(), 'subprocess.wait blocked the gevent hub')
        self.assertEqual(waiter.get(timeout=3), 0)

    @unittest.skipUnless(shutil.which('google-chrome'), 'google-chrome is not installed')
    def test_real_chrome_navigation_yields_and_reuses_connection(self):
        requested, release = Event(), Event()
        queue = Queue()
        connections = []
        body = b'<html><body><script>document.body.dataset.rendered="yes"</script></body></html>'

        def serve(sock, address):
            try:
                sock.settimeout(5)
                request = bytearray()
                while b'\r\n\r\n' not in request:
                    part = sock.recv(4096)
                    if not part:
                        return
                    request.extend(part)
                requested.set()
                if not release.wait(timeout=5):
                    raise TimeoutError('Test did not release HTTP response')
                sock.sendall(b'HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: '
                             + str(len(body)).encode() + b'\r\nConnection: close\r\n\r\n' + body)
            finally:
                sock.close()

        server = StreamServer(('127.0.0.1', 0), serve)
        server.start()
        self.addCleanup(server.stop)
        original_connect = tools.Websocket.connect

        def connect(ws, url, *args, **kwargs):
            connections.append(url)
            return original_connect(ws, url, *args, **kwargs)

        with patch.object(tools, 'browser_url_queue', queue), \
             patch.object(tools, 'chrome_browser_processes_started', 1), \
             patch.object(tools.Websocket, 'connect', connect):
            worker = gevent.spawn(tools.run_chrome_in_separate_cpu_process)
            self.addCleanup(worker.kill)
            url = f'http://127.0.0.1:{server.server_port}/slow'
            screenshot_url = 'data:text/html,<html><body>screenshot</body></html>'
            html_result, screenshot_result = AsyncResult(), AsyncResult()
            queue.put((html_result, url, 'html'))
            try:
                self.assertTrue(requested.wait(timeout=20), 'Chrome did not request the local page')
                # The worker is waiting in Websocket.recv while this greenlet keeps running.
                ticks = []
                for _ in range(5):
                    gevent.sleep(0.01)
                    ticks.append(True)
                self.assertEqual(len(ticks), 5)
                self.assertFalse(worker.ready())
                self.assertFalse(html_result.ready())
                release.set()
                queue.put((screenshot_result, screenshot_url, 'screenshot:png'))
                html = ('<html><!--' + 'x' * (5 * 1024 * 1024)
                        + '--><body><script>document.body.dataset.rendered="yes"</script>JPEG 雪</body></html>')
                rendered = tools.load_page_with_chrome(html)
                self.assertIn('data-rendered="yes"', rendered)
                jpeg_sizes = {}
                for quality in (100, 70, 0):
                    with self.subTest(quality=quality):
                        data = tools.load_page_with_chrome(html, f'screenshot:jpeg:{quality}')
                        self.assertIsInstance(data, str)
                        jpeg = base64.b64decode(data)
                        self.assertTrue(jpeg.startswith(b'\xff\xd8'))
                        self.assertTrue(jpeg.endswith(b'\xff\xd9'))
                        jpeg_sizes[quality] = len(jpeg)
                self.assertLess(jpeg_sizes[70], jpeg_sizes[100])
                queue.put(None)
                worker.get(timeout=20)
                self.assertIn('data-rendered="yes"', html_result.get(timeout=0))
                self.assertTrue(base64.b64decode(screenshot_result.get(timeout=0)).startswith(b'\x89PNG\r\n\x1a\n'))
                self.assertEqual(len(connections), 1)
                self.assertEqual(tools.chrome_browser_processes_started, 0)
            finally:
                release.set()
                worker.kill()
