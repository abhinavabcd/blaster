"""Optional real HTTP/2 -> nginx -> HTTP/1.1 integration tests.

Requires nginx with http_v2 and curl with HTTP2 support. Uses isolated config,
loopback ephemeral ports, and no system nginx service or configuration changes.
Run: python -m unittest tests.test_http2_proxy -v
"""
import blaster
import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import gevent
from gevent import socket
from tests.test_http_adversarial import HTTPFixture


@unittest.skipUnless(shutil.which('nginx') and Path('/usr/bin/curl').exists(),
                     'requires nginx and /usr/bin/curl')
class TestHTTP2Proxy(HTTPFixture):
    def setUp(self):
        super().setUp()
        curl = subprocess.run(['/usr/bin/curl', '--version'], capture_output=True, text=True, check=True)
        nginx = subprocess.run([shutil.which('nginx'), '-V'], capture_output=True, text=True, check=True)
        if 'HTTP2' not in curl.stdout or '--with-http_v2_module' not in nginx.stderr:
            self.skipTest('requires HTTP/2 support in curl and nginx')
        directory = tempfile.TemporaryDirectory(prefix='blaster-h2-')
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            port = listener.getsockname()[1]
        self.proxy_url = f'http://127.0.0.1:{port}'
        config = root / 'nginx.conf'
        config.write_text(f'''
            daemon off;
            master_process off;
            pid {root}/nginx.pid;
            error_log {root}/error.log info;
            events {{ worker_connections 64; }}
            http {{
                access_log off;
                client_body_temp_path {root}/client;
                proxy_temp_path {root}/proxy;
                upstream blaster {{
                    server 127.0.0.1:{self.address[1]};
                    keepalive 4;
                }}
                server {{
                    listen 127.0.0.1:{port} http2;
                    location / {{
                        proxy_pass http://blaster;
                        proxy_http_version 1.1;
                        proxy_set_header Connection "";
                        proxy_request_buffering off;
                        proxy_buffering off;
                    }}
                }}
            }}
        ''')
        self.proxy = subprocess.Popen([shutil.which('nginx'), '-p', str(root), '-c', str(config)],
                                      stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        self.addCleanup(self.stop_proxy)
        with gevent.Timeout(5, AssertionError('nginx failed to start')):
            while True:
                if self.proxy.poll() is not None:
                    self.fail(self.proxy.stderr.read().decode())
                try:
                    with socket.create_connection(('127.0.0.1', port), timeout=0.1):
                        break
                except OSError:
                    gevent.sleep(0.01)

    def stop_proxy(self):
        if self.proxy.poll() is None:
            self.proxy.terminate()
            try:
                self.proxy.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.proxy.kill()
                self.proxy.wait(timeout=3)
        self.proxy.stderr.close()

    def curl(self, *args, input=None):
        result = subprocess.run(
            ['/usr/bin/curl', '--noproxy', '*', '--http2-prior-knowledge', '--silent',
             '--show-error', '--max-time', '5', '--write-out', '\n%{http_version} %{response_code}\n',
             *args], input=input, capture_output=True, timeout=7)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        return result.stdout

    def test_http2_get(self):
        output = self.curl(self.proxy_url + '/probe')
        body, version_status = output.strip().rsplit(b'\n', 1)
        self.assertEqual(version_status, b'2 200')
        self.assertEqual(json.loads(body)['path'], '/probe')

    def test_http2_unknown_length_upload(self):
        # No Content-Length: nginx must frame HTTP/2 DATA as a chunked H1 upload.
        output = self.curl('-X', 'POST', '-H', 'Content-Length:', '-T', '-',
                           self.proxy_url + '/probe', input=b'x=123')
        body, version_status = output.strip().rsplit(b'\n', 1)
        self.assertEqual(version_status, b'2 200')
        self.assertEqual(json.loads(body)['body'], 'x=123')
        self.assertEqual(self.calls, [('/probe', b'x=123')])
        self.assertEqual(self.received_headers[0].get('transfer-encoding'), 'chunked')

    def test_http2_bodyless_responses_and_reused_upstream(self):
        for status in (204, 304):
            with self.subTest(status=status):
                output = self.curl(self.proxy_url + f'/status/{status}', self.proxy_url + '/probe')
                self.assertTrue(output.startswith(f'\n2 {status}\n'.encode()), output)
                self.assertTrue(output.endswith(b'\n2 200\n'), output)
                self.assertNotIn(b'representation', output)
                self.assertEqual(self.peer_ports[-2], self.peer_ports[-1], 'nginx did not reuse upstream')

    def test_http2_head(self):
        output = self.curl('--head', self.proxy_url + '/probe')
        self.assertIn(b'HTTP/2 200', output)
        self.assertTrue(output.endswith(b'\n2 200\n'), output)
        self.assertNotIn(b'{"path"', output)

    def test_http2_response_framing(self):
        for kind in ('dict', 'list'):
            with self.subTest(kind=kind):
                output = self.curl(self.proxy_url + f'/framing/{kind}')
                self.assertEqual(output, b'correct\n2 200\n')


if __name__ == '__main__':
    unittest.main()
