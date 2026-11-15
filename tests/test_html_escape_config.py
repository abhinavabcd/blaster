"""The startup config supplies the route default; explicit routes override it."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
PROBE = '''
import json, sys
import blaster
from blaster import config
if len(sys.argv) > 1:
    config.load(sys.argv[1])
from blaster.server import App, Request
from blaster.schema import Object, Str, schema
class ConfigLabel(Object):
    name: Str(maxlen=3)
schema(ConfigLabel)
app = App()
@app.route('/default')
def default(): pass
@app.route('/enabled', escape_html=True)
def enabled(): pass
@app.route('/disabled', escape_html=False)
def disabled(): pass
reads = []
for handler in app.route_handlers:
    req = Request(None)
    req.escape_html = handler['escape_html']
    reads.append(req._expose(ConfigLabel.from_dict({'name': '<a>'})).name)
print(json.dumps({
    'global': config.DEFAULT_HTML_ESCAPE,
    'routes': [h['escape_html'] for h in app.route_handlers],
    'request': Request(None).escape_html,
    'schema': ConfigLabel.from_dict({'name': '<a>'}).to_dict(),
    'reads': reads,
}))
'''


class TestHTMLEscapeConfig(unittest.TestCase):
    def probe(self, value=None, yaml_value=None):
        env = dict(os.environ, IS_PROD='1')
        env.pop('DEFAULT_HTML_ESCAPE', None)
        if value is not None:
            env['DEFAULT_HTML_ESCAPE'] = value
        with tempfile.TemporaryDirectory() as directory:
            args = [sys.executable, '-c', PROBE]
            if yaml_value is not None:
                path = Path(directory) / 'app.yaml'
                path.write_text(json.dumps({'DEFAULT_HTML_ESCAPE': yaml_value}))
                args.append(str(path))
            output = subprocess.check_output(args, cwd=ROOT, env=env, text=True)
        return json.loads(output.splitlines()[-1])

    def test_enabled_by_default_and_overridable_per_route(self):
        result = self.probe()
        self.assertIs(result['global'], True)
        self.assertEqual(result['routes'], [True, True, False])
        self.assertIs(result['request'], True)
        self.assertEqual(result['schema'], {'name': '<a>'})
        self.assertEqual(result['reads'], ['&lt;a&gt;', '&lt;a&gt;', '<a>'])

    def test_global_disable_and_explicit_enable(self):
        for value in ('false', '0', 'off'):
            with self.subTest(value=value):
                result = self.probe(value)
                self.assertIs(result['global'], False)
                self.assertEqual(result['routes'], [False, True, False])
                self.assertIs(result['request'], False)
                self.assertEqual(result['schema'], {'name': '<a>'})
                self.assertEqual(result['reads'], ['<a>', '&lt;a&gt;', '<a>'])

    def test_application_yaml_overrides_environment_default(self):
        result = self.probe('true', yaml_value=False)
        self.assertIs(result['global'], False)
        self.assertEqual(result['routes'], [False, True, False])
        self.assertEqual(result['reads'], ['<a>', '&lt;a&gt;', '<a>'])
