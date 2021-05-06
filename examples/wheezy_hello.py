from gevent import monkey
monkey.patch_all()
from gevent import pywsgi
from wheezy.http import HTTPResponse
from wheezy.http import WSGIApplication
from wheezy.routing import url
from wheezy.web.handlers import BaseHandler
from wheezy.web.middleware import bootstrap_defaults
from wheezy.web.middleware import path_routing_middleware_factory

def test(request):
    response = HTTPResponse()
    response.write('<html><body>Hello World</body></html>')
    return response


routes = [
    url('test', test, name='test')
]


options = {}
main = WSGIApplication(
    middleware=[
        bootstrap_defaults(url_mapping=routes),
        path_routing_middleware_factory
    ],
    options=options
)


server = pywsgi.WSGIServer(('127.0.0.1', 5000), main, backlog=128000)
server.serve_forever()

