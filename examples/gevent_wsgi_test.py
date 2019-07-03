from gevent.pywsgi import WSGIServer


def hello(environ, start_response):
    path = environ['PATH_INFO'] 
    if(path == '/test' and environ['REQUEST_METHOD'] == 'GET'):
        status = '200 OK'
        text = "<html><body>Hello World</body></html>"
    else:
        status = '404 Not Found'
        text = "Not Found"

    #print("0", path, "0")

    body = text.encode('utf-8')
    response_headers = [
        ('Content-type', 'text/plain; charset=utf-8'),
        ('Content-Length', str(len(body)))]
    start_response(status, response_headers)
    return [body]


WSGIServer(('0.0.0.0', 8081), hello, log=None).serve_forever()
