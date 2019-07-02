'''
Created on 22-Aug-2017

@author: abhinav
'''

# argparse==1.2.1
# gevent==1.2.1
# greenlet==0.4.12
# protobuf==3.2.0
# boto3==1.4.4


import ujson as json
import re
import urllib.parse
from gevent.server import StreamServer
from gevent.pywsgi import WSGIServer
import time
import types
import gevent
import signal
from blaster.constants import LOG_TYPE_SERVER_INFO
from blaster.common_funcs_and_datastructures import cur_ms
import functools
import six
#http://www.gevent.org/gevent.monkey.html#patching should be first line

is_server_running = True

request_handlers = []
exit_listeners = []


push_tasks = {} #name: func

default_wsgi_headers = [
        ('Content-Type', 'text/html')
    ]
default_stream_headers = [
        'Content-Type: text/html',
        'Access-Control-Allow-Origin: *'
    ]



def server_log(log_type, cur_millis=None, **kwargs):
    if(not cur_millis):
        cur_millis = cur_ms()
    print(log_type , cur_millis, json.dumps(kwargs))







def write_data(socket, *_data):
    for data in _data:
        if(isinstance(data, six.text_type)):
            data = data.encode()
        n = 0
        l = len(data)
        while(n<l):
            sent = socket.send(data[n:])
            n += sent
            if(sent<0):
                return False
    return True



class SocketDataReader():
    
    is_eof = False
    sock= None
    store = None
    _n = 0
    def __init__(self, sock):
        self.sock = sock
        self.store = bytearray()
        self._n = 0
    
    def read(self, n):
        if(self.is_eof): return None
        while(self._n+n > len(self.store)):
            data = self.sock.recv(1024)
            if(not data):
                self.is_eof = True
                break
            self.store.extend(data)
        ret = self.store[self._n:self._n+n]
        self._n+=n
        return ret
        
            
            
    def read_line(self):
        if(self.is_eof): return None
        n = self._n
        line_found = False
        while(not line_found):
            if(n>=len(self.store)):
                data = self.sock.recv(1024)
                if(not data):
                    self.is_eof = True
                    break
                self.store.extend(data) #fetch more data
                
            if(self.store[n]==ord(b'\n')):
                line_found = True
            n+=1
        
        
        ret = self.store[self._n: n]
        self._n = n
        return ret

    
####parse query string
def parse_qs_modified(qs, keep_blank_values=0, strict_parsing=0):
    _dict = {}
    for name, value in urllib.parse.parse_qsl(qs, keep_blank_values, strict_parsing):
        if name in _dict:
            _values = _dict[name]
            if(not isinstance(_values , list)):#make it a list of values
                _dict[name] = _values = [_values]
            _values.append(value)
        else:
            _dict[name] = value
    return _dict

#decorator
def process_post_params(func):
    def new_func(*args, **kwargs):
        post_data = kwargs.get("post_data",None)
        query_params = kwargs["query_params"]
        if(post_data):
            query_params.update(parse_qs_modified(post_data.decode('utf-8')))
        ret = func(*args, **kwargs)
        return ret
    
    new_func._original = getattr(func, "_original", func)
    return new_func

#decorator
def process_post_data(func):
    def new_func(*args, **kwargs):
        post_data = kwargs.get("post_data",None)
        query_params = kwargs["query_params"]
        if(post_data):
            query_params.update(json.loads(post_data.decode('utf-8')))
        ret = func(*args, **kwargs)
        return ret
    
    new_func._original = getattr(func, "_original", func)
    return new_func



LOG_TYPE_REQUEST = 0
LOG_TYPE_REQUEST_ERROR = -1

def handle_connection(socket, address): 
    socket_data_reader = SocketDataReader(socket)
    is_okay = True#reuse the connection to handle another
    post_data = None
    while(is_okay):
        request_line = socket_data_reader.read_line()
        if(not request_line): return
        request_params = {}
        cur_millis = int(1000*time.time())
        try:
            request_type , request_path , http_version = request_line.decode("utf-8").split(" ")
            query_start_index = request_path.find("?")
            if(query_start_index!=-1):
                request_params = parse_qs_modified(request_path[query_start_index+1:])
                request_path = request_path[:query_start_index]
        
            server_log(LOG_TYPE_REQUEST , request_type=request_type , path=request_path) 
            headers = {}
            while(True):#keep reading headers
                l = socket_data_reader.read_line()
                if(l==b'\r\n'):
                    break
                if( not l):
                    return
                header_type , data  =  l.split(b": ",1)
                headers[header_type.decode()] = data
            
            if(request_type == "POST" and headers.get("Content-Length", None)):
                n = int(headers.get("Content-Length",b"0").strip(b" \r\n"))
                if(n>0):
                    post_data = bytearray()
                    while(len(post_data) < n):
                        bts = socket_data_reader.read(n)
                        if(not bts):
                            break
                        post_data.extend(bts)
                    #request_params.update(urlparse.parse_qs(str(post_data)))
            ##app specific headers
            ret = status = response_headers = body = None        
            for handler in request_handlers:
                args = handler[0].match(request_path)
                func = handler[1]
                kwargs = {}
                kwargs["query_params"] = request_params
                kwargs["headers"] = headers
                if(post_data):
                    kwargs["post_data"] = post_data
                    
                
                if(args!=None):
                    fargs = args.groups()
                    if(fargs):
                        ret = func(socket, *fargs , **kwargs)
                        break
                    else:
                        ret = func(socket, **kwargs)
                        break
            
            #handle various return values from handler functions
            if(ret!=None):
                if(isinstance(ret, tuple)):
                    l = len(ret)
                    if(l>0):
                        body = ret[-1]
                    if(l>1):
                        response_headers = ret[-2]
                    if(l>2):
                        status = ret[-3]
                else:
                    if(isinstance(ret, dict) or isinstance(ret, list)):
                        #if it's object , return json encoded
                        body = json.dumps(ret)
                    else:
                        body = ret
    
            
            
                if(body!=None):#if body doesn't exist , the handler function is holding the connection and handling it manually 
                    status = status or b"200 OK"
                    response_headers = response_headers or default_stream_headers
                    is_okay = write_data(socket, b"HTTP/1.1 ", status, b"\r\n")
                    for header in response_headers:
                        is_okay = is_okay and write_data(socket, header, b"\r\n")
                    is_okay = is_okay and write_data(socket, b"Content-Length: ",str(len(body)), b"\r\n\r\n")            
                    is_okay = is_okay and write_data(socket, body)
                
                
                    if(not headers.get("keep-alive",None)):
                        socket.close()
                        is_okay = False
        except Exception as ex:
            socket.close()
            server_log(LOG_TYPE_REQUEST_ERROR , cur_millis,  data=str(ex))
            if(True):
                import traceback
                traceback.print_exc()                
            return
        
        

global_route_handlers = []
def route_handler(regex):
    def _decorator(func):
        global_route_handlers.append((regex, func))
        def new_func(*args, **kwargs):
            return func(*args, **kwargs)        

        new_func._original = getattr(func, "_original", func)
        return new_func
    
    return _decorator

    

stream_server = None
def exit_handler():
    server_log(LOG_TYPE_SERVER_INFO,  data="exiting server")
    global is_server_running, stream_server
    
    is_server_running = False
    
    for i in exit_listeners:
        i()
    stream_server.stop()#should handle all connections fgracefully
        #wait for queue to flush/process
        
def start_stream_server(port=80, handlers= [], **ssl_args):
    global_route_handlers.sort(key=functools.cmp_to_key(lambda x, y: len(y[0])-len(x[0]))) #reverse sort by length
    global_route_handlers.extend(handlers)
    for regex, handler in global_route_handlers:
        if(isinstance(regex, str)):
            regex = re.compile(regex)
        request_handlers.append((regex, handler))
        
        
    #request_handlers.sort(key = lambda x:x[0] , reverse=True)
    global stream_server
    stream_server = StreamServer(
    ('', port), handle_connection, **ssl_args)
    
    gevent.signal(signal.SIGINT, exit_handler)

    stream_server.serve_forever()
    
    

##################wsgi related handlers

def application(environ, start_response):
    ret = None
    for handler in request_handlers:
        args = handler[0].match(environ['PATH_INFO'])
        func = handler[1]
        if(args!=None):
            kwargs = {}
            kwargs["query_params"] = urllib.parse.parse_qs(environ['QUERY_STRING'])

            fargs = args.groups()
            if(fargs):
                ret = func(*fargs , **kwargs)
            else:
                ret = func(**kwargs)  
            
            break
        
    status = response_headers = body = None        
    if(ret!=None):
        if(isinstance(ret, tuple)):
            l = len(ret)
            if(l>0):
                body = ret[-1]
            if(l>1):
                response_headers = ret[-2]
            if(l>2):
                status = ret[-3]
        else:
            body = ret
        
        response_headers = response_headers or default_wsgi_headers
        status = status or "200 OK"
        start_response(status, response_headers)
        if(isinstance(body, list)):
            return body
        return [body]
    #close socket
    
    
def start_wsgi_server(port=80, handlers=[]):
    for regex, handler in handlers:
        if(isinstance(regex, str)):
            regex = re.compile(regex)
        request_handlers.append((regex, handler))
        
        
    request_handlers.sort(key = lambda x:x[0] , reverse=True)
    WSGIServer(('', port), application).serve_forever()
