'''
Created on 22-Aug-2017

@author: abhinav
'''
import blaster
from blaster.base import start_stream_server, route_handler
from blaster.common_funcs_and_datastructures import static_file_handler
import os

@route_handler("^/test$")
def do_test(sock , request_params=None, headers=None, post_data=None):
    return "<html><body>Hello World</body></html>"

@route_handler("^/hello")
def do_test(sock , request_params=None, headers=None, post_data=None):
    return {"hello": "world"}

##directly return an object , server will automatically convert to json
@route_handler("^/say/(.*)$")
def do_test2(sock, something, request_params=None, headers=None, post_data=None):
    return {"message" : str(something)} # something is a byte array !sorry

##directly return an object , server will automatically convert to json
@route_handler("^/echo_params/(.*)$")
def do_test3(sock, something, request_params=None, headers=None, post_data=None):
    return {"request_params" : request_params}

##FOR handling you post arguments as key=value&key2=value2 data
@route_handler("^/echo_post_params")
def do_test4(sock, request_params=None, headers=None, post_data=None):
    return {"request_params" : request_params}

##FOR handling you post as raw body
@route_handler("^/echo_post_data")
def do_test5(sock, request_params=None, headers=None, post_data=None):
    return {"post_data" : post_data}

##FOR handling you post as raw json body
@route_handler("^/echo_post_data")
def do_test6(sock, request_params=None, headers=None, post_data=None):
    return {"post_data_object" : request_params}

#respond custom headers
@route_handler("^/custom_header$")
def do_test7(sock, request_params=None, headers=None, post_data=None):
    return {"X-header": "something_random"}, {"post_data_object" : request_params}

@route_handler("^/custom_override_all_header$")
def do_test7(sock, request_params=None, headers=None, post_data=None):
    return ["X-header: something_random"], {"post_data_object" : request_params}

#custom status
@route_handler("^/custom_override_all_header$")
def do_test8(sock, request_params=None, headers=None, post_data=None):
    return "302 Redirect", {"Location": "https://google.com"}, "Redirecting you"




if __name__ == "__main__":
    start_stream_server(8081, handlers=[
            ('^/(.*)', static_file_handler(os.path.dirname(os.path.abspath(__file__)) + "/web/"))  # can server files too                  
    ])
