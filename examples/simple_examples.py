'''
Created on 22-Aug-2017

@author: abhinav
'''
import blaster
from blaster.base import start_stream_server, route_handler,\
    process_post_data
from blaster.common_funcs_and_datastructures import static_file_handler
import os


def do_nothing(a, **kwargs):
    pass


blaster.base.server_log = do_nothing


@route_handler("^/test$")
def do_test(sock , query_params=None, headers=None, post_data=None):
    return "<html><body>Hello World</body></html>"


##directly return an object , server will automatically convert to json
@route_handler("^/say/(.*)$") 
def do_test2(sock, something, query_params=None, headers=None, post_data=None):
    return {"message" : str(something)} # something is a byte array !sorry

##directly return an object , server will automatically convert to json
@route_handler("^/echo_params/(.*)$")
def do_test3(sock, something, query_params=None, headers=None, post_data=None):
    return {"query_params" : query_params}

##FOR handling you post arguments as key=value&key2=value2 data
@route_handler("^/echo_post_params")
@process_post_data
def do_test4(sock, query_params=None, headers=None, post_data=None):
    return {"query_params" : query_params}

##FOR handling you post as raw body
@route_handler("^/echo_post_data")
def do_test5(sock, query_params=None, headers=None, post_data=None):
    return {"post_data" : post_data}

##FOR handling you post as raw json body
@route_handler("^/echo_post_data")
@process_post_data
def do_test6(sock, query_params=None, headers=None, post_data=None):
    return {"post_data_object" : query_params}

#respond custom headers
@route_handler("^/custom_header$")
def do_test7(sock, query_params=None, headers=None, post_data=None):
    return {"X-header": "something_random"}, {"post_data_object" : query_params}

@route_handler("^/custom_override_all_header$")
def do_test7(sock, query_params=None, headers=None, post_data=None):
    return ["X-header: something_random"], {"post_data_object" : query_params}

#custom status
@route_handler("^/custom_override_all_header$")
def do_test8(sock, query_params=None, headers=None, post_data=None):
    return "302 Redirect", {"Location": "https://google.com"}, "Redirecting you"




if __name__ == "__main__":    
    start_stream_server(8081, handlers=[
            ('^/(.*)', static_file_handler(os.path.dirname(os.path.abspath(__file__))+"/web/"))  #can server files too                  
    ])