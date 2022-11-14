'''
Created on 22-Aug-2017

@author: abhinav
'''
import blaster
from blaster.server import start_server, route,\
    Request, static_file_handler
import os


@route("^/test$")
def do_test(request_params: Request):
    return "<html><body>Hello World</body></html>"


@route("^/hello")
def say_hello(request_params: Request):
    return ["content-type: application/json"], {"hello": "world"}


# directly return an object , server will automatically convert to json
@route("^/say/(.*)$")
def do_test2(something, request_params: Request):
    return {"message": something}  # something is a byte array !sorry


# directly return an object , server will automatically convert to json
@route("^/echo_params/{:something}$")
def do_test3(request_params: Request):
    return {"request_params": request_params._get}


@route("^/echo_params/{:+something}$")
def do_test_with_something(request_params: Request):
    return {"request_params": request_params._get}


# FOR handling you post arguments as key=value&key2=value2 data
@route("^/echo_post_params")
def do_test4(request_params: Request):
    return {"request_params": request_params}


# FOR handling you post as raw body
@route("^/echo_post_data")
def do_test5(request_params: Request):
    return {"post_data": request_params._post}


# FOR handling you post as raw json body
@route("^/echo_post_data")
def do_test6(request_params: Request):
    return {"post_data_object": request_params}


# respond custom headers
@route("^/custom_header$")
def do_test7(request_params: Request):
    return {"X-header": "something_random"}, {"post_data_object": request_params}


@route("^/custom_override_all_header$")
def override_all_headers(request_params: Request):
    return ["X-header: something_random"], {"post_data_object": request_params}

# custom status
@route("^/custom_override_all_header$")
def do_test8(request_params: Request):
    return "302 Redirect", {"Location": "https://google.com"}, "Redirecting you"


if __name__ == "__main__":
    num_procs = min(3, os.cpu_count())
    while(num_procs > 1):
        num_procs -= 1
        pid = os.fork()
        if(pid == 0):
            break
        print("Forking multiple servers...", pid)

    start_server(8081, handlers=[
            static_file_handler(
                '/',
                os.path.dirname(os.path.abspath(__file__)) + "/web/"
            )
        ]
    )
