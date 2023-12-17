'''
Created on 22-Aug-2017

@author: abhinav
'''
import blaster
# fork
from blaster.tools import CommandLineNamedArgs
if(_fork_workers := CommandLineNamedArgs.get("num_workers")):
	from blaster.utils.fork import blaster_fork
	blaster_fork(int(_fork_workers))


from blaster.server import start_server, route, Request, static_file_handler
import os


@route("/hello")
def say_hello(req: Request):
	return ["content-type: application/json"], {"hello": "world"}


@route("/items/{item_id}")
def read_item(item_id: int, q: str = None):
	return {"item_id": item_id, "q": q}


@route("/test")
def do_test(req: Request):
	return "<html><body>Hello World</body></html>"


# directly return an object , server will automatically convert to json
@route("/say/{+something}")
def do_test2(something: str, req: Request):
	return {"message": something}  # something is a byte array !sorry


# directly return an object , server will automatically convert to json
@route("/echo_params/{*something}")
def do_test3(req: Request, something: str):
	return {"req": req._get, "something": something}


# FOR handling you post as raw body
@route("/echo_post_data")
def do_test5(req: Request):
	return {"post_data": req._post}


# respond custom headers
@route("/custom_header")
def do_test7(req: Request):
	return {"X-header": "something_random"}, {"post_data_object": req}


@route("/custom_override_all_header")
def override_all_headers(req: Request):
	return ["X-header: something_random"], {"post_data_object": req}


# custom status
@route("/custom_override_all_header")
def do_test8(req: Request):
	return "302 Redirect", {"Location": "https://google.com"}, "Redirecting you"


if __name__ == "__main__":
	start_server(8081, handlers=[
		static_file_handler(
			'/',
			os.path.dirname(os.path.abspath(__file__)) + "/web/"
		)
	])
