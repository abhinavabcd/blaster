from blaster.server import start_server, route, Request, stop_all_apps
import unittest
import requests
import socket
from threading import Thread


@route("/test", methods=["POST"])
def test(req: Request, param1: str):
    return {"param1": param1}


class TestServer(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.server_thread = Thread(target=start_server, args=(8001,))
        cls.server_thread.start()

    @classmethod
    def tearDownClass(cls):
        stop_all_apps()
        cls.server_thread.join()

    def CHECK_SERVER_OKAY(self):
        for i in range(100):
            value1 = str(i)
            self.assertEqual(
                requests.post("http://localhost:8001/test", json={"param1": value1}).json()["param1"],
                value1
            )

    def test_server0(self):
        # Define the HTTP request parts
        request_line = "POST /test HTTP/1.1\r\n"
        headers = [
            "Host: example.com",
            "keep-alive: true",
            "Content-Type: application/x-www-form-urlencoded",
            "Content-Length: 13"
        ]
        body = "param1=value1"

        try:
            # Create a socket
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            # Connect to the server
            client_socket.connect(('localhost', 8001))

            # Send the request line
            client_socket.send(request_line.encode())

            # Send the headers as a single string
            headers_str = "\r\n".join(headers) + "\r\n\r\n"
            client_socket.send(headers_str.encode())

            client_socket.close()  # simulate broken socket

            self.CHECK_SERVER_OKAY()

        except Exception as e:
            print(f"An error occurred: {e}")

    def test_server1(self):

        # Define the HTTP request parts
        request_line = "POST /test HTTP/1.1\r\n"
        headers = [
            "Host: example.com",
            "Content-Type: application/x-www-form-urlencoded",
            "Content-Length: 13"
        ]
        body = "param1=value1"

        try:
            # Create a socket
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            # Connect to the server
            client_socket.connect(('localhost', 8001))

            # Send the request line
            client_socket.send(request_line.encode())

            # Send the headers as a single string
            headers_str = "\r\n".join(headers) + "\r\n\r\n"
            client_socket.send(headers_str.encode())

            # Send the body
            client_socket.send(body.encode())

            client_socket.close()
            self.CHECK_SERVER_OKAY()

        except Exception as e:
            print(f"An error occurred: {e}")
