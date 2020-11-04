from http.server import HTTPServer, BaseHTTPRequestHandler
import socketserver


class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'Hello, world!')


class SimpleHTTPTrigger():
    """
    Simple HTTP Trigger

    DO NOT USE IN PRODUCTION

    Listens on the specified port, passes the values in the query
    string into a pipeline.
    """
    def __init__(self, port=9000):
        self.port = port


    def engage(self, flow):
        httpd = HTTPServer(('localhost', self.port), SimpleHTTPRequestHandler)
        httpd.serve_forever()


if __name__ == '__main__':
    s = SimpleHTTPTrigger()
    s.engage(None)
