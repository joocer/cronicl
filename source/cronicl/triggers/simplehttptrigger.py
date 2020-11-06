"""
Simple HTTP Trigger

DO NOT USE IN PRODUCTION

Exposes a web server (default port 9000), passes values passes
values passed via the querystring as data into the flow.
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs


class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):

    def extract_query_string(self, path):
        return parse_qs(path[2:])

    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'Okay!')
        self.event_handler(self.extract_query_string(self.path))


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
        handler = SimpleHTTPRequestHandler
        httpd = HTTPServer(('localhost', self.port), handler)
        handler.event_handler = self.on_event
        httpd.serve_forever()

    def on_event(self, data):
        print(f"on event ({data})")


if __name__ == '__main__':
    s = SimpleHTTPTrigger()
    s.engage(None)
