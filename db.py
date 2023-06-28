from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import socketserver
import argparse


class DistributedKeyValueStore:
    def __init__(self):
        self.store = {}

    def set(self, key, value):
        self.store[key] = value

    def get(self, key):
        return self.store.get(key)

    def delete(self, key):
        if key in self.store:
            del self.store[key]

    def get_all_pairs(self):
        return self.store


class NodeRequestHandler(BaseHTTPRequestHandler):
    store = DistributedKeyValueStore()

    def _set_key(self, data):
        key = data.get('key')
        value = data.get('value')
        if key and value:
            self.store.set(key, value)
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'message': 'Key stored successfully.'}).encode())
        else:
            self.send_response(400)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'error': 'Invalid key-value pair.'}).encode())

    def _get_key(self, key):
        value = self.store.get(key)
        if value is not None:
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'value': value}).encode())
        else:
            self.send_response(404)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'message': 'Key not found.'}).encode())

    def _delete_key(self, key):
        self.store.delete(key)
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({'message': 'Key deleted successfully.'}).encode())

    def _get_node_pairs(self):
        pairs = self.store.get_all_pairs()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({'Key-Value Pairs': pairs}).encode())

    def do_PUT(self):
        if self.path == '/set':
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data.decode())
            self._set_key(data)
            print(self.store.store)
        else:
            self.send_response(404)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'message': 'Endpoint not found.'}).encode())

    def do_GET(self):
        if self.path == '/get':
            key = self.path.split('?')[1].split('=')[1]
            self._get_key(key)
        elif self.path == '/get_node_pairs':
            self._get_node_pairs()
        else:
            self.send_response(404)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'message': 'Endpoint not found.'}).encode())

    def do_DELETE(self):
        if self.path.startswith('/delete'):
            key = self.path.split('?')[1].split('=')[1]
            self._delete_key(key)
        else:
            self.send_response(404)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'message': 'Endpoint not found.'}).encode())


def run_node_server(server_class=HTTPServer, handler_class=NodeRequestHandler, port=8000):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print(f'Node server is running on http://localhost:{port}')
    httpd.serve_forever()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-port', '--port', dest='port', default='8000')
    args = parser.parse_args()

    run_node_server(port=int(args.port))
