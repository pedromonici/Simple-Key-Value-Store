from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import socketserver
import requests
import hashlib
import bisect
import urllib
import argparse

cluster_nodes = []

class http_server:
    def __init__(self, hash_ring, port):
        ProxyRequestHandler.hash_ring = hash_ring
        server_address = ('', port)
        server = HTTPServer(server_address, ProxyRequestHandler)
        print(f'Proxy server is running on http://localhost:{port}')
        server.serve_forever()

class ConsistentHashRing:
    def __init__(self, nodes, num_replicas=2):
        self.num_replicas = num_replicas
        self.nodes = []
        self.keys = []
        self.hash_ring = []
        for node in nodes:
            self.add_node(node)

    def add_node(self, node):
        self.nodes.append(node)
        for i in range(self.num_replicas):
            replica_key = self._get_replica_key(node, i)
            node_hash = self._hash(replica_key)
            self.keys.append(node_hash)
            self.hash_ring.append(node_hash)
        self.hash_ring.sort()

    def remove_node(self, node):
        index = self.nodes.index(node)
        start = index * self.num_replicas
        end = start + self.num_replicas
        del self.nodes[index]
        del self.keys[start:end]
        del self.hash_ring[start:end]

    def get_node(self, key):
        if not self.nodes:
            return None
        key_hash = self._hash(key)
        index = bisect.bisect_right(self.hash_ring, key_hash)
        if index == len(self.hash_ring):
            index = 0
        node_index = index // self.num_replicas
        return self.nodes[node_index]
        #return self.nodes[self.keys.index(self.hash_ring[index]) // self.num_replicas]

    def _get_replica_key(self, node, replica_num):
        return f'{node}:{replica_num}'

    def _hash(self, key):
        return int(hashlib.md5(str(key).encode()).hexdigest(), 16)


class ProxyRequestHandler(BaseHTTPRequestHandler):
    hash_ring = None

    def _forward_request(self, node, url, post_data=None):
        method = self.command
        headers = self.headers

        # Forward the request to the specified node
        response = requests.request(method, url, headers=headers, data=post_data)

        # Return the response from the node to the client
        self.send_response(response.status_code)
        for header, value in response.headers.items():
            self.send_header(header, value)
        self.end_headers()
        self.wfile.write(response.content)

    def do_GET(self):
        if self.path.startswith('/get'):
            key = self._parse_key_from_path()
            # Determine the node that contains the key
            node = self.hash_ring.get_node(key)
            if node is not None:
                url = f'http://{node}/get?key={key}'
                self._forward_request(node, url)
            else:
                self.send_response(404)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'message': 'Key not found.'}).encode())
        else:
            self.send_response(404)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'message': 'Endpoint not found.'}).encode())

    def do_PUT(self):
        if self.path == '/set':
            # Route the PUT request to the appropriate node
            post_data = self._parse_post_data()
            key, value = self._parse_key_and_value_from_request(post_data)
            node = self.hash_ring.get_node(key)

            if node is not None:
                url = f'http://{node}/set'
                self._forward_request(node, url, post_data)
            else:
                self.send_response(400)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'error': 'Cannot determine node for key.'}).encode())
        else:
            self.send_response(404)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'message': 'Endpoint not found.'}).encode())

    def do_DELETE(self):
        if self.path.startswith('/delete'):
            key = self._parse_key_from_path()
            # Determine the node that contains the key
            node = self.hash_ring.get_node(key)
            if node is not None:
                url = f'http://{node}/delete?key={key}'
                self._forward_request(node, url)
            else:
                self.send_response(404)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'message': 'Key not found.'}).encode())
        else:
            self.send_response(404)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'message': 'Endpoint not found.'}).encode())

    def _parse_key_from_path(self):
        key = self.path.split('?')[1].split('=')[1]
        return key

    def _parse_post_data(self):
        content_length = int(self.headers.get('Content-Length', 0))
        post_data = self.rfile.read(content_length)
        return post_data

    def _parse_key_and_value_from_request(self, post_data):
        data = json.loads(post_data.decode())
        key = data.get('key')
        value = data.get('value')
        return key, value


def run_proxy_server(server_class=HTTPServer, handler_class=ProxyRequestHandler, dbs="8000", port=8080):
    for db in dbs.split(' '):
        cluster_nodes.append(f'localhost:{db}')

    hash_ring = ConsistentHashRing(cluster_nodes)
    server = http_server(hash_ring, port)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-port', '--port', dest='port', default='8080')
    parser.add_argument('-db', '--database_servers', dest='db', default='8000')
    args = parser.parse_args()

    run_proxy_server(port=int(args.port), dbs=args.db)

