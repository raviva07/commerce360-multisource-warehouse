from __future__ import annotations
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != '/customers':
            self.send_response(404); self.end_headers(); return
        payload=(ROOT/'data/input/customers_api.json').read_bytes()
        self.send_response(200); self.send_header('Content-Type','application/json'); self.send_header('Content-Length',str(len(payload))); self.end_headers(); self.wfile.write(payload)
    def log_message(self, *_): pass

def start_server(port: int = 0):
    server=ThreadingHTTPServer(('127.0.0.1',port),Handler)
    return server, server.server_address[1]
