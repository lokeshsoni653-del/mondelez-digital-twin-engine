"""
Mondelēz Hub Plant — Static File Server
Serves the frontend dashboard over HTTP on a configurable port.
Uses Python's built-in http.server with CORS headers for local dev.
"""

import http.server
import threading
import os
import mimetypes
import logging

logger = logging.getLogger("http_server")

mimetypes.add_type("application/javascript", ".js")
mimetypes.add_type("text/css", ".css")


class CORSHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    """SimpleHTTPRequestHandler with permissive CORS and custom root directory."""

    BASE_DIR = os.path.join(os.path.dirname(__file__), "..", "frontend")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=self.BASE_DIR, **kwargs)

    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        super().end_headers()

    def log_message(self, fmt, *args):
        logger.debug(f"HTTP {fmt % args}")


def start_http_server(host: str = "0.0.0.0", port: int = 8080):
    """Start the static HTTP server in a daemon thread."""
    server = http.server.HTTPServer((host, port), CORSHTTPRequestHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    logger.info(f"Dashboard served at http://{host}:{port}/")
    return server
