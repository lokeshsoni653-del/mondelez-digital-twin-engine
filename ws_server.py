"""
Mondelēz Hub Plant — Digital Twin WebSocket Engine
Pure Python stdlib implementation (asyncio + socket)
RFC 6455 compliant WebSocket server with no external dependencies.
"""

import asyncio
import hashlib
import base64
import struct
import json
import socket
import threading
import logging
from typing import Set

logger = logging.getLogger("ws_server")


def _ws_handshake_response(key: str) -> bytes:
    """Compute RFC 6455 Sec-WebSocket-Accept and return HTTP upgrade response."""
    magic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
    sha1 = hashlib.sha1((key + magic).encode()).digest()
    accept = base64.b64encode(sha1).decode()
    resp = (
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Accept: {accept}\r\n"
        "Access-Control-Allow-Origin: *\r\n"
        "\r\n"
    )
    return resp.encode()


def _decode_ws_frame(data: bytes):
    """Decode a WebSocket frame. Returns (opcode, payload) or None if incomplete."""
    if len(data) < 2:
        return None
    b0, b1 = data[0], data[1]
    # opcode in lower 4 bits of first byte
    opcode = b0 & 0x0F
    masked = (b1 & 0x80) != 0
    payload_len = b1 & 0x7F

    offset = 2
    if payload_len == 126:
        if len(data) < offset + 2:
            return None
        payload_len = struct.unpack(">H", data[offset:offset+2])[0]
        offset += 2
    elif payload_len == 127:
        if len(data) < offset + 8:
            return None
        payload_len = struct.unpack(">Q", data[offset:offset+8])[0]
        offset += 8

    mask_key = b""
    if masked:
        if len(data) < offset + 4:
            return None
        mask_key = data[offset:offset+4]
        offset += 4

    if len(data) < offset + payload_len:
        return None

    payload = bytearray(data[offset:offset + payload_len])
    if masked:
        for i in range(len(payload)):
            payload[i] ^= mask_key[i % 4]

    return opcode, bytes(payload)


def _encode_ws_frame(data: str) -> bytes:
    """Encode a text WebSocket frame (server → client, unmasked)."""
    payload = data.encode("utf-8")
    length = len(payload)
    header = bytearray()
    header.append(0x81)  # FIN + text opcode
    if length <= 125:
        header.append(length)
    elif length <= 65535:
        header.append(126)
        header += struct.pack(">H", length)
    else:
        header.append(127)
        header += struct.pack(">Q", length)
    return bytes(header) + payload


def _encode_ws_close() -> bytes:
    """Encode a WebSocket close frame."""
    return b"\x88\x00"


class WebSocketClient:
    """Represents a single connected WebSocket client."""

    def __init__(self, conn: socket.socket, addr):
        self.conn = conn
        self.addr = addr
        self.alive = True
        self._lock = threading.Lock()

    def send(self, message: str) -> bool:
        """Thread-safe send. Returns False if client is dead."""
        if not self.alive:
            return False
        try:
            frame = _encode_ws_frame(message)
            with self._lock:
                self.conn.sendall(frame)
            return True
        except Exception:
            self.alive = False
            return False

    def close(self):
        self.alive = False
        try:
            self.conn.sendall(_encode_ws_close())
        except Exception:
            pass
        try:
            self.conn.close()
        except Exception:
            pass


class WebSocketServer:
    """
    Zero-dependency WebSocket server.
    Manages client connections and broadcasts messages to all subscribers.
    """

    def __init__(self, host: str = "0.0.0.0", port: int = 8765):
        self.host = host
        self.port = port
        self.clients: Set[WebSocketClient] = set()
        self._lock = threading.Lock()
        self._running = False

    def broadcast(self, message: str):
        """Send message to all alive clients; prune dead ones."""
        dead = set()
        with self._lock:
            snapshot = set(self.clients)
        for client in snapshot:
            if not client.send(message):
                dead.add(client)
        if dead:
            with self._lock:
                self.clients -= dead
            for c in dead:
                logger.debug(f"Pruned dead client {c.addr}")

    def _handle_client(self, conn: socket.socket, addr):
        """Handle one WebSocket connection lifecycle in its own thread."""
        logger.info(f"New connection from {addr}")
        # --- HTTP upgrade handshake ---
        try:
            raw = conn.recv(4096).decode("utf-8", errors="replace")
        except Exception:
            conn.close()
            return

        ws_key = None
        for line in raw.split("\r\n"):
            if line.lower().startswith("sec-websocket-key:"):
                ws_key = line.split(":", 1)[1].strip()
                break

        if not ws_key:
            # Serve a simple 404 for non-WS requests (healthcheck)
            try:
                conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK")
            except Exception:
                pass
            conn.close()
            return

        try:
            conn.sendall(_ws_handshake_response(ws_key))
        except Exception:
            conn.close()
            return

        client = WebSocketClient(conn, addr)
        with self._lock:
            self.clients.add(client)

        # --- Read loop (handle ping/pong and close frames) ---
        buf = b""
        conn.settimeout(60)
        try:
            while client.alive:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                buf += chunk
                result = _decode_ws_frame(buf)
                if result is None:
                    continue
                opcode, payload = result
                buf = b""
                if opcode == 0x8:  # Close
                    break
                elif opcode == 0x9:  # Ping → Pong
                    pong = bytearray([0x8A, len(payload)]) + payload
                    client.conn.sendall(bytes(pong))
        except Exception:
            pass
        finally:
            client.alive = False
            with self._lock:
                self.clients.discard(client)
            try:
                conn.close()
            except Exception:
                pass
            logger.info(f"Client {addr} disconnected")

    def start(self):
        """Start the WebSocket server in a background daemon thread."""
        self._running = True
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((self.host, self.port))
        server_sock.listen(50)
        logger.info(f"WebSocket server listening on ws://{self.host}:{self.port}")

        def _accept_loop():
            while self._running:
                try:
                    conn, addr = server_sock.accept()
                    t = threading.Thread(
                        target=self._handle_client,
                        args=(conn, addr),
                        daemon=True
                    )
                    t.start()
                except Exception as e:
                    if self._running:
                        logger.error(f"Accept error: {e}")

        t = threading.Thread(target=_accept_loop, daemon=True)
        t.start()
