"""
Mondelēz Hub Plant — Digital Twin Engine
Entry point: wires WebSocket server + telemetry pipeline + HTTP dashboard server.

Usage:
    python3 main.py [--ws-port 8765] [--http-port 8080] [--hz 1.0]

Then open: http://localhost:8080/
"""

import argparse
import json
import logging
import os
import signal
import sys
import time

# ── path setup ───────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(__file__))

from backend.ws_server          import WebSocketServer
from backend.telemetry_pipeline import TelemetryPipeline, DEFAULT_THRESHOLDS
from backend.http_server        import start_http_server

# ── logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("main")

THRESHOLDS_PATH = os.path.join(os.path.dirname(__file__), "config", "thresholds.json")


def load_thresholds() -> dict:
    """Load thresholds from config/thresholds.json, falling back to defaults."""
    if os.path.exists(THRESHOLDS_PATH):
        try:
            with open(THRESHOLDS_PATH) as f:
                t = json.load(f)
            logger.info(f"Loaded thresholds from {THRESHOLDS_PATH}")
            return t
        except Exception as e:
            logger.warning(f"Could not load thresholds ({e}), using defaults.")
    else:
        os.makedirs(os.path.dirname(THRESHOLDS_PATH), exist_ok=True)
        with open(THRESHOLDS_PATH, "w") as f:
            json.dump(DEFAULT_THRESHOLDS, f, indent=2)
        logger.info(f"Wrote default thresholds to {THRESHOLDS_PATH}")
    return DEFAULT_THRESHOLDS


def main():
    parser = argparse.ArgumentParser(description="Mondelēz Hub Digital Twin Engine")
    parser.add_argument("--ws-port",   type=int,   default=8765,  help="WebSocket port")
    parser.add_argument("--http-port", type=int,   default=8080,  help="Dashboard HTTP port")
    parser.add_argument("--hz",        type=float, default=1.0,   help="Telemetry tick rate (Hz)")
    parser.add_argument("--host",      type=str,   default="0.0.0.0", help="Bind host")
    args = parser.parse_args()

    thresholds = load_thresholds()

    logger.info("=" * 60)
    logger.info("  Mondelēz Hub Plant — Digital Twin Engine")
    logger.info("  Cadbury_Dairy_Milk_Main Production Line")
    logger.info("=" * 60)

    # ── Start WebSocket server ────────────────────────────────────────────────
    ws_server = WebSocketServer(host=args.host, port=args.ws_port)
    ws_server.start()

    # ── Start telemetry pipeline and wire broadcast ───────────────────────────
    pipeline = TelemetryPipeline(thresholds=thresholds, tick_rate_hz=args.hz)
    pipeline.register_callback(ws_server.broadcast)
    pipeline.start()

    # ── Start HTTP dashboard server ───────────────────────────────────────────
    start_http_server(host=args.host, port=args.http_port)

    logger.info(f"")
    logger.info(f"  WebSocket  →  ws://localhost:{args.ws_port}")
    logger.info(f"  Dashboard  →  http://localhost:{args.http_port}/")
    logger.info(f"  Tick rate  →  {args.hz} Hz")
    logger.info(f"")
    logger.info("  Press Ctrl+C to stop.")

    # ── Graceful shutdown ─────────────────────────────────────────────────────
    def _shutdown(sig, frame):
        logger.info("Shutting down…")
        pipeline.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # Keep main thread alive
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
