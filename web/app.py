"""Flask demo UI.

Two sample reads can be started at the same time.  Each read opens its own TCP
connection to the gateway, and both are given the same transaction id by
default, which is exactly the case the gateway has to disambiguate.  The meter
controls (delay / stall / offline / reverse order) go to the simulator's control
port.

The page never talks Modbus to the meter directly; only the gateway does.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from flask import Flask, jsonify, render_template, request

from common import client, modbus

app = Flask(__name__)

GATEWAY_HOST = os.environ.get("UI_GATEWAY_HOST", "127.0.0.1")
GATEWAY_PORT = int(os.environ.get("UI_GATEWAY_PORT", "5020"))
GATEWAY_STATUS_HOST = os.environ.get("UI_GATEWAY_STATUS_HOST", "127.0.0.1")
GATEWAY_STATUS_PORT = int(os.environ.get("UI_GATEWAY_STATUS_PORT", "8081"))
METER_CONTROL_HOST = os.environ.get("UI_METER_HOST", "127.0.0.1")
METER_CONTROL_PORT = int(os.environ.get("UI_METER_CONTROL_PORT", "8082"))
CLIENT_TIMEOUT = float(os.environ.get("UI_CLIENT_TIMEOUT", "6"))


def _now() -> float:
    return time.monotonic() * 1000.0


def _read_one(spec: dict) -> dict:
    started = _now()
    try:
        result = client.read_holding_registers(
            GATEWAY_HOST,
            GATEWAY_PORT,
            address=int(spec.get("address", 0)),
            quantity=int(spec.get("quantity", 2)),
            transaction_id=int(spec.get("transaction_id", 1)) & 0xFFFF,
            unit_id=int(spec.get("unit_id", 1)) & 0xFF,
            timeout=CLIENT_TIMEOUT,
        )
        return {"ok": True, **result.as_dict()}
    except client.ClientTimeout as exc:
        return {
            "ok": False,
            "error": str(exc),
            "error_kind": "timeout",
            "elapsed_ms": round(_now() - started, 1),
        }
    except modbus.ModbusExceptionPDU as exc:
        return {
            "ok": False,
            "error": str(exc),
            "error_kind": "exception",
            "exception_code": exc.exception_code,
            "exception_name": modbus.EXCEPTION_NAMES.get(exc.exception_code),
            "elapsed_ms": round(_now() - started, 1),
        }
    except Exception as exc:  # noqa: BLE001 - shown on the page
        return {
            "ok": False,
            "error": str(exc),
            "error_kind": "client",
            "elapsed_ms": round(_now() - started, 1),
        }


def _read_concurrently(specs: list[dict]) -> list[dict]:
    """Run every spec in its own thread, released by one shared barrier."""
    results: list[dict | None] = [None] * len(specs)
    barrier = threading.Barrier(len(specs))

    def run(index: int, spec: dict) -> None:
        barrier.wait(timeout=5)
        results[index] = _read_one(spec)

    threads = [
        threading.Thread(target=run, args=(index, spec), daemon=True)
        for index, spec in enumerate(specs)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(CLIENT_TIMEOUT + 3.0)
    return [r if r is not None else {"ok": False, "error": "read thread did not finish",
                                     "error_kind": "client"}
            for r in results]


@app.get("/")
def index():
    return render_template("index.html")


@app.get("/api/gateway/state")
def api_gateway_state():
    try:
        with urllib.request.urlopen(
            f"http://{GATEWAY_STATUS_HOST}:{GATEWAY_STATUS_PORT}/stats", timeout=3
        ) as response:
            return jsonify(json.loads(response.read().decode()))
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 502


@app.post("/api/read")
def api_read():
    spec = request.get_json(silent=True) or {}
    return jsonify(_read_one(spec))


@app.post("/api/read_both")
def api_read_both():
    body = request.get_json(silent=True) or {}
    specs = body.get("reads") or []
    if not 1 <= len(specs) <= 8:
        return jsonify({"error": "provide 1..8 reads"}), 400
    return jsonify({"reads": _read_concurrently(specs)})


@app.get("/api/meter/state")
def api_meter_state():
    try:
        with urllib.request.urlopen(
            f"http://{METER_CONTROL_HOST}:{METER_CONTROL_PORT}/state", timeout=3
        ) as response:
            return jsonify(json.loads(response.read().decode()))
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 502


@app.post("/api/meter/<action>")
def api_meter(action: str):
    allowed = {"delay", "mode", "offline", "stall", "reset", "window"}
    if action not in allowed:
        return jsonify({"ok": False, "error": "unknown action"}), 404
    payload = request.get_json(silent=True) or {}
    query = urllib.parse.urlencode(payload)
    url = f"http://{METER_CONTROL_HOST}:{METER_CONTROL_PORT}/{action}"
    if query:
        url += f"?{query}"
    req = urllib.request.Request(url, method="POST", data=b"")
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            body = response.read().decode()
            return jsonify(json.loads(body) if body else {})
    except urllib.error.HTTPError as exc:
        body = exc.read().decode()
        return jsonify({"ok": False, "error": body}), exc.code
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": f"meter control unreachable: {exc}"}), 502


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    app.run(
        host=os.environ.get("UI_HOST", "0.0.0.0"),
        port=int(os.environ.get("UI_PORT", "8080")),
        threaded=True,
    )


if __name__ == "__main__":
    main()
