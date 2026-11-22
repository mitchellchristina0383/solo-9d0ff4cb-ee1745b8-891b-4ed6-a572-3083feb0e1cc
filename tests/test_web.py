"""End-to-end tests driving the Flask UI against a live gateway + meter.

The gateway and meter run in a background thread with their own event loop, so
these tests exercise exactly the paths the browser would.
"""

from __future__ import annotations

import asyncio
import socket
import threading

import pytest

from gateway.core import GatewayConfig, ModbusGateway
from simulator.meter import MeterSimulator, register_value
from web import app as web_app


class BackgroundRig:
    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()
        self.meter: MeterSimulator | None = None
        self.gateway: ModbusGateway | None = None

    def run(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout=10)

    def start(self) -> None:
        async def _start():
            meter = MeterSimulator("127.0.0.1", 0)
            await meter.start()
            gateway = ModbusGateway(GatewayConfig(
                meter_host="127.0.0.1", meter_port=meter.bound_port,
                listen_host="127.0.0.1", listen_port=0,
                request_timeout=0.8, connect_timeout=0.5,
                reconnect_delay=0.1, late_response_quarantine=0.3,
            ))
            await gateway.start()
            for _ in range(100):
                if gateway.upstream_connected:
                    break
                await asyncio.sleep(0.02)
            assert gateway.upstream_connected
            return meter, gateway

        self.meter, self.gateway = self.run(_start())

    def stop(self) -> None:
        if self.gateway is not None:
            self.run(self.gateway.stop())
        if self.meter is not None:
            self.run(self.meter.stop())
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout=5)


@pytest.fixture
def rig(monkeypatch):
    rig = BackgroundRig()
    rig.start()
    monkeypatch.setattr(web_app, "GATEWAY_HOST", "127.0.0.1")
    monkeypatch.setattr(web_app, "GATEWAY_PORT", rig.gateway.listen_port)
    monkeypatch.setattr(web_app, "GATEWAY_STATUS_HOST", "127.0.0.1")
    monkeypatch.setattr(web_app, "GATEWAY_STATUS_PORT", _unused_port())
    monkeypatch.setattr(web_app, "METER_CONTROL_HOST", "127.0.0.1")
    monkeypatch.setattr(web_app, "METER_CONTROL_PORT", _unused_port())
    monkeypatch.setattr(web_app, "CLIENT_TIMEOUT", 3.0)
    try:
        yield rig
    finally:
        rig.stop()


def _unused_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_index_page_renders(rig):
    client = web_app.app.test_client()
    response = client.get("/")
    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "同时读取两路" in body
    assert "app.js" in body


def test_read_both_with_identical_transaction_ids(rig):
    rig.run(rig.meter.apply(mode="reverse", order_stagger_ms=60))
    client = web_app.app.test_client()
    response = client.post("/api/read_both", json={"reads": [
        {"address": 0, "quantity": 2, "transaction_id": 7, "unit_id": 1},
        {"address": 100, "quantity": 3, "transaction_id": 7, "unit_id": 1},
    ]})
    assert response.status_code == 200
    reads = response.get_json()["reads"]
    assert reads[0]["ok"] and reads[1]["ok"]
    assert reads[0]["transaction_id"] == 7 and reads[1]["transaction_id"] == 7
    assert reads[0]["registers"] == [register_value(0), register_value(1)]
    assert reads[1]["registers"] == [register_value(a) for a in (100, 101, 102)]


def test_ui_surfaces_timeout(rig):
    rig.run(rig.meter.apply(stall_next=1, stall_ms=1200))
    client = web_app.app.test_client()
    response = client.post("/api/read", json={
        "address": 0, "quantity": 1, "transaction_id": 3, "unit_id": 1,
    })
    payload = response.get_json()
    assert payload["ok"] is False
    assert payload["exception_code"] == 0x0B
    assert payload["elapsed_ms"] >= 700


def test_ui_surfaces_disconnect_and_recovery(rig):
    rig.run(rig.meter.apply(offline=True))
    client = web_app.app.test_client()
    payload = client.post("/api/read", json={
        "address": 0, "quantity": 1, "transaction_id": 4, "unit_id": 1,
    }).get_json()
    assert payload["ok"] is False
    assert payload["exception_code"] in (0x0A, 0x0B)

    rig.run(rig.meter.apply(offline=False))
    for _ in range(100):
        if rig.gateway.upstream_connected:
            break
        threading.Event().wait(0.05)
    payload = client.post("/api/read", json={
        "address": 5, "quantity": 2, "transaction_id": 4, "unit_id": 1,
    }).get_json()
    assert payload["ok"] is True
    assert payload["registers"] == [register_value(5), register_value(6)]


def test_meter_state_endpoint_is_bridged(rig, monkeypatch):
    """The UI's /api/meter/state proxy should return the simulator state."""
    import json

    # Start a throwaway status server on the simulator's control port.
    port = _unused_port()

    async def serve():
        async def handler(reader, writer):
            await reader.readuntil(b"\r\n\r\n")
            body = json.dumps(rig.meter.state()).encode()
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                b"Content-Length: " + str(len(body)).encode() + b"\r\n"
                b"Connection: close\r\n\r\n" + body
            )
            await writer.drain()
            writer.close()

        return await asyncio.start_server(handler, "127.0.0.1", port)

    server = rig.run(serve())
    monkeypatch.setattr(web_app, "METER_CONTROL_PORT", port)
    try:
        client = web_app.app.test_client()
        payload = client.get("/api/meter/state").get_json()
        assert "settings" in payload
        assert payload["settings"]["mode"] in ("normal", "reverse")
    finally:
        async def stop():
            server.close()
            await server.wait_closed()
        rig.run(stop())


def test_gateway_state_endpoint_reports_stats(rig, monkeypatch):
    status_port = _unused_port()
    from common.httpjson import JsonStatusServer

    async def serve():
        server = JsonStatusServer(rig.gateway.stats_snapshot, "127.0.0.1", status_port)
        await server.start()
        return server

    server = rig.run(serve())
    monkeypatch.setattr(web_app, "GATEWAY_STATUS_PORT", status_port)
    try:
        client = web_app.app.test_client()
        payload = client.get("/api/gateway/state").get_json()
        assert payload["upstream_connected"] is True
        assert "timeouts_total" in payload
    finally:
        rig.run(server.stop())
