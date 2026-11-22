"""Gateway process entry point.

Environment variables (all optional):

METER_HOST, METER_PORT            upstream meter (default 127.0.0.1:1502)
GATEWAY_LISTEN_HOST/PORT          Modbus side of the gateway (default 0.0.0.0:5020)
GATEWAY_REQUEST_TIMEOUT           seconds to wait for the meter (default 3.0)
GATEWAY_CONNECT_TIMEOUT           seconds for the meter TCP connect (default 2.0)
GATEWAY_RECONNECT_DELAY           pause between reconnect attempts (default 0.5)
GATEWAY_MAX_INFLIGHT              per-device in-flight cap (default 4096)
GATEWAY_STATUS_HOST/PORT          JSON status endpoint (default 0.0.0.0:8081)

The status endpoint serves GET /stats and GET /health, plus the demo controls
POST /pause and POST /resume, which hold and release the upstream write loop
so the send-queue behaviour can be observed live.
"""

from __future__ import annotations

import asyncio
import logging
import os

from common.httpjson import HttpJsonServer
from gateway.core import GatewayConfig, ModbusGateway


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def config_from_env() -> GatewayConfig:
    return GatewayConfig(
        meter_host=os.environ.get("METER_HOST", "127.0.0.1"),
        meter_port=_env_int("METER_PORT", 1502),
        listen_host=os.environ.get("GATEWAY_LISTEN_HOST", "0.0.0.0"),
        listen_port=_env_int("GATEWAY_LISTEN_PORT", 5020),
        request_timeout=_env_float("GATEWAY_REQUEST_TIMEOUT", 3.0),
        connect_timeout=_env_float("GATEWAY_CONNECT_TIMEOUT", 2.0),
        reconnect_delay=_env_float("GATEWAY_RECONNECT_DELAY", 0.5),
        max_inflight=_env_int("GATEWAY_MAX_INFLIGHT", 4096),
    )


def make_dispatcher(gateway: ModbusGateway):
    """GET for status, POST for the send-gate demo control."""

    async def dispatch(method: str, target: str) -> tuple[int, object]:
        path = target.split("?", 1)[0]
        if method == "GET":
            if path == "/stats":
                return 200, gateway.stats_snapshot()
            if path == "/health":
                return 200, {"ok": True}
            return 404, {"error": "not found"}
        if method == "POST":
            if path == "/pause":
                gateway.set_send_paused(True)
                return 200, gateway.stats_snapshot()
            if path == "/resume":
                gateway.set_send_paused(False)
                return 200, gateway.stats_snapshot()
            return 404, {"error": "not found"}
        return 405, {"error": "unsupported method"}

    return dispatch


async def serve() -> None:
    gateway = ModbusGateway(config_from_env())
    status = HttpJsonServer(
        make_dispatcher(gateway),
        host=os.environ.get("GATEWAY_STATUS_HOST", "0.0.0.0"),
        port=_env_int("GATEWAY_STATUS_PORT", 8081),
    )
    await gateway.start()
    await status.start()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    import signal

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # pragma: no cover - Windows
            pass
    await stop.wait()
    logging.getLogger(__name__).info("shutting down")
    await status.stop()
    await gateway.stop()


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(serve())


if __name__ == "__main__":
    main()
