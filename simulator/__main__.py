"""Meter simulator process entry point.

The simulated meter content is deterministic and lives in
:mod:`simulator.meter`.  Control endpoints (all except ``/state`` need POST):

  POST /delay?ms=1500          delay every response by N ms
  POST /mode?value=reverse     answer a burst of requests in reverse order
  POST /offline?value=1|0      drop the client and refuse new connections
  POST /stall?count=1&ms=5000  hold the next N responses for N ms
  POST /window?ms=40           spacing applied when answering in reverse
  POST /reset                  restore normal behaviour
  GET  /state                  current settings plus the last frames seen
"""

from __future__ import annotations

import asyncio
import logging
import os

from common.httpjson import HttpJsonServer
from simulator.meter import MeterSimulator

log = logging.getLogger(__name__)

CONTROL_ACTIONS = {"delay", "mode", "offline", "stall", "window", "reset"}


def _query(target: str) -> tuple[str, dict[str, str]]:
    path, _, raw = target.partition("?")
    params: dict[str, str] = {}
    for item in raw.split("&") if raw else []:
        if not item:
            continue
        key, _, value = item.partition("=")
        params[key] = value
    return path, params


def _as_bool(value: str) -> bool:
    return value.strip().lower() in ("1", "true", "yes", "on")


def make_dispatcher(meter: MeterSimulator):
    async def dispatch(method: str, target: str) -> tuple[int, object]:
        base, params = _query(target)
        if base in ("/state", "/stats"):
            if method != "GET":
                return 405, {"error": "use GET"}
            return 200, meter.state()
        action = base.lstrip("/")
        if action not in CONTROL_ACTIONS:
            return 404, {"error": "not found"}
        if method != "POST":
            return 405, {"error": "use POST for control actions"}
        try:
            if action == "delay":
                await meter.apply(delay_ms=float(params.get("ms", "0")))
            elif action == "mode":
                await meter.apply(mode=params.get("value", "normal"))
            elif action == "offline":
                await meter.apply(offline=_as_bool(params.get("value", "0")))
            elif action == "stall":
                await meter.apply(stall_next=int(params.get("count", "1")),
                                  stall_ms=float(params.get("ms", "5000")))
            elif action == "window":
                await meter.apply(order_stagger_ms=float(params.get("ms", "40")))
            elif action == "reset":
                await meter.apply(delay_ms=0, mode="normal", offline=False,
                                  stall_next=0, stall_ms=5000)
        except (TypeError, ValueError) as exc:
            return 400, {"error": str(exc)}
        return 200, meter.state()

    return dispatch


async def serve() -> None:
    meter = MeterSimulator(
        host=os.environ.get("METER_LISTEN_HOST", "0.0.0.0"),
        port=int(os.environ.get("METER_LISTEN_PORT", "1502")),
    )
    control = HttpJsonServer(
        make_dispatcher(meter),
        host=os.environ.get("METER_CONTROL_HOST", "0.0.0.0"),
        port=int(os.environ.get("METER_CONTROL_PORT", "8082")),
    )
    await meter.start()
    await control.start()

    stop = asyncio.Event()
    import signal
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # pragma: no cover - non-POSIX
            pass
    await stop.wait()
    await control.stop()
    await meter.stop()


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(serve())


if __name__ == "__main__":
    main()
