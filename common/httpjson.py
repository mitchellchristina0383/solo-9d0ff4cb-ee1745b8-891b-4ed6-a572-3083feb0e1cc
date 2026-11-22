"""Tiny asyncio HTTP/1.1 server for the status/control endpoints.

The gateway and the meter simulator each need a small JSON endpoint; pulling in
a web framework for that is overkill.  This module implements just enough HTTP
for a browser or ``curl``: one request per connection, ``Content-Length``
framed responses, and a dispatch callback supplied by the caller.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Awaitable, Callable

log = logging.getLogger(__name__)

MAX_HEAD_BYTES = 16 * 1024
READ_TIMEOUT = 5.0

# (method, request target) -> (status, payload); payload is JSON-serialised.
# The target keeps its query string; handlers split it themselves.
Dispatcher = Callable[[str, str], Awaitable[tuple[int, Any]]]

REASONS = {
    200: "OK",
    400: "Bad Request",
    404: "Not Found",
    405: "Method Not Allowed",
    414: "URI Too Long",
    500: "Internal Server Error",
}


class HttpJsonServer:
    def __init__(self, dispatch: Dispatcher, host: str, port: int) -> None:
        self._dispatch = dispatch
        self._host = host
        self._port = port
        self._server: asyncio.base_events.Server | None = None

    @property
    def bound_port(self) -> int:
        assert self._server is not None
        return self._server.sockets[0].getsockname()[1]

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, self._host, self._port)
        log.info("http endpoint listening on %s:%d", self._host, self.bound_port)

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _handle(self, reader: asyncio.StreamReader,
                      writer: asyncio.StreamWriter) -> None:
        try:
            try:
                head = await asyncio.wait_for(
                    reader.readuntil(b"\r\n\r\n"), timeout=READ_TIMEOUT
                )
            except (asyncio.IncompleteReadError, asyncio.LimitOverrunError,
                    asyncio.TimeoutError):
                await self._respond(writer, 400, {"error": "malformed request"})
                return
            if len(head) > MAX_HEAD_BYTES:
                await self._respond(writer, 414, {"error": "request too long"})
                return
            request_line = head.split(b"\r\n", 1)[0].decode("latin-1")
            method, _, target = request_line.partition(" ")
            # Keep the query string: the meter's control endpoints take their
            # parameters from it.
            target = target.split(" ", 1)[0]
            try:
                status, payload = await self._dispatch(method.upper(), target)
            except Exception as exc:  # noqa: BLE001 - reported to the caller
                log.exception("dispatch failed for %s %s", method, target)
                status, payload = 500, {"error": str(exc)}
            await self._respond(writer, status, payload)
        except (ConnectionError, OSError) as exc:
            log.debug("http client went away: %s", exc)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    @staticmethod
    async def _respond(writer: asyncio.StreamWriter, status: int,
                       payload: Any) -> None:
        body = json.dumps(payload, default=str).encode()
        reason = REASONS.get(status, "Error")
        header = (
            f"HTTP/1.1 {status} {reason}\r\n"
            "Content-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n"
            "Cache-Control: no-store\r\n"
            "Connection: close\r\n\r\n"
        ).encode("latin-1")
        writer.write(header + body)
        await writer.drain()


class JsonStatusServer(HttpJsonServer):
    """GET-only convenience wrapper around :class:`HttpJsonServer`."""

    def __init__(self, provider: Callable[[], Any], host: str, port: int,
                 routes: dict[str, Callable[[], Any]] | None = None) -> None:
        self._provider = provider
        self._routes = routes or {}

        async def dispatch(method: str, target: str) -> tuple[int, Any]:
            if method != "GET":
                return 405, {"error": "only GET is supported"}
            path = target.split("?", 1)[0]
            provider = self._routes.get(path)
            if provider is None and path == "/stats":
                provider = self._provider
            if provider is None:
                return 404, {"error": "not found"}
            result = provider()
            if asyncio.iscoroutine(result):
                result = await result
            return 200, result

        super().__init__(dispatch, host, port)
