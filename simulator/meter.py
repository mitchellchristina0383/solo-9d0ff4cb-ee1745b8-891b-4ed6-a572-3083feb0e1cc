"""A scriptable Modbus/TCP meter used as the single upstream device.

It deliberately enforces the constraints of the real hardware:

* only one client connection at a time -- a second connection is refused;
* responses can be delayed, held past a caller's timeout, or reordered so a
  later request is answered before an earlier one;
* it can be taken offline (connection dropped, new connections refused) and
  brought back.

The HTTP control surface lives in :mod:`simulator.__main__`; everything here is
plain asyncio so the tests can drive it directly.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import struct
from dataclasses import dataclass, field

from common import modbus

log = logging.getLogger(__name__)

DEFAULT_ORDER_STAGGER_MS = 40.0


def register_value(address: int) -> int:
    """Deterministic register contents so readings can be asserted on."""
    return (address * 3 + 7) & 0xFFFF


@dataclass
class MeterSettings:
    delay_ms: float = 0.0
    mode: str = "normal"  # "normal" or "reverse"
    offline: bool = False
    stall_next: int = 0
    stall_ms: float = 5000.0
    order_stagger_ms: float = DEFAULT_ORDER_STAGGER_MS


@dataclass
class _Connection:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    peer: str
    inbox: asyncio.Queue = field(default_factory=asyncio.Queue)
    inflight: set[asyncio.Task] = field(default_factory=set)
    worker: asyncio.Task | None = None
    open: bool = True


class MeterSimulator:
    def __init__(self, host: str = "0.0.0.0", port: int = 1502,
                 request_window: float = 0.05) -> None:
        self.host = host
        self.port = port
        self.request_window = request_window
        self.settings = MeterSettings()
        self.received: list[dict] = []
        self.sent: list[dict] = []
        self.connections_accepted = 0
        self.connections_rejected = 0
        self._server: asyncio.base_events.Server | None = None
        self._conn: _Connection | None = None
        self._lock = asyncio.Lock()

    @property
    def bound_port(self) -> int:
        assert self._server is not None
        return self._server.sockets[0].getsockname()[1]

    @property
    def connected(self) -> bool:
        return self._conn is not None and self._conn.open

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._on_client, self.host, self.port)
        log.info("meter simulator listening on %s:%d", self.host, self.bound_port)

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        conn = self._conn
        if conn is not None:
            await self._close_connection(conn)

    async def apply(self, **changes) -> None:
        """Update settings; ``offline`` changes take effect immediately."""
        if "delay_ms" in changes and changes["delay_ms"] is not None:
            self.settings.delay_ms = max(0.0, float(changes["delay_ms"]))
        if "mode" in changes and changes["mode"] is not None:
            mode = str(changes["mode"]).lower()
            if mode not in ("normal", "reverse"):
                raise ValueError("mode must be 'normal' or 'reverse'")
            self.settings.mode = mode
        if "stall_next" in changes and changes["stall_next"] is not None:
            self.settings.stall_next = max(0, int(changes["stall_next"]))
        if "stall_ms" in changes and changes["stall_ms"] is not None:
            self.settings.stall_ms = max(0.0, float(changes["stall_ms"]))
        if "order_stagger_ms" in changes and changes["order_stagger_ms"] is not None:
            self.settings.order_stagger_ms = max(0.0, float(changes["order_stagger_ms"]))
        if "offline" in changes and changes["offline"] is not None:
            await self.set_offline(bool(changes["offline"]))

    async def set_offline(self, offline: bool) -> None:
        self.settings.offline = offline
        if offline:
            conn = self._conn
            if conn is not None:
                log.info("meter going offline: dropping connection from %s", conn.peer)
                await self._close_connection(conn)
        else:
            log.info("meter back online, accepting connections")

    def state(self) -> dict:
        return {
            "connected": self.connected,
            "client": self._conn.peer if self._conn else None,
            "connections_accepted": self.connections_accepted,
            "connections_rejected": self.connections_rejected,
            "settings": {
                "delay_ms": self.settings.delay_ms,
                "mode": self.settings.mode,
                "offline": self.settings.offline,
                "stall_next": self.settings.stall_next,
                "stall_ms": self.settings.stall_ms,
                "order_stagger_ms": self.settings.order_stagger_ms,
            },
            "received": self.received[-20:],
            "sent": self.sent[-20:],
        }

    async def _on_client(self, reader: asyncio.StreamReader,
                         writer: asyncio.StreamWriter) -> None:
        peer_info = writer.get_extra_info("peername")
        peer = f"{peer_info[0]}:{peer_info[1]}" if peer_info else "unknown"
        if self.settings.offline:
            self.connections_rejected += 1
            log.info("refusing %s: meter is offline", peer)
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            return
        if self._conn is not None and self._conn.open:
            # The real meter only allows a single connection.
            self.connections_rejected += 1
            log.info("refusing %s: meter already has a client (%s)", peer,
                     self._conn.peer)
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            return

        conn = _Connection(reader=reader, writer=writer, peer=peer)
        self._conn = conn
        self.connections_accepted += 1
        log.info("meter accepted connection from %s", peer)
        conn.worker = asyncio.create_task(self._worker(conn), name="meter-worker")
        read_task = asyncio.create_task(self._read_loop(conn), name="meter-conn-reader")
        try:
            await read_task
        finally:
            await self._close_connection(conn)

    async def _read_loop(self, conn: _Connection) -> None:
        buffered = b""
        while True:
            try:
                chunk = await conn.reader.read(4096)
            except (ConnectionError, OSError):
                return
            if not chunk:
                return
            buffered += chunk
            try:
                frames, buffered = modbus.split_stream(buffered)
            except modbus.ModbusDecodeError as exc:
                log.warning("meter: bad stream from %s: %s", conn.peer, exc)
                return
            for raw in frames:
                try:
                    frame = modbus.parse_adu(raw)
                except modbus.ModbusDecodeError as exc:
                    log.warning("meter: bad frame from %s: %s", conn.peer, exc)
                    return
                conn.inbox.put_nowait(frame)
                self._record_received(frame)

    async def _worker(self, conn: _Connection) -> None:
        while conn.open:
            first = await conn.inbox.get()
            batch = [first]
            if self.settings.mode == "reverse" and self.request_window > 0:
                await asyncio.sleep(self.request_window)
                while not conn.inbox.empty():
                    batch.append(conn.inbox.get_nowait())
            for index, frame in enumerate(batch):
                self._schedule_response(conn, frame, index, len(batch))

    def _schedule_response(self, conn: _Connection, frame: modbus.RawFrame,
                           index: int, batch_size: int) -> None:
        settings = self.settings
        delay = settings.delay_ms / 1000.0
        if settings.stall_next > 0:
            settings.stall_next -= 1
            delay = max(delay, settings.stall_ms / 1000.0)
        if settings.mode == "reverse":
            # Later arrivals in the batch are answered first.
            delay += (batch_size - 1 - index) * settings.order_stagger_ms / 1000.0
        else:
            delay += index * 0.001
        task = asyncio.create_task(self._respond_later(conn, frame, delay))
        conn.inflight.add(task)
        task.add_done_callback(conn.inflight.discard)

    async def _respond_later(self, conn: _Connection, frame: modbus.RawFrame,
                             delay: float) -> None:
        try:
            if delay > 0:
                await asyncio.sleep(delay)
            if not conn.open or self.settings.offline:
                log.info("meter: suppressed response (connection closed/offline)")
                return
            response = self._build_response(frame)
            conn.writer.write(response)
            await conn.writer.drain()
            self._record_sent(frame, response)
        except (ConnectionError, OSError) as exc:
            log.info("meter: write failed on %s: %s", conn.peer, exc)

    def _record_received(self, frame: modbus.RawFrame) -> None:
        entry = {
            "txid": frame.transaction_id,
            "unit": frame.unit_id,
            "function": frame.function_code,
            "pdu": frame.pdu.hex(),
        }
        self.received.append(entry)
        del self.received[:-200]

    def _record_sent(self, frame: modbus.RawFrame, response: bytes) -> None:
        self.sent.append({
            "txid": frame.transaction_id,
            "bytes": response.hex(),
        })
        del self.sent[:-200]

    def _build_response(self, frame: modbus.RawFrame) -> bytes:
        if frame.function_code != modbus.FC_READ_HOLDING_REGISTERS:
            return modbus.ExceptionResponse(
                frame.transaction_id, frame.unit_id, frame.function_code,
                modbus.EXCEPTION_ILLEGAL_FUNCTION,
            ).encode()
        if len(frame.pdu) != 5:
            return modbus.ExceptionResponse(
                frame.transaction_id, frame.unit_id, frame.function_code,
                modbus.EXCEPTION_ILLEGAL_DATA_VALUE,
            ).encode()
        _, start, quantity = struct.unpack(">BHH", frame.pdu)
        if quantity < 1 or quantity > 125 or start + quantity > 0x10000:
            return modbus.ExceptionResponse(
                frame.transaction_id, frame.unit_id, frame.function_code,
                modbus.EXCEPTION_ILLEGAL_DATA_ADDRESS,
            ).encode()
        values = tuple(register_value(addr) for addr in range(start, start + quantity))
        return modbus.ReadHoldingResponse(
            frame.transaction_id, frame.unit_id, values
        ).encode()

    async def _close_connection(self, conn: _Connection) -> None:
        if not conn.open:
            return
        conn.open = False
        if self._conn is conn:
            self._conn = None
        if conn.worker is not None:
            conn.worker.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await conn.worker
        for task in list(conn.inflight):
            task.cancel()
        if conn.inflight:
            await asyncio.gather(*conn.inflight, return_exceptions=True)
        try:
            conn.writer.close()
            await conn.writer.wait_closed()
        except Exception:
            pass
        log.info("meter closed connection from %s", conn.peer)
