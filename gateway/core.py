"""Only FC03 is forwarded, and only to one physical meter connection.

The hard part is that two clients may pick the same transaction id.  The
gateway therefore rewrites the transaction id on the meter link to a value that
is unique among all requests currently in flight, keeps a map, and restores the
client's original id when the answer comes back.  Answers that no longer have a
live map entry -- because the request timed out or the client went away -- are
dropped.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import struct
from dataclasses import asdict, dataclass, field

from common import modbus

log = logging.getLogger(__name__)


@dataclass
class GatewayConfig:
    meter_host: str = "127.0.0.1"
    meter_port: int = 1502
    listen_host: str = "0.0.0.0"
    listen_port: int = 5020
    request_timeout: float = 3.0
    connect_timeout: float = 2.0
    reconnect_delay: float = 0.5
    max_inflight: int = 4096
    max_client_buffer: int = 64 * 1024
    # How long an abandoned meter-side transaction id stays reserved.  The
    # meter cannot tell us when it will give up on a request, so this is a
    # best-effort bound: it must exceed ``request_timeout`` by a comfortable
    # margin, otherwise a very late answer could be matched to a newer request
    # that reused the id.
    late_response_quarantine: float = 30.0

    def __post_init__(self) -> None:
        if self.late_response_quarantine < self.request_timeout * 2:
            self.late_response_quarantine = self.request_timeout * 2


@dataclass
class GatewayStats:
    clients_total: int = 0
    clients_connected: int = 0
    requests_total: int = 0
    responses_total: int = 0
    timeouts_total: int = 0
    late_responses_dropped: int = 0
    upstream_exceptions_relayed: int = 0
    rejected_unsupported: int = 0
    rejected_not_connected: int = 0
    rejected_busy: int = 0
    upstream_connects: int = 0
    upstream_disconnects: int = 0
    upstream_connect_failures: int = 0
    last_upstream_error: str | None = None


@dataclass
class _PendingRequest:
    internal_txid: int
    client_txid: int
    unit_id: int
    session: "_ClientSession"
    timer: asyncio.TimerHandle | None = None


@dataclass(eq=False)
class _ClientSession:
    gateway: "ModbusGateway"
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    session_id: int
    peer: str
    pending: set[int] = field(default_factory=set)
    active: bool = True
    _closed: bool = False
    _outbox: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=1024))
    _writer_task: asyncio.Task | None = None

    def start_writer(self) -> None:
        self._writer_task = asyncio.create_task(
            self._writer_loop(), name=f"client-{self.session_id}-writer"
        )

    def enqueue(self, data: bytes) -> None:
        if not self.active:
            return
        try:
            self._outbox.put_nowait(data)
        except asyncio.QueueFull:
            # The client is not draining its answers.  Dropping it is safer
            # than buffering without bound.
            log.warning("client %s is not draining responses, closing", self.peer)
            self.active = False
            asyncio.create_task(self.close())

    def enqueue_exception(self, snapshot: "_PendingRequest", code: int) -> None:
        response = modbus.ExceptionResponse(
            transaction_id=snapshot.client_txid,
            unit_id=snapshot.unit_id,
            function_code=modbus.FC_READ_HOLDING_REGISTERS,
            exception_code=code,
        )
        self.enqueue(response.encode())

    async def _writer_loop(self) -> None:
        try:
            while True:
                data = await self._outbox.get()
                if data is None:
                    return
                self.writer.write(data)
                await self.writer.drain()
        except asyncio.CancelledError:
            raise
        except (ConnectionError, OSError) as exc:
            log.info("client %s write failed: %s", self.peer, exc)
        finally:
            await self.close()

    async def run(self) -> None:
        buffer = b""
        try:
            while self.active:
                chunk = await self.reader.read(4096)
                if not chunk:
                    break
                buffer += chunk
                if len(buffer) > self.gateway.config.max_client_buffer:
                    log.warning("client %s sent an over-long stream, closing", self.peer)
                    break
                frames, buffer = modbus.split_stream(buffer)
                for raw in frames:
                    try:
                        frame = modbus.parse_adu(raw)
                    except modbus.ModbusDecodeError as exc:
                        log.info("client %s sent a malformed frame: %s", self.peer, exc)
                        continue
                    self.gateway._handle_client_frame(self, frame)
        except modbus.ModbusDecodeError as exc:
            log.warning("client %s sent an invalid MBAP stream: %s", self.peer, exc)
        except asyncio.CancelledError:
            raise
        except (ConnectionError, OSError) as exc:
            log.debug("client %s read ended: %s", self.peer, exc)
        finally:
            await self.close()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        was_active = self.active
        self.active = False
        if was_active:
            log.info("client %s disconnected, cancelling %d in-flight read(s)",
                     self.peer, len(self.pending))
        self.gateway._drop_session_pending(self)
        self.gateway._sessions.discard(self)
        self.gateway.stats.clients_connected = len(self.gateway._sessions)
        try:
            self._outbox.put_nowait(None)
        except asyncio.QueueFull:
            pass  # writer task is being cancelled anyway
        if self._writer_task is not None and self._writer_task is not asyncio.current_task():
            self._writer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._writer_task
        with contextlib.suppress(Exception):
            self.writer.close()
            await self.writer.wait_closed()


class _UpstreamLink:
    """Single TCP connection to the meter, with pipelined request support."""

    def __init__(self, gateway: "ModbusGateway") -> None:
        self._gateway = gateway
        self._queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._connected = False
        self._stopping = False
        self._supervisor: asyncio.Task | None = None

    @property
    def connected(self) -> bool:
        return self._connected

    def start(self) -> None:
        self._supervisor = asyncio.create_task(self._run(), name="upstream-supervisor")

    async def stop(self) -> None:
        self._stopping = True
        if self._supervisor is not None:
            self._supervisor.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._supervisor
        self._connected = False

    def send(self, data: bytes) -> None:
        if not self._connected:
            raise ConnectionError("meter link is down")
        self._queue.put_nowait(data)

    async def _run(self) -> None:
        cfg = self._gateway.config
        while not self._stopping:
            reason = "not connected"
            try:
                await self._serve_connection()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - defensive
                reason = f"supervisor error: {exc!r}"
                log.exception("meter supervisor error")
            if self._stopping:
                break
            log.info("meter link down (%s); reconnecting in %.2fs", reason,
                     cfg.reconnect_delay)
            await asyncio.sleep(cfg.reconnect_delay)

    async def _serve_connection(self) -> None:
        cfg = self._gateway.config
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(cfg.meter_host, cfg.meter_port),
                timeout=cfg.connect_timeout,
            )
        except (OSError, asyncio.TimeoutError) as exc:
            self._gateway.stats.upstream_connect_failures += 1
            self._gateway.stats.last_upstream_error = f"connect: {exc!r}"
            log.warning("cannot connect to meter %s:%s: %s", cfg.meter_host,
                        cfg.meter_port, exc)
            return

        self._queue = asyncio.Queue()
        self._connected = True
        self._gateway._on_link_established()
        self._gateway.stats.upstream_connects += 1
        log.info("meter link established to %s:%s", cfg.meter_host, cfg.meter_port)

        read_task = asyncio.create_task(self._read_loop(reader), name="meter-reader")
        write_task = asyncio.create_task(self._write_loop(writer), name="meter-writer")
        tasks = {read_task, write_task}
        reason = "link closed"
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task.cancelled():
                    reason = "task cancelled"
                elif task.exception() is not None:
                    reason = repr(task.exception())
                else:
                    reason = "meter closed the connection"
        finally:
            self._connected = False
            self._gateway.stats.upstream_disconnects += 1
            self._gateway.stats.last_upstream_error = reason
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            with contextlib.suppress(Exception):
                writer.close()
                await writer.wait_closed()
            while not self._queue.empty():
                with contextlib.suppress(asyncio.QueueEmpty):
                    self._queue.get_nowait()
            self._gateway._fail_all_pending(modbus.EXCEPTION_GATEWAY_TARGET_FAILED,
                                            reason)

    async def _read_loop(self, reader: asyncio.StreamReader) -> None:
        while True:
            header = await reader.readexactly(6)
            _, protocol_id, length = struct.unpack(">HHH", header)
            if protocol_id != modbus.PROTOCOL_ID:
                raise modbus.ModbusDecodeError(
                    f"unexpected protocol id {protocol_id} from meter"
                )
            if not 2 <= length <= modbus.MAX_PDU_LEN + 1:
                raise modbus.ModbusDecodeError(
                    f"unexpected MBAP length {length} from meter"
                )
            body = await reader.readexactly(length)
            frame = modbus.parse_adu(header + body)
            self._gateway._route_upstream_frame(frame)

    async def _write_loop(self, writer: asyncio.StreamWriter) -> None:
        while True:
            data = await self._queue.get()
            writer.write(data)
            await writer.drain()


class ModbusGateway:
    def __init__(self, config: GatewayConfig) -> None:
        self.config = config
        self.stats = GatewayStats()
        self._pending: dict[int, _PendingRequest] = {}
        self._tombstones: dict[int, float] = {}
        self._next_txid = 1
        self._sessions: set[_ClientSession] = set()
        self._next_session_id = 1
        self._server: asyncio.base_events.Server | None = None
        self._link = _UpstreamLink(self)
        self._running = False

    @property
    def listen_port(self) -> int:
        assert self._server is not None
        return self._server.sockets[0].getsockname()[1]

    @property
    def upstream_connected(self) -> bool:
        return self._link.connected

    @property
    def inflight(self) -> int:
        return len(self._pending)

    async def start(self) -> None:
        self._running = True
        self._server = await asyncio.start_server(
            self._on_client, self.config.listen_host, self.config.listen_port
        )
        ports = [sock.getsockname()[1] for sock in self._server.sockets]
        log.info("gateway listening on %s", ", ".join(map(str, ports)))
        self._link.start()

    async def stop(self) -> None:
        self._running = False
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        sessions = list(self._sessions)
        for session in sessions:
            await session.close()
        await self._link.stop()
        self._fail_all_pending(modbus.EXCEPTION_GATEWAY_TARGET_FAILED, "gateway stopping")

    async def _on_client(self, reader: asyncio.StreamReader,
                         writer: asyncio.StreamWriter) -> None:
        peer_info = writer.get_extra_info("peername")
        peer = f"{peer_info[0]}:{peer_info[1]}" if peer_info else "unknown"
        session = _ClientSession(
            gateway=self,
            reader=reader,
            writer=writer,
            session_id=self._next_session_id,
            peer=peer,
        )
        self._next_session_id += 1
        self._sessions.add(session)
        self.stats.clients_total += 1
        self.stats.clients_connected = len(self._sessions)
        log.info("client %s connected (session %d)", peer, session.session_id)
        session.start_writer()
        try:
            await session.run()
        except asyncio.CancelledError:
            raise
        except Exception:  # pragma: no cover - defensive
            log.exception("client session %d failed unexpectedly", session.session_id)
        finally:
            await session.close()

    def _handle_client_frame(self, session: _ClientSession, frame: modbus.RawFrame) -> None:
        if not session.active:
            return
        try:
            request = modbus.decode_read_holding_request(frame)
        except modbus.ModbusExceptionPDU as exc:
            self.stats.rejected_unsupported += 1
            self._send_exception(session, frame.transaction_id, frame.unit_id,
                                 exc.exception_code, function_code=frame.function_code)
            return
        except modbus.ModbusDecodeError as exc:
            log.info("client %s sent a malformed read: %s", session.peer, exc)
            self._send_exception(session, frame.transaction_id, frame.unit_id,
                                 modbus.EXCEPTION_ILLEGAL_DATA_VALUE)
            return

        if request.start_address + request.quantity > 0x10000:
            self._send_exception(session, request.transaction_id, request.unit_id,
                                 modbus.EXCEPTION_ILLEGAL_DATA_ADDRESS)
            return

        self.stats.requests_total += 1
        if len(self._pending) >= self.config.max_inflight:
            self.stats.rejected_busy += 1
            self._send_exception(session, request.transaction_id, request.unit_id,
                                 modbus.EXCEPTION_SERVER_DEVICE_BUSY)
            return
        if not self._link.connected:
            self.stats.rejected_not_connected += 1
            self._send_exception(session, request.transaction_id, request.unit_id,
                                 modbus.EXCEPTION_GATEWAY_PATH_UNAVAILABLE)
            return

        internal_txid = self._allocate_txid()
        if internal_txid is None:
            self.stats.rejected_busy += 1
            self._send_exception(session, request.transaction_id, request.unit_id,
                                 modbus.EXCEPTION_SERVER_DEVICE_BUSY)
            return

        pending = _PendingRequest(
            internal_txid=internal_txid,
            client_txid=request.transaction_id,
            unit_id=request.unit_id,
            session=session,
        )
        self._pending[internal_txid] = pending
        session.pending.add(internal_txid)
        pending.timer = asyncio.get_running_loop().call_later(
            self.config.request_timeout, self._on_request_timeout, internal_txid
        )
        log.debug("route client txid 0x%04X -> meter txid 0x%04X "
                  "(session %d, addr %d, qty %d)", request.transaction_id,
                  internal_txid, session.session_id, request.start_address,
                  request.quantity)

        forwarded = modbus.ReadHoldingRequest(
            transaction_id=internal_txid,
            unit_id=request.unit_id,
            start_address=request.start_address,
            quantity=request.quantity,
        ).encode()
        try:
            self._link.send(forwarded)
        except ConnectionError:
            self._finish_pending(internal_txid, quarantine=True)
            self.stats.rejected_not_connected += 1
            self._send_exception(session, request.transaction_id, request.unit_id,
                                 modbus.EXCEPTION_GATEWAY_PATH_UNAVAILABLE)

    def _route_upstream_frame(self, frame: modbus.RawFrame) -> None:
        internal_txid = frame.transaction_id
        pending = self._pending.get(internal_txid)
        if pending is None:
            self.stats.late_responses_dropped += 1
            log.info("dropping late/unknown meter response txid 0x%04X", internal_txid)
            return

        try:
            decoded = modbus.decode_read_holding_response(frame)
        except modbus.ModbusExceptionPDU as exc:
            self._finish_pending(internal_txid)
            self.stats.upstream_exceptions_relayed += 1
            log.info("meter exception for client txid 0x%04X: code 0x%02X",
                     pending.client_txid, exc.exception_code)
            self._send_exception(pending.session, pending.client_txid,
                                 pending.unit_id, exc.exception_code)
            return
        except modbus.ModbusDecodeError:
            # Framing is intact but the payload is unusable, which means the
            # meter is misbehaving.  Let the error escape into the link reader
            # task: the supervisor will tear the connection down, fail every
            # in-flight read and reconnect, so a desynchronised meter cannot
            # silently keep serving requests.
            raise

        self._finish_pending(internal_txid)
        self.stats.responses_total += 1
        response = modbus.ReadHoldingResponse(
            transaction_id=pending.client_txid,
            unit_id=pending.unit_id,
            registers=decoded.registers,
        ).encode()
        pending.session.enqueue(response)

    def _finish_pending(self, internal_txid: int, quarantine: bool = False) -> _PendingRequest | None:
        pending = self._pending.pop(internal_txid, None)
        if pending is None:
            return None
        if pending.timer is not None:
            pending.timer.cancel()
            pending.timer = None
        pending.session.pending.discard(internal_txid)
        if quarantine:
            self._tombstones[internal_txid] = (
                asyncio.get_running_loop().time() + self.config.late_response_quarantine
            )
        return pending

    def _on_request_timeout(self, internal_txid: int) -> None:
        pending = self._finish_pending(internal_txid, quarantine=True)
        if pending is None:
            return
        self.stats.timeouts_total += 1
        if pending.session.active:
            log.warning("read timed out: client txid 0x%04X (meter txid 0x%04X, "
                        "session %d); sending gateway exception 0x0B",
                        pending.client_txid, internal_txid,
                        pending.session.session_id)
            pending.session.enqueue_exception(
                pending, modbus.EXCEPTION_GATEWAY_TARGET_FAILED
            )
        else:
            log.info("read timed out for an already-closed client (txid 0x%04X)",
                     pending.client_txid)

    def _drop_session_pending(self, session: _ClientSession) -> None:
        now = asyncio.get_running_loop().time()
        for internal_txid in list(session.pending):
            pending = self._finish_pending(internal_txid)
            if pending is not None:
                self._tombstones[internal_txid] = now + self.config.late_response_quarantine

    def _fail_all_pending(self, exception_code: int, reason: str) -> None:
        if not self._pending:
            return
        log.warning("failing %d in-flight read(s): %s", len(self._pending), reason)
        pending_list = list(self._pending.items())
        for internal_txid, _ in pending_list:
            pending = self._finish_pending(internal_txid, quarantine=True)
            if pending is None or not pending.session.active:
                continue
            pending.session.enqueue_exception(pending, exception_code)

    def _send_exception(self, session: _ClientSession, transaction_id: int,
                        unit_id: int, exception_code: int,
                        function_code: int = modbus.FC_READ_HOLDING_REGISTERS) -> None:
        if not session.active:
            return
        response = modbus.ExceptionResponse(
            transaction_id=transaction_id,
            unit_id=unit_id,
            function_code=function_code,
            exception_code=exception_code,
        )
        session.enqueue(response.encode())

    def _allocate_txid(self) -> int | None:
        now = asyncio.get_running_loop().time()
        expired = [txid for txid, until in self._tombstones.items() if until <= now]
        for txid in expired:
            del self._tombstones[txid]
        for _ in range(0x10000):
            candidate = self._next_txid
            self._next_txid = (self._next_txid + 1) & 0xFFFF
            if candidate not in self._pending and candidate not in self._tombstones:
                return candidate
        return None

    def _on_link_established(self) -> None:
        """Clear every trace of the previous meter session.

        A reconnect means the meter forgot whatever it had buffered, so any
        internal transaction id from the old connection can never be answered
        again.  Dropping the tombstones here keeps the id space usable while
        still guaranteeing that a response from the *current* connection can
        never be confused with an abandoned request: a late answer on the old
        socket is simply never read, because that socket is gone.
        """
        tombstones = len(self._tombstones)
        self._tombstones.clear()
        log.info("meter link re-established; cleared %d stale txid tombstone(s)",
                 tombstones)

    def stats_snapshot(self) -> dict:
        snapshot = asdict(self.stats)
        snapshot["inflight"] = len(self._pending)
        snapshot["upstream_connected"] = self._link.connected
        return snapshot
