"""A small Modbus/TCP client used by the demo page and the tests.

Deliberately dumb: one transaction per call, one connection per call, and no
automatic retries.  The demo runs one call per worker thread so two reads can
genuinely overlap.
"""

from __future__ import annotations

import socket
import struct
import time
from dataclasses import dataclass

from . import modbus


class ClientError(Exception):
    """The read failed before a complete Modbus answer arrived."""


class ClientTimeout(ClientError):
    pass


@dataclass
class ReadResult:
    transaction_id: int
    registers: list[int]
    elapsed_ms: float
    unit_id: int = 1

    def as_dict(self) -> dict:
        return {
            "transaction_id": self.transaction_id,
            "unit_id": self.unit_id,
            "registers": self.registers,
            "elapsed_ms": round(self.elapsed_ms, 1),
        }


def _recv_exactly(sock: socket.socket, count: int, deadline: float) -> bytes:
    chunks = bytearray()
    while len(chunks) < count:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ClientTimeout("timed out waiting for an answer")
        sock.settimeout(remaining)
        try:
            chunk = sock.recv(count - len(chunks))
        except socket.timeout as exc:
            raise ClientTimeout("timed out waiting for an answer") from exc
        if not chunk:
            raise ClientError("peer closed the connection")
        chunks += chunk
    return bytes(chunks)


def read_holding_registers(host: str, port: int, address: int, quantity: int,
                           transaction_id: int = 1, unit_id: int = 1,
                           timeout: float = 5.0) -> ReadResult:
    """Perform one FC03 read on a fresh connection and return the result.

    The transaction id and unit id are checked against the answer, so the demo
    can prove that the gateway restores the caller's transaction id rather than
    leaking the one it used on the meter link.
    """
    started = time.monotonic()
    deadline = started + timeout
    try:
        sock = socket.create_connection((host, port), timeout=timeout)
    except OSError as exc:
        raise ClientError(f"cannot connect to gateway: {exc}") from exc
    try:
        sock.sendall(modbus.ReadHoldingRequest(transaction_id, unit_id, address,
                                               quantity).encode())
        header = _recv_exactly(sock, 7, deadline)
        txid, protocol_id, length, response_unit = struct.unpack(">HHHB", header)
        if protocol_id != modbus.PROTOCOL_ID:
            raise ClientError(f"bad protocol id {protocol_id}")
        body = _recv_exactly(sock, length - 1, deadline)
        frame = modbus.parse_adu(header + body)
        if frame.transaction_id != transaction_id:
            raise ClientError(
                f"transaction id mismatch: sent 0x{transaction_id:04X}, "
                f"got 0x{frame.transaction_id:04X}"
            )
        if frame.unit_id != unit_id:
            raise ClientError(f"unit id mismatch: sent {unit_id}, got {frame.unit_id}")
        decoded = modbus.decode_read_holding_response(frame)
        return ReadResult(
            transaction_id=decoded.transaction_id,
            registers=list(decoded.registers),
            elapsed_ms=(time.monotonic() - started) * 1000.0,
            unit_id=decoded.unit_id,
        )
    finally:
        try:
            sock.close()
        except OSError:
            pass
