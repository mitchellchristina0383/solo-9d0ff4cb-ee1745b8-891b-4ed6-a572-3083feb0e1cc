"""Minimal Modbus/TCP codec.

Only the pieces this project needs are implemented: the MBAP header, function
code 03 (Read Holding Registers), exception responses and the exception codes.

The decode side is intentionally tolerant: the gateway must be able to parse
whatever the meter sends back well enough to route it, even when it carries an
error.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass

MBAP_LEN = 7
# protocol id, then pdu length, must fit a 16 bit unsigned; 254 is the
# documented maximum PDU size for Modbus/TCP.
MAX_PDU_LEN = 254
MAX_ADU_LEN = MBAP_LEN + MAX_PDU_LEN

PROTOCOL_ID = 0

FC_READ_HOLDING_REGISTERS = 0x03

EXCEPTION_ILLEGAL_FUNCTION = 0x01
EXCEPTION_ILLEGAL_DATA_ADDRESS = 0x02
EXCEPTION_ILLEGAL_DATA_VALUE = 0x03
EXCEPTION_SERVER_DEVICE_FAILURE = 0x04
EXCEPTION_ACKNOWLEDGE = 0x05
EXCEPTION_SERVER_DEVICE_BUSY = 0x06
EXCEPTION_GATEWAY_PATH_UNAVAILABLE = 0x0A
EXCEPTION_GATEWAY_TARGET_FAILED = 0x0B

EXCEPTION_NAMES = {
    EXCEPTION_ILLEGAL_FUNCTION: "illegal function",
    EXCEPTION_ILLEGAL_DATA_ADDRESS: "illegal data address",
    EXCEPTION_ILLEGAL_DATA_VALUE: "illegal data value",
    EXCEPTION_SERVER_DEVICE_FAILURE: "server device failure",
    EXCEPTION_ACKNOWLEDGE: "acknowledge",
    EXCEPTION_SERVER_DEVICE_BUSY: "server device busy",
    EXCEPTION_GATEWAY_PATH_UNAVAILABLE: "gateway path unavailable",
    EXCEPTION_GATEWAY_TARGET_FAILED: "gateway target failed to respond",
}


class ModbusDecodeError(ValueError):
    """The byte stream cannot be interpreted as a Modbus/TCP PDU."""


class ModbusExceptionPDU(Exception):
    """The peer answered with a Modbus exception response."""

    def __init__(self, function_code: int, exception_code: int):
        self.function_code = function_code
        self.exception_code = exception_code
        name = EXCEPTION_NAMES.get(exception_code, "unknown exception")
        super().__init__(f"Modbus exception 0x{exception_code:02X} ({name})")

    @property
    def is_gateway_error(self) -> bool:
        return self.exception_code in (
            EXCEPTION_GATEWAY_PATH_UNAVAILABLE,
            EXCEPTION_GATEWAY_TARGET_FAILED,
        )


@dataclass(frozen=True)
class ReadHoldingRequest:
    transaction_id: int
    unit_id: int
    start_address: int
    quantity: int

    def encode(self) -> bytes:
        pdu = struct.pack(">BHH", FC_READ_HOLDING_REGISTERS,
                          self.start_address, self.quantity)
        return _encode_mbap(self.transaction_id, self.unit_id, pdu) + pdu


@dataclass(frozen=True)
class ReadHoldingResponse:
    transaction_id: int
    unit_id: int
    registers: tuple[int, ...]

    def encode(self) -> bytes:
        payload = struct.pack(">B", FC_READ_HOLDING_REGISTERS)
        payload += struct.pack(">B", len(self.registers) * 2)
        payload += struct.pack(f">{len(self.registers)}H", *self.registers)
        return _encode_mbap(self.transaction_id, self.unit_id, payload) + payload


@dataclass(frozen=True)
class ExceptionResponse:
    transaction_id: int
    unit_id: int
    function_code: int
    exception_code: int

    def encode(self) -> bytes:
        pdu = struct.pack(">BB", self.function_code | 0x80, self.exception_code)
        return _encode_mbap(self.transaction_id, self.unit_id, pdu) + pdu


def _encode_mbap(transaction_id: int, unit_id: int, pdu: bytes) -> bytes:
    if not 0 <= transaction_id <= 0xFFFF:
        raise ValueError("transaction id out of range")
    if not 0 <= unit_id <= 0xFF:
        raise ValueError("unit id out of range")
    if len(pdu) > MAX_PDU_LEN:
        raise ValueError("PDU too long")
    return struct.pack(">HHHB", transaction_id, PROTOCOL_ID, len(pdu) + 1, unit_id)


@dataclass(frozen=True)
class RawFrame:
    """A decoded ADU. ``pdu`` excludes the MBAP header."""

    transaction_id: int
    unit_id: int
    pdu: bytes

    @property
    def function_code(self) -> int:
        return self.pdu[0]

    @property
    def is_exception(self) -> bool:
        return bool(self.pdu[0] & 0x80)


def parse_adu(data: bytes) -> RawFrame:
    """Parse a complete ADU (MBAP + PDU).

    Raises :class:`ModbusDecodeError` when the framing is unusable.  The caller
    is responsible for having read exactly one ADU off the wire.
    """
    if len(data) < MBAP_LEN + 1:
        raise ModbusDecodeError("frame shorter than minimum Modbus/TCP ADU")
    transaction_id, protocol_id, length, unit_id = struct.unpack(">HHHB", data[:MBAP_LEN])
    if protocol_id != PROTOCOL_ID:
        raise ModbusDecodeError(f"unexpected protocol id {protocol_id}")
    if not 2 <= length <= MAX_PDU_LEN + 1:
        raise ModbusDecodeError(f"unexpected MBAP length {length}")
    if len(data) != MBAP_LEN - 1 + length:
        raise ModbusDecodeError(
            f"length mismatch: header says {length}, got {len(data) - (MBAP_LEN - 1)}"
        )
    return RawFrame(transaction_id=transaction_id, unit_id=unit_id,
                    pdu=data[MBAP_LEN:])


def split_stream(buffer: bytes) -> tuple[list[bytes], bytes]:
    """Split ``buffer`` into complete ADUs and the unconsumed remainder.

    Modbus/TCP is a length-prefixed stream, so framing can be recovered without
    any per-request knowledge -- which is what lets several requests be in
    flight on one socket at once.
    """
    frames: list[bytes] = []
    offset = 0
    while len(buffer) - offset >= MBAP_LEN:
        _, protocol_id, length = struct.unpack(">HHH", buffer[offset:offset + 6])
        if protocol_id != PROTOCOL_ID or not 2 <= length <= MAX_PDU_LEN + 1:
            raise ModbusDecodeError("unusable MBAP header in stream")
        frame_len = 6 + length
        if len(buffer) - offset < frame_len:
            break
        frames.append(buffer[offset:offset + frame_len])
        offset += frame_len
    return frames, buffer[offset:]


def decode_read_holding_request(frame: RawFrame) -> ReadHoldingRequest:
    if frame.function_code != FC_READ_HOLDING_REGISTERS:
        raise ModbusExceptionPDU(frame.function_code, EXCEPTION_ILLEGAL_FUNCTION)
    if len(frame.pdu) != 5:
        raise ModbusDecodeError("read holding registers PDU must be 5 bytes")
    _, start, quantity = struct.unpack(">BHH", frame.pdu)
    if quantity < 1 or quantity > 125:
        raise ModbusExceptionPDU(FC_READ_HOLDING_REGISTERS, EXCEPTION_ILLEGAL_DATA_VALUE)
    return ReadHoldingRequest(frame.transaction_id, frame.unit_id, start, quantity)


def decode_read_holding_response(frame: RawFrame) -> ReadHoldingResponse:
    if frame.is_exception:
        if len(frame.pdu) != 2:
            raise ModbusDecodeError("exception PDU must be 2 bytes")
        fn, code = struct.unpack(">BB", frame.pdu)
        if fn & 0x7F != FC_READ_HOLDING_REGISTERS:
            raise ModbusDecodeError("exception for an unexpected function code")
        raise ModbusExceptionPDU(FC_READ_HOLDING_REGISTERS, code)
    if frame.function_code != FC_READ_HOLDING_REGISTERS:
        raise ModbusDecodeError(f"unexpected function code {frame.function_code}")
    if len(frame.pdu) < 2:
        raise ModbusDecodeError("response PDU too short")
    byte_count = frame.pdu[1]
    if byte_count % 2 or len(frame.pdu) != 2 + byte_count:
        raise ModbusDecodeError("response byte count does not match PDU length")
    count = byte_count // 2
    registers = struct.unpack(f">{count}H", frame.pdu[2:])
    return ReadHoldingResponse(frame.transaction_id, frame.unit_id, tuple(registers))
