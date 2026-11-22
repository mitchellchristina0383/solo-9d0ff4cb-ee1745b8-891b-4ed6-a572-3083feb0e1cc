"""Unit tests for the Modbus/TCP codec."""

import struct

import pytest

from common import modbus


def test_request_round_trip():
    request = modbus.ReadHoldingRequest(0xBEEF, 7, 100, 3)
    raw = request.encode()
    frame = modbus.parse_adu(raw)
    assert frame.transaction_id == 0xBEEF
    assert frame.unit_id == 7
    decoded = modbus.decode_read_holding_request(frame)
    assert decoded == request


def test_response_round_trip():
    response = modbus.ReadHoldingResponse(0x0102, 3, (10, 20, 30))
    frame = modbus.parse_adu(response.encode())
    decoded = modbus.decode_read_holding_response(frame)
    assert decoded.registers == (10, 20, 30)
    assert decoded.transaction_id == 0x0102


def test_exception_response_is_raised():
    raw = modbus.ExceptionResponse(1, 1, modbus.FC_READ_HOLDING_REGISTERS, 0x0B).encode()
    frame = modbus.parse_adu(raw)
    with pytest.raises(modbus.ModbusExceptionPDU) as excinfo:
        modbus.decode_read_holding_response(frame)
    assert excinfo.value.exception_code == 0x0B
    assert excinfo.value.is_gateway_error


def test_unsupported_function_code_is_illegal_function():
    pdu = struct.pack(">BHH", 0x04, 0, 1)
    adu = struct.pack(">HHHB", 5, 0, len(pdu) + 1, 1) + pdu
    frame = modbus.parse_adu(adu)
    with pytest.raises(modbus.ModbusExceptionPDU) as excinfo:
        modbus.decode_read_holding_request(frame)
    assert excinfo.value.exception_code == modbus.EXCEPTION_ILLEGAL_FUNCTION


def test_quantity_bounds():
    pdu = struct.pack(">BHH", 0x03, 0, 0)
    adu = struct.pack(">HHHB", 5, 0, len(pdu) + 1, 1) + pdu
    with pytest.raises(modbus.ModbusExceptionPDU):
        modbus.decode_read_holding_request(modbus.parse_adu(adu))


def test_split_stream_handles_partial_and_multiple_frames():
    a = modbus.ReadHoldingRequest(1, 1, 0, 1).encode()
    b = modbus.ReadHoldingRequest(2, 1, 1, 1).encode()
    frames, rest = modbus.split_stream(a + b + a[:3])
    assert frames == [a, b]
    assert rest == a[:3]


def test_parse_adu_rejects_bad_protocol_id():
    raw = bytearray(modbus.ReadHoldingRequest(1, 1, 0, 1).encode())
    raw[2:4] = b"\x00\x01"
    with pytest.raises(modbus.ModbusDecodeError):
        modbus.parse_adu(bytes(raw))
