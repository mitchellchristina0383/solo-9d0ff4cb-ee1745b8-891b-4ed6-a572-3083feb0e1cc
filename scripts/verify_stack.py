#!/usr/bin/env python3
"""Reproducible smoke run for the three scenarios the brief asks for.

Run it against a live stack (docker compose up) or against local processes:

    python scripts/verify_stack.py \
        --gateway 127.0.0.1:5020 \
        --gateway-status 127.0.0.1:8081 \
        --meter-control 127.0.0.1:8082

It drives the public interfaces only (the Modbus port, the gateway's status
endpoint and the meter's control endpoint), so it works the same way against a
compressed stack.

Scenarios
  1. same transaction id, two connections, meter answers the later one first
  2. read times out, then a retry with the same transaction id stays correct,
     and the answer that arrives late is dropped
  3. meter disconnects while a read is in flight, new reads fail fast, and
     reads work again after the meter returns
"""

from __future__ import annotations

import argparse
import json
import socket
import struct
import sys
import time
import urllib.request

sys.path.insert(0, ".")

from common import modbus  # noqa: E402


class Failure(Exception):
    pass


def http(method: str, url: str) -> dict:
    req = urllib.request.Request(url, method=method, data=b"" if method == "POST" else None)
    with urllib.request.urlopen(req, timeout=5) as response:
        body = response.read().decode()
    return json.loads(body) if body else {}


def open_read(host: str, port: int, txid: int, address: int, quantity: int,
              unit: int = 1, timeout: float = 6.0) -> socket.socket:
    sock = socket.create_connection((host, port), timeout=timeout)
    sock.sendall(modbus.ReadHoldingRequest(txid, unit, address, quantity).encode())
    return sock


def recv_frame(sock: socket.socket, timeout: float) -> modbus.RawFrame:
    sock.settimeout(timeout)
    header = b""
    while len(header) < 7:
        chunk = sock.recv(7 - len(header))
        if not chunk:
            raise Failure("gateway closed the connection")
        header += chunk
    _, protocol_id, length, _ = struct.unpack(">HHHB", header)
    if protocol_id != 0:
        raise Failure(f"bad protocol id {protocol_id}")
    body = b""
    while len(body) < length - 1:
        chunk = sock.recv(length - 1 - len(body))
        if not chunk:
            raise Failure("gateway closed mid-frame")
        body += chunk
    return modbus.parse_adu(header + body)


def expect_registers(frame: modbus.RawFrame, expected_txid: int,
                     addresses: list[int]) -> list[int]:
    if frame.transaction_id != expected_txid:
        raise Failure(f"transaction id {frame.transaction_id:#06x} != {expected_txid:#06x}")
    if frame.is_exception:
        raise Failure(f"unexpected exception 0x{frame.pdu[1]:02X}")
    payload = modbus.decode_read_holding_response(frame)
    expected = [((a * 3 + 7) & 0xFFFF) for a in addresses]
    if list(payload.registers) != expected:
        raise Failure(f"registers {list(payload.registers)} != {expected} (wrong client got the answer?)")
    return expected


def expect_exception(frame: modbus.RawFrame, expected_txid: int, code: int) -> None:
    if frame.transaction_id != expected_txid:
        raise Failure(f"transaction id {frame.transaction_id:#06x} != {expected_txid:#06x}")
    if not frame.is_exception:
        raise Failure("expected an exception response")
    if frame.pdu[1] != code:
        raise Failure(f"exception 0x{frame.pdu[1]:02X} != 0x{code:02X}")


def wait_for(predicate, timeout: float, interval: float = 0.1) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


class Stack:
    def __init__(self, args) -> None:
        self.gw_host, self.gw_port = _split(args.gateway)
        self.status = _base_url(args.gateway_status)
        self.meter = _base_url(args.meter_control)

    def stats(self) -> dict:
        return http("GET", f"{self.status}/stats")

    def control(self, action: str) -> dict:
        return http("POST", f"{self.meter}/{action}")

    def reset(self) -> None:
        self.control("reset")
        wait_for(lambda: self.stats().get("upstream_connected"), 10)

    def offline(self, value: bool) -> None:
        self.control(f"offline?value={1 if value else 0}")


def _split(value: str) -> tuple[str, int]:
    host, _, port = value.rpartition(":")
    return host or "127.0.0.1", int(port)


def _base_url(value: str) -> str:
    if value.startswith(("http://", "https://")):
        return value.rstrip("/")
    return "http://" + value.rstrip("/")


def scenario_same_txid(stack: Stack) -> str:
    stack.reset()
    stack.control("mode?value=reverse")
    stack.control("window?ms=80")
    before = stack.stats()
    a = open_read(stack.gw_host, stack.gw_port, 0x0077, 0, 2)
    b = open_read(stack.gw_host, stack.gw_port, 0x0077, 200, 3)
    try:
        # Both are in flight before either answer is read: that is the point.
        frame_a = recv_frame(a, 6.0)
        frame_b = recv_frame(b, 6.0)
        regs_a = expect_registers(frame_a, 0x0077, [0, 1])
        regs_b = expect_registers(frame_b, 0x0077, [200, 201, 202])
    finally:
        a.close()
        b.close()
    after = stack.stats()
    if after["late_responses_dropped"] != before["late_responses_dropped"]:
        raise Failure("a response was dropped unexpectedly")
    return (f"A={regs_a} B={regs_b}; both kept client txid 0x0077; "
            f"meter exceptions/connects unchanged ({after['upstream_connects']})")


def scenario_timeout_retry(stack: Stack) -> str:
    stack.reset()
    # Hold the next answer well past the gateway's read timeout.
    stack.control("stall?count=1&ms=4000")
    sock = open_read(stack.gw_host, stack.gw_port, 0x0009, 10, 2)
    try:
        frame = recv_frame(sock, 5.0)
        expect_exception(frame, 0x0009, modbus.EXCEPTION_GATEWAY_TARGET_FAILED)
        timed_out_at = time.monotonic()
    finally:
        sock.close()
    # Retry with the same transaction id while the late answer is still held.
    sock = open_read(stack.gw_host, stack.gw_port, 0x0009, 10, 2)
    try:
        frame = recv_frame(sock, 5.0)
        regs = expect_registers(frame, 0x0009, [10, 11])
    finally:
        sock.close()
    # Now let the stalled answer arrive and confirm it is counted as dropped.
    before = stack.stats()["late_responses_dropped"]
    if not wait_for(lambda: stack.stats()["late_responses_dropped"] > before, 8.0):
        raise Failure("the late answer was not reported as dropped")
    elapsed = (time.monotonic() - timed_out_at) * 1000
    return (f"timeout after {elapsed:.0f} ms -> exception 0x0B; retry returned {regs}; "
            f"late answer dropped (total {stack.stats()['late_responses_dropped']})")


def scenario_disconnect(stack: Stack) -> str:
    stack.reset()
    stack.control("stall?count=1&ms=8000")
    sock = open_read(stack.gw_host, stack.gw_port, 0x0011, 20, 1)
    try:
        time.sleep(0.3)  # make sure the read is in flight at the meter
        stack.offline(True)
        frame = recv_frame(sock, 4.0)
        expect_exception(frame, 0x0011, modbus.EXCEPTION_GATEWAY_TARGET_FAILED)
    finally:
        sock.close()
    if not wait_for(lambda: not stack.stats()["upstream_connected"], 5.0):
        raise Failure("gateway still reports the meter as connected")
    # A read while the meter is away must fail immediately, not hang.
    started = time.monotonic()
    sock = open_read(stack.gw_host, stack.gw_port, 0x0011, 20, 1)
    try:
        frame = recv_frame(sock, 3.0)
        expect_exception(frame, 0x0011, modbus.EXCEPTION_GATEWAY_PATH_UNAVAILABLE)
    finally:
        sock.close()
    offline_ms = (time.monotonic() - started) * 1000

    stack.offline(False)
    if not wait_for(lambda: stack.stats()["upstream_connected"], 10.0):
        raise Failure("gateway did not reconnect")
    sock = open_read(stack.gw_host, stack.gw_port, 0x0011, 20, 2)
    try:
        frame = recv_frame(sock, 5.0)
        regs = expect_registers(frame, 0x0011, [20, 21])
    finally:
        sock.close()
    connects = stack.stats()["upstream_connects"]
    return (f"in-flight read failed with 0x0B; offline read failed in {offline_ms:.0f} ms "
            f"with 0x0A; after recovery read returned {regs} (connects={connects})")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway", default="127.0.0.1:5020")
    parser.add_argument("--gateway-status", default="127.0.0.1:8081")
    parser.add_argument("--meter-control", default="127.0.0.1:8082")
    args = parser.parse_args()
    stack = Stack(args)

    scenarios = [
        ("same transaction id, reordered answers", scenario_same_txid),
        ("timeout, retry, late answer dropped", scenario_timeout_retry),
        ("meter disconnect, fail fast, recover", scenario_disconnect),
    ]
    failures = 0
    for title, run in scenarios:
        print(f"[....] {title}")
        try:
            detail = run(stack)
        except Exception as exc:  # noqa: BLE001 - report and continue
            failures += 1
            print(f"[FAIL] {title}: {exc}")
        else:
            print(f"[ OK ] {title}: {detail}")
    stack.reset()
    print(f"\n{len(scenarios) - failures}/{len(scenarios)} scenarios passed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
