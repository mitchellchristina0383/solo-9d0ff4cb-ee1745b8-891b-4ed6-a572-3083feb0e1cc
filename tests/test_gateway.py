"""Async integration tests: real sockets, real gateway, scripted meter.

Each test builds its own meter + gateway pair on ephemeral ports so the tests
can run in parallel with nothing external.
"""

from __future__ import annotations

import asyncio
import struct

import pytest

from common import modbus
from gateway.core import GatewayConfig, ModbusGateway
from simulator.meter import MeterSimulator, register_value


class Client:
    """Thin asyncio Modbus client used inside the tests."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer

    @classmethod
    async def connect(cls, port: int) -> "Client":
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        return cls(reader, writer)

    def send_read(self, transaction_id: int, address: int, quantity: int,
                  unit_id: int = 1) -> bytes:
        raw = modbus.ReadHoldingRequest(transaction_id, unit_id, address,
                                        quantity).encode()
        self.writer.write(raw)
        return raw

    async def read_frame(self, timeout: float = 5.0) -> modbus.RawFrame:
        header = await asyncio.wait_for(self.reader.readexactly(6), timeout)
        _, protocol_id, length = struct.unpack(">HHH", header)
        assert protocol_id == 0
        body = await asyncio.wait_for(self.reader.readexactly(length), timeout)
        return modbus.parse_adu(header + body)

    async def read_or_exception(self, timeout: float = 5.0):
        frame = await self.read_frame(timeout)
        if frame.is_exception:
            return frame, frame.pdu[1]
        return modbus.decode_read_holding_response(frame), None

    async def close(self) -> None:
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except Exception:
            pass


@pytest.fixture
async def rig():
    meter = MeterSimulator(host="127.0.0.1", port=0, request_window=0.05)
    await meter.start()
    gateway = ModbusGateway(GatewayConfig(
        meter_host="127.0.0.1",
        meter_port=meter.bound_port,
        listen_host="127.0.0.1",
        listen_port=0,
        request_timeout=1.0,
        connect_timeout=1.0,
        reconnect_delay=0.1,
        late_response_quarantine=0.2,
    ))
    await gateway.start()
    for _ in range(100):
        if gateway.upstream_connected:
            break
        await asyncio.sleep(0.02)
    assert gateway.upstream_connected, "gateway did not connect to the meter"
    try:
        yield meter, gateway
    finally:
        await gateway.stop()
        await meter.stop()


async def test_basic_read_and_transaction_id_echo(rig):
    _, gateway = rig
    client = await Client.connect(gateway.listen_port)
    try:
        client.send_read(0x1234, 10, 3)
        response, code = await client.read_or_exception()
        assert code is None
        assert response.transaction_id == 0x1234
        assert response.registers == tuple(register_value(a) for a in (10, 11, 12))
    finally:
        await client.close()


async def test_same_transaction_id_from_two_clients_reversed_order(rig):
    meter, gateway = rig
    await meter.apply(mode="reverse", order_stagger_ms=60)
    a = await Client.connect(gateway.listen_port)
    b = await Client.connect(gateway.listen_port)
    try:
        a.send_read(0x0042, 0, 2)
        b.send_read(0x0042, 200, 2)
        # Let both reach the meter and the meter answer the later one first.
        await asyncio.sleep(0.2)
        response_a, _ = await a.read_or_exception()
        response_b, _ = await b.read_or_exception()
        assert response_a.transaction_id == 0x0042
        assert response_b.transaction_id == 0x0042
        assert response_a.registers == (register_value(0), register_value(1))
        assert response_b.registers == (register_value(200), register_value(201))
        # The meter must have seen distinct transaction ids.
        seen = {entry["txid"] for entry in meter.received}
        assert len(seen) == 2
        assert gateway.stats.late_responses_dropped == 0
    finally:
        await a.close()
        await b.close()


async def test_many_pipelined_same_txid_reads(rig):
    meter, gateway = rig
    await meter.apply(mode="reverse", order_stagger_ms=20)
    client = await Client.connect(gateway.listen_port)
    try:
        # One connection, several requests in flight, all sharing txid 0x00AA.
        expected = {}
        for index, address in enumerate([0, 50, 100, 150, 200]):
            client.send_read(0x00AA, address, 1)
            expected[address] = (register_value(address),)
        results = {}
        for _ in range(5):
            response, code = await client.read_or_exception()
            assert code is None
            assert response.transaction_id == 0x00AA
            results[response.registers[0]] = response
        for address, registers in expected.items():
            assert registers == (address * 3 + 7 & 0xFFFF,)
        assert len(results) == 5
        assert gateway.stats.responses_total == 5
    finally:
        await client.close()


async def test_timeout_reports_error_then_late_response_is_dropped(rig):
    meter, gateway = rig
    await meter.apply(stall_next=1, stall_ms=1500)
    client = await Client.connect(gateway.listen_port)
    try:
        client.send_read(0x0001, 0, 1)
        # The gateway timeout is 1.0s, so the client must outwait it.
        response, code = await client.read_or_exception(timeout=1.6)
        assert code == modbus.EXCEPTION_GATEWAY_TARGET_FAILED
        assert response.transaction_id == 0x0001
        assert gateway.stats.timeouts_total == 1

        # The meter is still holding the original response; a fresh request with
        # the same client txid must get its own answer, not the late one.
        client.send_read(0x0001, 300, 1)
        response2, code2 = await client.read_or_exception(timeout=3.0)
        assert code2 is None
        assert response2.registers == (register_value(300),)

        await asyncio.sleep(0.5)
        assert gateway.stats.late_responses_dropped >= 1
    finally:
        await client.close()


async def test_timeout_then_retry_after_late_response_arrives_is_clean(rig):
    meter, gateway = rig
    await meter.apply(stall_next=1, stall_ms=1200)
    client = await Client.connect(gateway.listen_port)
    try:
        client.send_read(9, 5, 1)
        _, code = await client.read_or_exception(timeout=1.6)
        assert code == modbus.EXCEPTION_GATEWAY_TARGET_FAILED
        # Wait for the late response to actually land, then retry.
        await asyncio.sleep(0.4)
        client.send_read(9, 5, 1)
        response, code = await client.read_or_exception(timeout=2.0)
        assert code is None
        assert response.transaction_id == 9
        assert response.registers == (register_value(5),)
        assert gateway.stats.late_responses_dropped >= 1
    finally:
        await client.close()


async def test_disconnect_fails_inflight_reads_and_recovers(rig):
    meter, gateway = rig
    await meter.apply(stall_next=1, stall_ms=5000)
    client = await Client.connect(gateway.listen_port)
    try:
        client.send_read(0x0101, 0, 2)
        await asyncio.sleep(0.1)  # let it reach the meter
        await meter.set_offline(True)
        response, code = await client.read_or_exception(timeout=1.5)
        assert code == modbus.EXCEPTION_GATEWAY_TARGET_FAILED
        assert response.transaction_id == 0x0101
        assert not gateway.upstream_connected

        # While offline the gateway must answer immediately, not hang.
        started = asyncio.get_running_loop().time()
        client.send_read(0x0102, 0, 1)
        _, code = await client.read_or_exception(timeout=0.5)
        assert code == modbus.EXCEPTION_GATEWAY_PATH_UNAVAILABLE
        assert asyncio.get_running_loop().time() - started < 0.4

        await meter.set_offline(False)
        for _ in range(100):
            if gateway.upstream_connected:
                break
            await asyncio.sleep(0.05)
        assert gateway.upstream_connected, "gateway did not reconnect"
        client.send_read(0x0103, 0, 1)
        response, code = await client.read_or_exception(timeout=2.0)
        assert code is None
        assert response.registers == (register_value(0),)
    finally:
        await client.close()


async def test_late_response_after_client_disconnect_is_dropped(rig):
    meter, gateway = rig
    await meter.apply(stall_next=1, stall_ms=400)
    doomed = await Client.connect(gateway.listen_port)
    survivor = await Client.connect(gateway.listen_port)
    try:
        doomed.send_read(0x0005, 0, 1)
        await asyncio.sleep(0.05)
        await doomed.close()
        await asyncio.sleep(0.05)

        # Same client txid, different client, while the first answer is still
        # held by the meter.
        survivor.send_read(0x0005, 400, 2)
        response, code = await survivor.read_or_exception(timeout=2.0)
        assert code is None
        assert response.registers == (register_value(400), register_value(401))
        assert response.transaction_id == 0x0005

        await asyncio.sleep(0.6)
        assert gateway.stats.late_responses_dropped >= 1
        assert gateway.stats.timeouts_total == 0
    finally:
        await survivor.close()


async def test_unsupported_function_code_is_rejected(rig):
    _, gateway = rig
    client = await Client.connect(gateway.listen_port)
    try:
        pdu = struct.pack(">BHH", 0x04, 0, 1)
        adu = struct.pack(">HHHB", 0x2200, 0, len(pdu) + 1, 1) + pdu
        client.writer.write(adu)
        response, code = await client.read_or_exception()
        assert code == modbus.EXCEPTION_ILLEGAL_FUNCTION
        assert response.transaction_id == 0x2200
        assert gateway.stats.rejected_unsupported == 1
        # The meter must never see the unsupported request.
        assert all(entry["function"] == 3 for entry in rig[0].received)
    finally:
        await client.close()


async def test_meter_exception_is_relayed(rig):
    meter, gateway = rig
    client = await Client.connect(gateway.listen_port)
    try:
        client.send_read(0x0300, 0xFFF0, 32)  # runs past 0xFFFF
        response, code = await client.read_or_exception()
        # Either the gateway or the meter rejects the range; both must answer
        # with an exception carrying the client's transaction id.
        assert code is not None
        assert response.transaction_id == 0x0300
    finally:
        await client.close()


async def test_stress_same_txid_across_two_clients(rig):
    """40 overlapping reads, all with txid 0x0001, split over two clients."""
    meter, gateway = rig
    await meter.apply(mode="reverse", order_stagger_ms=5)
    a = await Client.connect(gateway.listen_port)
    b = await Client.connect(gateway.listen_port)
    expected_a = {}
    expected_b = {}
    try:
        for index in range(20):
            addr_a = index * 2
            addr_b = 1000 + index * 2
            a.send_read(0x0001, addr_a, 2)
            b.send_read(0x0001, addr_b, 2)
            expected_a[(register_value(addr_a), register_value(addr_a + 1))] = False
            expected_b[(register_value(addr_b), register_value(addr_b + 1))] = False
        for _ in range(20):
            for client, expected in ((a, expected_a), (b, expected_b)):
                response, code = await client.read_or_exception(timeout=6.0)
                assert code is None
                assert response.transaction_id == 0x0001
                key = tuple(response.registers)
                assert key in expected, "answer delivered to the wrong client"
                assert not expected[key], "duplicate answer"
                expected[key] = True
        assert all(expected_a.values()) and all(expected_b.values())
        assert gateway.stats.responses_total == 40
        assert gateway.stats.late_responses_dropped == 0
        assert gateway.stats.timeouts_total == 0
    finally:
        await a.close()
        await b.close()


async def test_timed_out_queued_read_is_never_sent(rig):
    """Paused sender: a read that times out in the send queue stays unsent."""
    meter, gateway = rig
    gateway.set_send_paused(True)
    client = await Client.connect(gateway.listen_port)
    try:
        client.send_read(0x0001, 10, 2)
        await asyncio.sleep(0.1)
        assert gateway.stats_snapshot()["send_queue"] == 1
        # The read timeout (1.0s in the rig) still applies while queued.
        response, code = await client.read_or_exception(timeout=2.0)
        assert code == modbus.EXCEPTION_GATEWAY_TARGET_FAILED
        assert response.transaction_id == 0x0001
        assert gateway.stats.timeouts_total == 1
        # The slot is freed the moment the read times out.
        assert gateway.inflight == 0
        assert meter.received_total == 0

        gateway.set_send_paused(False)
        await asyncio.sleep(0.2)  # let the writer sweep the stale entry
        assert meter.received_total == 0, "timed-out read leaked to the meter"

        # A fresh read goes through; the meter has seen exactly one request.
        client.send_read(0x0002, 20, 1)
        response, code = await client.read_or_exception(timeout=2.0)
        assert code is None
        assert response.registers == (register_value(20),)
        assert meter.received_total == 1
        assert gateway.stats.cancelled_before_send == 1
    finally:
        await client.close()


async def test_queued_read_cancelled_on_client_disconnect(rig):
    """A queued read whose client goes away frees its slot and is not sent."""
    meter, gateway = rig
    gateway.set_send_paused(True)
    doomed = await Client.connect(gateway.listen_port)
    try:
        doomed.send_read(0x0005, 0, 1)
        await asyncio.sleep(0.1)
        assert gateway.inflight == 1
        await doomed.close()
        for _ in range(100):
            if gateway.inflight == 0:
                break
            await asyncio.sleep(0.02)
        assert gateway.inflight == 0, "slot not freed well before the read timeout"
        assert gateway.stats.cancelled_before_send == 1

        gateway.set_send_paused(False)
        await asyncio.sleep(0.2)
        assert meter.received_total == 0, "abandoned read leaked to the meter"
        assert gateway.stats.timeouts_total == 0
    finally:
        await doomed.close()


async def test_queued_reads_are_sent_in_enqueue_order_after_resume(rig):
    """Reads that stay valid while queued are sent FIFO once resumed."""
    meter, gateway = rig
    gateway.set_send_paused(True)
    client = await Client.connect(gateway.listen_port)
    try:
        for index, address in enumerate([0, 50, 100]):
            client.send_read(0x0100 + index, address, 1)
        await asyncio.sleep(0.1)
        assert gateway.stats_snapshot()["send_queue"] == 3

        gateway.set_send_paused(False)
        answered = []
        for _ in range(3):
            response, code = await client.read_or_exception(timeout=2.0)
            assert code is None
            answered.append((response.transaction_id, response.registers))
        assert [txid for txid, _ in answered] == [0x0100, 0x0101, 0x0102]
        assert [regs[0] for _, regs in answered] == [
            register_value(0), register_value(50), register_value(100)
        ]
        # The meter saw them in enqueue order (internal txids are sequential).
        txids = [entry["txid"] for entry in meter.received]
        assert len(txids) == 3
        assert txids == sorted(txids)
        assert gateway.stats.cancelled_before_send == 0
    finally:
        await client.close()


async def test_inflight_cap_counts_queued_reads_and_timeout_frees_slots(rig):
    """Queued reads count against max_inflight; timed-out ones free slots."""
    meter, gateway = rig
    gateway.config.max_inflight = 2
    gateway.set_send_paused(True)
    client = await Client.connect(gateway.listen_port)
    try:
        client.send_read(0x0001, 0, 1)
        client.send_read(0x0002, 1, 1)
        await asyncio.sleep(0.1)
        assert gateway.inflight == 2
        # The cap is reached by queued reads alone: the third is refused.
        client.send_read(0x0003, 2, 1)
        response, code = await client.read_or_exception(timeout=1.0)
        assert code == modbus.EXCEPTION_SERVER_DEVICE_BUSY
        assert response.transaction_id == 0x0003
        assert gateway.stats.rejected_busy == 1

        # Both queued reads time out (1.0s) and free their slots at once.
        for _ in range(2):
            response, code = await client.read_or_exception(timeout=2.0)
            assert code == modbus.EXCEPTION_GATEWAY_TARGET_FAILED
        assert gateway.inflight == 0

        # A new read is accepted again even while the sender is paused...
        client.send_read(0x0004, 3, 1)
        await asyncio.sleep(0.1)
        assert gateway.inflight == 1
        # ...and only it reaches the meter after resume.
        gateway.set_send_paused(False)
        response, code = await client.read_or_exception(timeout=2.0)
        assert code is None
        assert response.registers == (register_value(3),)
        assert meter.received_total == 1
        assert gateway.stats.cancelled_before_send == 2
    finally:
        await client.close()


async def test_written_request_is_unaffected_by_pause(rig):
    """A read already written to the meter is answered even after a pause."""
    meter, gateway = rig
    client = await Client.connect(gateway.listen_port)
    try:
        client.send_read(0x0007, 5, 2)
        for _ in range(100):
            if meter.received_total == 1:
                break
            await asyncio.sleep(0.02)
        assert meter.received_total == 1
        gateway.set_send_paused(True)
        response, code = await client.read_or_exception(timeout=2.0)
        assert code is None
        assert response.registers == (register_value(5), register_value(6))
        assert gateway.stats.responses_total == 1
    finally:
        await client.close()


async def test_txid_reuse_while_stale_entry_is_queued(rig):
    """A txid freed by an unwritten read can be reused before the queue drains.

    The stale queue entry must stay a no-op even though its txid now belongs
    to a different, live request.  (``_next_txid`` is rewound by hand to
    simulate the id space wrapping around while the writer is paused.)
    """
    meter, gateway = rig
    gateway.set_send_paused(True)
    client = await Client.connect(gateway.listen_port)
    try:
        client.send_read(0x0001, 10, 1)  # will time out in the queue
        response, code = await client.read_or_exception(timeout=2.0)
        assert code == modbus.EXCEPTION_GATEWAY_TARGET_FAILED
        assert not gateway._tombstones, "unwritten reads must not be tombstoned"
        # Force the internal txid of the dead read to be handed out again.
        gateway._next_txid = 1
        client.send_read(0x0001, 500, 1)
        await asyncio.sleep(0.1)
        gateway.set_send_paused(False)
        response, code = await client.read_or_exception(timeout=2.0)
        assert code is None
        assert response.transaction_id == 0x0001
        assert response.registers == (register_value(500),)
        # The meter saw exactly one request -- the live one (address 500),
        # not the stale entry that shares its internal txid.
        assert meter.received_total == 1
        assert meter.received[0]["pdu"] == "0301f40001"
    finally:
        await client.close()
