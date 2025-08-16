#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
===========================================================
 Script Name:   initialization_test.py
 Author:        Ian Redden
 Created:       2025-08-15
 Description:   Initializes DMR858S modules from NiceRF
 Version:       1.0
===========================================================

Usage:
    python initialization_test.py

Notes:
    - In config section below, set the variables
    - Requires Python 3.13+, pySerial

Changelog:
    v1.0 - Initial release.
"""

__author__    = "Ian Redden"
__email__     = "iredden@gmail.com"
__version__   = "1.0"
__license__   = "MIT"
__status__    = "Development"  # Other values: "Production", "Prototype"

import time
import struct
import threading
import queue
from dataclasses import dataclass
from typing import Optional, Tuple, Callable, Dict
import serial  # pyserial


# ---------- Packet model ----------
@dataclass
class Packet:
    head: int = 0x68
    cmd: int = 0x01          # Example command
    rw_flag: int = 0x01      # 0x00=read, 0x01=write
    sr_flag: int = 0x01      # 0x01=start, 0x00=done (module->host uses other values)
    checksum: int = 0x0000   # (not used by module; included for completeness)
    length: int = 0
    data: bytes = b""
    tail: int = 0x10

    def to_bytes(self) -> bytes:
        self.length = len(self.data)
        return (
            struct.pack("!BBBBHH",
                        self.head, self.cmd, self.rw_flag, self.sr_flag,
                        self.checksum, self.length)
            + self.data
            + struct.pack("!B", self.tail)
        )

    @classmethod
    def from_bytes(cls, packet: bytes):
        try:
            head, cmd, rw_flag, sr_flag, checksum = struct.unpack("!BBBBH", packet[:6])
            data_length = struct.unpack("!H", packet[6:8])[0]
            data = packet[8:8 + data_length]
            tail = packet[8 + data_length]
        except Exception:
            return None
        if head != 0x68 or tail != 0x10:
            return None
        return cls(head, cmd, rw_flag, sr_flag, checksum, data_length, data, tail)


# ---------- Config ----------
PORT = "COM1"              #
BAUD = 57600               # No need to change baud rate.  It likes 57,600!

RX_HZ = 444_462_500        # 444.462500 MHz
TX_HZ = 449_462_500        # 449.462500 MHz
RADIO_ID = 1_234_456       # 3-byte big-endian
TG = 1                     # talkgroup
TG_TYPE = 2                # 1 = private, 2 = group, 4 = all
TS = 1                     # timeslot (1 or 2)
COLOR_CODE = 1             # color code (0-15)

# ---------- Command builders ----------
def frame(cmd: int, payload: bytes = b"") -> bytes:
    print(payload.hex(" "))
    return Packet(cmd=cmd, rw_flag=0x01, sr_flag=0x01, data=payload).to_bytes()

def cmd_reset():                      # 0xF0: payload 1A 01
    return frame(0xF0, b"\x1A\x01")

def cmd_check_init():                 # 0x1A, payload 01
    return frame(0x1A, b"\x01")

def cmd_set_contact_id(contact_id: int, contact_type: int):
    return frame(0x18, contact_type.to_bytes(1, "big") + contact_id.to_bytes(3, "big"))

def cmd_set_freq(tx_hz: int, rx_hz: int):   # 0x0D, TX then RX, little-endian
    return frame(0x0D,
                 tx_hz.to_bytes(4, "little")
                 + rx_hz.to_bytes(4, "little"))

def cmd_set_timeslot(timeslot: int): # either 0x01 or 0x02
    return frame(0x33, timeslot.to_bytes(1, "big"))

def cmd_set_color_code(color_code: int):
    return frame(0x31, color_code.to_bytes(1, "big"))

def cmd_set_radio_id(radio_id: int):
    return frame(0x1B, radio_id.to_bytes(3, "big"))

def cmd_disable_encryption():
    return frame(0x19, b"\xff")

def cmd_check_current_channel():
    return frame(0x1D, b"\x01")

def cmd_clear_rx_group():
    return frame(0x30, b"\x01")

def cmd_add_contact_to_rx_group(rx_group_id: int, contact_id: int):
    return frame(0x29, rx_group_id.to_bytes(1, "big") + contact_id.to_bytes(3, "big"))

# ---------- Serial link with send queue & reader ----------
class SerialLink:
    """
    - send(bytes): enqueue a frame to write
    - request(bytes, expect_cmd, timeout): send and block waiting for a reply whose cmd == expect_cmd
    - add_rx_hook(cmd, fn): register a passive callback for frames matching cmd (non-blocking observers)
    """
    def __init__(self, port: str, baud: int, inter_frame_gap: float = 0.010):
        self.port_name = port
        self.baud = baud
        self.inter_frame_gap = inter_frame_gap

        self.ser: Optional[serial.Serial] = None
        self._send_q: "queue.Queue[bytes]" = queue.Queue()
        self._rx_q: "queue.Queue[bytes]" = queue.Queue()

        # waiters keyed by expected cmd -> list of queues to signal
        self._waiters: Dict[int, "queue.Queue[bytes]"] = {}

        # optional passive hooks per cmd
        self._hooks: Dict[int, Callable[[bytes], None]] = {}

        self._stop = threading.Event()
        self._writer_t = threading.Thread(target=self._writer_loop, daemon=True)
        self._reader_t = threading.Thread(target=self._reader_loop, daemon=True)
        self._dispatcher_t = threading.Thread(target=self._dispatch_loop, daemon=True)

    def open(self):
        self.ser = serial.Serial(self.port_name, self.baud, timeout=0.05)
        print(f"[port] opened {self.port_name} @ {self.baud}")
        self._stop.clear()
        self._writer_t.start()
        self._reader_t.start()
        self._dispatcher_t.start()

    def close(self):
        self._stop.set()
        # Put sentinels to unblock queues
        try:
            self._send_q.put_nowait(b"")
            self._rx_q.put_nowait(b"")
        except Exception:
            pass
        # Close serial after threads have a chance to exit
        time.sleep(0.1)
        if self.ser and self.ser.is_open:
            self.ser.close()
            print("[port] closed")

    # ---- public API ----
    def send(self, frame_bytes: bytes):
        """Fire-and-forget send (queued; non-blocking)."""
        self._send_q.put(frame_bytes)

    def request(self, frame_bytes: bytes, expect_cmd: int, timeout: float = 1.0) -> Optional[bytes]:
        """
        Send, then wait for first received frame with cmd==expect_cmd (across all traffic).
        Returns raw frame bytes or None on timeout.
        """
        waiter_q: "queue.Queue[bytes]" = queue.Queue(maxsize=1)
        self._register_waiter(expect_cmd, waiter_q)
        try:
            print(f">> {frame_bytes.hex(" ")}")
            self._send_q.put(frame_bytes)
            try:
                return waiter_q.get(timeout=timeout)
            except queue.Empty:
                return None
        finally:
            self._unregister_waiter(expect_cmd, waiter_q)

    def add_rx_hook(self, cmd: int, fn: Callable[[bytes], None]):
        """Passive observer for frames of a given cmd (does not consume waiter slots)."""
        self._hooks[cmd] = fn

    # ---- internal: writer/read/dispatch ----
    def _writer_loop(self):
        while not self._stop.is_set():
            try:
                item = self._send_q.get(timeout=0.05)
            except queue.Empty:
                continue
            if not item:
                continue
            if self.ser and self.ser.is_open:
                try:
                    self.ser.write(item)
                except Exception as e:
                    print(f"[writer] write error: {e}")
            time.sleep(self.inter_frame_gap)  # gentle pacing

    def _parse_call(self, hex_bytes: bytes):
        if len(hex_bytes) != 4:
            raise ValueError("Expected exactly 4 bytes")

        call_type = hex_bytes[0]  # first byte
        radio_id = int.from_bytes(hex_bytes[1:], byteorder="big")  # last 3 bytes, MSB first

        call_types = {
            1: "Private Call",
            2: "Group Call",
            4: "All Call"
        }

        return {
            "call_type": call_types.get(call_type, f"Unknown ({call_type})"),
            "radio_id": radio_id
        }
        
    def _debug_frame(self, frame_bytes: bytes):
        print(f"<< {frame_bytes.hex(" ")}")

        if frame_bytes.startswith(b"\x68"):
            # Decode the packet
            pkt = Packet.from_bytes(frame_bytes)
            if pkt:
                # Print packet details
                print(f"[debug] cmd=0x{pkt.cmd:02X} rw=0x{pkt.rw_flag:02X} sr=0x{pkt.sr_flag:02X} "
                    f"len={pkt.length} data={pkt.data.hex(' ')}")

                if pkt.cmd == 0x06 and pkt.sr_flag == 0x60:
                    # convert pkt.data.hex msb to integer
                    print(self._parse_call(pkt.data))
            else:
                print("[debug] invalid frame format")

    def _reader_loop(self):
        buf = bytearray()
        while not self._stop.is_set():
            try:
                if not self.ser or not self.ser.is_open:
                    time.sleep(0.05)
                    continue
                chunk = self.ser.read(self.ser.in_waiting or 1)
                if not chunk:
                    continue
                buf.extend(chunk)

                # Extract frames
                while True:
                    # find header
                    try:
                        i = buf.index(0x68)
                    except ValueError:
                        buf.clear()
                        break
                    if i:
                        del buf[:i]
                    if len(buf) < 8:
                        break
                    ln = (buf[6] << 8) | buf[7]
                    need = 8 + ln + 1
                    if len(buf) < need:
                        break
                    frame_bytes = bytes(buf[:need])
                    del buf[:need]
                    if frame_bytes.endswith(b"\x10"):
                        self._debug_frame(frame_bytes)                        
                        self._rx_q.put(frame_bytes)
                    else:
                        # bad tail; slide 1
                        pass
            except Exception as e:
                print(f"[reader] error: {e}")
                time.sleep(0.05)

    def _dispatch_loop(self):
        while not self._stop.is_set():
            try:
                fr = self._rx_q.get(timeout=0.05)
            except queue.Empty:
                continue
            if not fr:
                continue

            pkt = Packet.from_bytes(fr)
            if not pkt:
                continue

            # Notify any waiter expecting this cmd
            waiters = self._waiters.get(pkt.cmd)
            if waiters:
                # deliver to first waiter (typical request/response)
                try:
                    wq = waiters.pop(0)
                    wq.put_nowait(fr)
                except Exception:
                    pass

            # Call passive hook if any
            hook = self._hooks.get(pkt.cmd)
            if hook:
                try:
                    hook(fr)
                except Exception as e:
                    print(f"[hook {pkt.cmd:02X}] error: {e}")

    def _register_waiter(self, cmd: int, qobj: "queue.Queue[bytes]"):
        self._waiters.setdefault(cmd, []).append(qobj)

    def _unregister_waiter(self, cmd: int, qobj: "queue.Queue[bytes]"):
        lst = self._waiters.get(cmd)
        if not lst:
            return
        try:
            lst.remove(qobj)
        except ValueError:
            pass
        if not lst:
            self._waiters.pop(cmd, None)


# ---------- Helpers built on SerialLink ----------
def wait_for_port(port: str, baud: int, retry_delay=0.3) -> SerialLink:
    while True:
        try:
            link = SerialLink(port, baud)
            link.open()
            return link
        except Exception:
            time.sleep(retry_delay)

def wait_init_done(link: SerialLink, total_timeout=10.0, ping_every=0.25) -> bool:
    """
    Poll 0x1A until we see module reply with cmd==0x1A and sr_flag==0x00 (Done).
    """
    t0 = time.time()
    last = 0.0

    # Passive hook prints unexpected SR codes for 0x1A
    def hook(fr: bytes):
        pkt = Packet.from_bytes(fr)
        if pkt and pkt.sr_flag not in (0x00,):  # non-done states
            print(f"[init] status cmd=0x1A sr=0x{pkt.sr_flag:02X}")

    link.add_rx_hook(0x1A, hook)

    while time.time() - t0 < total_timeout:
        now = time.time()
        if now - last >= ping_every:
            link.send(cmd_check_init())
            last = now

        # Non-blocking check: wait up to ping_every for a 0x1A
        fr = link.request(cmd_check_init(), expect_cmd=0x1A, timeout=ping_every)
        if fr:
            pkt = Packet.from_bytes(fr)
            if pkt and pkt.rw_flag == 0x00:  # module->host 'read'
                if pkt.sr_flag == 0x00:
                    print("[init] module reports Done")
                    return True
                elif pkt.sr_flag == 0x09:
                    print("[init] checksum error (unexpected)")
                    # keep looping
    return False

# ---------- Main ----------
def main():
    # Optional hard reset then re-open (uncomment if you wire reset)
    link = wait_for_port(PORT, BAUD)
    link.send(cmd_reset())
    link.close()

    time.sleep(3)

    # wait for port
    link = wait_for_port(PORT, BAUD)
    
    # the order below of initialization cmds matters!
    try:
        # Poll 0x1A until Done
        if not wait_init_done(link, total_timeout=12.0):
            print("[init] timeout waiting for 0x1A Done; exiting")
            return

        # Set Transmit Frequency
        resp = link.request(cmd_set_freq(TX_HZ, RX_HZ), expect_cmd=0x0D, timeout=1)
        if resp: print(f"[set_freq] ack received {resp.hex(" ")}")

        # Set Timeslot
        resp = link.request(cmd_set_timeslot(TS), expect_cmd=0x33, timeout=1)
        if resp: print(f"[set_timeslot] ack received {resp.hex(" ")}")

        # Set Color Code
        resp = link.request(cmd_set_color_code(COLOR_CODE), expect_cmd=0x31, timeout=1)
        if resp: print(f"[set_color_code] ack received {resp.hex(" ")}")

        # Set Radio ID
        resp = link.request(cmd_set_radio_id(RADIO_ID), expect_cmd=0x1B, timeout=1)
        if resp: print(f"[set_radio_id] ack received {resp.hex(" ")}")

        # Set Disable Encryption
        resp = link.request(cmd_disable_encryption(), expect_cmd=0x19, timeout=1)
        if resp: print(f"[disable_encryption] ack received {resp.hex(" ")}")
        
        # clear receive group list
        resp = link.request(cmd_clear_rx_group(), expect_cmd=0x30, timeout=1)
        if resp: print(f"[clear_rx_group] ack received {resp.hex(" ")}")
        
        # add contact to receive group list (use receive group 32)
        resp = link.request(cmd_add_contact_to_rx_group(32, TG), expect_cmd=0x29, timeout=1)
        if resp: print(f"[add_contact_to_rx_group] ack received {resp.hex(" ")}")

        # Set Contact ID
        resp = link.request(cmd_set_contact_id(TG, TG_TYPE), expect_cmd=0x19, timeout=1)
        if resp: print(f"[set_contact_id] ack received {resp.hex(" ")}")

        print("[DONE] DMR module Programmed.")

        # Keep reading in background; foreground just idle
        while True:
            time.sleep(0.5)

    except KeyboardInterrupt:
        pass
    finally:
        link.close()


if __name__ == "__main__":
    main()
