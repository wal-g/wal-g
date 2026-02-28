#!/usr/bin/env python3
import socket
import time
import sys
import threading
from datetime import datetime

disconnect_counter = 0
class DisconnectBinlogProxy:
    def __init__(self, listen_port, target_host, target_port, planned_disconnects=10):
        self.listen_port = listen_port
        self.target_host = target_host
        self.target_port = target_port
        self.planned_disconnects = planned_disconnects

        self.disconnect_count = 0
        self.disconnects_completed = False

        self.total_bytes = 0
        self.running = True

        self._lock = threading.Lock()

    def log(self, msg):
        timestamp = datetime.now().strftime("%Y/%m/%d %H:%M:%S.%f")
        print(f"{timestamp} [Proxy] {msg}", flush=True)

    def should_disconnect(self, session_bytes):
        global disconnect_counter
        if self.disconnects_completed:
            return False
        if disconnect_counter == 0:
            disconnect_counter += 1
            return (session_bytes > 25_000) and (self.disconnect_count < self.planned_disconnects)
        else:
            return (session_bytes > 37_000) and (self.disconnect_count < self.planned_disconnects)


    def connect_to_server(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(10)
        s.connect((self.target_host, self.target_port))
        s.settimeout(None)
        return s

    def hexdump(self, data, max_len=128):
        cut = data[:max_len]
        hex_part = " ".join(f"{b:02x}" for b in cut)
        ascii_part = "".join(chr(b) if 32 <= b < 127 else "." for b in cut)
        return f"{hex_part}\n{ascii_part}"

    def parse_mysql_command(self, packet):
        if len(packet) < 5:
            return None

        payload_len = packet[0] | (packet[1] << 8) | (packet[2] << 16)
        seq = packet[3]

        if len(packet) < 4 + payload_len:
            return None

        command = packet[4]
        return payload_len, seq, command


    def forward(self, src, dst, direction, stop_event, session_state):
        try:
            while not stop_event.is_set():
                data = src.recv(8192)
                if not data:
                    self.log(f"{direction}: EOF")
                    stop_event.set()
                    return

                parsed = self.parse_mysql_command(data)
                if parsed:
                    payload_len, seq, cmd = parsed
                    if cmd in (0x12, 0x15, 0x1e):
                        cmd_name = {
                            0x12: "COM_BINLOG_DUMP",
                            0x15: "COM_REGISTER_SLAVE",
                            0x1e: "COM_BINLOG_DUMP_GTID",
                        }.get(cmd, f"UNKNOWN(0x{cmd:02x})")

                        self.log(
                            f"[Proxy][DEBUG] MySQL cmd={cmd_name} "
                            f"payload_len={payload_len} seq={seq} total_bytes={len(data)}"
                        )
                        self.log("[Proxy][DEBUG] HEXDUMP:\n" + self.hexdump(data))

                        try:
                            dst.sendall(data)
                            self.log(f"[Proxy][DEBUG] Successfully sent {cmd_name} to {direction}")
                        except Exception as send_err:
                            self.log(f"[Proxy][ERROR] Failed to send {cmd_name}: {send_err}")
                            raise
                    else:
                        dst.sendall(data)
                else:
                    dst.sendall(data)


                session_state['bytes'] += len(data)

                with self._lock:
                    self.total_bytes += len(data)
                    current_session_bytes = session_state['bytes']
                    current_total = self.total_bytes
                    current_disconnect_count = self.disconnect_count
                    completed = self.disconnects_completed

                mode = "STABLE" if completed else f"DISCONNECT_MODE({current_disconnect_count}/{self.planned_disconnects})"
                self.log(f"[{mode}] {direction}: {len(data)} bytes (session={current_session_bytes}, total={current_total})")

                if direction == "Server->Client" and self.should_disconnect(session_state['bytes']):
                    with self._lock:
                        self.disconnect_count += 1
                        new_count = self.disconnect_count
                        self.log(f"Disconnect #{new_count}/{self.planned_disconnects} at session_bytes={session_state['bytes']}, total={self.total_bytes}")

                        if self.disconnect_count >= self.planned_disconnects:
                            self.disconnects_completed = True
                            self.log(f"Completed all {self.planned_disconnects} planned disconnects; next sessions will be stable.")

                    import time
                    time.sleep(0.1)

                    try: src.shutdown(socket.SHUT_RDWR)
                    except: pass
                    try: dst.shutdown(socket.SHUT_RDWR)
                    except: pass
                    try: src.close()
                    except: pass
                    try: dst.close()
                    except: pass

                    stop_event.set()
                    return

        except Exception as e:
            self.log(f"{direction}: error: {e}")
            stop_event.set()

    def handle_client(self, client_sock):
        peer = None
        try:
            peer = client_sock.getpeername()
        except:
            pass
        self.log(f"Client connected from {peer}")

        try:
            server_sock = self.connect_to_server()
            with self._lock:
                dc = self.disconnect_count
            self.log(f"Connected to binlog server (disconnect_count={dc})")
        except Exception as e:
            self.log(f"Failed to connect to binlog server: {e}")
            try:
                client_sock.close()
            except:
                pass
            return

        client_sock.settimeout(None)
        server_sock.settimeout(None)

        session_state = {'bytes': 0}

        stop_event = threading.Event()

        t1 = threading.Thread(
            target=self.forward,
            args=(client_sock, server_sock, "Client->Server", stop_event, session_state),
            daemon=True
        )
        t2 = threading.Thread(
            target=self.forward,
            args=(server_sock, client_sock, "Server->Client", stop_event, session_state),
            daemon=True
        )
        t1.start()
        t2.start()

        while not stop_event.is_set():
            time.sleep(0.05)

        try:
            client_sock.close()
        except:
            pass
        try:
            server_sock.close()
        except:
            pass

        self.log("Connections closed; waiting for next client...")

    def start(self):
        ls = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ls.bind(("127.0.0.1", self.listen_port))
        ls.listen(5)

        self.log(f"Listening on 127.0.0.1:{self.listen_port}")
        self.log(f"Will make {self.planned_disconnects} planned disconnects, then work stably")

        try:
            while self.running:
                client, _ = ls.accept()
                t = threading.Thread(target=self.handle_client, args=(client,), daemon=True)
                t.start()
        except KeyboardInterrupt:
            self.log("Shutting down...")
        finally:
            self.running = False
            try:
                ls.close()
            except:
                pass

if __name__ == "__main__":
    if len(sys.argv) != 5:
        timestamp = datetime.now().strftime("%Y/%m/%d %H:%M:%S.%f")
        print(f"{timestamp} Usage: binlog_proxy.py <listen_port> <target_host> <target_port> <planned_disconnects>", flush=True)
        sys.exit(1)

    proxy = DisconnectBinlogProxy(
        listen_port=int(sys.argv[1]),
        target_host=sys.argv[2],
        target_port=int(sys.argv[3]),
        planned_disconnects=int(sys.argv[4]),
    )
    proxy.start()