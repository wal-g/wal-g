#!/usr/bin/env python3
import socket
import time
import sys
import select

class DisconnectBinlogProxy:
    def __init__(self, listen_port, target_host, target_port, planned_disconnects=3):
        self.listen_port = listen_port
        self.target_host = target_host
        self.target_port = target_port
        self.running = True
        self.client_socket = None
        self.server_socket = None
        self.disconnect_count = 0
        self.planned_disconnects = planned_disconnects
        self.bytes_transferred = 0
        self.connection_start_time = None
        self.total_bytes_transferred = 0
        self.disconnects_completed = False

    def connect_to_server(self):
        try:
            if self.server_socket:
                self.server_socket.close()

            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.settimeout(10)
            self.server_socket.connect((self.target_host, self.target_port))
            print(f"[Proxy] Connected to binlog server (disconnect #{self.disconnect_count})")
            return True
        except Exception as e:
            print(f"[Proxy] Failed to connect to binlog server: {e}")
            return False

    def should_disconnect(self):
        if self.disconnects_completed:
            return False

        if not self.connection_start_time:
            return False

        if self.bytes_transferred > 256 and self.disconnect_count < self.planned_disconnects:
            return True

        return False

    def handle_client_connection(self, client_socket):
        self.client_socket = client_socket
        print(f"[Proxy] Client connected from {client_socket.getpeername()}")

        if not self.connect_to_server():
            print("[Proxy] Initial connection to server failed")
            return

        self.connection_start_time = time.time()
        self.bytes_transferred = 0

        try:
            while self.running:
                if self.should_disconnect():
                    print(f"[Proxy] Planned disconnect #{self.disconnect_count + 1}/{self.planned_disconnects} after {self.bytes_transferred} bytes")
                    print(f"[Proxy] Total bytes transferred so far: {self.total_bytes_transferred}")

                    self.server_socket.close()
                    time.sleep(2)

                    self.disconnect_count += 1

                    if self.disconnect_count >= self.planned_disconnects:
                        self.disconnects_completed = True
                        print(f"[Proxy] Completed all {self.planned_disconnects} planned disconnects. Now working in stable mode.")

                    if not self.connect_to_server():
                        print("[Proxy] Reconnection failed, closing client connection")
                        break

                    self.connection_start_time = time.time()
                    self.bytes_transferred = 0

                try:
                    ready_sockets, _, error_sockets = select.select(
                        [self.client_socket, self.server_socket], [],
                        [self.client_socket, self.server_socket], 1.0
                    )

                    if error_sockets:
                        print("[Proxy] Socket error detected")
                        break

                    if not ready_sockets:
                        continue

                    if self.client_socket in ready_sockets:
                        try:
                            data = self.client_socket.recv(8192)
                            if not data:
                                print("[Proxy] Client disconnected")
                                break
                            self.server_socket.send(data)
                            self.bytes_transferred += len(data)
                            self.total_bytes_transferred += len(data)

                            mode = "STABLE" if self.disconnects_completed else f"DISCONNECT_MODE({self.disconnect_count}/{self.planned_disconnects})"
                            print(f"[Proxy] [{mode}] Client->Server: {len(data)} bytes (session: {self.bytes_transferred}, total: {self.total_bytes_transferred})")
                        except Exception as e:
                            print(f"[Proxy] Error forwarding client->server: {e}")
                            break

                    if self.server_socket in ready_sockets:
                        try:
                            data = self.server_socket.recv(8192)
                            if not data:
                                print("[Proxy] Server disconnected")
                                break
                            self.client_socket.send(data)
                            self.bytes_transferred += len(data)
                            self.total_bytes_transferred += len(data)

                            mode = "STABLE" if self.disconnects_completed else f"DISCONNECT_MODE({self.disconnect_count}/{self.planned_disconnects})"
                            print(f"[Proxy] [{mode}] Server->Client: {len(data)} bytes (session: {self.bytes_transferred}, total: {self.total_bytes_transferred})")
                        except Exception as e:
                            print(f"[Proxy] Error forwarding server->client: {e}")
                            break

                except Exception as e:
                    print(f"[Proxy] Select error: {e}")
                    break

        except Exception as e:
            print(f"[Proxy] Connection handling error: {e}")
        finally:
            final_mode = "STABLE" if self.disconnects_completed else "INCOMPLETE"
            print(f"[Proxy] Connection closed in {final_mode} mode")
            print(f"[Proxy] Total disconnects: {self.disconnect_count}/{self.planned_disconnects}")
            print(f"[Proxy] Total bytes transferred: {self.total_bytes_transferred}")
            if self.client_socket:
                self.client_socket.close()
            if self.server_socket:
                self.server_socket.close()

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(('127.0.0.1', self.listen_port))
        server_socket.listen(1)

        print(f"[Proxy] Listening on port {self.listen_port}")
        print(f"[Proxy] Will make {self.planned_disconnects} planned disconnects, then work stably")

        try:
            while self.running:
                try:
                    client_socket, addr = server_socket.accept()
                    self.handle_client_connection(client_socket)
                except Exception as e:
                    if self.running:
                        print(f"[Proxy] Accept error: {e}")
        finally:
            server_socket.close()
            print("[Proxy] Server socket closed")

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: binlog_proxy.py <listen_port> <target_host> <target_port> <planned_disconnects>")
        sys.exit(1)

    listen_port = int(sys.argv[1])
    target_host = sys.argv[2]
    target_port = int(sys.argv[3])
    planned_disconnects = int(sys.argv[4])

    proxy = DisconnectBinlogProxy(listen_port, target_host, target_port, planned_disconnects)
    proxy.start()