import socket
import threading


class LongConnectionServer:
    def __init__(self, host='0.0.0.0', port=12345):
        self.host = host
        self.port = port
        self.server_socket = None
        self.clients = {}  # 存储客户端连接 {client_id: socket}
        self.client_lock = threading.Lock()

    def start(self):
        """启动服务端"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        print(f"Server started on {self.host}:{self.port}")

        try:
            while True:
                client_socket, client_address = self.server_socket.accept()
                print(f"New connection from {client_address}")

                # 为每个客户端创建独立线程
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, client_address)
                )
                client_thread.daemon = True
                client_thread.start()

        except KeyboardInterrupt:
            print("Server is shutting down...")
        finally:
            self.stop()

    def handle_client(self, client_socket, client_address):
        """处理客户端连接"""
        client_id = f"{client_address[0]}:{client_address[1]}"

        with self.client_lock:
            self.clients[client_id] = client_socket

        try:
            while True:
                data = client_socket.recv(1024)
                if not data:
                    break

                message = data.decode('utf-8')
                print(f"Received from {client_id}: {message}")

                # 简单回显消息
                response = f"Echo: {message}"
                client_socket.sendall(response.encode('utf-8'))

        except ConnectionResetError:
            print(f"Client {client_id} disconnected abruptly")
        except Exception as e:
            print(f"Error with client {client_id}: {e}")
        finally:
            with self.client_lock:
                if client_id in self.clients:
                    del self.clients[client_id]
            client_socket.close()
            print(f"Connection with {client_id} closed")

    def broadcast(self, message):
        """向所有客户端广播消息"""
        with self.client_lock:
            for client_id, client_socket in self.clients.items():
                try:
                    client_socket.sendall(message.encode('utf-8'))
                except Exception as e:
                    print(f"Failed to send to {client_id}: {e}")

    def stop(self):
        """停止服务端"""
        if self.server_socket:
            # 关闭所有客户端连接
            with self.client_lock:
                for client_id, client_socket in self.clients.items():
                    try:
                        client_socket.close()
                    except:
                        pass
                self.clients.clear()

            self.server_socket.close()
            print("Server stopped")


if __name__ == "__main__":
    server = LongConnectionServer()
    server.start()
