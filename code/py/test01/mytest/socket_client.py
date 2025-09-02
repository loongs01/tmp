import socket
import threading
import time


class LongConnectionClient:
    def __init__(self, host='127.0.0.1', port=12345):
        self.host = host
        self.port = port
        self.client_socket = None
        self.running = False

    def connect(self):
        """连接到服务端"""
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.client_socket.connect((self.host, self.port))
            self.running = True
            print(f"Connected to server at {self.host}:{self.port}")

            # 启动接收消息的线程
            receive_thread = threading.Thread(target=self.receive_messages)
            receive_thread.daemon = True
            receive_thread.start()

            return True
        except Exception as e:
            print(f"Connection failed: {e}")
            return False

    def receive_messages(self):
        """接收服务端消息"""
        while self.running:
            try:
                data = self.client_socket.recv(1024)
                if not data:
                    print("Server disconnected")
                    self.disconnect()
                    break

                message = data.decode('utf-8')
                print(f"Received from server: {message}")

            except ConnectionResetError:
                print("Server disconnected abruptly")
                self.disconnect()
            except Exception as e:
                print(f"Error receiving message: {e}")
                self.disconnect()

    def send_message(self, message):
        """发送消息到服务端"""
        if not self.running or not self.client_socket:
            print("Not connected to server")
            return False

        try:
            self.client_socket.sendall(message.encode('utf-8'))
            return True
        except Exception as e:
            print(f"Failed to send message: {e}")
            self.disconnect()
            return False

    def disconnect(self):
        """断开与服务端的连接"""
        if self.running:
            self.running = False
            if self.client_socket:
                self.client_socket.close()
            print("Disconnected from server")

    def start_interactive(self):
        """启动交互式客户端"""
        if not self.connect():
            return

        try:
            while self.running:
                message = input("Enter message (or 'quit' to exit): ")
                if message.lower() == 'quit':
                    break

                if not self.send_message(message):
                    break

        except KeyboardInterrupt:
            print("\nClient interrupted")
        finally:
            self.disconnect()


if __name__ == "__main__":
    client = LongConnectionClient()
    client.start_interactive()
