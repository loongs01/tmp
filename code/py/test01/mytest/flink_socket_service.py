import socket

HOST = 'localhost'
PORT = 9999

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, PORT))
    s.listen(1)
    print(f"Listening on {HOST}:{PORT} ...")
    conn, addr = s.accept()
    print(f"Connected by {addr}")
    with conn:
        while True:
            try:
                line = input()
                if not line:
                    continue
                # 每次输入一行，发送一行，Flink 端能实时收到
                conn.sendall((line + '\n').encode('utf-8'))
            except (KeyboardInterrupt, EOFError):
                print("Server stopped.")
                break
