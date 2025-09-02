# import asyncio
# import websockets
#
#
# async def test_client():
#     async with websockets.connect("ws://192.168.18.154:8765") as websocket:
#         while True:
#             message = await websocket.recv()
#             print(f"Received: {message}")
#
#
# asyncio.run(test_client())

import asyncio
import websockets


async def test_client():
    async with websockets.connect("ws://192.168.18.154:8765") as websocket:
        print("Connected to the WebSocket server.")
        try:
            while True:
                # 持续接收消息
                message = await websocket.recv()
                print(f"Received: {message}")
        except websockets.ConnectionClosed:
            print("Connection to the WebSocket server has been closed.")
        except Exception as e:
            print(f"An error occurred: {e}")


asyncio.run(test_client())
