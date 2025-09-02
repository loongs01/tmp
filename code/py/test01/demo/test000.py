import asyncio
import websockets


async def send_video(websocket):
    # 读取 MP4 文件
    with open(r"D:\test\01\result_1748419030.mp4", 'rb') as video_file:
        video_data = video_file.read()

    # 发送视频数据
    await websocket.send(video_data)


start_server = websockets.serve(send_video, "192.168.18.154", 8765)

asyncio.get_event_loop().run_until_complete(start_server)
print("WebSocket server started on ws://localhost:8765")
asyncio.get_event_loop().run_forever()
