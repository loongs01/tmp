import os
import asyncio
import websockets
import logging
import time

# 配置日志
logging.basicConfig(level=logging.INFO)


async def send_video(websocket):
    """
    WebSocket 回调函数：处理客户端连接并发送视频文件。
    """
    # video_path = r'D:\debug\02\output_video.mp4'

    T1 = time.time()
    print(T1)
    # video_path = r"D:\test\01\50FF.mp4"

    with open(r"D:\test\01\result_1748419030.mp4", 'rb') as video_file:
        video_data = video_file.read()

        # 发送视频数据
        await websocket.send(video_data)

    print(time.time())
    print(f"time:{time.time() - T1}")


async def main():
    """
    启动 WebSocket 服务器。
    """
    async with websockets.serve(send_video, "192.168.18.154", 8765):
        logging.info("WebSocket server started on ws://192.168.18.154:8765")
        await asyncio.Future()  # 永久运行，直到手动终止


if __name__ == "__main__":
    asyncio.run(main())
