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
    video_path = r"D:\test\01\50FF.mp4"
    try:
        # 检查文件是否存在
        if not os.path.exists(video_path):
            await websocket.send("Video file not found")
            logging.error("Video file not found")
            return

        # 发送文件大小（可选）
        file_size = os.path.getsize(video_path)
        await websocket.send(f"FILE_SIZE:{file_size}")
        logging.info(f"Sending video file: {video_path} (Size: {file_size} bytes)")

        # 分块发送视频文件
        with open(video_path, 'rb') as file:
            while True:
                data = file.read(1024)  # 每次读取 1024 字节
                if not data:
                    break
                await websocket.send(data)
                await asyncio.sleep(0.01)  # 模拟流控制（可选）
        logging.info("Video file sent successfully")
    except Exception as e:
        logging.error(f"Error: {e}")
        await websocket.send(f"Error: {e}")
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
