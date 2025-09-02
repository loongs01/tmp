import os
import numpy as np
import cv2
import subprocess

# 从环境变量读取 RTMP 地址
rtmp_url = os.getenv('RTMP_URL', 'rtmp://default-url/live/stream')

# 生成示例图像数据（BGR 格式）
num_frames = 100
height, width = 480, 640
image_data = np.zeros((num_frames, height, width, 3), dtype=np.uint8)

for i in range(num_frames):
    frame = np.zeros((height, width, 3), dtype=np.uint8)
    cv2.putText(frame, f"Frame {i + 1}", (50, 240), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2)
    frame_bgr = cv2.cvtColor(frame, cv2.COLOR_RGB2BGR)  # 确保是 BGR 格式
    image_data[i] = frame_bgr

# 生成示例音频数据（16位 PCM）
sample_rate = 16000
duration = num_frames / 25  # 假设帧率是25，总时长=帧数/帧率
t = np.linspace(0, duration, int(sample_rate * duration), endpoint=False)
frequency = 440
audio_data = 0.5 * np.sin(2 * np.pi * frequency * t)
audio_data = (audio_data * 32767).astype(np.int16)

# 启动 FFmpeg 进程
process = subprocess.Popen(
    [
        'ffmpeg',
        '-y',
        '-f', 'rawvideo',
        '-pixel_format', 'bgr24',  # 输入为 BGR 格式
        '-video_size', f'{width}x{height}',
        '-framerate', '25',
        '-i', '-',
        '-f', 's16le',
        '-ar', str(sample_rate),
        '-ac', '1',
        '-i', '-',
        '-c:v', 'libx264',
        '-c:a', 'aac',
        '-f', 'flv',
        rtmp_url
    ],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE
)

try:
    # 交错写入视频和音频数据（简化版：先视频后音频）
    for frame in image_data:
        process.stdin.write(frame.tobytes())

    process.stdin.write(audio_data.tobytes())
    process.stdin.flush()

    # 等待 FFmpeg 完成（或保持后台运行）
    process.stdin.close()
    process.wait(timeout=10)  # 等待10秒后退出

    # 检查错误
    stderr = process.stderr.read().decode()
    if process.returncode != 0:
        print("FFmpeg error:", stderr)
    else:
        print("Streaming completed (or still running).")

except Exception as e:
    print(f"Error: {e}")
finally:
    if process.poll() is None:
        process.terminate()
