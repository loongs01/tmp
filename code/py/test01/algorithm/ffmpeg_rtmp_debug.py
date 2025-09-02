import numpy as np
import cv2
import subprocess
import time

# 生成示例图像数据
num_frames = 100
height, width = 480, 640
image_data = np.zeros((num_frames, height, width, 3), dtype=np.uint8)

for i in range(num_frames):
    cv2.putText(image_data[i], f"Frame {i + 1}", (50, 240), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2)

# 生成示例音频数据
sample_rate = 16000
duration = num_frames / 25
t = np.linspace(0, duration, int(sample_rate * duration), endpoint=False)
frequency = 440
audio_data = 0.5 * np.sin(2 * np.pi * frequency * t)
audio_data = (audio_data * 32767).astype(np.int16)

# FFmpeg 路径和 RTMP 地址
ffmpeg_path = r'D:\app\ffmpeg-7.1.1-essentials_build\ffmpeg-7.1.1-essentials_build\bin\ffmpeg.exe'
rtmp_url = 'rtmp://121.15.190.196/live/livestream?secret=gg690eb858d046638529a68168550999'

# 启动 FFmpeg 进程
process = subprocess.Popen(
    [
        ffmpeg_path,
        '-y',
        '-f', 'rawvideo',
        '-pixel_format', 'rgb24',
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
        '-shortest',  # 确保流在较短输入结束时终止
        rtmp_url
    ],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    bufsize=1024  # 减小缓冲区大小
)

try:
    frame_size = width * height * 3
    samples_per_frame = int(sample_rate / 25)

    for i in range(num_frames):
        # 写入视频帧
        process.stdin.write(image_data[i].tobytes())
        process.stdin.flush()  # 强制刷新缓冲区

        # 写入音频数据
        start = i * samples_per_frame
        end = start + samples_per_frame
        if end <= len(audio_data):
            process.stdin.write(audio_data[start:end].tobytes())
        else:
            silent = np.zeros(samples_per_frame, dtype=np.int16)
            process.stdin.write(silent.tobytes())
        process.stdin.flush()

    # 关闭输入管道并等待进程结束
    process.stdin.close()
    process.wait(timeout=10)  # 设置超时避免卡死

    # 检查 FFmpeg 错误
    if process.returncode != 0:
        print("FFmpeg error:", process.stderr.read().decode('utf-8'))

except Exception as e:
    print("Error:", e)
    process.kill()
    process.wait()
