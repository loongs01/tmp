import numpy as np
import cv2
from scipy.io import wavfile
import os

# 设置输出目录为 D 盘的特定文件夹
output_dir = r'D:\debug'  # 你可以根据需要修改文件夹名称

# 如果目录不存在则创建
os.makedirs(output_dir, exist_ok=True)

# 生成示例图像数据（例如，100帧480x640的黑色图像）
num_frames = 100  # 假设25fps，总时长4秒
height, width = 480, 640
image_data = np.zeros((num_frames, height, width, 3), dtype=np.uint8)

for i in range(num_frames):
    cv2.putText(image_data[i], f"Frame {i + 1}", (50, 240), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2)
print(image_data.shape)

# 保存视频到指定路径
output_video_path = os.path.join(output_dir, 'output_video.avi')
fourcc = cv2.VideoWriter_fourcc(*'XVID')  # 使用 XVID 编码
fps = 25
video_writer = cv2.VideoWriter(output_video_path, fourcc, fps, (width, height))

for frame in image_data:
    video_writer.write(frame)

video_writer.release()
print(f"Video saved to {output_video_path}")

# 生成示例音频数据（匹配视频时长）
sample_rate = 16000
duration = num_frames / 25  # 根据帧数计算音频时长（4秒）
t = np.linspace(0, duration, int(sample_rate * duration), endpoint=False)
frequency = 440  # 频率为440 Hz
audio_data = 0.5 * np.sin(2 * np.pi * frequency * t)  # 生成正弦波
audio_data = (audio_data * 32767).astype(np.int16)  # 转换为16位PCM格式

# 保存音频到指定路径
output_audio_path = os.path.join(output_dir, 'output_audio.wav')
wavfile.write(output_audio_path, sample_rate, audio_data)
print(f"Audio saved to {output_audio_path}")
