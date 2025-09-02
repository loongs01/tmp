# import numpy as np
# import ffmpeg
# import threading
# import queue
# import time

# # RTMP 服务器地址
# rtmp_url = 'rtmp://121.15.190.196/live/livestream?secret=gg690eb858d046638529a68168550999'  # 确保地址正确

# # 配置参数
# width, height = 640, 480
# audio_sample_rate = 16000
# audio_fps = 50
# video_fps = 25

# # 模拟数据生成
# num_frames = 100   # 视频帧数
# audio_block_size = audio_sample_rate // audio_fps  # 320 samples/block
# samples_per_vframe = audio_block_size * 2  # 640 samples

# # 生成测试数据
# video_frames = [np.random.randint(0, 256, (height, width, 3), dtype=np.uint8) for _ in range(num_frames)]
# audio_data = np.random.randint(-32768, 32767, num_frames * samples_per_vframe, dtype=np.int16)

# # 创建线程安全的队列
# audio_queue = queue.Queue()
# video_queue = queue.Queue()

# # 填充音频队列（40ms per chunk）
# for i in range(num_frames * 2):
#     start = i * audio_block_size
#     end = start + audio_block_size
#     audio_queue.put(audio_data[start:end])

# # 填充视频队列
# for frame in video_frames:
#     video_queue.put(frame)

# # FFmpeg进程配置
# # 分别创建视频和音频输入流
# video_input = ffmpeg.input('pipe:', format='rawvideo', pix_fmt='rgb24', s=f'{width}x{height}', r=video_fps,)
# audio_input = ffmpeg.input('pipe:', format='s16le', ac=1, ar=audio_sample_rate)

# # 合并音视频流并输出到RTMP
# process = (
#     ffmpeg
#     .output(video_input, audio_input, rtmp_url, format='flv', 
#            vcodec='libx264', preset='ultrafast', tune='zerolatency',
#            acodec='aac', audio_bitrate='126k')
#     .overwrite_output()
#     .run_async(pipe_stdin=True)
# )

# # 音视频写入线程
# def audio_writer():
#     while not audio_queue.empty():
#         chunk = audio_queue.get()
#         process.stdin.write(chunk.tobytes())
#         time.sleep(1 / audio_fps)  # 根据音频帧率等待

# def video_writer():
#     while not video_queue.empty():
#         frame = video_queue.get()
#         process.stdin.write(frame.tobytes())
#         time.sleep(1 / video_fps)  # 根据视频帧率等待

# # 启动线程
# audio_thread = threading.Thread(target=audio_writer)
# video_thread = threading.Thread(target=video_writer)

# audio_thread.start()
# video_thread.start()

# audio_thread.join()
# video_thread.join()

# process.stdin.close()
# process.wait()

# import numpy as np
# import cv2
# import subprocess

# # 生成示例图像数据（例如，100帧480x640的黑色图像）
# num_frames = 100
# height, width = 480, 640
# image_data = np.zeros((num_frames, height, width, 3), dtype=np.uint8)

# for i in range(num_frames):
#     cv2.putText(image_data[i], f"Frame {i + 1}", (50, 240), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2)

# # 生成示例音频数据（例如，简单的正弦波音频）
# sample_rate = 16000
# duration = 5  # 持续时间为5秒
# t = np.linspace(0, duration, sample_rate * duration, endpoint=False)
# frequency = 440  # 频率为440 Hz
# audio_data = 0.5 * np.sin(2 * np.pi * frequency * t)  # 生成正弦波

# # 将音频数据转换为16位PCM格式
# audio_data = (audio_data * 32767).astype(np.int16)

# rtmp_url = 'rtmp://121.15.190.196/live/livestream?secret=gg690eb858d046638529a68168550999'  # 确保地址正确


# # 创建 FFmpeg 进程
# process = subprocess.Popen(
#     [
#         'ffmpeg',
#         '-y',  # 覆盖输出文件
#         '-f', 'rawvideo',  # 输入格式为原始视频
#         '-pixel_format', 'rgb24',  # 像素格式
#         '-video_size', f'{width}x{height}',  # 视频尺寸
#         '-framerate', '25',  # 帧率
#         '-i', '-',  # 从标准输入读取图像数据
#         '-f', 's16le',  # 输入音频格式
#         '-ar', str(sample_rate),  # 音频采样率
#         '-ac', '1',  # 声道数量
#         '-i', '-',  # 从标准输入读取音频数据
#         '-c:v', 'libx264',  # 视频编码
#         '-c:a', 'aac',  # 音频编码
#         '-shortest',  # 输出视频长度与最短输入流相同
#         'output.mp4'  # 输出文件
#     ],
#     stdin=subprocess.PIPE,
#     stdout=subprocess.PIPE,
#     stderr=subprocess.PIPE
# )

# # 向 FFmpeg 进程写入图像数据和音频数据
# try:
#     # 存储所有图像数据和音频数据
#     input_data = b''.join(frame.tobytes() for frame in image_data) + audio_data.tobytes()

#     # 将数据传递给 FFmpeg 进程
#     stdout, stderr = process.communicate(input=input_data)

#     # 输出 FFmpeg 的错误信息（如果有的话）
#     if process.returncode != 0:
#         print("FFmpeg error:", stderr.decode())
#     else:
#         print("Video created successfully!")

# except Exception as e:
#     print(f"An error occurred: {e}")


import numpy as np
import cv2
import subprocess

# 生成示例图像数据（例如，100帧480x640的黑色图像）
num_frames = 100
height, width = 480, 640
image_data = np.zeros((num_frames, height, width, 3), dtype=np.uint8)

for i in range(num_frames):
    cv2.putText(image_data[i], f"Frame {i + 1}", (50, 240), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2)

# 生成示例音频数据（例如，简单的正弦波音频）
sample_rate = 16000
duration = 5  # 持续时间为5秒
t = np.linspace(0, duration, sample_rate * duration, endpoint=False)
frequency = 440  # 频率为440 Hz
audio_data = 0.5 * np.sin(2 * np.pi * frequency * t)  # 生成正弦波
audio_data = (audio_data * 32767).astype(np.int16)  # 转换为16位PCM格式

# 创建 FFmpeg 进程，设置输出为 RTMP 地址
rtmp_url = 'rtmp://121.15.190.196/live/livestream?secret=gg690eb858d046638529a68168550999'  # 确保地址正确
process = subprocess.Popen(
    [
        'ffmpeg',
        '-y',  # 覆盖输出文件
        '-f', 'rawvideo',  # 输入格式为原始视频
        '-pixel_format', 'rgb24',  # 像素格式
        '-video_size', f'{width}x{height}',  # 视频尺寸
        '-framerate', '25',  # 帧率
        '-i', '-',  # 从标准输入读取图像数据
        '-f', 's16le',  # 输入音频格式
        '-ar', str(sample_rate),  # 音频采样率
        '-ac', '1',  # 声道数量
        '-i', '-',  # 从标准输入读取音频数据
        '-c:v', 'libx264',  # 视频编码
        '-c:a', 'aac',  # 音频编码
        '-f', 'flv',  # 输出格式
        rtmp_url  # RTMP 输出地址
    ],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE
)

try:
    # 向 FFmpeg 进程写入图像数据和音频数据
     # 向 FFmpeg 进程写入图像数据
    for frame in image_data:
        process.stdin.write(frame.tobytes())

    # 向 FFmpeg 进程写入音频数据
    process.stdin.write(audio_data.tobytes())

    # 关闭标准输入
    process.stdin.close()

    # 等待 FFmpeg 完成（对于 RTMP 推流，可能会一直运行）
    stdout, stderr = process.communicate()

    # 输出 FFmpeg 的错误信息（如果有的话）
    if process.returncode != 0:
        print("FFmpeg error:", stderr.decode())
    else:
        print("Streaming to RTMP server successfully!")

except Exception as e:
    print(f"An error occurred: {e}")