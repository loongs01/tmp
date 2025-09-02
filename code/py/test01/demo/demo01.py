
import os
import subprocess


# 假设帧率为25帧/秒
frame_rate = 25
frame_duration = 1 / frame_rate  # 每帧的时长（0.04秒）

# 音频文件路径
audio_file = '/root/autodl-tmp/data/audio/help.wav'
# 输出音频帧的文件夹
audio_frame_folder = '/root/autodl-tmp/results/audio_frames'
#os.makedirs(audio_frame_folder, exist_ok=True)

# # 使用FFmpeg分割音频
# ffmpeg_split_command = [
#     'ffmpeg',
#     '-i', audio_file,
#     '-f', 'segment',
#     '-segment_time', str(frame_duration),  # 每段时长（0.04秒）
#     '-c', 'copy',
#     os.path.join(audio_frame_folder, '%04d.wav')
# ]

# # 执行分割命令
# subprocess.run(ffmpeg_split_command)

# 图像帧文件夹
image_folder = "/root/autodl-tmp/results/ori_imgs"

# 输出文件
output_file = 'output.mp4'
rtmp_url = "rtmp://121.15.190.196/live/livestream?secret=gg690eb858d046638529a68168550999"



# 获取所有音频和图像文件
image_frames = sorted(os.listdir(image_folder))
audio_frames = sorted(os.listdir(audio_frame_folder))
frame_count = min(len(image_frames), len(audio_frames))  # 确保数量一致

# 构建输入文件的完整路径
image_input = [os.path.join(image_folder, img) for img in image_frames]
audio_input = [os.path.join(audio_frame_folder, audio) for audio in audio_frames]

# 构建FFmpeg命令
ffmpeg_command = [
    'ffmpeg',
    '-framerate', str(frame_rate),  # 设置帧率25
    '-pattern_type', 'glob',  # 支持通配符
    '-i', f'{image_folder}/*.png',  # 输入图片（支持通配符）
    '-pattern_type', 'glob',  # 支持通配符
    '-i', f'/root/autodl-tmp/results/audio_frames/0000.wav',  # 输入音频帧（支持通配符）
    '-map', '0:v',  # 映射第一个输入（图像）的视频流
    '-map', '1:a',  # 映射第二个输入（音频）的音频流
    '-c:v', 'libx264',  # 视频编码器
    '-c:a', 'aac',  # 音频编码器
    '-shortest',  # 确保输出以较短的流结束
    rtmp_url
]



# 执行FFmpeg命令
subprocess.run(ffmpeg_command)
