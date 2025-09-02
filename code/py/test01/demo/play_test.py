import subprocess
from algorithm import video_split

# 图片文件夹路径
# image_folder = "/root/autodl-tmp/results/ori_imgs"
image_folder = r'D:\test\temp_frames'
# 音频文件路径
audio_file = r"D:\test\01\result_1748419030.mp4"  # 推流必须持续，如果不持续会有延时。测试一秒的推

# 获取图片地址列表
# images = [os.path.join(image_folder, img) for img in os.listdir(image_folder) if img.endswith(('.jpg'))]

# 确保路径为标准格式
# input_pattern = os.path.join(image_folder, '0.jpg')

# 推流地址（SRS的RTMP地址）
# rtmp_url = "rtmp://121.15.190.196/live/livestream?secret=gg690eb858d046638529a68168550999"
rtmp_url = 'rtmp://192.168.10.115/live/livestream'

# # FFmpeg命令
# ffmpeg_command = [
#     'ffmpeg',
#     #'-loop', '1',  # 循环推流
#     '-framerate', '1',  # 帧率，1帧/秒
#     '-i', images[0],  # 第一张图片
#     '-c:v', 'libx264',      # 视频编码器
#     '-preset', 'veryfast',  # 编码速度
#     '-tune', 'stillimage',  # 优化静态图像
#     '-pix_fmt', 'yuv420p',  # 像素格式
#    # '-vf', 'scale=1280:720',  # 分辨率
#     '-f', 'flv',  # 输出格式
#     rtmp_url
# ]
#
# # 启动FFmpeg进程
# process = subprocess.Popen(ffmpeg_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#
# # 等待进程结束
# process.communicate()


# FFmpeg命令

for path in video_split.split_video_into_parts(r"D:\test\01\result_1748419030.mp4", r"D:\test\01\split", 5):
            ffmpeg_command = [
                r'D:\app\ffmpeg-7.1.1-essentials_build\ffmpeg-7.1.1-essentials_build\bin\ffmpeg.exe',
                # '-loop', '1',
                # '-framerate', '25',  # 设置推流帧率为 25 帧/秒
                # '-pattern_type', 'glob',  # 使用通配符匹配图片
                # '-i', f'{image_folder}\*.png',  # 输入图片（支持通配符）
                # '-i', input_pattern,
                '-i', path,  # 输入音频
                '-vf', 'scale=out_color_matrix=bt709,format=yuv420p',  # 视频滤镜设置
                '-c:v', 'libx264',  # 视频编码器
                '-c:a', 'aac',  # 音频编码器
                '-preset', 'veryfast',  # 编码速度
                '-crf', '16',  # 质量控制
                '-shortest',  # 确保推流在音频结束时停止
                '-f', 'flv',  # 输出格式
                rtmp_url  # RTMP 输出地址
            ]

            # 启动FFmpeg进程
            process = subprocess.Popen(ffmpeg_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            # 等待进程结束
            stdout, stderr = process.communicate()

            # 打印FFmpeg输出（调试用）
            print(stdout.decode())
            print(stderr.decode())
