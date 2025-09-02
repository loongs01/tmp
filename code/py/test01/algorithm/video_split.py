import os
import subprocess


def split_video_into_parts(input_path, output_dir, num_parts=5):
    """
    将视频平均切割为指定份数
    :param input_path: 输入视频路径
    :param output_dir: 输出目录
    :param num_parts: 切割的份数
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 获取视频文件名（不带扩展名）
    video_name = os.path.splitext(os.path.basename(input_path))[0]

    # 获取视频总时长（秒）
    cmd_duration = f'ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "{input_path}"'
    duration = float(subprocess.check_output(cmd_duration, shell=True).decode('utf-8').strip())

    # 计算每个片段的时长
    segment_duration = duration / num_parts
    # 存放视频切片绝对路径
    path_list = []

    # 切割视频
    for i in range(num_parts):
        start_time = i * segment_duration
        end_time = (i + 1) * segment_duration
        output_path = os.path.join(output_dir, f"{video_name}_part_{i + 1}.mp4")
        # 路径存在覆盖 -y
        cmd = f'ffmpeg -y -i "{input_path}" -ss {start_time:.2f} -to {end_time:.2f} -c copy "{output_path}"'
        subprocess.run(cmd, shell=True, check=True)
        print(f"切割完成: {output_path}")
        print(type(output_path), output_path)
        path_list.append(output_path)

    return path_list


if __name__ == "__main__":
    input_video = r"D:\test\01\result_1748419030.mp4"  # 替换为你的视频路径
    output_directory = r"D:\test\01\split"  # 替换为输出目录
    num_segments = 5  # 切割为 5 份

    print(split_video_into_parts(input_video, output_directory, num_segments))
