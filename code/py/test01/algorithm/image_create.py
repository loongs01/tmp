import cv2
import numpy as np

# # 定义参数
# num_frames = 100  # 帧数
# height, width = 480, 640  # 图像高度和宽度
# channels = 3  # 图像通道数（RGB）
#
# # 生成 100 帧 480x640 的黑色图像
# image_data = np.zeros((num_frames, height, width, channels), dtype=np.uint8)

# 定义参数
num_frames = 100  # 帧数
height, width = 480, 640  # 图像高度和宽度
channels = 3  # 图像通道数（RGB）

# 生成 100 帧 480x640 的黑色图像
image_data = np.zeros((num_frames, height, width, channels), dtype=np.uint8)

# 在每一帧上添加文本
for i in range(num_frames):
    # 在图像上添加文本（BGR 格式）
    cv2.putText(
        image_data[i],  # 输入图像（第 i 帧）
        f"Frame {i + 1}",  # 文本内容
        (50, 240),  # 文本位置（左上角坐标）
        cv2.FONT_HERSHEY_SIMPLEX,  # 字体类型
        1,  # 字体大小
        (255, 255, 255),  # 文本颜色（白色，BGR 格式）
        2  # 文本粗细
    )

# 打印数组形状以验证
print("图像数据的形状:", image_data.shape)

# 可选：显示第一帧以验证效果
cv2.imshow("Frame 1", image_data[0])
cv2.waitKey(0)
cv2.destroyAllWindows()
