import cv2
import matplotlib.pyplot as plt

# 读取图像
image = cv2.imread(r"C:\Users\Licz.1\Desktop\tmp\test.png")

if image is None:
    print("无法加载图像，请检查路径或文件是否存在")
else:
    # 转换为 RGB 格式
    image_rgb = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)

    # 用 OpenCV 显示（BGR 格式）
    cv2.imshow("Original Image (BGR)", image)
    cv2.waitKey(0)
    cv2.destroyAllWindows()

    # 用 Matplotlib 显示（RGB 格式）
    plt.imshow(image_rgb)
    plt.axis('off')
    plt.show()
