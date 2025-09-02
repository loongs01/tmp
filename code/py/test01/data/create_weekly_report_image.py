from PIL import Image, ImageDraw, ImageFont
import textwrap
import os


def create_weekly_report_image(output_path="weekly_report.png"):
    # === 1. 设置图片尺寸和背景 ===
    width, height = 1200, 1600  # 增大尺寸，避免文字拥挤
    background_color = (255, 255, 255)  # 白色背景
    image = Image.new("RGB", (width, height), background_color)
    draw = ImageDraw.Draw(image)

    # === 2. 自动检测中文字体（从 C:\Windows\Fonts 查找） ===
    fonts_dir = "C:/Windows/Fonts"  # Windows 字体目录
    possible_fonts = ["simhei.ttf", "msyh.ttf", "msyh.ttc", "simsun.ttc"]  # 常见中文字体

    font_path = None
    for font_name in possible_fonts:
        test_path = os.path.join(fonts_dir, font_name)
        if os.path.exists(test_path):
            font_path = test_path
            break

    if not font_path:
        raise FileNotFoundError(
            "未找到中文字体！请确保以下字体之一存在于 C:/Windows/Fonts/:\n"
            "- simhei.ttf (黑体)\n"
            "- msyh.ttf (微软雅黑)\n"
            "- simsun.ttc (宋体)\n"
            "或手动指定字体路径。"
        )

    # === 3. 加载字体（标题和正文分开） ===
    try:
        title_font = ImageFont.truetype(font_path, 36)  # 标题字体
        body_font = ImageFont.truetype(font_path, 24)  # 正文字体
    except Exception as e:
        raise RuntimeError(f"字体加载失败！请检查文件是否损坏。\n错误详情: {e}")

    # === 4. 定义内容 ===
    title = "本周工作内容与下周计划"

    weekly_content = """
    一、数据校验与清洗
    1. DWS层库表校验与清洗
    2. ODS库表数据构造与验证

    二、数据构造与模拟
    3. 用户域主题表模拟数据构造

    三、ETL程序优化
    1. ODS至DWD层ETL优化
    2. 非结构化数据清洗开发

    四、NLP模型开发与调研
    1. 自定义NLP提取器开发
    2. 主流NLP技术框架调研

    五、模型结构调整与数据更新
    1. 用户域模型结构调整
    """

    next_week_plan = """
    一、自定义NLP提取器优化
    二、DWD明细表清洗脚本开发
    三、NLP模型技术对比验证
    四、大模型数据需求对接
    """

    issues_solutions = """
    问题：数据清洗规则不完善
    解决方案：加强调研与测试

    问题：需求不断调整
    解决方案：加强需求沟通与确认
    """

    innovations_thoughts = """
    创新想法：
    - 结合自定义NLP与商业化API
    - 设计可配置化清洗规则模板

    感想：
    - 数据清洗复杂且具挑战性
    - 需求沟通与确认至关重要
    """

    # === 5. 优化文本绘制（自动换行 + 合理间距） ===
    def draw_text_with_wrap(draw, text, x, y, font, max_width, line_spacing=10, paragraph_spacing=20):
        """
        :param draw: ImageDraw 对象
        :param text: 要绘制的文本
        :param x: 起始 X 坐标
        :param y: 起始 Y 坐标
        :param font: 使用的字体
        :param max_width: 最大宽度（像素）
        :param line_spacing: 行间距（默认 10px）
        :param paragraph_spacing: 段落间距（默认 20px）
        """
        # 使用 textwrap 换行
        lines = textwrap.wrap(text, width=40)  # 调整每行字符数

        current_y = y
        for line in lines:
            if not line.strip():  # 空行（段落间隔）
                current_y += paragraph_spacing
                continue

            draw.text((x, current_y), line, font=font, fill=(0, 0, 0))
            line_height = font.getbbox("A")[3] - font.getbbox("A")[1]  # 计算行高
            current_y += line_height + line_spacing

    # === 6. 绘制内容到图片 ===
    # 标题
    draw.text((50, 30), title, fill=(0, 0, 0), font=title_font)

    # 本周工作内容
    draw.text((50, 100), "一、本周工作内容", fill=(0, 0, 0), font=title_font)
    draw_text_with_wrap(draw, weekly_content, 50, 180, body_font, width - 100)

    # 下周工作计划
    draw.text((50, 500), "二、下周工作计划", fill=(0, 0, 0), font=title_font)
    draw_text_with_wrap(draw, next_week_plan, 50, 580, body_font, width - 100)

    # 遇到的问题与解决方案
    draw.text((50, 800), "三、遇到的问题与解决方案", fill=(0, 0, 0), font=title_font)
    draw_text_with_wrap(draw, issues_solutions, 50, 880, body_font, width - 100)

    # 创新想法与感想
    draw.text((50, 1100), "四、创新想法与感想", fill=(0, 0, 0), font=title_font)
    draw_text_with_wrap(draw, innovations_thoughts, 50, 1180, body_font, width - 100)

    # === 7. 保存图片 ===
    image.save(output_path, dpi=(300, 300))  # 高分辨率输出
    print(f"图片已保存至 {os.path.abspath(output_path)}")


# 调用函数生成图片
if __name__ == "__main__":
    create_weekly_report_image()