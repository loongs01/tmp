import csv
import os
import mysql.connector
import pandas as pd
from pathlib import Path
import datetime
import matplotlib.pyplot as plt
import matplotlib as mpl
from matplotlib.font_manager import FontProperties
import warnings
from matplotlib import gridspec

# 禁用所有警告
warnings.filterwarnings("ignore")


def export_data_to_csv(table_name, file_name):
    """导出数据库表数据到CSV文件"""
    try:
        # 数据库配置
        db_config = {
            'host': '192.168.10.105',
            'user': 'licz.1',
            'password': 'GjFmT5NEiE',
            'database': 'sq_liufengdb'
        }

        # 连接数据库
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # 获取列信息
        cursor.execute("""
                       SELECT COLUMN_NAME, COLUMN_COMMENT
                       FROM INFORMATION_SCHEMA.COLUMNS
                       WHERE TABLE_SCHEMA = %s
                         AND TABLE_NAME = %s
                       ORDER BY ORDINAL_POSITION
                       """, (db_config['database'], table_name))

        columns_info = cursor.fetchall()
        column_names = [info[0] for info in columns_info]
        column_comments = [info[1] or info[0] for info in columns_info]

        # 查询数据
        cursor.execute(f"""
            SELECT * FROM {table_name} 
            --  WHERE user_id IN ('25123451','25123452','25123453','25123454','25123455') 
            ORDER BY user_id ASC
        """)
        datas = cursor.fetchall()

        # 创建输出目录
        base_dir = Path.home() / 'Desktop' / '用户域各主题表数据样例'
        base_dir.mkdir(parents=True, exist_ok=True)
        file_path = base_dir / file_name

        # 格式化数据
        def format_value(value):
            if value is None:
                return ''
            if isinstance(value, (datetime.date, datetime.datetime)):
                return value.strftime('%Y-%m-%d %H:%M:%S')
            return str(value)

        # 写入CSV文件
        with open(file_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(column_comments)
            writer.writerows([[format_value(item) for item in row] for row in datas])

        print(f"数据成功导出到 {file_path}")
        return file_path

    except mysql.connector.Error as err:
        print(f"数据库错误: {err}")
        return None
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()


def set_chinese_font():
    """设置支持中文的字体"""
    try:
        # 尝试加载常见中文字体
        font_path = None
        possible_fonts = [
            'msyh.ttc',  # 微软雅黑
            'simhei.ttf',  # 黑体
            'simsun.ttc',  # 宋体
            'arialuni.ttf'  # Arial Unicode
        ]

        for font in possible_fonts:
            try:
                font_prop = FontProperties(fname=font)
                plt.rcParams['font.family'] = font_prop.get_name()
                mpl.rcParams['axes.unicode_minus'] = False
                return True
            except:
                continue

        # 回退到系统默认字体
        plt.rcParams['font.family'] = 'sans-serif'
        plt.rcParams['font.sans-serif'] = ['Microsoft YaHei', 'SimHei', 'Arial Unicode MS']
        mpl.rcParams['axes.unicode_minus'] = False
        return True
    except Exception as e:
        print(f"字体设置失败: {str(e)}")
        return False


def csv_to_table_image(csv_file, image_file, max_rows=20):
    """将CSV转换为表格图片（解决重叠问题）"""
    try:
        # 设置后端和字体
        plt.switch_backend('Agg')
        set_chinese_font()

        # 读取数据
        data = pd.read_csv(csv_file, encoding='utf-8-sig')
        display_data = data.head(max_rows)

        # 动态计算表格尺寸
        num_cols = len(display_data.columns)
        num_rows = len(display_data)

        # 根据内容调整图形大小
        col_width = max([len(str(x)) for x in display_data.columns] + [10])
        row_height = 0.5
        fig_width = min(20, max(10, num_cols * col_width * 0.2))
        fig_height = min(20, max(6, num_rows * row_height * 1.5))

        # 创建图形和表格
        fig = plt.figure(figsize=(fig_width, fig_height))
        gs = gridspec.GridSpec(1, 1)
        ax = plt.subplot(gs[0])
        ax.axis('off')

        # 创建表格并设置样式
        table = ax.table(
            cellText=display_data.values,
            colLabels=display_data.columns,
            cellLoc='center',
            loc='center',
            colWidths=[0.15] * num_cols  # 固定列宽
        )

        # 自动调整字体大小防止重叠
        table.auto_set_font_size(False)
        font_size = self_adjust_font_size(num_cols, num_rows)
        table.set_fontsize(font_size)

        # 设置单元格样式
        for key, cell in table.get_celld().items():
            cell.set_edgecolor('#dddddd')
            cell.set_height(0.1)  # 增加行高
            if key[0] == 0:  # 表头
                cell.set_text_props(weight='bold', color='#333333')
                cell.set_facecolor('#f0f0f0')
            else:
                cell.set_facecolor('white')

        # 调整布局防止重叠
        plt.tight_layout(pad=3.0)  # 增加内边距
        gs.tight_layout(fig, pad=3.0)

        # 保存图片
        plt.savefig(
            image_file,
            format='png',
            bbox_inches='tight',
            dpi=300,
            pad_inches=0.5,  # 增加图片边距
            facecolor='white'
        )
        plt.close()
        print(f"表格图片已优化保存到 {image_file}")

    except Exception as e:
        print(f"生成图片出错: {str(e)}")
        if 'plt' in locals():
            plt.close()


def self_adjust_font_size(num_cols, num_rows):
    """根据表格尺寸自动调整字体大小"""
    base_size = 10
    if num_cols > 15:
        return max(6, base_size - (num_cols - 15) * 0.2)
    if num_rows > 30:
        return max(6, base_size - (num_rows - 30) * 0.1)
    return base_size


if __name__ == "__main__":
    csv_path = export_data_to_csv('ads_user_profile_da', '用户画像汇总表.csv')
    if csv_path:
        image_path = Path(csv_path).with_suffix('.png')
        csv_to_table_image(csv_path, image_path)
