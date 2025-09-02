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
import json
import textwrap
from matplotlib.patches import Rectangle

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
            WHERE user_id IN ('25123451','25123452','25123453','25123454','25123455') 
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


def csv_to_table_image(csv_file, image_file, max_rows=20, max_cols=10, max_cell_len=30):
    """将CSV转换为表格图片，支持列分页、内容缩略和自适应，防止重叠"""
    import math
    try:
        plt.switch_backend('Agg')
        set_chinese_font()

        # 读取数据
        data = pd.read_csv(csv_file, encoding='utf-8-sig')
        display_data = data.head(max_rows)
        total_cols = len(display_data.columns)
        num_rows = len(display_data)

        # 分页处理
        num_pages = math.ceil(total_cols / max_cols)
        for page in range(num_pages):
            start_col = page * max_cols
            end_col = min((page + 1) * max_cols, total_cols)
            sub_columns = display_data.columns[start_col:end_col]
            sub_data = display_data[sub_columns].copy()

            # 内容缩略处理
            def truncate(val):
                val = str(val)
                if len(val) > max_cell_len:
                    return val[:max_cell_len - 3] + '...'
                return val

            sub_data = sub_data.applymap(truncate)

            # 动态列宽
            col_widths = []
            for col in sub_columns:
                maxlen = max([len(str(x)) for x in [col] + list(sub_data[col])])
                col_widths.append(min(0.25, max(0.12, maxlen * 0.012)))

            fig_width = sum(col_widths) * 20
            fig_height = min(20, max(6, num_rows * 0.7))

            fig = plt.figure(figsize=(fig_width, fig_height))
            gs = gridspec.GridSpec(1, 1)
            ax = plt.subplot(gs[0])
            ax.axis('off')

            table = ax.table(
                cellText=sub_data.values,
                colLabels=sub_columns,
                cellLoc='center',
                loc='center',
                colWidths=col_widths
            )
            table.auto_set_font_size(False)
            font_size = self_adjust_font_size(len(sub_columns), num_rows)
            table.set_fontsize(font_size)

            for key, cell in table.get_celld().items():
                cell.set_edgecolor('#dddddd')
                cell.set_height(0.13)
                if key[0] == 0:
                    cell.set_text_props(weight='bold', color='#333333')
                    cell.set_facecolor('#f0f0f0')
                else:
                    cell.set_facecolor('white')

            plt.tight_layout(pad=3.0)
            gs.tight_layout(fig, pad=3.0)

            # 保存图片，分页命名
            if num_pages == 1:
                img_file = image_file
            else:
                img_file = Path(image_file).with_stem(f"{Path(image_file).stem}_part{page + 1}")
            plt.savefig(
                img_file,
                format='png',
                bbox_inches='tight',
                dpi=300,
                pad_inches=0.5,
                facecolor='white'
            )
            plt.close()
            print(f"表格图片已优化保存到 {img_file}")

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


def get_chinese_font():
    # Windows常见字体绝对路径
    possible_fonts = [
        r'C:\\Windows\\Fonts\\msyh.ttc',  # 微软雅黑
        r'C:\\Windows\\Fonts\\simhei.ttf',  # 黑体
        r'C:\\Windows\\Fonts\\simsun.ttc',  # 宋体
        r'C:\\Windows\\Fonts\\arialuni.ttf'  # Arial Unicode
    ]
    for font in possible_fonts:
        try:
            return FontProperties(fname=font)
        except Exception:
            continue
    # 回退
    return FontProperties()


def data_to_json(table_name, image_file, max_rows=20, max_cell_len=200, wrap_width=70, font_size=16,
                 key_color='#1a237e'):
    """将数据库查询数据以美观JSON格式输出为图片（紧凑布局/缩略/高亮key/防重叠/防乱码）"""
    try:
        # 数据库配置
        db_config = {
            'host': '192.168.10.105',
            'user': 'licz.1',
            'password': 'GjFmT5NEiE',
            'database': 'sq_liufengdb'
        }
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 0,1")
        col_names = [desc[0] for desc in cursor.description]
        cursor.fetchall()  # 清空结果，防止Unread result found
        cursor.execute(
            f"SELECT * FROM {table_name} WHERE user_id IN ('25123451') ORDER BY user_id ASC LIMIT {max_rows}")
        datas = cursor.fetchall()
        result = []
        for row in datas:
            item = {}
            for k, v in zip(col_names, row):
                if isinstance(v, (datetime.date, datetime.datetime)):
                    v = v.strftime('%Y-%m-%d %H:%M:%S')
                if v is None:
                    v = ''
                v = str(v)
                if len(v) > max_cell_len:
                    v = v[:max_cell_len - 3] + '...'
                item[k] = v
            result.append(item)
        import json
        json_str = json.dumps(result, ensure_ascii=False, indent=2)
        # 手动分行，分离key和value，key高亮
        lines = []
        for line in json_str.split('\n'):
            if ': ' in line:
                prefix, value = line.split(': ', 1)
                wrapped_value = textwrap.wrap(value, width=wrap_width, replace_whitespace=False,
                                              drop_whitespace=False) or ['']
                for i, val_line in enumerate(wrapped_value):
                    if i == 0:
                        lines.append((prefix, ': ' + val_line))
                    else:
                        lines.append((' ' * len(prefix), '  ' + val_line))
            else:
                lines.append((line, ''))
        # 紧凑布局：固定宽高，y等分
        n = len(lines)
        fig_width = 16
        fig_height = min(2 + 0.3 * n, 20)
        plt.switch_backend('Agg')
        set_chinese_font()
        zh_font = get_chinese_font()
        fig, ax = plt.subplots(figsize=(fig_width, fig_height))
        ax.axis('off')
        for i, (prefix, value) in enumerate(lines):
            y = 1 - (i + 1) / (n + 2)  # 顶部到底部均匀分布
            if value:
                ax.text(0.01, y, prefix, fontsize=font_size, fontproperties=zh_font, fontfamily='monospace', va='top',
                        ha='left', color=key_color, fontweight='bold')
                ax.text(0.01 + len(prefix) * 0.012, y, value, fontsize=font_size, fontproperties=zh_font,
                        fontfamily='monospace', va='top', ha='left', color='black')
            else:
                ax.text(0.01, y, prefix, fontsize=font_size, fontproperties=zh_font, fontfamily='monospace', va='top',
                        ha='left', color='black')
        plt.subplots_adjust(left=0.01, right=0.99, top=0.99, bottom=0.01)
        # 创建输出目录
        base_dir = Path.home() / 'Desktop' / '用户域各主题表数据样例'
        base_dir.mkdir(parents=True, exist_ok=True)
        file_path = base_dir / image_file

        plt.savefig(file_path, format='png', bbox_inches='tight', dpi=200, pad_inches=0.2, facecolor='white')
        plt.close()
        print(f"JSON图片已保存到 {file_path}")
    except Exception as e:
        print(f"生成JSON图片出错: {str(e)}")
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()


if __name__ == "__main__":
    csv_path = export_data_to_csv('ads_user_profile_da', '用户画像汇总表.csv')
    if csv_path:
        image_path = Path(csv_path).with_suffix('.png')
        csv_to_table_image(csv_path, image_path)
data_to_json('ads_user_profile_da', '用户画像JSON.png')
