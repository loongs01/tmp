import csv
import os
import mysql.connector
from pathlib import Path
import datetime


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
            -- WHERE user_id IN ('25123451','25123452','25123453','52847682','94270657') 
            ORDER BY user_id ASC
        """)
        datas = cursor.fetchall()

        # 创建输出目录
        base_dir = Path.home() / 'Desktop' / '用户域各主题表数据样例/衣食住行'
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


if __name__ == "__main__":
    # 导出数据
    csv_path = export_data_to_csv('dws_user_transport_trip_agg_di', '用户出行行为日汇总表.csv')
    print(csv_path)
