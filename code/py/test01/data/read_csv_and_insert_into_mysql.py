import csv
import mysql.connector
from mysql.connector import Error


def read_csv_and_insert_into_mysql(csv_file_path, host, user, password, database, table_name):
    try:
        # 连接到MySQL数据库
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        if connection.is_connected():
            cursor = connection.cursor()

            # 执行参数化查询以获取列顺序
            query = """
                    SELECT COLUMN_NAME, COLUMN_COMMENT
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = %s
                      AND TABLE_NAME = %s
                    ORDER BY ORDINAL_POSITION
                    """
            cursor.execute(query, (database, table_name))

            columns_info = cursor.fetchall()
            column_names = [info[0] for info in columns_info]
            print(column_names)

            # 打开CSV文件并读取数据
            with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
                csv_reader = csv.DictReader(csv_file)

                # 假设CSV文件的第一行是列名
                csv_columns = csv_reader.fieldnames
                print(csv_columns)

                # 构建SQL插入语句
                placeholders = ', '.join(['%s'] * len(column_names))
                print(placeholders)
                sql = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({placeholders})"
                print(sql)
                # 逐行读取CSV文件并插入到MySQL表中
                for row in csv_reader:
                    cursor.execute(sql, tuple(row[column] for column in csv_columns))

            # 提交事务
            connection.commit()
            print("数据插入成功！")

    except Error as e:
        print(f"错误: {e}")
        if connection.is_connected():
            connection.rollback()

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL连接已关闭")


# 使用示例
csv_file_path = r'C:\Users\Licz.1\Desktop\用户域各主题表数据样例\用户基础信息表.csv'  # 替换为你的CSV文件路径
host = '192.168.10.105'  # 替换为你的MySQL服务器地址
user = 'licz.1'  # 替换为你的MySQL用户名
password = 'GjFmT5NEiE'  # 替换为你的MySQL密码
database = 'sq_liufengdb'  # 替换为你的数据库名
table_name = 'dim_user_info_di_0'  # 替换为你的表名

read_csv_and_insert_into_mysql(csv_file_path, host, user, password, database, table_name)
