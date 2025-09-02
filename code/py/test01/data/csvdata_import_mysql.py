import csv
from contextlib import nullcontext

import mysql.connector
from mysql.connector import Error


def read_csv_and_insert_into_mysql(csv_file_path, host, user, password, database, table_name):
    connection = None
    cursor = None
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

            # 获取数据库表列信息
            query = """
                    SELECT COLUMN_NAME, COLUMN_COMMENT, DATA_TYPE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = %s
                      AND TABLE_NAME = %s
                    ORDER BY ORDINAL_POSITION
                    """
            cursor.execute(query, (database, table_name))
            columns_info = cursor.fetchall()
            db_column_names = [info[0] for info in columns_info if info[0] != 'id']
            db_column_comments = [info[1] for info in columns_info if info[1] != '自增主键id']
            db_data_types = {info[1]: info[2] for info in columns_info if info[1] != '自增主键id'}
            print("数据库列名:", db_column_names)

            # 打开CSV文件并读取数据
            with (open(csv_file_path, mode='r', encoding='utf-8-sig') as csv_file):  # 使用 utf-8-sig 编码去除 BOM
                csv_reader = csv.DictReader(csv_file)
                csv_columns = [col.strip() for col in csv_reader.fieldnames if
                               col != '自增主键id'] if csv_reader.fieldnames else []
                print("CSV列名:", csv_columns)

                # 检查CSV列是否包含所有数据库列
                missing_columns = [col for col in db_column_comments if col not in csv_columns]
                if missing_columns:
                    raise ValueError(f"CSV文件中缺少以下数据库列: {missing_columns}")

                # 构建SQL插入语句
                placeholders = ', '.join(['%s'] * len(db_column_names))
                sql = f"INSERT INTO {table_name} ({', '.join(db_column_names)}) VALUES ({placeholders})"
                print("SQL语句:", sql)
                print(enumerate(csv_reader, 1))
                print(next(csv_reader))
                # 逐行读取CSV文件并插入到MySQL表中
                for row_num, row in enumerate(csv_reader, 1):
                    try:
                        # 准备数据，确保顺序与数据库列一致
                        data = []
                        for col in db_column_comments:
                            if col in row:
                                value = row[col].strip() if row[col] is not None else None
                                if value == '':
                                    value = None
                                # 这里可以添加数据类型转换逻辑
                                # if db_data_types[col] == 'int':
                                #     value = int(value)
                                data.append(value)
                            else:
                                data.append(None)  # 或者设置默认值

                        cursor.execute(sql, tuple(data))
                    except Exception as e:
                        print(f"第 {row_num} 行数据插入失败: {e}")
                        connection.rollback()
                        continue

            # 提交事务
            connection.commit()
            print("数据插入成功！")

    except Error as e:
        print(f"数据库错误: {e}")
        if connection and connection.is_connected():
            connection.rollback()
    except Exception as e:
        print(f"发生错误: {e}")
    finally:
        if connection and connection.is_connected():
            if cursor:
                cursor.close()
            connection.close()
            print("MySQL连接已关闭")


# 使用示例
csv_file_path = r'C:\Users\Licz.1\Desktop\用户域各主题表数据样例\用户基础信息表.csv'
host = '192.168.10.105'
user = 'licz.1'
password = 'GjFmT5NEiE'
database = 'sq_liufengdb'
table_name = 'dim_user_info_di_0'

read_csv_and_insert_into_mysql(csv_file_path, host, user, password, database, table_name)
