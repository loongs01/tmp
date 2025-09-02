import csv
import time
from collections import defaultdict

import mysql.connector


def export_data_csv(columns, datas, location):
    with open(location, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([columns])
        for data in datas:
            writer.writerow([str(data)])
        return


def get_tables_with_userid(db_connection):
    """获取数据库中所有包含user_id字段的表名"""
    cursor = db_connection.cursor()
    try:
        # 查询INFORMATION_SCHEMA获取所有包含user_id列的表
        query = """
                SELECT TABLE_NAME
                -- , TABLE_SCHEMA
                -- , t.*
                FROM INFORMATION_SCHEMA.COLUMNS as t
                WHERE COLUMN_NAME = 'user_id'
                  AND TABLE_SCHEMA = %s
                  and t.TABLE_NAME not like 'dws%'
                  and t.TABLE_NAME not like 'ads%'
                  and t.TABLE_NAME not like 'dwd_user%'
                """
        cursor.execute(query, (db_connection.database,))
        return [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()


def get_all_userid_data(db_connection, table_name):
    """获取指定表中所有user_id数据"""
    cursor = db_connection.cursor()
    try:
        query = f"SELECT user_id FROM {table_name}"
        cursor.execute(query)
        return [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()


def get_userid_datas():
    start_time = time.time()
    # 建立数据库连接
    db = mysql.connector.connect(
        host="192.168.10.105",  # 数据库服务器地址
        user="licz.1",  # 数据库用户名
        password="GjFmT5NEiE",  # 数据库密码
        database="sq_liufengdb"  # 数据库名称
    )

    try:
        # 获取所有包含user_id字段的表
        tables = get_tables_with_userid(db)
        print(f"包含user_id字段的表: {tables}")

        # 存储所有user_id数据
        all_user_ids = defaultdict(list)

        data_lists = []

        # 获取每个表的user_id数据
        for table in tables:
            user_ids = get_all_userid_data(db, table)
            all_user_ids[table] = user_ids
            # print(f"表 {table} 中的user_id数量: {len(user_ids)}")
            data_lists.extend(user_ids)
        print(f'user_id去重前总数量: {len(data_lists)}')
        data_lists = [str(item) for item in data_lists]
        data_lists = list(set(item.strip() for item in data_lists))
        data_lists.sort()
        print(type(data_lists[:5][0]))
        # export_data_csv(r'user_id', data_lists, r'C:\Users\Licz.1\Desktop\tmp\用户id.csv')

        print(f"user_id去重后总数量: {len(data_lists)}")
        # 打印结果
        # print("\n所有表中的user_id数据:")
        # for table, user_ids in all_user_ids.items():
        #     print(f"\n表 {table} 的user_id数据:")
        #     print(user_ids[:10])  # 只打印前10个作为示例
        #     if len(user_ids) > 10:
        #         print(f"...(共 {len(user_ids)} 条记录)")

        end_time1 = time.time()
        tootal_time = end_time1 - start_time
        print(f"tootal_time: {tootal_time:.6f}秒")

        return data_lists

    finally:
        # 关闭数据库连接
        db.close()
        end_time = time.time()
        tootal_time = end_time - start_time
        print(f"tootal_time: {tootal_time:.6f}秒")


if __name__ == '__main__':
    print(type(get_userid_datas()))
