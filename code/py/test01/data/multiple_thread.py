import csv
import time
from collections import defaultdict
import mysql.connector
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


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


def get_table_userid_data(db_config, table_name, lock, result_dict):
    """线程安全地获取指定表中所有user_id数据"""
    try:
        # 每个线程创建自己的数据库连接
        db = mysql.connector.connect(**db_config)
        cursor = db.cursor()

        query = f"SELECT user_id FROM {table_name}"
        cursor.execute(query)
        user_ids = [row[0] for row in cursor.fetchall()]

        # 使用锁保护共享数据结构
        with lock:
            result_dict[table_name] = user_ids

    except Exception as e:
        print(f"Error processing table {table_name}: {str(e)}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'db' in locals():
            db.close()


def get_userid_datas():
    start_time = time.time()
    # 数据库配置
    db_config = {
        "host": "192.168.10.105",
        "user": "licz.1",
        "password": "GjFmT5NEiE",
        "database": "sq_liufengdb"
    }

    # 建立主数据库连接(仅用于获取表名)
    db = mysql.connector.connect(**db_config)

    try:
        # 获取所有包含user_id字段的表
        tables = get_tables_with_userid(db)
        print(f"包含user_id字段的表: {tables}")

        # 使用线程锁保护共享数据结构
        lock = threading.Lock()
        result_dict = defaultdict(list)

        # 创建线程池
        with ThreadPoolExecutor(max_workers=10) as executor:
            # 提交所有任务
            futures = {
                executor.submit(get_table_userid_data, db_config, table, lock, result_dict): table
                for table in tables
            }

            # 等待所有任务完成
            for future in as_completed(futures):
                table = futures[future]
                try:
                    future.result()  # 获取结果(如果有异常会在这里抛出)
                except Exception as e:
                    print(f"Table {table} processing failed: {str(e)}")

        # 合并所有结果
        all_user_ids = []
        for user_ids in result_dict.values():
            all_user_ids.extend(user_ids)

        print(f'user_id去重前总数量: {len(all_user_ids)}')
        data_lists = [str(item) for item in all_user_ids]
        data_lists = list(set(item.strip() for item in data_lists))
        data_lists.sort()

        print(f"user_id去重后总数量: {len(data_lists)}")
        end_time= time.time()
        total_time = end_time - start_time
        print(f'total_time: {total_time:.6f}')

        return data_lists
    finally:
        db.close()


if __name__ == '__main__':
    print(type(get_userid_datas()))
