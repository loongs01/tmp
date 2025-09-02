import csv
import random
from datetime import timedelta, datetime

import mysql.connector
from collections import defaultdict
from faker import Faker

faker = Faker('zh_CN')


def export_data_csv(columns, datas, location):
    with open(location, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([columns])
        for data in datas:
            # writer.writerow(data)
            writer.writerow([data])
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
            print(f"表 {table} 中的user_id数量: {len(user_ids)}")
            data_lists.extend(user_ids)
        print(f'user_id去重前总数量: {len(data_lists)}')
        data_lists = [str(item) for item in data_lists]
        data_lists = list(set(item.strip() for item in data_lists))
        data_lists.sort()
        print(data_lists[:5])
        print(data_lists[:5][0])
        print(type(data_lists[:5][0]))
        export_data_csv(r'user_id', data_lists, r'C:\Users\Licz.1\Desktop\tmp\用户id.csv')

        print(f"user_id去重后总数量: {len(data_lists)}")
        # 打印结果
        # print("\n所有表中的user_id数据:")
        # for table, user_ids in all_user_ids.items():
        #     print(f"\n表 {table} 的user_id数据:")
        #     print(user_ids[:10])  # 只打印前10个作为示例
        #     if len(user_ids) > 10:
        #         print(f"...(共 {len(user_ids)} 条记录)")
        return data_lists
    finally:
        # 关闭数据库连接
        db.close()


user_ids = get_userid_datas()
print(type(user_ids))


def generate_user_data(db_config):
    # 定义一些随机数据生成器
    def random_date(start_date, end_date):
        time_between_dates = end_date - start_date
        days_between_dates = time_between_dates.days
        random_number_of_days = random.randrange(days_between_dates)
        random_date = start_date + timedelta(days=random_number_of_days)
        return random_date.strftime('%Y-%m-%d')

    def random_user_level():
        return random.choice([1, 2, 3, 4, 5])

    def random_rfm_score():
        return f"{random.randint(1, 5)}-{random.randint(1, 5)}-{random.randint(1, 5)}"

    def random_user_lifecycle():
        lifecycles = ['新用户', '成长期', '成熟期', '衰退期', '流失']
        return random.choice(lifecycles)

    def random_boolean():
        return random.choice([0, 1])

    def random_text():
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        # 获取列的值
        cursor.execute("""
                       select company_address
                       -- , school_address
                       from dim_user_info_di_tmp
                       where id < 16
                       """)

        data = cursor.fetchall()
        company_address_list = []
        for row in data:
            company_address_list.append(row[0])
        return company_address_list

    company_address_list = random_text()
    # 生成数据
    data = []
    for user_id in get_userid_datas():
        record = (
            None,  # id is auto-increment
            user_id,
            random_date(datetime(2025, 1, 1), datetime(2025, 12, 31)),
            faker.name(),
            random.choice(['男', '女']),
            faker.ssn(),
            random_date(datetime(1970, 1, 1), datetime(2000, 12, 31)),
            faker.street_address(),
            faker.address(),
            random.randint(18, 60),
            f'Work {user_id}',
            f'login{user_id}',
            'password' + str(user_id),
            random.choice([1, 2]),
            random.randint(1630000000, 1730000000),
            None if random.choice([True, False]) else 'Reason',
            random_user_level(),
            random_rfm_score(),
            random_user_lifecycle(),
            random_boolean(),
            random_boolean(),
            random_boolean(),
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        )

        data.append(record)

    return data


def insert_data_to_mysql(data, db_config):
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    insert_query = """
                   INSERT INTO dim_user_info_di
                   (user_id, stat_date, name, sex, id_card, birthday, company_address, school_address, age, work,
                    login_name, password, status, logout_time, leave_reason, user_level, rfm_score, user_lifecycle,
                    is_high_value, is_premium, is_content_creator, create_time, update_time)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                           %s)
                   """

    try:
        print(data[:5])
        cursor.executemany(insert_query, data)
        connection.commit()
        print("数据插入成功！")
    except mysql.connector.Error as err:
        print(f"插入数据时出错: {err}")
    finally:
        cursor.close()
        connection.close()


# 数据库连接配置
db_config = {
    'host': '192.168.10.105',  # 数据库主机
    'user': 'licz.1',  # 数据库用户名
    'password': 'GjFmT5NEiE',  # 数据库密码
    'database': 'sq_liufengdb'  # 数据库名称
}
# 生成数据并打印
user_data = generate_user_data(db_config)
for record in user_data[:3]:
    print(record)
# 生成数据并插入到数据库
user_data = generate_user_data(db_config)
insert_data_to_mysql(user_data, db_config)
print(faker.address())