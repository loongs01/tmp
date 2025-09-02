import csv
import random
import string
from datetime import timedelta, datetime

import mysql.connector
from collections import defaultdict

from faker import Faker

# 初始化Faker库用于生成模拟数据
fake = Faker('zh_CN')


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
        print(type(data_lists[:5][0]))
        export_data_csv(r'user_id', data_lists, r'C:\Users\Licz.1\Desktop\tmp\用户id.csv')

        print(f"user_id去重后总数量: {len(data_lists)}")
        # 打印结果
        print("\n所有表中的user_id数据:")
        for table, user_ids in all_user_ids.items():
            print(f"\n表 {table} 的user_id数据:")
            print(user_ids[:2])  # 只打印前10个作为示例
            if len(user_ids) > 10:
                print(f"...(共 {len(user_ids)} 条记录)")
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

    # add
    # 生成随机字符串
    def random_string(length=8):
        letters = string.ascii_lowercase + string.digits
        return ''.join(random.choice(letters) for _ in range(length))

    # 生成随机日期
    def random_date(start, end):
        return start + timedelta(
            seconds=random.randint(0, int((end - start).total_seconds()))
        )

    # 生成性别
    def random_sex():
        return random.choice(['男', '女'])

    # 生成身份证号
    def random_id_card(sex=None):
        # 生成合理的身份证号（前17位）
        area_code = '110101'  # 北京市东城区
        birth_date = datetime.now().strftime('%Y%m%d')
        seq = ''.join([str(random.randint(0, 9)) for _ in range(3)])

        # 根据性别决定最后一位奇偶
        if sex == '男':
            last_digit = str(random.randint(1, 9))
        else:
            last_digit = str(random.randint(0, 8))

        return f"{area_code}{birth_date}{seq}{last_digit}"

    # 生成教育背景
    def random_education():
        educations = ['小学', '初中', '高中', '中专', '大专', '本科', '硕士', '博士']
        return random.choice(educations)

    # 生成职业
    def random_occupation():
        occupations = [
            '学生', '工程师', '教师', '医生', '护士', '律师', '设计师',
            '程序员', '产品经理', '销售', '市场专员', '会计', '金融分析师'
        ]
        return random.choice(occupations)

    # 生成工作信息
    def random_work():
        companies = [
            '阿里巴巴', '腾讯', '百度', '字节跳动', '华为', '小米', '京东',
            '美团', '滴滴', '拼多多', '网易', '360', '新浪'
        ]
        return random.choice(companies)

    # 生成地址
    def random_address():
        provinces = ['北京市', '上海市', '广东省', '江苏省', '浙江省', '四川省', '湖北省']
        cities = {
            '北京市': ['朝阳区', '海淀区', '东城区', '西城区', '丰台区', '石景山区'],
            '上海市': ['浦东新区', '徐汇区', '长宁区', '静安区', '普陀区', '虹口区'],
            '广东省': ['广州市', '深圳市', '东莞市', '佛山市', '中山市', '珠海市'],
            '江苏省': ['南京市', '苏州市', '无锡市', '常州市', '徐州市', '南通市'],
            '浙江省': ['杭州市', '宁波市', '温州市', '嘉兴市', '绍兴市', '金华市'],
            '四川省': ['成都市', '绵阳市', '德阳市', '宜宾市', '南充市', '达州市'],
            '湖北省': ['武汉市', '黄石市', '十堰市', '宜昌市', '襄阳市', '鄂州市']
        }

        province = random.choice(provinces)
        city = random.choice(cities[province]) if province in cities else random.choice(cities['北京市'])
        detail = fake.street_address()

        return f"{province}{city}{detail}"

    # 生成账号状态
    def random_status():
        return random.choice([1, 2])

    # 生成注销原因
    def random_leave_reason(status):
        if status == 2:  # 冻结状态
            reasons = ['违规操作', '长期不活跃', '安全风险', '其他原因']
            return random.choice(reasons)
        return None

    # 生成数据
    data = []
    for user_id in get_userid_datas():
        # record = (
        #     None,  # id is auto-increment
        #     user_id,
        #     random_date(datetime(2025, 1, 1), datetime(2025, 12, 31)),
        #     f'User{user_id}',
        #     random.choice(['男', '女']),
        #     f'ID{user_id:06}',
        #     random_date(datetime(1970, 1, 1), datetime(2000, 12, 31)),
        #     f'{random.choice(company_address_list)}',
        #     f'School Address {user_id}',
        #     random.randint(18, 60),
        #     f'Work {user_id}',
        #     f'login{user_id}',
        #     'password' + str(user_id),
        #     random.choice([1, 2]),
        #     random.randint(1630000000, 1730000000),
        #     None if random.choice([True, False]) else 'Reason',
        #     random_user_level(),
        #     random_rfm_score(),
        #     random_user_lifecycle(),
        #     random_boolean(),
        #     random_boolean(),
        #     random_boolean(),
        #     datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        #     datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # )
        record = {
            'user_id': user_id,
            'stat_date': random_date(datetime(2025, 1, 1), datetime(2025, 12, 31)),
            'user_name': fake.name(),
            'sex': random_sex(),
            'id_card': random_id_card(random_sex()),
            'birthday': fake.date_of_birth(minimum_age=18, maximum_age=80).strftime('%Y-%m-%d'),
            'home_address': random_address(),
            'company_address': random_address(),
            'school_address': random_address() if random.random() > 0.3 else None,  # 30%概率没有学校地址
            'occupation': random_occupation(),
            'education_background': random_education(),
            'age': random.randint(18, 60),
            'work': random_work(),
            'login_name': f"user{user_id}",
            'password': 'e10adc3949ba59abbe56e057f20f883e',  # MD5('123456')
            'status': random_status(),
            'logout_time': None,
            'leave_reason': None,
            'created_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'updated_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        data.append(record)

    return data


def insert_data_to_mysql(data, db_config):
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    insert_query = """
                   INSERT INTO dwd_user_info_di (user_id, stat_date, user_name, sex, id_card, birthday, home_address,
                                                 company_address, school_address, occupation, education_background,
                                                 age, work, login_name, password, status, logout_time, leave_reason,
                                                 created_time, updated_time)
                   VALUES (%(user_id)s, %(stat_date)s, %(user_name)s, %(sex)s, %(id_card)s,
                           %(birthday)s, %(home_address)s, %(company_address)s, %(school_address)s,
                           %(occupation)s, %(education_background)s, %(age)s, %(work)s,
                           %(login_name)s, %(password)s, %(status)s, %(logout_time)s,
                           %(leave_reason)s, %(created_time)s, %(updated_time)s)
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


if __name__ == '__main__':
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
