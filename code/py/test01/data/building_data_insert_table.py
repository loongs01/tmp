import csv
import random
import string
from datetime import timedelta, datetime, date

import mysql.connector
from collections import defaultdict

from faker import Faker

from data.building_column_iterator import column_iterator, get_datas

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
        return data_lists
    finally:
        # 关闭数据库连接
        db.close()


# user_ids = get_userid_datas()
# print(type(user_ids))


def generate_user_data(db_config):
    def random_date(start, end):
        """生成两个日期之间的随机日期"""
        # 确保 start 和 end 都是 datetime.datetime 对象
        if isinstance(start, date) and not isinstance(start, datetime):
            start = datetime.combine(start, datetime.min.time())
        if isinstance(end, date) and not isinstance(end, datetime):
            end = datetime.combine(end, datetime.min.time())

        delta = end - start
        random_days = random.randint(0, delta.days)
        return start + timedelta(days=random_days)

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
        return

    # data_source = get_datas()
    # preferred_genress = data_source['preferred_genres']
    # favorite_artists_or_celebritiess = data_source['favorite_artists_or_celebrities']
    # followed_entertainment_accountss = data_source['followed_entertainment_accounts']

    # ids = get_userid_datas()
    # 生成数据
    data = []
    for user_id in get_userid_datas():
        # 时间维度 partiiton
        stat_date = random_date(datetime(2027, 1, 1), datetime(2027, 12, 31)).date()
        # 假设的事件类型和描述
        event_types = ['结婚', '生子', '购房', '升学', '离职']
        event_descriptions = {
            '结婚': ['与爱人喜结连理', '举办了盛大的婚礼', '开始了婚姻生活'],
            '生子': ['迎来了第一个孩子', '家庭新增成员', '成为了父母'],
            '购房': ['购买了第一套房产', '搬入了新居', '实现了安居梦想'],
            '升学': ['考入了理想大学', '获得了硕士学位', '开始了博士研究'],
            '离职': ['辞去了工作', '开始了创业之旅', '寻找新的职业机会']
        }

        # 生成用户ID和事件ID

        event_id = f"EVT{user_id}{random.randint(1000, 9999)}"

        # 生成事件类型和描述
        event_type = random.choice(event_types)
        event_description = random.choice(event_descriptions.get(event_type, ['无详细描述']))

        # 根据表结构生成record
        record = (
            # None,  # id is auto-increment
            user_id,
            stat_date,
            fake.name(),  # user_name
            event_id,  # event_id
            event_type,  # event_type
            random_date(datetime(2000, 1, 1), datetime(2025, 12, 31)).date(),  # event_date
            event_description,  # event_description
            fake.name(),  # related_persons
            fake.address(),  # event_location
            random.randint(1, 10),  # event_importance_score
            random.randint(18, 60),  # user_age_at_event
            random.choice(['M', 'F', 'U']),  # user_gender
            random_address(),  # user_region
            random.choice(['消费增加', '职业转变', '生活方式改变', '无显著变化']),  # post_event_behavior_change
            round(random.uniform(10000, 10000000), 2),  # financial_impact_amount
            random.choice(['开心', '压力大', '平静', '兴奋']),  # emotional_impact_description
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),  # created_time
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # updated_time
        )

        # 写入数据
        data.append(record)

    return data


def insert_data_to_mysql(data, db_config):
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    insert_query = """
                   INSERT INTO dwd_user_life_events_di
                   ( user_id, stat_date, user_name, event_id, event_type, event_date,
                    event_description, related_persons, event_location, event_importance_score,
                    user_age_at_event, user_gender, user_region, post_event_behavior_change,
                    financial_impact_amount, emotional_impact_description, created_time, updated_time)
                   VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) \
                   """
    try:
        # 打印一条数据样例
        print('打印一条数据样例：' + str(data[0]))
        print('元素数据类型：' + str(type(data[0])))
        cursor.executemany(insert_query, data)
        connection.commit()
        print("数据插入成功！")
        print('数据插入量' + str(len(data)))
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
    # 生成数据并插入到数据库
    user_data = generate_user_data(db_config)
    insert_data_to_mysql(user_data, db_config)
