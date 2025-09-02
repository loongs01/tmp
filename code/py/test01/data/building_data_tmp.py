import random
import string
from datetime import datetime, timedelta
from faker import Faker
import pymysql
from pymysql.cursors import DictCursor

from data.building_data_from_dbfield import get_userid_datas

# 初始化Faker库用于生成模拟数据
fake = Faker('zh_CN')

# 数据库连接配置
db_config = {
    'host': '192.168.10.105',  # 数据库主机
    'user': 'licz.1',  # 数据库用户名
    'password': 'GjFmT5NEiE',  # 数据库密码
    'database': 'sq_liufengdb'  # 数据库名称
}


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


# 生成模拟用户数据
def generate_user_data(stat_date, user_count=100):
    users = []
    for user_id in get_userid_datas():
        sex = random_sex()

        # for i in range(1, user_count + 1):
        #     sex = random_sex()
        #     user_id = 10000000 + i  # 模拟用户ID从10000001开始

        user = {
            'user_id': user_id,
            'stat_date': stat_date,
            'user_name': fake.name(),
            'sex': sex,
            'id_card': random_id_card(sex),
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

        # 如果是冻结状态，设置注销原因
        if user['status'] == 2:
            user['leave_reason'] = random_leave_reason(user['status'])
            user['logout_time'] = int((datetime.now() - timedelta(days=random.randint(1, 30))).timestamp())

        users.append(user)

    return users


# 插入数据到数据库
def insert_users_to_db(users, stat_date):
    try:
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()

        # 检查分区是否存在，不存在则创建
        check_partition_sql = f"""
        SELECT PARTITION_NAME 
        FROM INFORMATION_SCHEMA.PARTITIONS 
        WHERE TABLE_NAME = 'dwd_user_info_di' 
        AND PARTITION_NAME = 'p{stat_date.replace("-", "")[:6]}'
        """
        cursor.execute(check_partition_sql)
        result = cursor.fetchone()

        if not result:
            # 创建新分区
            partition_name = f"p{stat_date.replace("-", "")[:6]}"
            add_partition_sql = f"""
            ALTER TABLE dwd_user_info_di ADD PARTITION (
                PARTITION {partition_name} VALUES LESS THAN (TO_DAYS('{stat_date}'))
            )
            """
            cursor.execute(add_partition_sql)
            print(f"Created new partition: {partition_name}")

        # 准备插入数据
        insert_sql = """
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

        # 批量插入
        cursor.executemany(insert_sql, users)
        conn.commit()
        print(f"Successfully inserted {len(users)} users for date {stat_date}")

    except Exception as e:
        print(f"Error inserting data: {e}")
        conn.rollback()
    finally:
        if conn:
            conn.close()


# 主函数
def main():
    # 生成最近30天的数据
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=30)

    current_date = start_date
    while current_date <= end_date:
        # 生成100个用户数据
        users = generate_user_data(current_date.strftime('%Y-%m-%d'), user_count=100)
        # 插入数据库
        insert_users_to_db(users, current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(days=1)


if __name__ == '__main__':
    main()
