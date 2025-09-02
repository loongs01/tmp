import mysql.connector
from pandas.io.sas.sas_constants import column_data_length_length


def get_column(db, table_schema, table_name):
    cursor = db.cursor()
    query = f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA ='{table_schema}'
              AND TABLE_NAME = '{table_name}'
            """
    cursor.execute(query)
    data = cursor.fetchall()
    column_list = []
    for row in data:
        if (row[0] not in ('id', 'user_id', 'stat_date', 'created_time', 'updated_time')):
            column_list.append(row[0])
    return column_list


def column_iterator(db, table_schema, table_name):
    cursor = db.cursor()
    columns = get_column(db, table_schema, table_name)
    data_dict = {}
    for column in columns:
        query = f'''select {column}
                   from {table_name}
                   -- where id < 16'''
        cursor.execute(query)
        data = cursor.fetchall()
        column_data = []
        for row in data:
            column_data.append(row[0])

        data_dict.update({column: list(set(column_data))})
    return data_dict


def get_datas():
    db = mysql.connector.connect(
        host="192.168.10.105",  # 数据库服务器地址
        user="licz.1",  # 数据库用户名
        password="GjFmT5NEiE",  # 数据库密码
        database="sq_liufengdb"  # 数据库名称
    )
    data = column_iterator(db, 'sq_liufengdb', 'dwd_user_life_events_di')
    # return data['user_name']
    # print(data['user_name'])
    return data


if __name__ == '__main__':
    db = mysql.connector.connect(
        host="192.168.10.105",  # 数据库服务器地址
        user="licz.1",  # 数据库用户名
        password="GjFmT5NEiE",  # 数据库密码
        database="sq_liufengdb"  # 数据库名称
    )
    # print(column_iterator(db, 'sq_liufengdb', 'dwd_user_basic_health_di'))
    # print(get_datas())
    # print(get_datas()['user_name'])
    # print(len(get_datas()['user_name']))

    data_sourse = get_datas()
    post_event_behavior_change = data_sourse['post_event_behavior_change']
    print(post_event_behavior_change)
