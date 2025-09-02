# 建立数据库连接
import mysql.connector

db = mysql.connector.connect(
    host="192.168.10.105",  # 数据库服务器地址
    user="licz.1",  # 数据库用户名
    password="GjFmT5NEiE",  # 数据库密码
    database="sq_liufengdb"  # 数据库名称
)

"""获取数据库中所有包含user_id字段的表名"""
cursor = db.cursor()
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
              and t.TABLE_NAME not like 'ads%' \
            """
    cursor.execute(query, (db.database,))
    # print(cursor.fetchall())
    datas = cursor.fetchall()
    print(datas)
    for row in datas:
        print(row)

    newlist = [row[0] for row in cursor.fetchall()]  # 列表推导式new_list = [expression for item in iterable if condition]

    print(newlist)
finally:
    cursor.close()
