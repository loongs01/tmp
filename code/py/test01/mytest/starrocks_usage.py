import mysql.connector
from mysql.connector import Error

try:
    conn = mysql.connector.connect(
        host="192.168.122.1",  # FE 节点 IP
        port=9030,  # MySQL 协议端口
        user="root",  # 用户名
        password="0205",  # 密码
        database="lcz",  # 数据库名
        connect_timeout=5  # 超时时间（秒）
    )

    if conn.is_connected():
        print("成功连接到 StarRocks")
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES;")  # 测试查询（StarRocks 可能不支持 SHOW USERS）
        for row in cursor:
            print(row)

except Error as e:
    print(f"连接失败: {e}")

finally:
    if 'conn' in locals() and conn.is_connected():
        cursor.close()
        conn.close()
        print("连接已关闭")
