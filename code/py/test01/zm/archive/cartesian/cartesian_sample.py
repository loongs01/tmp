import pymysql
from pymysql.err import MySQLError

# StarRocks 连接配置
STARROCKS_CONFIG = {
    "host": "10.2.8.36",
    "port": 9030,
    "user": "root",
    "password": "iPXE83EEZSrOBUfe",
    "database": "test",
    "charset": "utf8",
    # pymysql expects int port
    "cursorclass": pymysql.cursors.DictCursor,
}


def get_substitution_data():
    """从 StarRocks 获取替代物料数据（使用 PyMySQL，纯 Python 实现，避免 C 扩展崩溃）"""
    connection = None
    cursor = None
    try:
        connection = pymysql.connect(
            host=STARROCKS_CONFIG["host"],
            port=int(STARROCKS_CONFIG["port"]),
            user=STARROCKS_CONFIG["user"],
            password=STARROCKS_CONFIG["password"],
            database=STARROCKS_CONFIG["database"],
            charset=STARROCKS_CONFIG["charset"],
            cursorclass=STARROCKS_CONFIG["cursorclass"],
            autocommit=True,
            connect_timeout=10,
        )

        cursor = connection.cursor()

        query = """
        SELECT DISTINCT 
            matnr,
            idnrk_s,
            idnrk,
            alprf
        FROM ods.ods_sap_erp_substitutmaterials_test
        ORDER BY matnr, idnrk_s, alprf
        """

        cursor.execute(query)
        data = cursor.fetchall()
        print(data)
        return data

    except MySQLError as e:
        print(f"数据库连接或查询错误: {e}")
        return []
    except Exception as e:
        # Catch unexpected exceptions to avoid silent native crashes
        print(f"其他错误: {e}")
        return []
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass
        try:
            if connection:
                connection.close()
        except Exception:
            pass


if __name__ == "__main__":
    print(get_substitution_data())
