# coding: utf-8
# author: hmk


import pymysql.cursors
import sqlalchemy as sqla
from common.getConfig import GetConfig

class HandleMysql(object):
    # def __init__(self,hostname,user,password,dbname,port):
    def __init__(self,flag=None):
        if flag:
            host = GetConfig().get_db(flag, "host")
            user = GetConfig().get_db(flag, "user")
            password = GetConfig().get_db(flag, "passwd")
            db = GetConfig().get_db(flag, "db")
            port = GetConfig().getint_db(flag, "port")
            self.db_config = {
                'host': host,
                 'port': port,
                 'user': user,
                 'password': password,
                 'db': db
                }
        else:
            print("请指定数据库标识,如 adbUS3,adbUS3,adbUS3")
            exit(1)

    def conn_mysql(self):
        db_config=self.db_config
        self.conn = pymysql.connect(**db_config)
        self.cur = self.conn.cursor()
        return self.conn

    def exec_data_sql(self, sql, data):
        """执行操作数据的相关sql"""
        self.conn_mysql()
        self.cur.execute(sql, data)
        self.conn.commit()

    def execute_sql(self, sql):
        """执行操作数据的相关sql"""
        self.conn_mysql()
        self.cur.execute(sql)
        self.conn.commit()


    def search(self, sql):
        """执行查询sql"""
        self.conn_mysql()
        self.cur.execute(sql)
        return self.cur.fetchall()

    def close_mysql(self):
        """关闭数据库连接"""
        self.cur.close()
        self.conn.close()

    def conn_engine(self):
        db_config=self.db_config
        user=db_config['user']
        password = db_config['password']
        host = db_config['host']
        port = db_config['port']
        self.connect_adb=sqla.create_engine(
            "mysql+pymysql://%s:%s@%s:%s/chapters_log?charset=utf8"%(user,password,host,port)
        )
        return self.connect_adb

    def connect_dispose(self):
        self.connect_adb.dispose()

if __name__ == '__main__':
    import pandas as pd
    from common.getConfig import GetConfig
    host = GetConfig().get_db("adbUS2","host")
    user = GetConfig().get_db("adbUS2","user")
    password = GetConfig().get_db("adbUS2","passwd")
    db = GetConfig().get_db("adbUS2","db")
    port = GetConfig().getint_db("adbUS2","port")
    handleMysql = HandleMysql(host, user, password, db, port)
    sql = "select 1"
    # for i in test.search(sql):
    #     print(i)
    # test.close_mysql()
    res=handleMysql.conn_engine()
    data=pd.read_sql(sql, res)
    print(data)
    handleMysql.connect_dispose()
