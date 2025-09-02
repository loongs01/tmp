import sys
import time

import pymysql
import logging

logger = logging.getLogger(__name__)  # 操作日志对象

# for i in (1, 2, 3):
#     try:
#         conn = pymysql.connect(host='amv-2ev8c441hro07g58800000101o.ads.aliyuncs.com',
#                                port=3306,
#                                user='fyhd_dla',
#                                passwd='fyhd_dla123%',
#                                db='dw_data')
#         print("数据库连接成功")
#         break
#     except pymysql.err.OperationalError as e:
#         if i < 3:
#             logger.exception("数据库连接第{}次失败！".format(i))
#         else:
#             sys.exit(1)
#         raise

for i in (1, 2, 3):
    try:
        conn = pymysql.connect(host='am-2evbgp6s6eu7yci6e90650o.ads.aliyuncs.com',
                               port=3306,
                               user='lichaozhong',
                               passwd='PK50stAQJUujq#b6H8Eh4N2TLBWlGdXO',
                               db='dw_data')
        print("数据库连接成功")
        break
    except pymysql.err.OperationalError as e:
        if i < 3:
            logger.exception("数据库连接第{}次失败！".format(i))
        else:
            sys.exit(1)
        raise

print("跑批开始运行时间：" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

mycursor = conn.cursor()
sql = """select distinct CONCAT('show create table ',t.table_schema,'.',t.table_name,';'),1
--     t.table_comment
from
    information_schema. tables t
    ,information_schema. columns c
where t.table_schema=c.table_schema
and c.table_name = t.table_name
and t.table_schema='test_data'
and t.table_name not like 'dwd%'
and t.table_name not like 'dws%' 
and t.table_name not like 'dm%' 
and t.table_name not like 'myy_test%'
-- and t.table_comment like '%DAU%'
-- and c.column_name like '%dau%'
-- and c.column_comment like '%播放%'
limit 2
"""
try:
    mycursor.execute(sql)
    doc = mycursor.fetchall()
    # test
    print(type(doc))
    print(doc)
    print(doc[0])
    print(type(doc[0]))
    # for arg in doc:
    #     print(arg)
    with open(r'D:\code\py\demo.txt', 'a', encoding='utf-8') as file:
        # 使用 write 方法将文本写入文件
        for line in doc:
            arg = str(line[0]).replace('\',)', '').replace('(\'', '')
            print(arg)
            mycursor.execute(arg)
            cttuple = mycursor.fetchall()
            # test
            # print(cttuple[0][1])
            file.write(str(cttuple[0][1]) + ';\n')
    with open(r'D:\code\py\demo.txt', 'r', encoding='utf-8') as file1:
        file_read_comment = file1.read()
        # print(file_read_comment)
        count = file_read_comment.count('CREATE TABLE')
        print('建表语句个数:' + str(count))
    # file.close()







except Exception as e:
    print(e)
    if mycursor:
        mycursor.close()
    if conn:
        conn.close()
    # sys.exit(1)
finally:
    print("跑批结束运行时间：" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    print("关闭数据库及连接,关闭ssh服务")
    if mycursor:
        mycursor.close()
    if conn:
        conn.close()
