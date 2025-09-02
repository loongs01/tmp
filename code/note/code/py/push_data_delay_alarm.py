'''
@Project:mytest
@File:push_data_delay_alarm.py
@IDE:PyCharm
@Author:lichaozhong
@Date:2025/1/20 15:59
'''
import logging
import sys
import time
from datetime import datetime

import pymysql
import pytz

from common.alarm_dingding import Alarm_dingding

analysis_date = datetime.now().strftime("%Y-%m-%d")
print(analysis_date)

logger = logging.getLogger(__name__)  # 操作日志对象


def send_message(msg, phone='18898722985'):
    print('function')
    phoneList = phone.split(',')
    print(msg)
    Alarm_dingding.Dingtalk_alarm(phoneList, msg)


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
sql = """select 
*
from check_data.check_common_data_result as t
where 
      t.check_table like '%dwd_t04_reelshort_opc_detail_di%'
      and t.row_cnt=0 
-- and t.msg not like '%正常%'
order by check_date desc
limit 1
;
"""
try:
    print('代码debug')
    mycursor.execute(sql)
    doc = mycursor.fetchall()
    print(doc)
    print('sql查询数据量:' + str(len(doc)))
    if len(doc) > 0:
        send_message('opc数据延迟test')







except Exception as e:
    print(e)
    # if mycursor:
    #     mycursor.close()
    # if conn:
    #     conn.close()
    # sys.exit(1)
finally:
    print("跑批结束运行时间：" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    print("关闭数据库及连接,关闭ssh服务")
    if mycursor:
        mycursor.close()
    if conn:
        conn.close()
