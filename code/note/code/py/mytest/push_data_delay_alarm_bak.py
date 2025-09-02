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
from datetime import datetime, timedelta

import pymysql
import pytz

import mymodule
import common.push_alarm_dingding as push_alarm_dingding


def send_message(msg, phone=''):
    print('function')
    phoneList = phone.split(',')
    print(msg)
    push_alarm_dingding.Alarmdingding.Dingtalk_alarm(phoneList, msg)


if __name__ == '__main__':
    # analysis_date = datetime.now().strftime("%Y-%m-%d")
    analysis_date = datetime.now(pytz.timezone('America/Los_Angeles'))
    two_days = timedelta(days=2)
    analysis_date2 = analysis_date - two_days
    analysis_date = analysis_date.strftime("%Y-%m-%d")
    print(type(analysis_date))
    print(analysis_date)

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
    sql = '''select 
    *
    from check_data.check_common_data_result as t
    where 
          t.check_table like '%dwd_t04_reelshort_opc_detail_di%'
          and t.row_cnt=0
          and check_date=DATE_SUB('{}',INTERVAL 2 DAY)
    -- and t.msg not like '%正常%'
    order by check_date desc 
    ;
    '''.format(analysis_date)
    try:
        print('代码debug')
        mycursor.execute(sql)
        doc = mycursor.fetchall()
        print(doc)
        print('sql查询数据量:' + str(len(doc)))
        '''
        若数据延迟，钉钉群发送告警
        '''
        if len(doc) > 0:
            arg = analysis_date2.strftime("%Y-%m-%d") + ' opc第三方数据延迟test'
            phone = '18898722985'
            send_message(arg, phone)

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
