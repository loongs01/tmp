# -*- coding: UTF-8 -*-

# 程序加载模块，连接数据库
import pandas as pd
import pymysql
import pytz
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)  # 操作日志对象
import sys

import json
import pymongo
from pymongo import InsertOne, DeleteOne, ReplaceOne, UpdateOne
from pymongo.errors import BulkWriteError


# 日期解析
def date_deal():
    date_time = datetime.now(pytz.timezone('America/Los_Angeles'))
    etl_date = (date_time + timedelta(hours=-1)).strftime("%Y%m%d")
    analysis_date = (date_time + timedelta(hours=-1)).strftime("%Y-%m-%d")
    time_hour = (date_time + timedelta(hours=-1)).strftime("%Y%m%d%H")
    analysis_hour = (date_time + timedelta(hours=-1)).strftime("%H")
    print(etl_date, analysis_date, analysis_hour, time_hour)
    return etl_date, analysis_date, analysis_hour, time_hour

def one_exp_mongodb(mongoCol, collectlist):
    try:
        batch = []
        for idx, line in enumerate(collectlist):
            batch.append(ReplaceOne(
                {'etl_date': line.get("etl_date"),
                 'language_id': line.get("language_id")
                 'display_tag': line.get("display_tag")
                 'story_id': line.get("story_id")
                 'lang': line.get("lang")
                 'status': line.get("status")
                 'story_total_duration': line.get("story_total_duration")
                 'story_play_cnt': line.get("story_play_cnt")
                 }, line,
                upsert=True
            ))
            # print(batch)
            if (idx + 1) % 3000 == 0:
                # print(batch)
                try:
                    mongoCol.bulk_write(batch)
                    batch = []
                except BulkWriteError as e:
                    print("mongo error:", e.details)

        if batch:
            mongoCol.bulk_write(batch)
    except Exception as e:
        print("Exception:", e)


if __name__ == '__main__':
    from common.handleDB import HandleMysql
    from common.handleMongodb import HandleMongodb

    collection_name = 'story_play_statistic'
    date_str = sys.argv[1]
    # date_str="2023-09-19";
    analysis_date = datetime.strptime(date_str, "%Y-%m-%d")
    analysis_date = (analysis_date + timedelta(days=0)).strftime("%Y-%m-%d")
    etl_date, analysis_date, analysis_hour, time_hour = date_deal()
    if int(analysis_hour)%2==0 :
        print("两个小时跑一次，当前小时跳过")
        sys.exit(0)

    handleMysql = HandleMysql(flag="MONGODB-TEST-REELSHORT")
    us_adb = handleMysql.conn_mysql()
    for i in range(100):
        sql = """select etl_date
                        ,language_id
                        ,display_tag
                        ,story_id
                        ,lang,status
                        ,story_total_duration
                        ,story_play_cnt
                 from dm_reelshort_data.dm_t82_reelshort_story_play_statistic_di
                 where etl_date="{}"
          """.format(analysis_date,i)
        print("sql: " + sql)
        data = pd.read_sql(sql, us_adb)
        collectlist = json.loads(data.to_json(orient='records'))
        # 将数据导出到mongodb
        handleMongo = HandleMongodb("projectv_played", collection_name+'_'+str(i))
        myclient, mongoCol = handleMongo.conn_mongodb()
        one_exp_mongodb(mongoCol, collectlist)
        # handleMongo.find_mongo_info()
    if us_adb:
        us_adb.close()
    if myclient:
        myclient.close()
