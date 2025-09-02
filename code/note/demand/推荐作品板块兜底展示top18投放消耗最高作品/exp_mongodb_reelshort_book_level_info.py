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
                {'analysis_date': line.get("analysis_date"),
                 'id': line.get("id"),
                 'lang': line.get("lang"),
                 'rank': line.get("rank")
                 }, line,
                upsert=True
            ))
            # print(batch)
            if (idx + 1) % 2000 == 0:
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

    collection_name = 'reelshort_book_level_info'
    date_str = sys.argv[1]
    # date_str="2023-09-19";
    # date_str="2023-10-17";
    analysis_date = datetime.strptime(date_str, "%Y-%m-%d")
    seven_days = timedelta(days=7)
    analysis_date7=analysis_date-seven_days
    analysis_date = (analysis_date + timedelta(days=0)).strftime("%Y-%m-%d")
    analysis_date7 = analysis_date7.strftime("%Y-%m-%d")
    # etl_date, analysis_date, analysis_hour, time_hour = date_deal()
    handleMysql = HandleMysql(flag="adbUS3")
    us_adb = handleMysql.conn_mysql()
    sql = """select
                  cast("{}" as varchar) as analysis_date
                  ,t.id                 as id
                  ,t.lang               as lang
                  ,t.rank               as rank
                  from (select
                              t1.id
                              ,t1.lang
                              ,row_number() over(partition by t1.lang order by COALESCE(t2.spend, 0) desc) as rank
                        from (
                              select
                                  id
                                  ,book_id as short_book_id
                                  ,lang
                              from dim_data.dim_t99_reelshort_book_info
                              where status = 1
                              -- and book_source in (1, 2, 8)
                              group by
                                  id
                                  ,book_id
                                  ,lang
                        ) as t1
                        left join (
                                   SELECT
                                                   case
                                                     when short_book_id regexp '^[0-9]+-' then  SUBSTRING_INDEX(short_book_id, '-', 1)
                                                     when short_book_id regexp '^[0-9]+ ' then  SUBSTRING_INDEX(short_book_id, ' ', 1)
                                                     else short_book_id
                                                   end as short_book_id
                                                   ,sum(case when platform in (5,6) then 0 else if(spend=-1, 0, spend) end) as spend
                                   from dwd_data.dwd_t03_adjust_ad_detail_di
                                   where appname='reelshort'
                                   and collect_date>='2022-01-01'
                                   and type='country'
                                   group by
                                           case
                                             when short_book_id regexp '^[0-9]+-' then  SUBSTRING_INDEX(short_book_id, '-', 1)
                                             when short_book_id regexp '^[0-9]+ ' then  SUBSTRING_INDEX(short_book_id, ' ', 1)
                                             else short_book_id
                                           end
                        ) as t2
                        on t2.short_book_id = t1.short_book_id
                        ) as t
                  where t.rank<=30
          """.format(analysis_date)
    data = pd.read_sql(sql, us_adb)
    collectlist = json.loads(data.to_json(orient='records'))
    # 将数据导出到mongodb
    print("sql: " + sql)
    handleMongo = HandleMongodb("prod-rt", collection_name)
    myclient, mongoCol = handleMongo.conn_mongodb()
    # 删除 etl_date 小于 analysis_date7 的所有文档
    delete_result = mongoCol.delete_many({"analysis_date": {"$lt": analysis_date7}})
    # 输出删除的文档数量
    print(f"Deleted {delete_result.deleted_count} documents.")
    one_exp_mongodb(mongoCol, collectlist)
    handleMongo.find_mongo_info()
    if us_adb:
        us_adb.close()
    if myclient:
        myclient.close()
