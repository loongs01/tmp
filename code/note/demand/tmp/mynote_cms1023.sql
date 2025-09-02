rz -be 


/usr/local/bin/python3 /home/etl/scripts/reelshort/common/py_excute_sql.py /home/etl/scripts/reelshort/dwd/dwd_t04_reelshort_opc_push_detail_di.sql 2024-11-17


0 18 * * * sh /home/etl/task/cron_reelshort_check.sh > /dev/null 2>&1
30 17 * * * sh /home/etl/task/cron_reelshort_total_test.sh > /dev/null 2>&1


SELECT adb_VERSION();


--pro
/usr/local/bin/python3 /home/etl/scripts/reelshort/common/py_excute_sql.py /home/etl/scripts/reelshort/dwd/dwd_t04_reelshort_opc_push_detail_di.sql 2024-12-03

/usr/local/bin/python3 /home/etl/scripts/reelshort/common/py_excute_sql.py /home/etl/scripts/reelshort/dws/dws_t88_reelshort_opc_push_aggregate_di.sql 2024-12-02


-- 表名
db.createCollection("user_bookshelf_exposure_20");
-- 索引
db.getCollection("user_bookshelf_exposure_20").createIndex({
    uuid: NumberInt("1")
}, {
    name: "uuid_1_exposure"
});



-- 表名
db.createCollection("story_play_statistic");
-- 索引
db.getCollection("story_play_statistic").createIndex({
    etl_date:1,story_id:1
}, {
    name: "date_story_id"
});



-- adb

select 
REPLACE(JSON_EXTRACT(REPLACE(fcm_info,'.','_'), '$.google_c_a_m_l'),'"','')
,JSON_EXTRACT(REPLACE(fcm_info,'.','_'), '$.google_c_a_m_l')
,JSON_UNQUOTE(JSON_EXTRACT(REPLACE(fcm_info,'.','_'), '$.google_c_a_m_l'))
,if(JSON_EXTRACT(REPLACE(fcm_info,'.','_'), '$.google_c_a_m_l')=13723,'a','b')
,REPLACE(fcm_info,'.','_')
,JSON_EXTRACT(cast(fcm_info as varchar), '$.jump_type')
,JSON_EXTRACT(fcm_info , '$.from')
,fcm_info
,t.*
from dwd_data.dwd_t02_reelshort_push_stat_di  as t
where sub_event_name='intent_stat'
and etl_date='2024-11-26'
and fcm_message_id='0:1728832772281277%19dabe90f9fd7ecd'
limit 20
;



select 
FROM_UNIXTIME(t.created_at) -- 十位时间（秒）戳转日期  -- 13位时间戳精确到毫秒
,date_format(FROM_UNIXTIME(t.created_at),'%Y-%m-%d') -- 格式化日期
,t.*
from chapters_log.opc_fcm_push_log_cm1009 as t
where analysis_date='${TX_DATE}'
limit 10
;

SHOW PROCESSLIST;


-- 虽然 INFORMATION_SCHEMA 通常不包含正在执行的 SQL 语句的完整文本，但你可以使用 INFORMATION_SCHEMA.PROCESSLIST 表来查看当前活动的连接和它们正在执行的命令。
-- 这对于了解哪些连接是空闲的、正在执行的查询是什么类型的（如 SELECT、INSERT、UPDATE 等）是有用的

SELECT 
    ID,
    USER,
    HOST,
    DB,
    COMMAND,
    TIME,
    STATE,
    INFO
    ,*
FROM 
    INFORMATION_SCHEMA.PROCESSLIST
WHERE 
    COMMAND != 'Sleep' -- 过滤掉空闲连接
    and INFO like '%dwd_t04_reelshort_opc_push_detail_di%'
--     and INFO like '%dws_t88_reelshort_opc_push_aggregate_di%'
--     and DB='dwd_data'
;