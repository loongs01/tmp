-- 查看字段数据类型
db.user_bookshelf_exposure_3.aggregate([{
$project:{
_id: {$type: "$_id" },
uuid: {$type: "$uuid" },
exposure: {$type: "$exposure" },
}
}])


--查看字段数据类型
db.story_play_statistic.aggregate([{
$project:{
etl_date: {$type: "$etl_date" }
}
}])


--值不存在或值为 null,或''是空字符串 的文档数量
db.story_play_statistic.countDocuments({
  $or: [
    { story_id: { $exists: false } },
    { story_id: null },
    { story_id: '' }
  ]
})


db.target_operation_condition.find()

-- 1 表示升序，-1 表示降序
db.target_operation_condition.find().sort({"id": 1}) 


db.story_play_statistic.find({ etl_date: { $gt: '2024-11-25' } })   -- 查询大于

db.story_play_statistic.find({ etl_date: '2024-11-25' })    --等于

db.story_play_statistic.find({ etl_date: '2024-11-28',story_id:'674717e4afb2468fb5062683' })

--查询 count>日期
db.story_play_statistic.countDocuments({ etl_date: { $gt: '2024-11-25' } })

db.story_play_statistic.count({ etl_date: { $gt: '2024-11-25' } })

db.story_play_statistic.countDocuments({ etl_date: '2024-11-26' }) -- count

-- 小于日期
db.story_play_statistic.countDocuments({ etl_date: { $lt: '2024-11-28' } })
db.story_play_statistic.count({ etl_date: { $lt: '2024-11-25' } })


--查询 count=日期
db.story_play_statistic.count({ etl_date: '2024-11-27'})
db.story_play_statistic.countDocuments({ etl_date: '2024-11-25'})

--



--delete
 db.story_play_statistic.deleteMany({})
 
 // db.story_play_statistic.deleteMany({})


//  db.story_play_statistic.deleteMany({etl_date: { $lt: '2024-11-25' } }) --小于日期










--

path=/dolphinscheduler/etl/resources/scripts/reelshort/sync


--test

/usr/bin/python3 /dolphinscheduler/etl/resources/scripts/reelshort/sync/exp_mongodb_reelshort_story_play_init_di.py 2024-12-03


./tmp03_test.sh 2024-10-27 2024-11-15

-- test

/usr/bin/python3 /dolphinscheduler/etl/resources/scripts/reelshort/common/py_excute_sql.py /dolphinscheduler/etl/resources/scripts/reelshort/dm/dm_t82_reelshort_story_play_init_di.sql 2024-12-03

git clone -b push指标加工  https://gitlab.stardustgod.com/esee/data5-server.git

--维表
select
language_id
,language
from dim_data.chapter_book_language
group by language_id,language
order by language_id
;
--2
select
id
-- ,name_en_short
,name_en
,name_cn
from  chapters_log.language_info_v2
group by
id
-- ,name_en_short
,name_en
,name_cn
order by id
;

--3
select * from  chapters_log.country_info;

select * from chapters_log.country_info  t where t.tier='T2'


--python export 配置信息
D:\git\data-dw-sz\branches\v1.0.0\scripts\reelshort\sync\common\handleMongodb.py


/dolphinscheduler/etl/resources/scripts/config.ini


./tmp03.sh 2024-11-17 2024-11-25
10-31  11-16
10-25 11-5

./tmp03.sh 2024-11-19 2024-11-28

TX_DATE=$[yyyy-MM-dd-1]


${PATH}/scripts/reelshort/sync/exp_mongodb_reelshort_story_play_statistic_di.py

/usr/bin/python3 /dolphinscheduler/etl/resources/scripts/reelshort/common/py_excute_sql.py /home/etl/scripts/reelshort/dwd/dwd_t01_reelshort_device_info.sql ${TX_DATE}

/dolphinscheduler/etl/resources/scripts/reelshort/sync/exp_mongodb_reelshort_story_play_statistic_di.py 2024-11-25


/usr/bin/python3 /dolphinscheduler/etl/resources/scripts/reelshort/common/py_excute_sql.py /dolphinscheduler/etl/resources/scripts/reelshort/dwd/dm_t82_reelshort_story_play_statistic_di.sql 2024-11-25





/usr/bin/python3 ${PATH}/scripts/reelshort/common/py_excute_sql.py ${sqlfile} ${TX_DATE1}
exit $?



/usr/bin/python3 ${PATH}/scripts/reelshort/common/py_excute_sql.py ${sqlfile} ${TX_DATE1}
exit $?


${PATH}/scripts/reelshort/dwd/dm_t82_reelshort_story_play_statistic_di.sql


python3 /home/etl/scripts/reelshort/common/py_excute_sql.py /dolphinscheduler/etl/resources/scripts/reelshort/dwd/dwd_t02_reelshort_search_stat_di.sql ${TX_DATE}

--pro

/usr/local/bin/python3 /home/etl/scripts/reelshort/common/py_excute_sql.py /home/etl/scripts/reelshort/dm/dm_t82_reelshort_story_play_statistic_di.sql 2024-12-01
/usr/local/bin/python3 /home/etl/scripts/reelshort/sync/exp_mongodb_reelshort_story_play_statistic_di.py 2024-12-01
