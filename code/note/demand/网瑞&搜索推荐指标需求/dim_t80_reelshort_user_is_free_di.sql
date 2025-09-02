-- drop Table if exists dwd_data.dim_t80_reelshort_user_is_free_di;
-- Create Table if not exists dwd_data.dim_t80_reelshort_user_is_free_di(
-- id                  bigint AUTO_INCREMENT    comment '自增id'
-- ,analysis_date      date                     comment '分析日期'
-- ,uuid               varchar      comment '用户ID'
-- ,is_free            varchar      comment '1: 免费, 0:非免费(包括已解锁和未解锁)'
-- ,PRIMARY KEY (id,analysis_date,uuid)
-- )DISTRIBUTE BY HASH(analysis_date,id) PARTITION BY VALUE(DATE_FORMAT(analysis_date, '%Y%m')) LIFECYCLE 120 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='reelshort 用户搜索结果的书籍列表维度统计表';

-- 是否付费
delete from dwd_data.dim_t80_reelshort_user_is_free_di where analysis_date='${TX_DATE}';
insert into dwd_data.dim_t80_reelshort_user_is_free_di
select distinct
       null as id
       ,etl_date as analysis_date
       ,uuid
       ,is_free -- 1: 免费, 0:非免费(包括已解锁和未解锁)
from dwd_data.dwd_t02_reelshort_play_event_di as t
where etl_date='${TX_DATE}'
      and event_name='m_play_event'
;