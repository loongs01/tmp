-- drop Table if  exists dwd_data.dim_t80_reelshort_user_search_story_ids_di;
-- Create Table if not exists dwd_data.dim_t80_reelshort_user_search_story_ids_di(
-- id                  bigint AUTO_INCREMENT    comment '自增id'
-- ,analysis_date      date                     comment '分析日期'
-- ,uuid               varchar      comment '用户ID'
-- ,search_story_ids   varchar      comment '搜索结果页面中，搜索结果的书籍列表，元素结构是 书籍顺序#书籍ID#匹配规则'
-- ,PRIMARY KEY (id,analysis_date,uuid)
-- )DISTRIBUTE BY HASH(analysis_date,id) PARTITION BY VALUE(DATE_FORMAT(analysis_date, '%Y%m')) LIFECYCLE 120 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='reelshort 用户搜索结果的书籍列表维度统计表';

-- 搜索结果的书籍列表
delete from dwd_data.dim_t80_reelshort_user_search_story_ids_di where analysis_date='${TX_DATE}';
insert into dwd_data.dim_t80_reelshort_user_search_story_ids_di
select distinct
       null as id
       ,etl_date as analysis_date
       ,uuid
       ,if(search_story_ids='[]',1,0) as search_story_ids -- 搜索结果页面中，搜索结果的书籍列表，元素结构是 书籍顺序#书籍ID#匹配规则
from dwd_data.dwd_t02_reelshort_search_stat_di as t   -- dwd_data.dwd_t02_reelshort_custom_event_di
where etl_date='${TX_DATE}'
      and event_name='m_custom_event'
      and sub_event_name='search_stat'
;