drop Table if  exists dwd_data.dim_t80_reelshort_user_search_type_di;
Create Table if not exists dwd_data.dim_t80_reelshort_user_search_type_di(
id                  bigint AUTO_INCREMENT    comment '自增id'
,analysis_date      date                     comment '分析日期'
,uuid               varchar      comment '用户ID'
,search_type        varchar      comment '搜索方式'
,PRIMARY KEY (id,analysis_date,uuid)
)DISTRIBUTE BY HASH(analysis_date,id) PARTITION BY VALUE(DATE_FORMAT(analysis_date, '%Y%m')) LIFECYCLE 120 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='reelshort 用户搜索方式维度统计表';

-- 搜索方式
delete from dwd_data.dim_t80_reelshort_user_search_type_di where analysis_date='${TX_DATE}';
insert into dwd_data.dim_t80_reelshort_user_search_type_di
select distinct
       null as id
       ,etl_date as analysis_date
       ,uuid
       ,case
            when search_word_source like '%typing%' then 1
            when search_word_source like '%default_fill%' then 2
            when search_word_source like '%history_click_fill%' then 3
            when search_word_source like '%tag_click_fill%' then 4
            else 5
       end as search_type -- 搜索方式
from dwd_data.dwd_t02_reelshort_search_stat_di as t
where etl_date='${TX_DATE}'
      and event_name='m_custom_event'
      and sub_event_name='search_stat'
;