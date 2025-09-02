drop Table if  exists dwd_data.dim_t80_reelshort_user_recall_level_di;
Create Table if not exists dwd_data.dim_t80_reelshort_user_recall_level_di(
id                  bigint AUTO_INCREMENT    comment '自增id'
,analysis_date      date                     comment '分析日期'
,uuid               varchar      comment '用户ID'
,recall_level       varchar      comment '召回路'
,PRIMARY KEY (id,analysis_date,uuid)
)DISTRIBUTE BY HASH(analysis_date,id) PARTITION BY VALUE(DATE_FORMAT(analysis_date, '%Y%m')) LIFECYCLE 120 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='reelshort 用户召回路维度统计表';

-- 召回路
delete from dwd_data.dim_t80_reelshort_user_recall_level_di where analysis_date='${TX_DATE}';
insert into dwd_data.dim_t80_reelshort_user_recall_level_di
select distinct
      null as id
      ,etl_date as analysis_date
      ,uuid
      ,recall_level        -- 召回路
from (select distinct
             etl_date
             ,uuid
             ,recall_level -- 召回路
      FROM dwd_data.dwd_t02_reelshort_play_event_di as t
      where etl_date='${TX_DATE}'
            and event_name='m_play_event'
            -- and recall_level!=''
      union all
      select distinct
             etl_date
             ,uuid
             ,cast(nvl(json_extract(report_info,'$.recall_level'),'') as varchar) as recall_level
      from dwd_data.dwd_t02_reelshort_item_pv_di
      where etl_date='${TX_DATE}'
            and event_name='m_item_pv'
            -- and cast(nvl(json_extract(report_info,'$.recall_level'),'') as varchar)!=''
)
;



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
            when search_word_source like '%typing%' then 'typing'
            when search_word_source like '%default_fill%' then 'default_fill'
            when search_word_source like '%history_click_fill%' then 'history_click_fill'
            when search_word_source like '%tag_click_fill%' then 'tag_click_fill'
            else 'other'
       end as search_type -- 搜索方式
from dwd_data.dwd_t02_reelshort_search_stat_di as t
where etl_date='${TX_DATE}'
      and event_name='m_custom_event'
      and sub_event_name='search_stat'
;


drop Table if  exists dwd_data.dim_t80_reelshort_user_tab_id_di;
Create Table if not exists dwd_data.dim_t80_reelshort_user_tab_id_di(
id                  bigint AUTO_INCREMENT    comment '自增id'
,analysis_date      date                     comment '分析日期'
,uuid               varchar      comment '用户ID'
,tab_id             varchar      comment '频道页'
,description        varchar      comment '频道页名称'
,tab_id_name        varchar      comment '频道页id+（中文名）'
,PRIMARY KEY (id,analysis_date,uuid)
)DISTRIBUTE BY HASH(analysis_date,id) PARTITION BY VALUE(DATE_FORMAT(analysis_date, '%Y%m')) LIFECYCLE 120 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='reelshort 用户频道页维度统计表';


-- 频道页
delete from dwd_data.dim_t80_reelshort_user_tab_id_di where analysis_date='${TX_DATE}';
insert into dwd_data.dim_t80_reelshort_user_tab_id_di
select distinct
       null as id
       ,t.analysis_date
       ,t.uuid
       ,t.tab_id -- 频道页
       ,t1.tab_name
       ,concat(t.tab_id,t1.tab_name) as tab_id_name -- 频道页id+（中文名）
from (select distinct
            etl_date as analysis_date
            ,uuid
            ,cast(nvl(json_extract(item_list,'$.sub_page_id'),'') as varchar) as tab_id -- 频道页
      from dwd_data.dwd_t02_reelshort_item_pv_di
      where etl_date='${TX_DATE}'
      and event_name='m_item_pv'
)as t
left join dw_view.dts_project_v_hall_v3 as t1
on t1.tab_id=t.tab_id
;






drop Table if  exists dwd_data.dim_t80_reelshort_user_search_story_ids_di;
Create Table if not exists dwd_data.dim_t80_reelshort_user_search_story_ids_di(
id                  bigint AUTO_INCREMENT    comment '自增id'
,analysis_date      date                     comment '分析日期'
,uuid               varchar      comment '用户ID'
,search_story_ids   varchar      comment '搜索结果页面中，搜索结果的书籍列表，元素结构是 书籍顺序#书籍ID#匹配规则'
,PRIMARY KEY (id,analysis_date,uuid)
)DISTRIBUTE BY HASH(analysis_date,id) PARTITION BY VALUE(DATE_FORMAT(analysis_date, '%Y%m')) LIFECYCLE 120 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='reelshort 用户搜索结果的书籍列表维度统计表';

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



drop Table if exists dwd_data.dim_t80_reelshort_user_is_free_di;
Create Table if not exists dwd_data.dim_t80_reelshort_user_is_free_di(
id                  bigint AUTO_INCREMENT    comment '自增id'
,analysis_date      date                     comment '分析日期'
,uuid               varchar      comment '用户ID'
,is_free            varchar      comment '1: 免费, 0:非免费(包括已解锁和未解锁)'
,PRIMARY KEY (id,analysis_date,uuid)
)DISTRIBUTE BY HASH(analysis_date,id) PARTITION BY VALUE(DATE_FORMAT(analysis_date, '%Y%m')) LIFECYCLE 120 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='reelshort 用户搜索结果的书籍列表维度统计表';

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
