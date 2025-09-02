select
count(1)
from
(
select
cast(nvl(json_extract(properties,'$._url'),'') as varchar) as a
,SUBSTRING_INDEX(replace(cast(nvl(json_extract(properties,'$._url'),'')as varchar),concat('-',SUBSTRING_INDEX(cast(nvl(json_extract(properties,'$._url'),'') as varchar), '-', -1)),''),'.com/',-1)
,SUBSTRING_INDEX(cast(nvl(json_extract(properties,'$._url'),'')as varchar),'.com/',-1)
from chapters_log.public_event_data
where analysis_date='${TX_DATE}'
      and event_name in ('m_app_install','m_user_signin')
      and length(json_extract(properties,'$._url'))>0
--        and cast(nvl(json_extract(properties,'$._url'),'')as varchar) not like '%.com%'
group by
cast(nvl(json_extract(properties,'$._url'),'') as varchar)
,SUBSTRING_INDEX(replace(cast(nvl(json_extract(properties,'$._url'),'')as varchar),concat('-',SUBSTRING_INDEX(cast(nvl(json_extract(properties,'$._url'),'') as varchar), '-', -1)),''),'.com/',-1)


);
select
 SUBSTRING_INDEX(a, '/', 3)
 ,a
from
(select
cast(nvl(json_extract(properties,'$._referrer_url'),'') as varchar) as a
,properties
from chapters_log.public_event_data
where analysis_date='${TX_DATE}'
      and event_name in ('m_app_install','m_user_signin')
      and json_extract(properties,'$._referrer_url') not regexp 'google|direct|facebook|youtube|bing'
)
-- group by SUBSTRING_INDEX(a, '/', 3)
limit 300
;
-- model
drop Table if  exists dws_data.dws_t80_reelshort_user_bhv_statistic_di;
Create Table if not exists dws_data.dws_t80_reelshort_user_bhv_statistic_di(
id                          bigint AUTO_INCREMENT    comment '自增id'
,analysis_date              date                     comment '分析日期'
,os_type                    varchar                  comment '分发平台'
,channel_id                 varchar                  comment '分发渠道'
,country_id                 varchar                  comment '国家id'
,language_id                varchar                  comment '语言'
,os_version                 varchar                  comment 'APP主版本:操作系统版本号'



,display_tag                varchar                  comment '书籍标签'
,story_id                   varchar                  comment '书籍id'
,lang                       varchar                  comment '书籍语言'
,status                     int                      comment '书籍状态0-未上架，1-已上架，2-已下架'
,publish_at                 varchar                  comment '书籍上架时间'
,story_total_duration       bigint                   comment '书籍播放时长'
,story_play_cnt             bigint                   comment '书籍播放量(次数)'
,PRIMARY KEY (id,analysis_date,uuid)
)DISTRIBUTE BY HASH(analysis_date,story_id) PARTITION BY VALUE(DATE_FORMAT(analysis_date, '%Y%m')) LIFECYCLE 120 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='reelshort 用户行为统计表';


-- ref reelshort_opera_reten_user_detail_data.sql

-- reelshort订单汇总表
dws_data.dws_t85_reelshort_srv_order_detail_stat_di_v2

dwd_data.dwd_t05_reelshort_srv_order_detail_di
reelshort_opera_revenue_detail_data.sql



--
dwd_data.dim_t99_reelshort_user_lt_info   dwd_data.dwd_t01_reelshort_user_detail_info_di  dwd_data.dwd_t01_reelshort_user_info_di
vt_dwd_t01_reelshort_user_info_di    

--ddl
-- alter table dwd_data.dwd_t01_reelshort_device_start_di
-- add column url varchar comment '页面url',
-- add column referrer_url varchar comment 'reelshort web链接点击打开来源平台的链接';

-- alter table dwd_data.dwd_t02_reelshort_page_enter_exit_di
-- add column referrer_url varchar comment 'reelshort web链接点击打开来源平台的链接';

-- alter table dwd_data.dwd_t01_reelshort_device_start_di
-- add column story_id varchar comment '书籍id',
-- add column chap_id varchar comment '章节id';


alter table dwd_data.dwd_t01_reelshort_user_detail_info_di
add column recent_visit_source      varchar comment '最近一次访问来源'
,add column recent_visit_source_type varchar comment '最近一次访问来源类型'
,add column first_visit_landing_page varchar comment '首次访问着陆页'
,add column first_visit_landing_page_story_id varchar comment '首次访问着陆页书籍id'
,add column first_visit_landing_page_chap_id  varchar comment '首次访问着陆页章节id'
;

alter table dwd_data.dim_t99_reelshort_user_lt_info
add column first_visit_landing_page varchar comment '首次访问着陆页'
,add column first_visit_landing_page_story_id varchar comment '首次访问着陆页书籍id'
,add column first_visit_landing_page_chap_id  varchar comment '首次访问着陆页章节id'
;

alter table dws_data.dws_t80_reelshort_user_bhv_cnt_detail_di
-- add column trans_complete_amt decimal(18,4) comment '订单支付金额'
add column trans_start_amt   decimal(18,4) comment '拉起支付sdk开始支付金额'
,add column recent_visit_source      varchar comment '最近一次访问来源'
,add column recent_visit_source_type varchar comment '最近一次访问来源类型'
,add column first_visit_landing_page varchar comment '首次访问着陆页'
,add column first_visit_landing_page_story_id varchar comment '首次访问着陆页书籍id'
,add column first_visit_landing_page_chap_id  varchar comment '首次访问着陆页章节id'
;
-- alter table dw_view.vt_dim_t01_reelshort_user_info
-- add column first_visit_landing_page varchar comment '首次访问着陆页'
-- ,add column first_visit_landing_page_story_id varchar comment '首次访问着陆页书籍id'
-- ,add column first_visit_landing_page_chap_id  varchar comment '首次访问着陆页章节id'
-- ;


-- 删除
-- alter table dwd_data.dwd_t06_reelshort_srv_currency_change_detail_di
-- add column first_visit_landing_page varchar comment '首次访问着陆页'
-- ,add column first_visit_landing_page_story_id varchar comment '首次访问着陆页书籍id'
-- ,add column first_visit_landing_page_chap_id  varchar comment '首次访问着陆页章节id'
-- ,add column recent_visit_source      varchar comment '最近一次访问来源'
-- ,add column recent_visit_source_type varchar comment '最近一次访问来源类型'
-- ,add column book_type int comment '书籍类型：1-常规剧，2-互动剧'
-- ,add column coin_type varchar comment '虚拟币类型'
-- ;



--删除
-- alter table dwd_data.dwd_t02_reelshort_custom_event_di
-- add column search_story_ids varchar comment '搜索结果页面中，搜索结果的书籍列表，元素结构是 书籍顺序#书籍ID#匹配规则'
-- ;



-- 删除
-- alter table dws_data.dws_t82_reelshort_user_play_data5_detail_di
-- add column is_free int comment '1: 免费, 0:非免费(包括已解锁和未解锁)'
-- ,add column search_story_ids varchar comment '搜索结果页面中，搜索结果的书籍列表，元素结构是 书籍顺序#书籍ID#匹配规则'
-- ,add column sub_page_id varchar comment'频道页'
-- ;

-- alter table dws_data.dws_t82_reelshort_play_data5_bitmap_di
-- add column is_free int comment '1: 免费, 0:非免费(包括已解锁和未解锁)'
-- ,add column search_story_ids varchar comment '搜索结果页面中，搜索结果的书籍列表，元素结构是 书籍顺序#书籍ID#匹配规则'
-- ,add column sub_page_id varchar comment'频道页'
-- ;

-- alter table dws_data.dws_t85_reelshort_srv_order_detail_stat_di_v2
-- add column recent_visit_source      varchar comment '最近一次访问来源'
-- ,add column recent_visit_source_type varchar comment '最近一次访问来源类型'
-- ,add column first_visit_landing_page varchar comment '首次访问着陆页'
-- ,add column first_visit_landing_page_story_id varchar comment '首次访问着陆页书籍id'
-- ,add column first_visit_landing_page_chap_id  varchar comment '首次访问着陆页章节id'
-- ;

alter table dws_data.dws_t82_reelshort_user_play_chapter_detail_di
add column trans_start_amt   decimal(18,4) comment '拉起支付sdk开始支付金额'
,add column recent_visit_source      varchar comment '最近一次访问来源'
,add column recent_visit_source_type varchar comment '最近一次访问来源类型'
,add column first_visit_landing_page varchar comment '首次访问着陆页'
,add column first_visit_landing_page_story_id varchar comment '首次访问着陆页书籍id'
,add column first_visit_landing_page_chap_id  varchar comment '首次访问着陆页章节id'
;



alter table dws_data.dws_t82_reelshort_user_play_chapter_detail_di
add column book_type int comment '书籍类型：1-常规剧，2-互动剧';

-- alter table dws_data.dws_t82_reelshort_user_play_data5_detail_di
-- add column recent_visit_source      varchar comment '最近一次访问来源'
-- ,add column recent_visit_source_type varchar comment '最近一次访问来源类型'
-- ,add column first_visit_landing_page varchar comment '首次访问着陆页'
-- ,add column first_visit_landing_page_story_id varchar comment '首次访问着陆页书籍id'
-- ,add column first_visit_landing_page_chap_id  varchar comment '首次访问着陆页章节id'
-- ;

-- alter table dws_data.dws_t82_reelshort_play_data5_bitmap_di
-- add column recent_visit_source      varchar comment '最近一次访问来源'
-- ,add column recent_visit_source_type varchar comment '最近一次访问来源类型'
-- ,add column first_visit_landing_page varchar comment '首次访问着陆页'
-- ,add column first_visit_landing_page_story_id varchar comment '首次访问着陆页书籍id'
-- ,add column first_visit_landing_page_chap_id  varchar comment '首次访问着陆页章节id'
-- ;

-- drop Table if exists dim_data.dim_t99_data5_tag_aggregate_configs_bak;
-- CREATE TABLE if not exists dim_data.dim_t99_data5_tag_aggregate_configs_bak (
  -- `id` bigint AUTO_INCREMENT COMMENT '自增',
  -- `app_id` varchar COMMENT '项目id:cm1001-chapters,cm1003-kiss,cm1009-reelshort',
  -- `name` varchar(255) NOT NULL DEFAULT '' COMMENT '指标名称',
  -- `key` varchar(255) NOT NULL DEFAULT '' COMMENT '指标key',
  -- `value` array<varchar> NOT NULL DEFAULT '[]' COMMENT '特殊维度枚举值',
  -- `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  -- `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
  -- PRIMARY KEY (`id`)
-- ) DISTRIBUTE BY HASH(`id`) INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='mapleBi:特殊维度码值汇总表备份';


-- ALTER TABLE dwd_data.dwd_t82_reelshort_play_detail_di 
-- add COLUMN width bigint  COMMENT '视频宽度，player_start填起播分辨率'
-- after widths ;






exp_mongodb_dim_t99_data5_tag_configs.py
dim_t99_data5_tag_configs.sql


-- exp_mongodb_reelshort_story_play_statistic_di.py
-- mongodb
target_operation_condition      
      
      
      

	  
-- 书架	  
-- dim_data.dim_t99_reelshort_bookshelf_info	  
select  id
,bookshelf_name
-- ,created_at
-- ,updated_at 
,count(1) as ct
from 
(
select  
id
,bookshelf_name
,created_at
,updated_at
-- ,row_number() over (partition by id,bookshelf_name order by updated_at desc ) as rn
from chapters_log.dts_project_v_hall_v3_bookshelf
union all
select 
id
,bookshelf_name
,created_at
,updated_at 
from chapters_log.dts_project_v_hall_bookshelf
)
group by id,bookshelf_name
having ct>1 
;



-- alter table dws_data.dws_t82_reelshort_user_exposure_data5_detail_di
-- add column shelf_name varchar comment '书架名称'
-- ;





-- alter table dws_data.dws_t82_reelshort_user_play_data5_detail_di
-- add column shelf_name varchar comment '书架名称'
-- ;

-- 总收入增加shelf_id
select 
-- REGEXP_SUBSTR(cast(nvl(json_extract(properties, '$.play_trace_id'),'') as varchar),'#(.*?)#',1,1,NULL,1)
split_part(json_extract(properties, '$.play_trace_id'),'#',2)
,SUBSTRING_INDEX(SUBSTRING_INDEX(cast(nvl(json_extract(properties, '$.play_trace_id'),'') as varchar),'#',2 ),'#',-1)as play_trace_id
,SUBSTRING_INDEX(cast(nvl(json_extract(properties, '$.play_trace_id'),'') as varchar),'#',2 )
,cast(nvl(json_extract(properties, '$.play_trace_id'),'') as varchar) as play_trace_id
,properties
,t.*
-- cast(nvl(json_extract(properties,'$._shelf_id'),'') as varchar) as shelf_id -- 书架id
from chapters_log.public_event_data as t
where analysis_date='${TX_DATE}'
      and event_name='m_pay_event'
      and cast(nvl(json_extract(properties, '$.play_trace_id'),'') as varchar)!=''
--       and (cast(nvl(json_extract(properties,'$.shelf_id'),'') as varchar)!=''
--       or properties like '%shelf%')  play_trace_id
limit 10
;