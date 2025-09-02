-- create table if not exists dws_data.dws_t85_reelshort_srv_currency_change_statistic_di (
  -- `id` bigint AUTO_INCREMENT COMMENT '自增id',
 -- `uuid` varchar COMMENT '应用中用户ID',
 -- `analysis_date` date COMMENT '分析日期',
 -- `country_id` varchar COMMENT '国家id',
 -- `channel_id` varchar COMMENT '应用渠道(1:AVG10003,2:AVG20001)',
 -- `version` varchar COMMENT 'APP的应用版本(包体内置版本号versionname)',
 -- `language_id` varchar COMMENT '游戏语言id',
 -- `platform` varchar COMMENT '平台id (1-安卓,2-IOS，3-windows,4-macOs)',
 -- `user_type` int COMMENT '是否新用户1-是',
 -- `story_id` varchar COMMENT '书籍id',
  -- coin_type varchar comment '虚拟币类型'
  -- ,coin_expend int comment '虚拟币消耗数量'
  -- ,first_visit_landing_page varchar comment '首次访问着陆页'
  -- ,first_visit_landing_page_story_id varchar comment '首次访问着陆页书籍id'
  -- ,first_visit_landing_page_chap_id  varchar comment '首次访问着陆页章节id'
  -- ,recent_visit_source      varchar comment '最近一次访问来源'
  -- ,recent_visit_source_type varchar comment '最近一次访问来源类型'
  -- ,book_type int comment '书籍类型：1-常规剧，2-互动剧',
 -- primary key (`analysis_date`,`id`,uuid)
-- ) DISTRIBUTE BY HASH(`id`,`analysis_date`) PARTITION BY VALUE(`DATE_FORMAT(analysis_date, '%Y%m')`) LIFECYCLE 120 INDEX_ALL='Y' STORAGE_POLICY='COLD' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='V项目dws:业务服用户虚拟币分类汇总表'
-- ;

-- alter table dws_data.dws_t85_reelshort_srv_currency_change_statistic_di
-- add column af_network_name varchar comment '归因投放渠道'
-- ;



delete from dws_data.dws_t85_reelshort_srv_currency_change_statistic_di where analysis_date='${TX_DATE}';
-- SET @row_number = 0;
insert into dws_data.dws_t85_reelshort_srv_currency_change_statistic_di
select null as id  -- @row_number := @row_number + 1 AS id
      ,t1.uuid
      ,t1.analysis_date
      ,t1.country_id
      ,t1.channel_id
      ,t1.version
      ,t1.language_id
      ,t1.platform
      ,t1.user_type
      ,t1.story_id
      ,t1.coin_type -- 虚拟币类型
      ,t1.coin_expend -- 虚拟币消耗数量
      ,nvl(t2.first_visit_landing_page,'')          as first_visit_landing_page          -- 首次访问着陆页
      ,nvl(t2.first_visit_landing_page_story_id,'') as first_visit_landing_page_story_id -- 首次访问着陆页书籍id
      ,nvl(t2.first_visit_landing_page_chap_id,'')  as first_visit_landing_page_chap_id  -- 首次访问着陆页章节id
      ,nvl(t3.recent_visit_source,'')               as recent_visit_source               -- 最近一次访问来源
      ,nvl(t3.recent_visit_source_type,'')          as recent_visit_source_type          -- 最近一次访问来源类型
      ,nvl(t4.book_type,-999)                       as book_type                         -- 书籍类型：1-常规剧，2-互动剧
      ,nvl(t5.af_network_name,'')                   as af_network_name                   -- 归因投放渠道
from(select
          t.uuid
          ,t.analysis_date
          ,t.country_id
          ,t.channel_id
          ,t.version
          ,t.language_id
          ,t.platform
          ,t.user_type
          ,t.story_id
          ,'3' as coin_type -- 虚拟币类型
          ,cast(sum(t.coin_exp) as int) as coin_expend -- 虚拟币消耗数量
     from dwd_data.dwd_t06_reelshort_srv_currency_change_detail_di as t
     where etl_date='${TX_DATE}'
           and t.coin_exp>0
     group by t.uuid
              ,t.analysis_date
              ,t.country_id
              ,t.channel_id
              ,t.version
              ,t.language_id
              ,t.platform
              ,t.user_type
              ,t.story_id
     union all
     select
          t.uuid
          ,t.analysis_date
          ,t.country_id
          ,t.channel_id
          ,t.version
          ,t.language_id
          ,t.platform
          ,t.user_type
          ,t.story_id
          ,'2' as coin_type -- 虚拟币类型
          ,cast(sum(t.pay_bonus_exp) as int) as coin_expend -- 虚拟币消耗数量
     from dwd_data.dwd_t06_reelshort_srv_currency_change_detail_di as t
     where etl_date='${TX_DATE}'
           and t.pay_bonus_exp>0
     group by t.uuid
              ,t.analysis_date
              ,t.country_id
              ,t.channel_id
              ,t.version
              ,t.language_id
              ,t.platform
              ,t.user_type
              ,t.story_id
     union all
     select
          t.uuid
          ,t.analysis_date
          ,t.country_id
          ,t.channel_id
          ,t.version
          ,t.language_id
          ,t.platform
          ,t.user_type
          ,t.story_id
          ,'1' as coin_type -- 虚拟币类型
          ,cast(sum(t.free_bonus_exp) as int) as coin_expend -- 虚拟币消耗数量
     from dwd_data.dwd_t06_reelshort_srv_currency_change_detail_di as t
     where etl_date='${TX_DATE}'
           and t.free_bonus_exp>0
     group by t.uuid
              ,t.analysis_date
              ,t.country_id
              ,t.channel_id
              ,t.version
              ,t.language_id
              ,t.platform
              ,t.user_type
              ,t.story_id
) as t1
left join(select uuid
                 ,etl_date
                 ,first_visit_landing_page          -- 首次访问着陆页
                 ,first_visit_landing_page_story_id -- 首次访问着陆页书籍id
                 ,first_visit_landing_page_chap_id  -- 首次访问着陆页章节id
         from dwd_data.dim_t99_reelshort_user_lt_info
         where etl_date='${TX_DATE}'
)t2
on t2.uuid=t1.uuid and t2.etl_date=t1.analysis_date
left join(
         select distinct etl_date
               ,uuid
               -- ,af_network_name
               ,recent_visit_source -- 最近一次访问来源
               ,recent_visit_source_type -- 最近一次访问来源类型
         from dwd_data.dwd_t01_reelshort_user_detail_info_di
         where etl_date='${TX_DATE}'
) as t3
on t3.uuid=t1.uuid and t1.analysis_date=t3.etl_date
left join(
          select distinct id
                 ,book_type
           from dim_data.dim_t99_reelshort_book_info
) as t4
on t4.id=t1.story_id
left join (
           select distinct
                  uuid
                  ,af_network_name
           from dws_data.dim_t87_reelshort_ads_user_attr_df
           where etl_date='${TX_DATE}'
) as t5
on t5.uuid=t1.uuid
;