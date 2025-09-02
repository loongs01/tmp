-- drop Table if  exists dws_data.dws_t80_reelshort_user_chapter_unlock_statistic_di;
-- Create Table if not exists dws_data.dws_t80_reelshort_user_chapter_unlock_statistic_di(
-- id                                 bigint AUTO_INCREMENT    comment '自增id'
-- ,analysis_date                     date    comment '分析日期'
-- ,uuid                              varchar comment '用户ID'
-- ,country_id                        varchar comment '国家id'
-- ,channel_id                        varchar comment '应用渠道'
-- ,platform                          varchar comment '操作系统类型 (android/ios/windows/mac os)'
-- ,language_id                       varchar comment '游戏语言id'
-- ,version                           varchar comment 'APP的应用版本(包体内置版本号versionname)'
-- ,cversion                          varchar comment '应用游戏版本号'
-- ,user_type                         int     comment '用户类型'
-- ,recent_visit_source               varchar comment '最近一次访问来源'
-- ,recent_visit_source_type          varchar comment '最近一次访问来源类型'
-- ,first_visit_landing_page          varchar comment '首次访问着陆页'
-- ,first_visit_landing_page_story_id varchar comment '首次访问着陆页书籍id'
-- ,first_visit_landing_page_chap_id  varchar comment '首次访问着陆页章节id'
-- ,af_network_name                   varchar comment '归因投放渠道'
-- ,dlink_story_id                    varchar comment '书籍id'
-- ,chap_id                           varchar comment '解锁开始章节id'
-- ,story_id                          varchar comment '书籍id'
-- ,unlock_type                       varchar comment '1：用金币或bonus解锁 2:等免解锁'
-- ,is_pay                            int     comment '当天是否付费1-是，0-否'
-- ,vip_type                          int     comment 'sub_status用户订阅状态:0=未购买  1=生效中 2=已过期'
-- ,PRIMARY KEY (id,analysis_date,uuid)
-- )DISTRIBUTE BY HASH(analysis_date,id) PARTITION BY VALUE(DATE_FORMAT(analysis_date, '%Y%m')) LIFECYCLE 120 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='reelshort用户章节解锁统计表';

-- alter table dwd_data.dwd_t80_reelshort_user_chapter_unlock_di
-- add column af_network_name varchar comment '归因投放渠道'
-- ;

-- alter table dwd_data.dwd_t80_reelshort_user_chapter_unlock_di
-- add column chap_id varchar comment '解锁开始章节id'
-- ,add column story_id varchar comment '书籍id'
-- ,add column unlock_type varchar comment '1：用金币或bonus解锁 2:等免解锁'
-- ;




delete from dws_data.dws_t80_reelshort_user_chapter_unlock_statistic_di where analysis_date='${TX_DATE}';
insert into dws_data.dws_t80_reelshort_user_chapter_unlock_statistic_di
select
      null           as id                  -- 自增id
      ,t1.etl_date   as analysis_date       -- 分析日期
      ,t1.uuid                              -- 用户ID
      ,t1.country_id                        -- '国家id'
      ,t1.channel_id                        -- '应用渠道'
      ,t1.platform                          -- '操作系统类型 (android/ios/windows/mac os)'
      ,t1.language_id                       -- '游戏语言id'
      ,t1.version                           -- 'APP的应用版本(包体内置版本号versionname)'
      ,t1.cversion
      ,t2.user_type                         -- '用户类型'
      ,nvl(t2.recent_visit_source,'')                 as recent_visit_source                 -- 最近一次访问来源
      ,nvl(t2.recent_visit_source_type,'')            as recent_visit_source_type            -- 最近一次访问来源类型
      ,nvl(t3.first_visit_landing_page,'')            as first_visit_landing_page            -- 首次访问着陆页
      ,nvl(t3.first_visit_landing_page_story_id,'')   as first_visit_landing_page_story_id   -- 首次访问着陆页书籍id
      ,nvl(t3.first_visit_landing_page_chap_id,'')    as first_visit_landing_page_chap_id    -- 首次访问着陆页章节id
      ,nvl(t4.af_network_name,'')                     as af_network_name                     -- 归因投放渠道
      ,nvl(t4.dlink_story_id,'')                      as dlink_story_id
      ,t1.chap_id                                     -- 解锁开始章节id
      ,t1.story_id                                    -- 书籍id
      ,t1.unlock_type                                 -- 1：用金币或bonus解锁 2:等免解锁
      ,nvl(t2.is_pay,0)                               as is_pay
      ,nvl(t2.vip_type,0)                             as vip_type
from (
      select
            etl_date
            ,uuid
            ,country_id
            ,channel_id
            ,platform
            ,language_id
            ,version
            ,cversion
            ,chap_id
            ,story_id
            ,unlock_type
      from dwd_data.dwd_t04_reelshort_checkpoint_unlock_di as t
      where etl_date='${TX_DATE}'
            and event_name='m_checkpoint_unlock'
      group by
              etl_date
              ,uuid
              ,country_id
              ,channel_id
              ,platform
              ,language_id
              ,version
              ,cversion
              ,chap_id
              ,story_id
              ,unlock_type
) as t1
left join(
          select distinct
                 uuid
                 ,user_type
                 -- ,af_network_name
                 ,recent_visit_source -- 最近一次访问来源
                 ,recent_visit_source_type -- 最近一次访问来源类型
                 ,is_pay
                 ,vip_type
          from dwd_data.dwd_t01_reelshort_user_detail_info_di
          where etl_date='${TX_DATE}'
) as t2
on t2.uuid=t1.uuid
left join(
          select distinct
                 uuid
                 ,first_visit_landing_page          -- 首次访问着陆页
                 ,first_visit_landing_page_story_id -- 首次访问着陆页书籍id
                 ,first_visit_landing_page_chap_id  -- 首次访问着陆页章节id
          from dwd_data.dim_t99_reelshort_user_lt_info
          where etl_date='${TX_DATE}'
) as t3
on t3.uuid=t1.uuid
left join (
           select distinct
                  uuid
                  ,af_network_name
                  ,dlink_story_id
           from dws_data.dim_t87_reelshort_ads_user_attr_df
           where etl_date='${TX_DATE}'
) as t4
on t4.uuid=t1.uuid
;