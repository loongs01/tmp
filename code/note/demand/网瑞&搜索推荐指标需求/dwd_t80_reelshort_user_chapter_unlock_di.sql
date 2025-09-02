-- drop Table if  exists dwd_data.dwd_t80_reelshort_user_chapter_unlock_di;
-- Create Table if not exists dwd_data.dwd_t80_reelshort_user_chapter_unlock_di(
-- id                                 bigint AUTO_INCREMENT    comment '自增id'
-- ,analysis_date                     date         comment '分析日期'
-- ,uuid                              varchar      comment '用户ID'
-- ,country_id                        varchar      comment '国家id'
-- ,channel_id                        varchar      comment '应用渠道'
-- ,platform                          varchar      comment '操作系统类型 (android/ios/windows/mac os)'
-- ,language_id                       varchar      comment '游戏语言id'
-- ,version                           varchar      comment 'APP的应用版本(包体内置版本号versionname)'
-- ,user_type                         int          comment '用户类型'
-- ,recent_visit_source               varchar      comment '最近一次访问来源'
-- ,recent_visit_source_type          varchar      comment '最近一次访问来源类型'
-- ,first_visit_landing_page          varchar      comment '首次访问着陆页'
-- ,first_visit_landing_page_story_id varchar      comment '首次访问着陆页书籍id'
-- ,first_visit_landing_page_chap_id  varchar      comment '首次访问着陆页章节id'

-- ,PRIMARY KEY (id,analysis_date,uuid)
-- )DISTRIBUTE BY HASH(analysis_date,id) PARTITION BY VALUE(DATE_FORMAT(analysis_date, '%Y%m')) LIFECYCLE 120 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='reelshort 用户剧集解锁统计表';

-- alter table dwd_data.dwd_t80_reelshort_user_chapter_unlock_di
-- add column af_network_name varchar comment '归因投放渠道'
-- ;

alter table dwd_data.dwd_t80_reelshort_user_chapter_unlock_di
add column chap_id varchar comment '解锁开始章节id'
,add column story_id varchar comment '书籍id'
,add column unlock_type varchar comment '1：用金币或bonus解锁 2:等免解锁'
;

`cversion` varchar COMMENT '应用游戏版本号'



delete from dwd_data.dwd_t80_reelshort_user_chapter_unlock_di where analysis_date='${TX_DATE}';
insert into dwd_data.dwd_t80_reelshort_user_chapter_unlock_di
select
      null           as id                  -- 自增id
      ,t1.etl_date   as analysis_date       -- 分析日期
      ,t1.uuid                              -- 用户ID
      ,t1.country_id                        -- '国家id'
      ,t1.channel_id                        -- '应用渠道'
      ,t1.platform                          -- '操作系统类型 (android/ios/windows/mac os)'
      ,t1.language_id                       -- '游戏语言id'
      ,t1.version                           -- 'APP的应用版本(包体内置版本号versionname)'
      ,t2.user_type                         -- '用户类型'
      ,nvl(t2.recent_visit_source,'')                 as recent_visit_source                 -- 最近一次访问来源
      ,nvl(t2.recent_visit_source_type,'')            as recent_visit_source_type            -- 最近一次访问来源类型
      ,nvl(t3.first_visit_landing_page,'')            as first_visit_landing_page            -- 首次访问着陆页
      ,nvl(t3.first_visit_landing_page_story_id,'')   as first_visit_landing_page_story_id   -- 首次访问着陆页书籍id
      ,nvl(t3.first_visit_landing_page_chap_id,'')    as first_visit_landing_page_chap_id    -- 首次访问着陆页章节id
      ,nvl(t2.af_network_name,'')                     as af_network_name                     -- 归因投放渠道
      ,t1.chap_id                           -- 解锁开始章节id
      ,t1.story_id                          -- 书籍id
      ,t1.unlock_type                       -- 1：用金币或bonus解锁 2:等免解锁

from (
      select
            etl_date
            ,uuid
            ,country_id
            ,channel_id
            ,platform
            ,language_id
            ,version
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
) as t1
left join(
          select distinct
                 uuid
                 ,user_type
                 ,af_network_name
                 ,recent_visit_source -- 最近一次访问来源
                 ,recent_visit_source_type -- 最近一次访问来源类型
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
;