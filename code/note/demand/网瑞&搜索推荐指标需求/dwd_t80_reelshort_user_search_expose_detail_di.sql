-- drop Table if  exists dwd_data.dwd_t80_reelshort_user_search_expose_detail_di;
-- Create Table if not exists dwd_data.dwd_t80_reelshort_user_search_expose_detail_di(
-- id                  bigint AUTO_INCREMENT    comment '自增id'
-- ,analysis_date      date                     comment '分析日期'
-- ,uuid               varchar      comment '用户ID'
-- ,country_id         varchar      comment '国家id'
-- ,channel_id         varchar      comment '应用渠道'
-- ,platform           varchar      comment '操作系统类型 (android/ios/windows/mac os)'
-- ,language_id        varchar      comment '游戏语言id'
-- ,version            varchar      comment 'APP的应用版本(包体内置版本号versionname)'
-- ,user_type          int          comment '用户类型'
-- ,PRIMARY KEY (id,analysis_date,uuid)
-- )DISTRIBUTE BY HASH(analysis_date,id) PARTITION BY VALUE(DATE_FORMAT(analysis_date, '%Y%m')) LIFECYCLE 120 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='reelshort 用户书架曝光行为统计表';

-- alter table dwd_data.dwd_t80_reelshort_user_search_expose_detail_di
-- add column af_network_name varchar comment '归因投放渠道'
-- ;



delete from dwd_data.dwd_t80_reelshort_user_search_expose_detail_di where analysis_date='${TX_DATE}';
insert into dwd_data.dwd_t80_reelshort_user_search_expose_detail_di
select
null                        as id                -- 自增id
,t1.etl_date                as analysis_date     -- 分析日期
,t1.uuid                                         -- 用户ID
,t1.country_id                                   -- '国家id'
,t1.channel_id                                   -- '应用渠道'
,t1.platform                                     -- '操作系统类型 (android/ios/windows/mac os)'
,t1.language_id                                  -- '游戏语言id'
,t1.version                                      -- 'APP的应用版本(包体内置版本号versionname)'
,nvl(t2.user_type,-999)     as user_type         -- 用户类型
,nvl(t2.af_network_name,'') as af_network_name   -- 归因投放渠道
from (
      select
            etl_date
            ,uuid
            ,country_id
            ,channel_id
            ,platform
            ,language_id
            ,version
      from dwd_data.dwd_t02_reelshort_page_enter_exit_di as t
      where etl_date='${TX_DATE}'
            and event_name='m_page_enter'
            and page_name='discover'
      group by
              etl_date
              ,uuid
              ,country_id
              ,channel_id
              ,platform
              ,language_id
              ,version
) as t1
left join
        (
        select distinct
               uuid
               ,user_type
               ,af_network_name
        from dwd_data.dwd_t01_reelshort_user_detail_info_di
        where etl_date='${TX_DATE}'
) as t2
on t2.uuid=t1.uuid
;