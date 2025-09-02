-- drop Table if  exists dwd_data.dwd_t80_reelshort_user_search_detail_di;
Create Table if not exists dwd_data.dwd_t80_reelshort_user_search_detail_di(
id                  bigint AUTO_INCREMENT    comment '自增id'
,analysis_date      date                     comment '分析日期'
,uuid               varchar      comment '用户ID'
,country_id         varchar      comment '国家id'
,channel_id         varchar      comment '应用渠道'
,platform           varchar      comment '操作系统类型 (android/ios/windows/mac os)'
,language_id        varchar      comment '游戏语言id'
,os_version         varchar      comment '操作系统版本号'
,page_name          varchar      comment '当前页面名称(业务自定义）'
,shelf_id           varchar      comment '书架id'
,user_type          int          comment '用户类型'
,PRIMARY KEY (id,analysis_date,uuid)
)DISTRIBUTE BY HASH(analysis_date,story_id) PARTITION BY VALUE(DATE_FORMAT(analysis_date, '%Y%m')) LIFECYCLE 120 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='reelshort 用户行为统计表';




delete from dwd_data.dwd_t80_reelshort_user_search_detail_di where etl_date='${TX_DATE}';
insert into dwd_data.dwd_t80_reelshort_user_search_detail_di
select
null           as id            -- 自增id
,t1.etl_date   as analysis_date -- 分析日期
,t1.uuid                        -- 用户ID
,t1.country_id                  -- '国家id'
,t1.channel_id                  -- '应用渠道'
,t1.platform                    -- '操作系统类型 (android/ios/windows/mac os)'
,t1.language_id                 -- '游戏语言id'
,t1.os_version                  -- '操作系统版本号'
,t1.page_name                   -- '当前页面名称(业务自定义）'
,t1.shelf_id                    -- '书架id'
,t2.user_type                   -- '用户类型'
from (
      select
            etl_date
            ,uuid
            ,country_id
            ,channel_id
            ,platform
            ,language_id
            ,os_version
            ,page_name
            ,shelf_id
      from dwd_data.dwd_t02_reelshort_custom_event_di as t
      where etl_date='${TX_DATE}'
            and event_name='m_custom_event'
            and sub_event_name='search_stat'
      group by
              etl_date
              ,uuid
              ,country_id
              ,channel_id
              ,platform
              ,language_id
              ,os_version
              ,page_name
              ,shelf_id
) as t1
left join
        (
        select
        uuid
        ,max(user_type) as user_type
        from dwd_data.dwd_t01_reelshort_user_detail_info_di
        where etl_date='${TX_DATE}'
        group by uuid
) as t2
on t2.uuid=t1.uuid