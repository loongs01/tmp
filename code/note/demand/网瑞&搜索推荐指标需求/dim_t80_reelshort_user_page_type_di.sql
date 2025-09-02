-- drop Table if  exists dwd_data.dim_t80_reelshort_user_page_type_di;
-- Create Table if not exists dwd_data.dim_t80_reelshort_user_page_type_di(
-- id                  bigint auto_increment comment '自增id'
-- ,analysis_date      date                  comment '分析日期'
-- ,country_id         varchar               comment '国家id'
-- ,channel_id         varchar               comment '应用渠道'
-- ,platform           varchar               comment '操作系统类型 (android/ios/windows/mac os)'
-- ,language_id        varchar               comment '游戏语言id'
-- ,version            varchar               comment 'APP的应用版本(包体内置版本号versionname)'
-- ,action             varchar               comment '用户行为'
-- ,user_type          int                   comment '用户类型'
-- ,search_type        varchar               comment '搜索方式'
-- ,page_type          varchar               comment '页面类型:分别对应默认页面和结果页面'
-- ,uuid               varchar               comment '用户ID'
-- ,PRIMARY KEY (id,analysis_date,uuid)
-- )DISTRIBUTE BY HASH(analysis_date,id) PARTITION BY VALUE(DATE_FORMAT(analysis_date, '%Y%m')) LIFECYCLE 120 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}'
-- COMMENT='reelshort 用户页面类型统计表'
-- ;

-- 增加一对多维度关联
-- left join (select
                 -- uuid           as uuid1
                 -- ,analysis_date as analysis_date1
                 -- ,country_id    as country_id1
                 -- ,channel_id    as channel_id1
                 -- ,platform      as platform1
                 -- ,language_id   as language_id1
                 -- ,version       as version1
                 -- ,action        as action1
                 -- ,user_type     as user_type1
                 -- ,search_type   as search_type1
                 -- ,page_type     as page_type1
-- from dwd_data.dim_t80_reelshort_user_page_type_di
-- where analysis_date='${TX_DATE}'
      -- and tab_id!=''
-- ) as t1
-- on t1.uuid1=t.uuid
   -- and t1.analysis_date1= t.analysis_date
   -- and t1.country_id1   = t.country_id
   -- and t1.channel_id1   = t.channel_id
   -- and t1.platform1     = t.platform
   -- and t1.language_id1  = t.language_id
   -- and t1.version1      = t.version
   -- and t1.action1       = t.action
   -- and t1.user_type1    = t.user_type
   -- and t1.search_type1  = t.search_type
   -- and t1.page_type1    = t.page_type







-- 获取派生条件page_type -- 页面类型:分别对应默认页面和结果页面
delete from dwd_data.dim_t80_reelshort_user_page_type_di where analysis_date='${TX_DATE}';
insert into dwd_data.dim_t80_reelshort_user_page_type_di
select
       null as id
       ,t1.analysis_date
       ,t1.country_id                                   -- 国家id
       ,t1.channel_id                                   -- 应用渠道
       ,t1.platform                                     -- 操作系统类型 (android/ios/windows/mac os)
       ,t1.language_id                                  -- 游戏语言id
       ,t1.version                                      -- APP的应用版本(包体内置版本号versionname)
       ,t1.action                                       -- 用户行为
       ,nvl(t2.user_type,-999) as user_type             -- 用户类型
       ,t1.search_type                                  -- 搜索方式
       ,t1.page_type           as page_type             -- 页面类型:分别对应默认页面和结果页面
       ,t1.uuid
from (select distinct
             t1.etl_date             as analysis_date
             ,t1.country_id                             -- 国家id
             ,t1.channel_id                             -- 应用渠道
             ,t1.platform                               -- 操作系统类型 (android/ios/windows/mac os)
             ,t1.language_id                            -- 游戏语言id
             ,t1.version                                -- APP的应用版本(包体内置版本号versionname)
             ,t1.action                                 -- 用户行为
             ,case
                  when t1.search_word_source like '%typing%' then 1
                  when t1.search_word_source like '%default_fill%' then 2
                  when t1.search_word_source like '%history_click_fill%' then 3
                  when t1.search_word_source like '%tag_click_fill%' then 4
                  else 5
              end          as search_type               -- 搜索方式
             ,t1.page_type as page_type                 -- 页面类型:分别对应默认页面和结果页面
             ,t1.uuid
      from dwd_data.dwd_t02_reelshort_search_stat_di as t1
      where etl_date='${TX_DATE}'
            and event_name='m_custom_event'
            and sub_event_name='search_stat'
            and channel_id in (6,11)  -- 网端渠道id：6,11
) as t1
left join(
          select distinct
                 uuid
                 ,user_type
                 -- ,af_network_name
          from dwd_data.dwd_t01_reelshort_user_detail_info_di
          where etl_date='${TX_DATE}'
) as t2
on t2.uuid=t1.uuid
;