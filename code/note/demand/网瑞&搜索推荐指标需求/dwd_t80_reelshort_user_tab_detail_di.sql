-- drop Table if  exists dwd_data.dwd_t80_reelshort_user_tab_detail_di;
-- Create Table if not exists dwd_data.dwd_t80_reelshort_user_tab_detail_di(
-- id                  bigint auto_increment comment '自增id'
-- ,analysis_date      date                  comment '分析日期'
-- ,country_id         varchar               comment '国家id'
-- ,channel_id         varchar               comment '应用渠道'
-- ,platform           varchar               comment '操作系统类型 (android/ios/windows/mac os)'
-- ,language_id        varchar               comment '游戏语言id'
-- ,version            varchar               comment 'APP的应用版本(包体内置版本号versionname)'
-- ,page_name          varchar               comment '当前页面名称(业务自定义）'
-- ,tab_id             varchar               comment '频道页'
-- ,tab_page_name      varchar               comment '频道页名称'
-- ,user_type          int                   comment '用户类型'
-- ,af_network_name    varchar               comment '归因投放渠道'
-- ,uuid               varchar               comment '用户id'
-- ,tab_cnt            bigint                comment '频道页曝光次数'
-- ,PRIMARY KEY (id,analysis_date,uuid)
-- )DISTRIBUTE BY HASH(analysis_date,id) PARTITION BY VALUE(DATE_FORMAT(analysis_date, '%Y%m')) LIFECYCLE 120 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}'
-- COMMENT='reelshort 用户书籍封面行为明细表'
-- ;




-- 频道页指标
delete from dwd_data.dwd_t80_reelshort_user_tab_detail_di where analysis_date='${TX_DATE}';
insert into dwd_data.dwd_t80_reelshort_user_tab_detail_di
select
       null as id
       ,t.analysis_date
       ,t.country_id                                   -- 国家id
       ,t.channel_id                                   -- 应用渠道
       ,t.platform                                     -- 操作系统类型 (android/ios/windows/mac os)
       ,t.language_id                                  -- 游戏语言id
       ,t.version                                      -- APP的应用版本(包体内置版本号versionname)
       ,t.page_name                                    -- 当前页面名称(业务自定义）
       ,t.tab_id                                       -- 频道页
       ,nvl(t1.tab_name,'') as tab_page_name           -- 频道页名称
       -- ,concat(t.tab_id,t1.tab_name) as tab_id_name -- 频道页id+（中文名）
       ,nvl(t2.user_type,-999)     as user_type        -- 用户类型
       ,nvl(t2.af_network_name,'') as af_network_name  -- 归因投放渠道
       ,t.uuid
       ,t.tab_cnt                  as tab_cnt          -- 频道页曝光次数
from (select
            etl_date as analysis_date
            ,country_id                             -- 国家id
            ,channel_id                             -- 应用渠道
            ,platform                               -- 操作系统类型 (android/ios/windows/mac os)
            ,language_id                            -- 游戏语言id
            ,version                                -- APP的应用版本(包体内置版本号versionname)
            ,page_name                              -- 当前页面名称(业务自定义）
            ,cast(nvl(json_extract(item_list,'$.sub_page_id'),'') as varchar) as tab_id -- 频道页
            ,uuid
            ,count(1) as tab_cnt -- 频道页曝光次数
      from dwd_data.dwd_t02_reelshort_item_pv_di
      where etl_date='${TX_DATE}'
      and event_name='m_item_pv'
      group by etl_date
               ,country_id
               ,channel_id
               ,platform
               ,language_id
               ,version
               ,page_name
               ,tab_id
               ,uuid
)as t
left join dw_view.dts_project_v_hall_v3 as t1
on t1.tab_id=t.tab_id
left join(select distinct
                 uuid
                 ,user_type
                 ,af_network_name
          from dwd_data.dwd_t01_reelshort_user_detail_info_di
          where etl_date='${TX_DATE}'
) as t2
on t2.uuid=t.uuid
;