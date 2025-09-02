drop Table if  exists dws_data.dws_t02_reelshort_user_gid_statistic_di;
Create Table if not exists dws_data.dws_t02_reelshort_user_gid_statistic_di(
id                                 bigint AUTO_INCREMENT    comment '自增id'
,analysis_date                     date    comment '分析日期'
,uuid                              varchar comment '用户ID'
,country_id                        varchar comment '国家id'
,channel_id                        varchar comment '应用渠道'
,platform                          varchar comment '操作系统类型 (android/ios/windows/mac os)'
,language_id                       varchar comment '游戏语言id'
,version                           varchar comment 'APP的应用版本(包体内置版本号versionname)'
,cversion                          varchar comment '应用游戏版本号'
,user_type                         int     comment '用户类型'
-- ,recent_visit_source               varchar comment '最近一次访问来源'
-- ,recent_visit_source_type          varchar comment '最近一次访问来源类型'
-- ,first_visit_landing_page          varchar comment '首次访问着陆页'
-- ,first_visit_landing_page_story_id varchar comment '首次访问着陆页书籍id'
-- ,first_visit_landing_page_chap_id  varchar comment '首次访问着陆页章节id'
,af_network_name                   varchar comment '归因投放渠道'
,dlink_story_id                    varchar comment '书籍id'
-- ,chap_id                           varchar comment '解锁开始章节id'
,story_id                          varchar comment '书籍id'
-- ,unlock_type                       varchar comment '1：用金币或bonus解锁 2:等免解锁'
,is_pay                            int     comment '当天是否付费1-是，0-否'
,vip_type                          int     comment 'sub_status用户订阅状态:0=未购买  1=生效中 2=已过期'
,app_sku                           varchar comment '应用商品ID'
,channel_sku                       varchar comment '支付平台商品ID'
,order_src                         varchar comment '付费场景'
,ctime                             bigint  comment '设备系统当前时间戳(秒级)'
,PRIMARY KEY (id,analysis_date,uuid)
)DISTRIBUTE BY HASH(analysis_date,id) PARTITION BY VALUE(DATE_FORMAT(analysis_date, '%Y%m')) LIFECYCLE 120 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='reelshort用户商品事实统计表';





delete from dws_data.dws_t02_reelshort_user_gid_statistic_di where analysis_date='${TX_DATE}';
insert into dws_data.dws_t02_reelshort_user_gid_statistic_di
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
      ,nvl(t4.af_network_name,'') as af_network_name -- 归因投放渠道
      ,nvl(t4.dlink_story_id,'')  as dlink_story_id
      -- ,t1.chap_id                           -- 解锁开始章节id
      ,t1.story_id                          -- 书籍id
      -- ,t1.unlock_type                       -- 1：用金币或bonus解锁 2:等免解锁
      ,nvl(t2.is_pay,0)           as is_pay
      ,nvl(t2.vip_type,0)         as vip_type
      ,t1.app_sku                 -- 应用商品ID
      ,t1.channel_sku
      ,t1.position                as order_src
      ,t1.ctime
from (select distinct
             t1.etl_date
             ,t1.uuid
             ,t1.country_id
             ,t1.channel_id
             ,t1.platform
             ,t1.language_id
             ,t1.version
             ,t1.cversion
             ,t1.story_id
             ,t1.app_sku
             ,t1.ctime
             ,if(position is null or position='', page_name, position) as position
             ,substring_index(replace(temp_table.item, '"', ''), '#', -1) as channel_sku
      from (select distinct
                   t1.etl_date
                   ,t1.uuid
                   ,t1.country_id
                   ,t1.channel_id
                   ,t1.platform
                   ,t1.language_id
                   ,t1.version
                   ,t1.cversion
                   ,t1.story_id
                   ,t1.app_sku
                   ,t1.ctime
				   ,t1.page_name
                   ,cast(json_extract(properties, "$.position") as string) as position
                   , split(replace(replace(cast(json_extract(properties, "$.item_list") as string), "[", ""), "]", ""),',') as numbers_array
            from dwd_data.dwd_t02_reelshort_custom_event_di as t1
            where etl_date='${TX_DATE}'
                  and page_name not in ('earn_rewards', 'discover', 'for_you')
                  and sub_event_name='page_item_impression'
            ) as t1
      cross join unnest(numbers_array) as temp_table(item)
      union all
      select distinct
             t1.etl_date
             ,t1.uuid
             ,t1.country_id
             ,t1.channel_id
             ,t1.platform
             ,t1.language_id
             ,t1.version
             ,t1.cversion
             ,t1.story_id
             ,t1.app_sku
             ,t1.ctime
             ,'付费弹窗' as position
             , channel_sku
      from dwd_data.dwd_t02_reelshort_custom_event_di  as t1
      where etl_date='${TX_DATE}'
            and sub_event_name in ('exclusive_gift_popup', 'pay_popup')
            and action in ("page_show", "show")
) as t1
left join(
          select distinct
                 uuid
                 ,user_type
                 -- ,af_network_name
                 -- ,recent_visit_source -- 最近一次访问来源
                 -- ,recent_visit_source_type -- 最近一次访问来源类型
                 ,is_pay
                 ,vip_type
          from dwd_data.dwd_t01_reelshort_user_detail_info_di
          where etl_date='${TX_DATE}'
) as t2
on t2.uuid=t1.uuid
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