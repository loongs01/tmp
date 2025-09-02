create or replace view dw_view.dws_t80_reelshort_user_chapter_unlock_statistic_di as
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
      from dw_view.dwd_t04_reelshort_checkpoint_unlock_di as t
      -- where etl_date='${TX_DATE}'
            -- and event_name='m_checkpoint_unlock'
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
left join (select uuid
                   ,analysis_date
                   ,case when right(media_type,4)='_int' then split_part(media_type,'_',1)
                       when lower(media_type) in('true','false') then 'others'
                       when media_type='FB' then 'fb'
                       when media_type='' then 'organic'
                       else media_type
                   end as af_network_name
                   ,if(book_id in('undefined','null'),'',book_id) as dlink_story_id
                   ,row_number()over(partition by uuid order by analysis_date desc) as rk
            from dw_view.dws_t87_reelshort_book_attr_di
           
)t4
on t4.uuid=t1.uuid and t4.rk=1
union all 

;