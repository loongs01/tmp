-- drop Table if  exists dwd_data.dwd_t80_reelshort_user_shelf_expose_detail_di;
-- Create Table if not exists dwd_data.dwd_t80_reelshort_user_shelf_expose_detail_di(
-- id                  bigint AUTO_INCREMENT    comment '自增id'
-- ,analysis_date      date                     comment '分析日期'
-- ,uuid               varchar      comment '用户ID'
-- ,country_id         varchar      comment '国家id'
-- ,channel_id         varchar      comment '应用渠道'
-- ,platform           varchar      comment '操作系统类型 (android/ios/windows/mac os)'
-- ,language_id        varchar      comment '游戏语言id'
-- ,version            varchar      comment 'APP的应用版本(包体内置版本号versionname)'
-- ,page_name          varchar      comment '当前页面名称(业务自定义）'
-- ,shelf_id           varchar      comment '书架id'
-- ,user_type          int          comment '用户类型'
-- ,PRIMARY KEY (id,analysis_date,uuid)
-- )DISTRIBUTE BY HASH(analysis_date,id) PARTITION BY VALUE(DATE_FORMAT(analysis_date, '%Y%m')) LIFECYCLE 120 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='reelshort 用户书架曝光行为统计表';




delete from dwd_data.dwd_t80_reelshort_user_shelf_expose_detail_di where analysis_date='${TX_DATE}';
insert into dwd_data.dwd_t80_reelshort_user_shelf_expose_detail_di
select
      null                        as id                -- 自增id
      ,t1.etl_date                as analysis_date     -- 分析日期
      ,t1.uuid                                         -- 用户ID
      ,t1.country_id                                   -- 国家id
      ,t1.channel_id                                   -- 应用渠道
      ,t1.platform                                     -- 操作系统类型 (android/ios/windows/mac os)
      ,t1.language_id                                  -- 游戏语言id
      ,t1.version                                      -- APP的应用版本(包体内置版本号versionname)
      ,t1.page_name                                    -- 当前页面名称(业务自定义）
      ,t1.shelf_id                                     -- 书架id
      ,nvl(t2.user_type,-999)     as user_type         -- 用户类型
      ,nvl(t2.af_network_name,'') as af_network_name   -- 归因投放渠道
      ,nvl(t3.shelf_name,'')      as shelf_name        -- 书架名称
from (
      select
            etl_date
            ,uuid
            ,country_id
            ,channel_id
            ,platform
            ,language_id
            ,version
            ,page_name
            ,shelf_id
      from dwd_data.dwd_t02_reelshort_item_pv_di as t
      where etl_date='${TX_DATE}'
            and event_name='m_item_pv'
      group by
              etl_date
              ,uuid
              ,country_id
              ,channel_id
              ,platform
              ,language_id
              ,version
              ,page_name
              ,shelf_id
      union all
      select
            etl_date
            ,uuid
            ,country_id
            ,channel_id
            ,platform
            ,language_id
            ,version
            ,page_name
            ,shelf_id
      from dwd_data.dwd_t02_reelshort_custom_event_di as t
      where etl_date='${TX_DATE}'
            and event_name='m_custom_event'
            and sub_event_name in('billboard_click'
                                 ,'continue_watching_guide_click'
                                 ,'story_rec_popup_click'
                                 )
      group by
              etl_date
              ,uuid
              ,country_id
              ,channel_id
              ,platform
              ,language_id
              ,version
              ,page_name
              ,shelf_id
      union all
      select
            etl_date
            ,uuid
            ,country_id
            ,channel_id
            ,platform
            ,language_id
            ,version
            ,page_name
            ,shelf_id
      from dwd_data.dwd_t02_reelshort_item_pv_di as t
      where etl_date='${TX_DATE}'
            and event_name='m_custom_event'
            and sub_event_name='iad_track_stat'
      group by
              etl_date
              ,uuid
              ,country_id
              ,channel_id
              ,platform
              ,language_id
              ,version
              ,page_name
              ,shelf_id
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
left join(select shelf_id
                 ,shelf_name
                 ,row_number() over(partition by shelf_id order by priority asc) as rn
          from(select
                     shelf_id       as shelf_id
                     ,shelf_name_cn as shelf_name
                     ,1             as priority
               from dim_data.dim_t99_reelshort_bookshelf_info
               union all
               select
                     shelf_id
                     ,shelf_name_cn
                     ,2 as priority
               from ana_data.dim_t99_reelshort_bookshelf_info
               union all
               select
                     id
                     ,case when bookshelf_name is null then case when ui_style=1  then '基础布局'
                                                                 when ui_style=2  then '基础布局排行'
                                                                 when ui_style=3  then '继续观看'
                                                                 when ui_style=4  then '列表式布局'
                                                                 when ui_style=5  then '推荐布局'
                                                                 when ui_style=6  then '预告书架'
                                                                 when ui_style=7  then '即将上架'
                                                                 when ui_style=8  then '限时免费布局'
                                                                 when ui_style=9  then 'APP引流'
                                                                 when ui_style=10 then '排行榜'
                                                                 when ui_style=11 then '自动播放'
                                                                 when ui_style=12 then '活动横幅布局'
                                                                 when ui_style=13 then '滚动布局'
                                                                 when ui_style=15 then '三列布局'
                                                                 when ui_style=16 then '两列布局'
                                                                 when ui_style=17 then '中横幅'
                                                                 when ui_style=18 then '九宫格瀑布流'
                                                                 when ui_style=19 then '双列瀑布流'
                                                                 when ui_style=20 then 'Hot'
                                                            end
                           else bookshelf_name
                      end as shelf_name
                     -- ,row_number() over (partition by id,bookshelf_name order by updated_at desc ) as rn
                     ,3 as priority
               from chapters_log.dts_project_v_hall_v3_bookshelf
               union all
               select
                     id
                     ,case when bookshelf_name is null then case when ui_style=1  then '基础布局'
                                                                 when ui_style=2  then '基础布局排行'
                                                                 when ui_style=3  then '继续观看'
                                                                 when ui_style=4  then '列表式布局'
                                                                 when ui_style=5  then '推荐布局'
                                                                 when ui_style=6  then '预告书架'
                                                                 when ui_style=7  then '即将上架'
                                                                 when ui_style=8  then '限时免费布局'
                                                                 when ui_style=9  then 'APP引流'
                                                                 when ui_style=10 then '排行榜'
                                                                 when ui_style=11 then '自动播放'
                                                                 when ui_style=12 then '活动横幅布局'
                                                                 when ui_style=13 then '滚动布局'
                                                                 when ui_style=15 then '三列布局'
                                                                 when ui_style=16 then '两列布局'
                                                                 when ui_style=17 then '中横幅'
                                                                 when ui_style=18 then '九宫格瀑布流'
                                                                 when ui_style=19 then '双列瀑布流'
                                                                 when ui_style=20 then 'Hot'
                                                            end
                           else bookshelf_name
                      end as shelf_name
                      ,4 as priority
               from chapters_log.dts_project_v_hall_bookshelf
               )
)as t3
on t3.shelf_id=t1.shelf_id and t3.rn=1
;