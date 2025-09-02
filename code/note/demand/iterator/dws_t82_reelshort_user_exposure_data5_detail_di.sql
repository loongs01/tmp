
-- drop table if exists dws_data.dws_t82_reelshort_user_exposure_data5_detail_di;
create table if not exists dws_data.dws_t82_reelshort_user_exposure_data5_detail_di (
  id bigint auto_increment,
  uuid varchar default '' comment '用户uuid,即idfa或者adid',
  analysis_date date comment '分析日期',
  is_pay int comment '1 支付用户，0 未付费用户',
  is_login int comment '是否登录1-是',
  user_type int comment '1-新用户',
  vip_type int default '0' comment 'sub_status用户订阅状态:0=未购买  1=生效中 2=已过期',
  af_network_name varchar default '' comment 'appsflyer归因渠道（媒体来源）',
  af_channel varchar default '' comment 'appsflyer渠道',
  country_id varchar default '' comment '国家id',
  channel_id varchar default '' comment '渠道id',
  version varchar default '' comment '底包版本',
  cversion varchar default '' comment '代码版本',
  res_version varchar default '' comment '资源版本',
  language_id varchar default '' comment '语言id',
  platform varchar default '' comment '平台id',

  sub_event_name varchar default '' comment 'sub_event_name',
  scene_name varchar default '' comment '场景名称',
  page_name varchar default '' comment '页面名称',
  pre_page_name varchar default '' comment '上一个页面名称',
  action varchar default '' comment 'action',
  shelf_id varchar default '' comment '书架id',
  story_id varchar default '' comment '书籍id',
  referrer_story_id varchar default '' comment '来源书籍id',
  chap_id varchar default '' comment '章节id',
  chap_order_id int comment '章节序列id',

  report_cnt int comment '上报次数',
  show_cnt int comment '展示次数',
  click_cnt int comment '点击次数',
  etl_date date comment '数据日期',
  primary key (id,uuid,etl_date,chap_id)
) distribute by hash(uuid,etl_date,chap_id) partition by value(date_format(etl_date, '%y%m')) lifecycle 120 index_all='y' storage_policy='mixed' hot_partition_count=6 engine='xuanwu' table_properties='{"format":"columnstore"}' comment='reelshort用户曝光详情表统计表';




delete from dws_data.dws_t82_reelshort_user_exposure_data5_detail_di where etl_date='${TX_DATE}';
insert into dws_data.dws_t82_reelshort_user_exposure_data5_detail_di
select
    null as id
    ,t1.uuid
    ,t1.analysis_date
    ,is_pay
    ,is_login
    ,user_type
    ,vip_type
    ,t4.af_network_name
    ,af_channel
    ,country_id
    ,channel_id
    ,version
    ,cversion
    ,res_version
    ,language_id
    ,platform
    ,sub_event_name
    ,scene_name
    ,page_name
    ,pre_page_name
    ,action
    ,t1.shelf_id
    ,story_id
    ,referrer_story_id
    ,chap_id
    ,chap_order_id
    ,report_cnt
    ,show_cnt
    ,click_cnt
    -- ,story_ids
    ,t1.analysis_date as etl_date
    ,nvl(t3.shelf_name,'')      as shelf_name        -- 书架名称
from
(
  select
        coalesce(t1.uuid,t2.uuid,t4.uuid,t3.uuid) as uuid
        ,coalesce(t1.analysis_date,t2.analysis_date,t4.analysis_date,t3.analysis_date) as analysis_date
        ,coalesce(t1.country_id,t2.country_id,t4.country_id,t3.country_id) as country_id
        ,coalesce(t1.channel_id,t2.channel_id,t4.channel_id,t3.channel_id) as channel_id
        ,coalesce(t1.version,t2.version,t4.version,t3.version) as version
        ,coalesce(t1.cversion,t2.cversion,t4.cversion,t3.cversion) as cversion
        ,coalesce(t1.res_version,t2.res_version,t4.res_version,t3.res_version) as res_version
        ,coalesce(t1.language_id,t2.language_id,t4.language_id,t3.language_id) as language_id
        ,coalesce(t1.platform,t2.platform,t4.platform,t3.platform) as platform
        ,coalesce(t1.sub_event_name,t2.sub_event_name,t4.sub_event_name,t3.sub_event_name) as sub_event_name
        ,coalesce(t1.scene_name,t2.scene_name,t4.scene_name,t3.scene_name) as scene_name
        ,coalesce(t1.page_name,t2.page_name,t4.page_name,t3.page_name) as page_name
        ,'' as pre_page_name
        ,coalesce(t1.action,t2.action,t4.action,t3.action) as action
        ,coalesce(t1.shelf_id,t2.shelf_id,t4.shelf_id,t3.shelf_id) as shelf_id
        ,coalesce(t1.story_id,t2.story_id,t4.story_id,t3.story_id) as story_id
        ,'' as referrer_story_id
        ,'' as chap_id
        ,0 as chap_order_id
        ,coalesce(t1.report_cnt,t2.report_cnt,t4.report_cnt,t3.report_cnt,0) as report_cnt
        ,cast(coalesce(t2.show_cnt,t4.show_cnt,t3.show_cnt,t1.show_cnt,0) as int) as show_cnt
        ,cast(coalesce(t2.click_cnt,t3.click_cnt,0) as int) as click_cnt
  from 
  ( -- m_item_pv 书籍封面曝光
    select 
      uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform
      ,'m_item_pv' as sub_event_name,scene_name,page_name,shelf_id,story_id
      ,'item_pv_show' as action -- cover  Discover / Library / Story Ending
      ,cast(count(*) as int) as report_cnt -- 上报次数，在页面内去重上报，只要有露出就会报
      ,cast(count(*) as int) as show_cnt -- 上报次数，在页面内去重上报，只要有露出就会报
    from dwd_data.dwd_t02_reelshort_item_pv_di 
    where etl_date='${TX_DATE}'
    group by uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform
      ,scene_name,page_name,item_type,shelf_id
      ,story_id
  ) t1

  full join 
  ( -- iad_track_stat 应用内宣发广告曝光
    select
      uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform
      ,sub_event_name,scene_name,page_name,shelf_id
      ,book_id as story_id
      ,if(action='show','iad_track_show','iad_track_click') as action
      ,cast(count(*) as int) as report_cnt -- 上报次数，在页面内去重上报，只要有露出就会报
      ,cast(sum(if(action='show',1,0)) as int) as show_cnt -- banner广告曝光次数
      ,cast(sum(if(action='click',1,0)) as int) as click_cnt -- banner广告点击次数
    from dwd_data.dwd_t01_reelshort_iad_track_di 
    where etl_date='${TX_DATE}' and action in('show','click') and page_name in('discover','home')
    group by uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform
      ,sub_event_name,scene_name,page_name,action,shelf_id
      ,book_id
  ) t2
  on t1.uuid=t2.uuid and t1.analysis_date=t2.analysis_date 
        and t1.country_id=t2.country_id and t1.channel_id=t2.channel_id and t1.version=t2.version and t1.cversion=t2.cversion and t1.res_version=t2.res_version and t1.language_id=t2.language_id and t1.platform=t2.platform 
        and t1.story_id=t2.story_id 
        -- and t1.chap_id=t2.chap_id and t1.chap_order_id=t2.chap_order_id
        and t1.sub_event_name=t2.sub_event_name
        and t1.scene_name=t2.scene_name
        and t1.page_name=t2.page_name
        -- and t1.pre_page_name=t2.pre_page_name
  full join 
  ( -- story_rec_popup_click 播放器内书籍推荐弹窗曝光
    select
      uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform
      ,sub_event_name,scene_name,page_name,shelf_id
      ,story_id
      ,cast(count(*) as int) as report_cnt -- 上报次数，在页面内去重上报，只要有露出就会报
      ,if(action='show','story_rec_popup_show','story_rec_popup_click') as action
      ,cast(sum(if(action='show',1,0)) as int) as show_cnt -- rec广告曝光次数
      ,cast(sum(if(action='book_click',1,0)) as int) as click_cnt -- rec广告点击次数
    from dwd_data.dwd_t02_reelshort_custom_event_di
    where etl_date='${TX_DATE}' and sub_event_name='story_rec_popup_click'
      and action in ('show','book_click')
    group by uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform
      ,scene_name,page_name,action,shelf_id
      ,story_id
  ) t3
  on t1.uuid=t3.uuid and t1.analysis_date=t3.analysis_date 
        and t1.country_id=t3.country_id and t1.channel_id=t3.channel_id and t1.version=t3.version and t1.cversion=t3.cversion and t1.res_version=t3.res_version and t1.language_id=t3.language_id and t1.platform=t3.platform 
        and t1.story_id=t3.story_id 
        -- and t1.chap_id=t3.chap_id and t1.chap_order_id=t3.chap_order_id
        and t1.sub_event_name=t3.sub_event_name
        and t1.scene_name=t3.scene_name
        and t1.page_name=t3.page_name
        -- and t1.pre_page_name=t3.pre_page_name

  full join 
  ( -- foryou 曝光
    select
      uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform
      ,sub_event_name,scene_name,page_name,shelf_id
      ,story_id
      ,cast(count(*) as int) as report_cnt -- 上报次数，在页面内去重上报，只要有露出就会报
      ,'for_you_show' as action
      ,cast(count(*) as int) as show_cnt -- 上报次数，在页面内去重上报，只要有露出就会报
    from dwd_data.dwd_t02_reelshort_custom_event_di
    where etl_date='${TX_DATE}' and sub_event_name='play_start' and page_name='for_you' and shelf_id='10001'
    group by uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform
      ,scene_name,page_name,action,shelf_id
      ,story_id
  ) t4
  on t1.uuid=t4.uuid and t1.analysis_date=t4.analysis_date 
        and t1.country_id=t4.country_id and t1.channel_id=t4.channel_id and t1.version=t4.version and t1.cversion=t4.cversion and t1.res_version=t4.res_version and t1.language_id=t4.language_id and t1.platform=t4.platform 
        and t1.story_id=t4.story_id 
        -- and t1.chap_id=t4.chap_id and t1.chap_order_id=t4.chap_order_id
        and t1.sub_event_name=t4.sub_event_name
        and t1.scene_name=t4.scene_name
        and t1.page_name=t4.page_name
        -- and t1.pre_page_name=t4.pre_page_name
) t1 
join  
(
   select 
        uuid
        ,etl_date 
        ,is_pay
        ,is_login
        ,user_type
        -- ,af_network_name
        ,af_channel
        ,af_campaign
        ,af_adset
        ,af_ad
        ,vip_type 
   from dwd_data.dwd_t01_reelshort_user_detail_info_di
   where etl_date='${TX_DATE}' 
) t2 
on t1.uuid=t2.uuid
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
left join (
           select distinct
                  uuid
                  ,af_network_name
           from dws_data.dim_t87_reelshort_ads_user_attr_df
           where etl_date='${TX_DATE}'
) as t4
on t4.uuid=t1.uuid
;
