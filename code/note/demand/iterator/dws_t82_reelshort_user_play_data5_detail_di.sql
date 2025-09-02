
CREATE TABLE if not exists dmd_data.dws_t82_reelshort_user_play_data5_detail_di_01 (
  id bigint AUTO_INCREMENT,
  uuid varchar,
  analysis_date date,
  country_id varchar,
  channel_id varchar,
  version varchar,
  cversion varchar,
  res_version varchar,
  language_id varchar,
  platform varchar,
  join_col varchar,
  sub_event_name varchar,
  scene_name varchar,
  page_name varchar,
  pre_page_name varchar,
  action varchar,
  shelf_id varchar,
  story_id varchar,
  referrer_story_id varchar,
  chap_order_id int,
  chap_id varchar,
  video_id varchar,
  report_cnt int,
  cover_click_cnt int,
  play_start_cnt int,
  play_complete_cnt int,
  first_report_time timestamp,
  last_report_time timestamp,
  play_duration int,
  PRIMARY KEY (id,analysis_date,uuid)
) DISTRIBUTE BY HASH(id) partition by value(date_format(analysis_date, '%y%m%d')) lifecycle 1 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}'
;

CREATE TABLE if not exists dmd_data.dws_t82_reelshort_user_play_data5_detail_di_02 (
  id bigint AUTO_INCREMENT,
  uuid varchar,
  analysis_date date,
  country_id varchar,
  channel_id varchar,
  version varchar,
  cversion varchar,
  res_version varchar,
  language_id varchar,
  platform varchar,
  join_col varchar,
  sub_event_name varchar,
  scene_name varchar,
  page_name varchar,
  pre_page_name varchar,
  action varchar,
  shelf_id varchar,
  story_id varchar,
  chap_id varchar,
  chap_order_id int,
  report_cnt int,
  click_cnt int,
  first_report_time timestamp,
  last_report_time timestamp,
  PRIMARY KEY (id,analysis_date,uuid)
) DISTRIBUTE BY HASH(id) partition by value(date_format(analysis_date, '%y%m%d')) lifecycle 1 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}'
;

CREATE TABLE if not exists dmd_data.dws_t82_reelshort_user_play_data5_detail_di_03 (
  id bigint AUTO_INCREMENT,
  uuid varchar,
  analysis_date date,
  etl_date date,
  country_id varchar,
  channel_id varchar,
  version varchar,
  cversion varchar,
  res_version varchar,
  language_id varchar,
  platform varchar,
  join_col varchar,
  sub_event_name varchar,
  scene_name varchar,
  pre_page_name varchar,
  shelf_id varchar,
  action varchar,
  story_id varchar,
  chap_id varchar,
  chap_order_id int,
  report_cnt int,
  show_cnt int,
  click_cnt int,
  first_report_time timestamp,
  last_report_time timestamp,
  page_name varchar,
  PRIMARY KEY (id,analysis_date,uuid)
) DISTRIBUTE BY HASH(id) partition by value(date_format(analysis_date, '%y%m%d')) lifecycle 1 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}'
;

CREATE TABLE if not exists dmd_data.dws_t82_reelshort_user_play_data5_detail_di_04_tmp (
  id bigint AUTO_INCREMENT,
  uuid varchar,
  analysis_date date,
  country_id varchar,
  channel_id varchar,
  version varchar,
  cversion varchar,
  res_version varchar,
  language_id varchar,
  platform varchar,
  sub_event_name varchar,
  scene_name varchar,
  page_name varchar,
  pre_page_name varchar,
  action varchar,
  shelf_id2 varchar,
  story_id varchar,
  chap_id varchar,
  chap_order_id int,
  report_cnt int,
  story_ids_size int,
  first_report_time timestamp,
  last_report_time timestamp,
  play_duration int,
  online_times int,
  PRIMARY KEY (id,analysis_date,uuid)
) DISTRIBUTE BY HASH(id) partition by value(date_format(analysis_date, '%y%m%d')) lifecycle 1 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}'

;

CREATE TABLE if not exists dmd_data.dws_t82_reelshort_user_play_data5_detail_di_04 (
  id bigint AUTO_INCREMENT,
  uuid varchar,
  analysis_date date,
  country_id varchar,
  channel_id varchar,
  version varchar,
  cversion varchar,
  res_version varchar,
  language_id varchar,
  platform varchar,
  sub_event_name varchar,
  scene_name varchar,
  page_name varchar,
  pre_page_name varchar,
  action varchar,
  shelf_id2 varchar,
  story_id varchar,
  chap_id varchar,
  chap_order_id int,
  report_cnt int,
  story_ids_size int,
  first_report_time timestamp,
  last_report_time timestamp,
  play_duration int,
  online_times int,
  shelf_id varchar,
  show_cnt decimal(10, 4),
  click_cnt decimal(10, 4),
  PRIMARY KEY (id,analysis_date,uuid)
) DISTRIBUTE BY HASH(id) partition by value(date_format(analysis_date, '%y%m%d')) lifecycle 1 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}'
;

delete from dmd_data.dws_t82_reelshort_user_play_data5_detail_di_01 where analysis_date='${TX_DATE}';
insert into dmd_data.dws_t82_reelshort_user_play_data5_detail_di_01
        select
            null as id,uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform
            ,concat_ws('|',country_id,channel_id,version,cversion,res_version,language_id,platform) as join_col
            ,sub_event_name,scene_name,page_name,pre_page_name,if(sub_event_name in('play_start','play_end'),type,'') as action,cast(shelf_id as varchar) as shelf_id
            ,story_id,if(shelf_id in ('30001', '30003', '30004', '30002', '30005'),referrer_story_id,'') as referrer_story_id,chap_order_id,chap_id
            ,max(video_id) as video_id -- 视频id
            ,cast(count(*) as int) as report_cnt -- 上报次数
            ,cast(sum(if(sub_event_name='cover_click',1,0)) as int) as cover_click_cnt -- 封面点击次数
            ,cast(sum(if(sub_event_name='play_start',1,0)) as int) as play_start_cnt
            ,cast(sum(if(sub_event_name='play_end' and type='complete',1,0)) as int) as play_complete_cnt
            ,min(stime) as first_report_time -- 首次上报时间
            ,max(stime) as last_report_time -- 最后一次上报时间
            -- ,cast(max(case
            --         when sub_event_name='play_end' and t2.id2 is not null and event_duration<0 then 0
            --         when sub_event_name='play_end' and t2.id2 is not null and event_duration<10000 then event_duration
            --         when sub_event_name='play_end' and t2.id2 is not null and event_duration>10000 then chap_total_duration
            --         when sub_event_name='play_end' and t2.id2 is null and event_duration>chap_total_duration then chap_total_duration
            --         else event_duration end) as int) as play_duration -- 播放时长
            ,cast(max(case
                when sub_event_name='play_end' and event_duration<0 then 0
                when sub_event_name='play_end' and t2.id2 is not null and event_duration<500 then event_duration
                when sub_event_name='play_end' and t2.id2 is not null and event_duration>500 then chap_total_duration
                when sub_event_name='play_end' and t2.id2 is null and event_duration>chap_total_duration then chap_total_duration
                else event_duration end) as int) as play_duration
        from dwd_data.dwd_t02_reelshort_play_event_di t1
        left join (select id as id2 from chapters_log.reelshort_book_info where analysis_date='${TX_DATE}' and book_type=2) t2 on t1.story_id=t2.id2
        where etl_date='${TX_DATE}' and sub_event_name in('play_start','play_end','cover_click') and story_id not in ('','0')
        group by uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform
            ,sub_event_name,scene_name,page_name,pre_page_name,if(sub_event_name in('play_start','play_end'),type,''),shelf_id
            ,story_id,referrer_story_id,chap_id,chap_order_id;


delete from dmd_data.dws_t82_reelshort_user_play_data5_detail_di_02 where analysis_date='${TX_DATE}';
insert into dmd_data.dws_t82_reelshort_user_play_data5_detail_di_02
       select
            null as id,uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform
            ,concat_ws('|',country_id,channel_id,version,cversion,res_version,language_id,platform) as join_col
            ,sub_event_name
            ,scene_name
            ,page_name
            ,'' pre_page_name
            ,cast(action_type as varchar) as action
            ,'' shelf_id
            ,story_id,chap_id,chap_order_id
            ,cast(count(*) as int) as report_cnt -- 上报次数
            ,cast(count(*) as int) as click_cnt -- 点击次数
            ,min(stime) as first_report_time -- 首次上报时间
            ,max(stime) as last_report_time -- 最后一次上报时间
       from dwd_data.dwd_t02_reelshort_user_action_book_di
       where etl_date='${TX_DATE}' and sub_event_name in ('chap_list_click') -- 'chap_favorite','book_collect',
            -- and story_id not in ('','0')
       group by uuid,analysis_date,etl_date,country_id,channel_id,version,cversion,res_version,language_id,platform
            ,sub_event_name,scene_name,page_name,action_type
            ,story_id,chap_id,chap_order_id
            ;

delete from dmd_data.dws_t82_reelshort_user_play_data5_detail_di_03 where analysis_date='${TX_DATE}';
insert into dmd_data.dws_t82_reelshort_user_play_data5_detail_di_03
        select
            null as id,uuid,analysis_date,etl_date,country_id,channel_id,version,cversion,res_version,language_id,platform
            ,concat_ws('|',country_id,channel_id,version,cversion,res_version,language_id,platform) as join_col
            ,sub_event_name,scene_name,pre_page_name,shelf_id
            ,case
                when sub_event_name='watch_full_drama_popup' and action='show' then 'watch_full_drama_popup_show'
                when sub_event_name='watch_full_drama_popup' and action='click' then 'watch_full_drama_popup_click'
                when sub_event_name='story_rec_popup_click' and action='show' then 'story_rec_popup_show'
                when sub_event_name='story_rec_popup_click' and action='book_click' then 'story_rec_popup_book_click'
                when sub_event_name='story_rec_popup_click' and action='play_click' then 'story_rec_popup_play_click'
                when sub_event_name='story_rec_popup_click' and action='book_switch' then 'story_rec_popup_book_switch'
                when sub_event_name='story_rec_popup_click' and action='close' then 'story_rec_popup_close'
                when sub_event_name='story_rec_popup_click' and action='complete_play' then 'story_rec_popup_complete_play'
                else action end as action
            ,story_id,chap_id,chap_order_id
            ,cast(count(*) as int) as report_cnt -- 上报次数
            ,cast(sum(case when sub_event_name='watch_full_drama_popup' and action='show' then 1
                      when sub_event_name='story_rec_popup_click' and action='show' then 1
                      else 0 end
                      ) as int) as show_cnt
            ,cast(sum(case when sub_event_name='watch_full_drama_popup' and action='click' then 1
                      when sub_event_name='story_rec_popup_click' and action in('book_click','play_click','book_switch','close') then 1
                      else 0 end
                      ) as int) as click_cnt
            ,min(stime) as first_report_time -- 首次上报时间
            ,max(stime) as last_report_time -- 最后一次上报时间
            ,case
                when sub_event_name='story_rec_popup_click' and nvl(cast(json_extract(properties,'$.rec_scene') as varchar),'')='1' then 'story_rec_popup_click_1'
                when sub_event_name='story_rec_popup_click' and nvl(cast(json_extract(properties,'$.rec_scene') as varchar),'')='2' then 'story_rec_popup_click_2'
                when sub_event_name='story_rec_popup_click' and nvl(cast(json_extract(properties,'$.rec_scene') as varchar),'')='3' then 'story_rec_popup_click_3'
                when sub_event_name='watch_full_drama_popup' then page_name
                else '' end as page_name
        from dwd_data.dwd_t02_reelshort_custom_event_di
        where etl_date='${TX_DATE}' and sub_event_name in ('story_rec_popup_click','watch_full_drama_popup')
            and story_id not in ('','0')
        group by uuid,analysis_date,etl_date,country_id,channel_id,version,cversion,res_version,language_id,platform
            ,sub_event_name,scene_name,pre_page_name,action,shelf_id
            ,story_id,chap_id,chap_order_id
            ,case
                when sub_event_name='story_rec_popup_click' and nvl(cast(json_extract(properties,'$.rec_scene') as varchar),'')='1' then 'story_rec_popup_click_1'
                when sub_event_name='story_rec_popup_click' and nvl(cast(json_extract(properties,'$.rec_scene') as varchar),'')='2' then 'story_rec_popup_click_2'
                when sub_event_name='story_rec_popup_click' and nvl(cast(json_extract(properties,'$.rec_scene') as varchar),'')='3' then 'story_rec_popup_click_3'
                when sub_event_name='watch_full_drama_popup' then page_name
                else '' end
            ,case
                when sub_event_name='watch_full_drama_popup' and action='show' then 'watch_full_drama_popup_show'
                when sub_event_name='watch_full_drama_popup' and action='click' then 'watch_full_drama_popup_click'
                when sub_event_name='story_rec_popup_click' and action='show' then 'story_rec_popup_show'
                when sub_event_name='story_rec_popup_click' and action='book_click' then 'story_rec_popup_book_click'
                when sub_event_name='story_rec_popup_click' and action='play_click' then 'story_rec_popup_play_click'
                when sub_event_name='story_rec_popup_click' and action='book_switch' then 'story_rec_popup_book_switch'
                when sub_event_name='story_rec_popup_click' and action='close' then 'story_rec_popup_close'
                when sub_event_name='story_rec_popup_click' and action='complete_play' then 'story_rec_popup_complete_play'
                else action end;


delete from dmd_data.dws_t82_reelshort_user_play_data5_detail_di_04_tmp where analysis_date='${TX_DATE}';
insert into dmd_data.dws_t82_reelshort_user_play_data5_detail_di_04_tmp
        select
            null as id,uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform
            ,sub_event_name
            ,scene_name
            ,page_name
            ,pre_page_name
            ,action
            ,shelf_id  shelf_id2
            ,nvl(split_part(col,'#',2),'') as story_id,chap_id,chap_order_id
            ,cast(max(if(page_name='search_stat_result' and split_part(col,'#',2) is null,search_null_cnt,report_cnt)) as int) as report_cnt
            ,story_ids_size
--             ,cast(sum(if(action in('default_page_show','result_page_show'),1,0))/size(story_ids) as decimal(10,4)) as show_cnt
--             ,cast(sum(if(action in('search_bar_click','tag_click','story_click','search_click','search_click','history_bin_click','history_delete_click'),1,0))/size(story_ids) as decimal(10,4)) as click_cnt
            ,min(first_report_time) as first_report_time -- 首次上报时间
            ,max(last_report_time) as last_report_time -- 最后一次上报时间
            ,0 as play_duration
            ,0 as online_times
        from
        (
            select
                min(id) as id
                ,uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform
                ,sub_event_name
                ,scene_name
                ,case
                    when page_type='default' then 'default_search_stat'
                    when page_type='result' then 'result_search_stat'
                    else page_name end as page_name
                ,'' as pre_page_name
                ,action
--                 ,if(search_word_source='typing','typing_search_stat',search_word_source) as shelf_id
                ,search_word_source as shelf_id
                ,'' chap_id,0 chap_order_id
                ,cast(count(*) as int) as report_cnt -- 上报次数
                ,min(stime) as first_report_time -- 首次上报时间
                ,max(stime) as last_report_time -- 最后一次上报时间
                ,if(search_story_ids in('[]',''),search_rec_story_ids,search_story_ids)
                ,split(replace(replace(replace(if(search_story_ids in('[]',''),search_rec_story_ids,search_story_ids),'[',''),']',''),'"',''),',') as story_ids
                ,size(split(replace(replace(replace(if(search_story_ids in('[]',''),search_rec_story_ids,search_story_ids),'[',''),']',''),'"',''),',')) as story_ids_size
                ,cast(count(distinct if(search_story_ids in ('', '[]') and page_type='result' ,ctime)) as int) as search_null_cnt -- 搜索无结果页次数
            from dwd_data.dwd_t02_reelshort_search_stat_di
            where etl_date='${TX_DATE}'
            group by uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform
                ,sub_event_name,search_word_source,page_type,action,scene_name
                -- ,story_id,chap_id,chap_order_id
                ,if(search_story_ids in('[]',''),search_rec_story_ids,search_story_ids)
        )
        cross join unnest(story_ids) as tmp(col)
        group by uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform
            ,sub_event_name,scene_name,page_name,action,pre_page_name,shelf_id
            ,nvl(split_part(col,'#',2),''),chap_id,chap_order_id
            ;

delete from dmd_data.dws_t82_reelshort_user_play_data5_detail_di_04 where analysis_date='${TX_DATE}';
insert into dmd_data.dws_t82_reelshort_user_play_data5_detail_di_04
    select
        *
                ,if(shelf_id2='typing','typing_search_stat',shelf_id2) as shelf_id
            ,cast(if(action in('default_page_show','result_page_show'),report_cnt,0)/story_ids_size as decimal(10,4)) as show_cnt
            ,cast(if(action not in('default_page_show','result_page_show'),report_cnt,0)/story_ids_size as decimal(10,4)) as click_cnt
    from dmd_data.dws_t82_reelshort_user_play_data5_detail_di_04_tmp
            ;








create table if not exists dws_data.dws_t82_reelshort_user_play_data5_detail_di (
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
  show_cnt decimal(10,4) comment '展示次数',
  click_cnt decimal(10,4) comment '点击次数',
  cover_click_cnt int comment '封面点击次数',
  play_start_cnt int comment '播放开始次数',
  play_complete_cnt int comment '播放完成次数',
  first_report_time timestamp comment '首次上报时间',
  last_report_time timestamp comment '末次上报时间',
  video_id varchar default '' comment '视频id',
  play_duration int default '0' comment '剧集播放时长',
  online_times int default '0' comment '在线时长，s',
  -- story_ids  array<varchar>  comment 'search_rec_story_ids+search_story_ids',
  etl_date date comment '数据日期',
  primary key (id,uuid,etl_date,chap_id)
) distribute by hash(uuid,etl_date,chap_id) partition by value(date_format(etl_date, '%y%m')) lifecycle 120 index_all='y' storage_policy='mixed' hot_partition_count=6 engine='xuanwu' table_properties='{"format":"columnstore"}' comment='reelshort用户内容分发详情表统计表';

delete from dws_data.dws_t82_reelshort_user_play_data5_detail_di where etl_date='${TX_DATE}';
insert into dws_data.dws_t82_reelshort_user_play_data5_detail_di
select
    null as id
    ,t1.uuid
    ,t1.analysis_date
    ,is_pay
    ,is_login
    ,user_type
    ,vip_type
    ,t5.af_network_name
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
    ,cover_click_cnt
    ,play_start_cnt -- 播放开始次数
    ,play_complete_cnt -- 播放完成次数
    ,first_report_time
    ,last_report_time
    ,video_id
    ,play_duration
    ,online_times
    -- ,story_ids
    ,t1.analysis_date as etl_date
    ,coalesce(t2.recent_visit_source,'')                          as recent_visit_source                 -- 最近一次访问来源
    ,coalesce(t2.recent_visit_source_type,'')                     as recent_visit_source_type            -- 最近一次访问来源类型
    ,coalesce(t3.first_visit_landing_page,'')                     as first_visit_landing_page            -- 首次访问着陆页
    ,coalesce(t3.first_visit_landing_page_story_id,'')            as first_visit_landing_page_story_id   -- 首次访问着陆页书籍id
    ,coalesce(t3.first_visit_landing_page_chap_id,'')             as first_visit_landing_page_chap_id    -- 首次访问着陆页章节id
    ,nvl(t4.shelf_name,'')                                        as shelf_name                          -- 书架名称
from
(
    select
        coalesce(t1.uuid,t2.uuid,t3.uuid,t4.uuid ) as uuid
        ,coalesce(t1.analysis_date,t2.analysis_date,t3.analysis_date,t4.analysis_date ) as analysis_date
        ,coalesce(t1.country_id,t2.country_id,t3.country_id,t4.country_id ) as country_id
        ,coalesce(t1.channel_id,t2.channel_id,t3.channel_id,t4.channel_id ) as channel_id
        ,coalesce(t1.version,t2.version,t3.version,t4.version ) as version
        ,coalesce(t1.cversion,t2.cversion,t3.cversion,t4.cversion ) as cversion
        ,coalesce(t1.res_version,t2.res_version,t3.res_version,t4.res_version ) as res_version
        ,coalesce(t1.language_id,t2.language_id,t3.language_id,t4.language_id ) as language_id
        ,coalesce(t1.platform,t2.platform,t3.platform,t4.platform ) as platform
        ,coalesce(t1.sub_event_name,t2.sub_event_name,t3.sub_event_name,t4.sub_event_name ) as sub_event_name
        ,coalesce(t1.scene_name,t2.scene_name,t3.scene_name,t4.scene_name ) as scene_name
        ,coalesce(t1.page_name,t2.page_name,t3.page_name,t4.page_name ) as page_name
        ,coalesce(t1.pre_page_name,t2.pre_page_name,t3.pre_page_name,t4.pre_page_name ) as pre_page_name
        ,coalesce(t1.action,t2.action,t3.action,t4.action ) as action
        ,coalesce(t1.shelf_id,t2.shelf_id,t3.shelf_id,t4.shelf_id ) as shelf_id
        ,coalesce(t1.story_id,t2.story_id,t3.story_id,t4.story_id ) as story_id
        ,nvl(t1.referrer_story_id,'') as referrer_story_id
        ,coalesce(t1.chap_id,t2.chap_id,t3.chap_id,t4.chap_id ) as chap_id
        ,coalesce(t1.chap_order_id,t2.chap_order_id,t3.chap_order_id,t4.chap_order_id,0 ) as chap_order_id

        ,coalesce(t1.report_cnt,t2.report_cnt,t3.report_cnt,t4.report_cnt ) as report_cnt
        ,cast(coalesce(t3.show_cnt,t4.show_cnt,0.0) as decimal(10,4) )as show_cnt
        ,cast(coalesce(t2.click_cnt,t3.click_cnt,t4.click_cnt,0.0) as decimal(10,4)) as click_cnt
        ,nvl(cover_click_cnt,0) as cover_click_cnt
        ,coalesce(t1.first_report_time,t2.first_report_time,t3.first_report_time,t4.first_report_time ) as first_report_time
        ,coalesce(t1.last_report_time,t2.last_report_time,t3.last_report_time,t4.last_report_time ) as last_report_time
        ,t1.video_id
        ,nvl(t1.play_duration,0) as play_duration -- 播放时长
        ,nvl(t1.play_start_cnt,0) as play_start_cnt -- 播放开始次数
        ,nvl(t1.play_complete_cnt,0) as play_complete_cnt -- 播放完成次数
        ,0 as online_times -- 在线时长/s
    from dmd_data.dws_t82_reelshort_user_play_data5_detail_di_01 t1 -- 8521w
    full join dmd_data.dws_t82_reelshort_user_play_data5_detail_di_02 t2
        on t1.uuid=t2.uuid and t1.analysis_date=t2.analysis_date
        and t1.country_id=t2.country_id and t1.channel_id=t2.channel_id and t1.version=t2.version and t1.cversion=t2.cversion and t1.res_version=t2.res_version and t1.language_id=t2.language_id and t1.platform=t2.platform
        and t1.story_id=t2.story_id and t1.chap_id=t2.chap_id and t1.chap_order_id=t2.chap_order_id
        and t1.sub_event_name=t2.sub_event_name
        and t1.scene_name=t2.scene_name
        and t1.page_name=t2.page_name
        and t1.pre_page_name=t2.pre_page_name
    full join dmd_data.dws_t82_reelshort_user_play_data5_detail_di_03 t3 -- 379
        on t1.uuid=t3.uuid and t1.analysis_date=t3.analysis_date
        and t1.country_id=t3.country_id and t1.channel_id=t3.channel_id and t1.version=t3.version and t1.cversion=t3.cversion and t1.res_version=t3.res_version and t1.language_id=t3.language_id and t1.platform=t3.platform
        and t1.story_id=t3.story_id and t1.chap_id=t3.chap_id and t1.chap_order_id=t3.chap_order_id
        and t1.sub_event_name=t3.sub_event_name
        and t1.scene_name=t3.scene_name
        and t1.page_name=t3.page_name
        and t1.pre_page_name=t3.pre_page_name
        and t1.action=t3.action
        and t1.shelf_id=t3.shelf_id
    full join dmd_data.dws_t82_reelshort_user_play_data5_detail_di_04 t4 -- 471
        on t1.uuid=t4.uuid and t1.analysis_date=t4.analysis_date
        and t1.country_id=t4.country_id and t1.channel_id=t4.channel_id and t1.version=t4.version and t1.cversion=t4.cversion and t1.res_version=t4.res_version and t1.language_id=t4.language_id and t1.platform=t4.platform
        and t1.story_id=t4.story_id and t1.chap_id=t4.chap_id and t1.chap_order_id=t4.chap_order_id
        and t1.sub_event_name=t4.sub_event_name
        and t1.scene_name=t4.scene_name
        and t1.page_name=t4.page_name
        and t1.action=t4.action
        and t1.shelf_id=t4.shelf_id
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
        ,recent_visit_source -- 最近一次访问来源
        ,recent_visit_source_type -- 最近一次访问来源类型
   from dwd_data.dwd_t01_reelshort_user_detail_info_di
   where etl_date='${TX_DATE}'
) t2
on t1.uuid=t2.uuid
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
)as t4
on t4.shelf_id=t1.shelf_id and t4.rn=1
left join (
           select distinct
                  uuid
                  ,af_network_name
           from dws_data.dim_t87_reelshort_ads_user_attr_df
           where etl_date='${TX_DATE}'
) as t5
on t5.uuid=t1.uuid
where t1.analysis_date='${TX_DATE}'
;
