drop Table if  exists dm_reelshort_data.dm_t82_reelshort_play_aggregate_di;
Create Table if not exists dm_reelshort_data.dm_t82_reelshort_play_aggregate_di(
id                   bigint auto_increment comment '自增id'
,analysis_date       date                  comment '分析日期'
,uuid                varchar               comment '用户UUID,即IDFA或者ADID'
,analysis_date       date                  comment '分析日期'
,etl_date            date                  comment '数据日期'
,is_pay              int                   comment '1:支付用户，0:未付费用户'
,is_login            int                   comment '是否登录：1-是'
,user_type           int                   comment '1-新用户'
,country_id          varchar               comment '国家id'
,channel_id          varchar               comment '渠道id'
,version             varchar               comment '底包版本'
,cversion            varchar               comment '代码版本'
,res_version         varchar               comment '资源版本'
,language_id         varchar               comment '语言id'
,platform            varchar               comment '平台id'
,os_version          varchar               comment '系统版本'
,device_id           varchar               comment '应用用于标识设备的唯一ID:Android=androidid,ios=idfv'
,ad_id               varchar               comment '广告ID :Android=google adid IOS=idfa'
,story_id            varchar               comment '书籍id'
,chap_id             varchar               comment '章节id'
,chap_order_id       int                   comment '章节序列id'
,is_free             int                   comment '1:免费,0:非免费(包括已解锁和未解锁)'
,chap_total_duration int                   comment '当前章节总时长(单位：秒)'
,t_book_id           varchar               comment '书籍id（不同语言也一致）'
,video_id            varchar               comment '视频id'
,play_duration       int                   comment 'play_end时统计视频实际播放时间(单位：秒)'
,create_time         datetime              comment '创建时间'
,PRIMARY KEY (id,analysis_date,uuid)
)DISTRIBUTE BY HASH(analysis_date,uuid) PARTITION BY VALUE(DATE_FORMAT(analysis_date,'%Y%m')) LIFECYCLE 120 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='reelshort埋点播放指标统计';


delete from dm_reelshort_data.dm_t82_reelshort_play_aggregate_di where analysis_date='${TX_DATE}';
insert into dm_reelshort_data.dm_t82_reelshort_play_aggregate_di
select
      min(id) as id
      ,uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id
      ,story_id
      ,chap_id
      ,chap_order_id
      ,max(is_free) as is_free
      ,max(chap_total_duration) as chap_total_duration
      ,max(video_id) as video_id
      ,cast(sum(play_duration) as int) as play_duration
      ,max(max_process) as max_process
      ,sum(cover_click_cnt) as cover_click_cnt
      ,sum(play_enter_cnt) as play_enter_cnt
      ,sum(play_start_cnt) as play_start_cnt
      ,sum(play_end_cnt) as play_end_cnt
      ,sum(play_start_begin_cnt) as play_start_begin_cnt
      ,sum(play_start_pause_off_cnt) as play_start_pause_off_cnt
      ,sum(play_end_complete_cnt) as play_end_complete_cnt
      ,sum(play_end_pause_on_cnt) as play_end_pause_on_cnt
      ,min(min_play_ctime) as min_play_ctime
      ,sum(player_play_start_cnt) as player_play_start_cnt
      ,sum(player_play_end_cnt) as player_play_end_cnt
      ,etl_date
      ,t_book_id
      ,cast(sum(40001_play_duration) as int) as 40001_play_duration
from
    (
    select
        min(t1.id) as id
        ,uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id
        ,story_id
        ,chap_id
        ,chap_order_id
        ,max(is_free) as is_free
        ,max(chap_total_duration) as chap_total_duration
        ,video_id
        ,cast(max(case
                when sub_event_name='play_end' and event_duration<0 then 0
                when sub_event_name='play_end' and t2.id is not null and event_duration<500 then event_duration
                when sub_event_name='play_end' and t2.id is not null and event_duration>500 then chap_total_duration
                when sub_event_name='play_end' and t2.id is null and event_duration>chap_total_duration then chap_total_duration
                else event_duration end) as int) as play_duration
        ,max(process) as max_process
        ,count(if(sub_event_name='cover_click',t1.id)) as cover_click_cnt
        ,count(if(sub_event_name='play_enter',t1.id)) as play_enter_cnt
        ,count(if(sub_event_name='play_start',t1.id)) as play_start_cnt
        ,count(if(sub_event_name='play_end',t1.id)) as play_end_cnt
        ,count(if(sub_event_name='play_start' and type='begin',t1.id)) as play_start_begin_cnt
        ,count(if(sub_event_name='play_start' and type='pause_off',t1.id)) as play_start_pause_off_cnt
        ,count(if(sub_event_name='play_end' and type='complete',t1.id)) as play_end_complete_cnt
        ,count(if(sub_event_name='play_end' and type='pause_on',t1.id)) as play_end_pause_on_cnt
        ,min(if(sub_event_name='play_start',ctime)) as min_play_ctime
        ,count(if(sub_event_name='play_start' and page_name='player',t1.id)) as player_play_start_cnt
        ,count(if(sub_event_name='play_end' and page_name='player',t1.id)) as player_play_end_cnt
        ,etl_date
        ,t_book_id
        ,cast(max(case
                when sub_event_name='play_end' and shelf_id=40001 and event_duration<0 then 0
                when sub_event_name='play_end' and shelf_id=40001 and t2.id is not null and event_duration<500 then event_duration
                when sub_event_name='play_end' and shelf_id=40001 and t2.id is not null and event_duration>500 then chap_total_duration
                when sub_event_name='play_end' and shelf_id=40001 and t2.id is null and event_duration>chap_total_duration then chap_total_duration
                when sub_event_name='play_end' and shelf_id=40001 and t2.id is null and event_duration<chap_total_duration then event_duration
                else 0 end) as int) as 40001_play_duration
        
        ,max(cast( JSON_EXTRACT(properties, '$.play_duration') as bigint)) as play_duration
        ,count(if(cast(JSON_EXTRACT(properties, '$._action') as varchar)='player_start',t1.id)) as player_start_cnt     -- 播放开始次数
        ,count(if(cast(JSON_EXTRACT(properties, '$._action') as varchar)='player_success',t1.id)) as player_success_cnt -- 播放成功次数
        ,count(if(cast(JSON_EXTRACT(properties, '$._action') as varchar)='player_end',t1.id)) as player_end_cnt         -- 播放结束次数
        ,count(if(cast(JSON_EXTRACT(properties, '$.open_time') as bigint)<=500,t1.id)) as open_500ms_cnt                    -- 打开时间大于等于500ms次数
        ,count(if(cast(JSON_EXTRACT(properties, '$.open_time') as bigint)<=1000,t1.id)) as open_1s_cnt                      -- 打开时间小于1s次数
        ,count(if(cast(JSON_EXTRACT(properties, '$.buffer_count') as bigint)>0,t1.id)) as buffer_cnt                        -- 卡顿次数
        ,sum(cast(JSON_EXTRACT(properties, '$.buffer_time') as bigint)) as buffer_times                                    -- 卡顿时长
        ,count(if(cast(JSON_EXTRACT(properties, '$.hw_decode') as bigint)=1,t1.id)) as hw_decode_cnt                        -- 硬解次数
        ,count(if(cast(JSON_EXTRACT(properties, '$.end_type') as bigint)=1 and cast(JSON_EXTRACT(properties, '$.load_time') as bigint)<=3000 ,t1.id)) as 3s_quit_cnt  -- 3s退出次数
        ,count(if(cast(JSON_EXTRACT(properties, '$.open_time') as bigint)>3000,t1.id)) as open_3s_cnt                       -- 打开时间大于3s的次数
    from  dwd_data.dwd_t02_reelshort_play_event_di t1
    left join (select id from chapters_log.reelshort_book_info where analysis_date='${TX_DATE}' and book_type=2) t2 on t1.story_id=t2.id
    where etl_date='${TX_DATE}'  and story_id not in ('','0')
    group by uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id
             ,story_id
             ,chap_id
             ,chap_order_id
             ,t_book_id
             ,video_id -- 互动剧同一章节，会有多个video_id
    )
group by uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id
         ,story_id
         ,chap_id
         ,chap_order_id
         ,t_book_id
;