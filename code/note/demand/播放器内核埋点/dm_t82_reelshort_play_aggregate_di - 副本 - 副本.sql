drop Table if  exists dm_reelshort_data.dm_t82_reelshort_play_aggregate_di;
Create Table if not exists dm_reelshort_data.dm_t82_reelshort_play_aggregate_di(
id                         bigint auto_increment comment '自增id'
,analysis_date             date                  comment '分析日期'
,country_id                varchar               comment '国家id'
,os_type                   varchar               comment '操作系统类型 （android/ios/macos/windows/...）'
,app_channel_id            varchar               comment '包体外发渠道ID (GoolgePlay=gp  Apple AppStore =appl'
,app_version               varchar               comment 'APP 的应用版本（包体内置版本号 versionname  ）'
,player_ver                varchar               comment 'sdk版本'
,width                     varchar               comment '分辨率'
,is_encrypt                int                   comment '是否加密'
,scene                     varchar               comment '播放场景:scene_type，2为foryou，3普通剧，4互动剧'



,app_lang                  varchar               comment '游戏语言'
,app_user_id               varchar               comment '应用中用户ID'
,story_id                  varchar               comment '作品id'
,chap_id                   varchar               comment '选集id'
,chap_order_id             varchar               comment '选集序号id，比如1、2、3'
,chap_session_id           varchar               comment '单次播放id，用于区分同一个视频多次播放'
,video_id                  varchar               comment '短剧视频id'
,clip_id                   varchar               comment '片段id，当scene_type=4互动剧详情时上报'
,is_long_video             int                   comment '短/长视频:0,否；1，是；video_duration<=300为短视频，默认短视频'
,action                    varchar               comment '播放行为'
,play_duration             bigint                comment 'play_end时统计视频实际播放时间(单位：秒)'
-- ,player_start_cnt          bigint                comment '播放开始次数'
-- ,player_success_cnt        bigint                comment '播放成功次数'
-- ,player_end_cnt            bigint                comment '播放结束次数'
,open_500ms_cnt            bigint                comment '打开时间小于等于1s次数'
,open_1s_cnt               bigint                comment '打开时间小于等于2s次数'
,buffer_cnt                bigint                comment '卡顿次数'
,buffer_time               bigint                comment '卡顿时长'
,hw_decode_cnt             bigint                comment '硬解次数'
,3s_quit_cnt               bigint                comment '3s退出次数'
,open_3s_cnt               bigint                comment '打开时间大于3s的次数'
,prepare_time              bigint                comment '起播前耗时(由业务层传入):单位s，原单位ms'
,create_time               datetime              comment '创建时间'
,PRIMARY KEY (id,analysis_date,uuid)
)DISTRIBUTE BY HASH(analysis_date,uuid) PARTITION BY VALUE(DATE_FORMAT(analysis_date,'%Y%m')) LIFECYCLE 120 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='reelshort埋点播放指标统计';


delete from dm_reelshort_data.dm_t82_reelshort_play_aggregate_di where analysis_date='${TX_DATE}';
/*+ cte_execution_mode=shared */
insert into dm_reelshort_data.dm_t82_reelshort_play_aggregate_di
with reelshort_perf_detail as(
     select
         id
         ,analysis_date                                                                         -- 分析日期
         ,country_id                                                                            -- 国家id
         ,os_type                                                                               -- 操作系统类型 （android/ios/macos/windows/...）
         ,app_channel_id                                                                        -- 包体外发渠道ID (GoolgePlay=gp  Apple AppStore =apple)
         ,app_version                                                                           -- APP 的应用版本（包体内置版本号 versionname  ）
         ,json_extract(properties,'$.player_ver')                            as player_ver      -- sdk版本
         ,json_extract(properties,'$.width')                                 as width           -- 分辨率
         ,json_extract(properties,'$.encrypt')                               as is_encrypt      -- 是否加密
         ,json_extract(properties,'$.scene')                                 as scene           -- 播放场景

         ,app_lang                                                                              -- 游戏语言
         ,app_user_id                                                                           -- 应用中用户ID
         ,json_extract(properties,'$._story_id')                             as story_id        -- 作品id
         ,json_extract(properties,'$._chap_id')                              as chap_id         -- 选集id
         ,json_extract(properties,'$._chap_order_id')                        as chap_order_id   -- 选集序号id，比如1、2、3
         ,json_extract(properties,'$._chap_session_id')                      as chap_session_id -- 单次播放id，用于区分同一个视频多次播放
         ,json_extract(properties,'$.video_id')                              as video_id        -- 短剧视频id
         ,json_extract(properties,'$.clip_id')                               as clip_id         -- 片段id，当scene_type=4互动剧详情时上报
         ,cast(JSON_EXTRACT(properties, '$.play_duration') as bigint)        as play_duration   -- play_end时统计视频实际播放时间(单位：秒)
         ,cast(JSON_EXTRACT(properties, '$._action') as varchar)             as action
         ,cast(JSON_EXTRACT(properties, '$.open_time') as bigint)            as open_time       -- 打开时间
         ,cast(JSON_EXTRACT(properties, '$.buffer_count') as bigint)         as buffer_count    -- 卡顿次数
         ,cast(JSON_EXTRACT(properties, '$.buffer_time') as bigint)          as buffer_time     -- 卡顿时长
         ,cast(JSON_EXTRACT(properties, '$.hw_decode') as bigint)            as hw_decode       -- 硬解次数
         ,cast(JSON_EXTRACT(properties, '$.end_type') as bigint)             as end_type        -- end_type=1指未起播退出
         ,cast(JSON_EXTRACT(properties, '$.load_time') as bigint)            as load_time       -- 3s退出次数
         ,cast(JSON_EXTRACT(properties, '$.prepare_time') as bigint)/1000    as prepare_time    -- 起播前耗时(由业务层传入)，原单位ms
         ,cast(JSON_EXTRACT(properties, '$.video_duration') as bigint)       as video_duration
     from  chapters_log.reelshort_perf_log as t1
     where analysis_date='${TX_DATE}'
     -- and cast(JSON_EXTRACT(properties, '$.video_duration') as bigint) is not null
)
select
    null                                                      as id                  -- 自增id
    ,analysis_date                                                                  -- 分析日期
    ,country_id                                                                     -- 国家id
    ,os_type                                                                        -- 操作系统类型 （android/ios/macos/windows/...）
    ,app_channel_id                                                                 -- 包体外发渠道ID (GoolgePlay=gp  Apple AppStore =apple)
    ,app_version                                                                    -- APP 的应用版本（包体内置版本号 versionname  ）
    ,player_ver
    ,width
    ,is_encrypt
    ,scene
    ,app_lang                                                                       -- 游戏语言
    ,app_user_id                                                                    -- 应用中用户ID
    ,story_id                                                                       -- 作品id
    ,chap_id                                                                        -- 选集id
    ,chap_order_id                                                                  -- 选集序号id，比如1、2、3
    ,chap_session_id                                                                -- 单次播放id，用于区分同一个视频多次播放
    ,video_id                                                                       -- 短剧视频id
    ,clip_id                                                                        -- 片段id，当scene_type=4互动剧详情时上报
    ,if(video_duration<=300,0,1)                              as is_long_video      -- 短/长视频:0,否；1，是；video_duration<=300为短视频，默认短视频
    ,action
    ,sum(play_duration)                                       as play_duration      -- play_end时统计视频实际播放时间(单位：秒)
    -- ,count(if(action='player_start',t1.id))                   as player_start_cnt   -- 播放开始次数
    -- ,count(if(action='player_success',t1.id))                 as player_success_cnt -- 播放成功次数
    -- ,count(if(action='player_end',t1.id))                     as player_end_cnt     -- 播放结束次数
    ,count(if(open_time<=1000,t1.id))                         as open_1s_cnt        -- 打开时间小于等于1s次数
    ,count(if(open_time<=2000,t1.id))                         as open_2s_cnt        -- 打开时间小于等于2s次数
    ,count(if(buffer_count>0,t1.id))                          as buffer_cnt         -- 卡顿次数
    ,sum(buffer_time)                                         as buffer_time        -- 卡顿时长
    ,count(if(hw_decode=1,t1.id))                             as hw_decode_cnt      -- 硬解次数
    ,count(if(end_type=1 and load_time>3000 ,t1.id))          as 3s_quit_cnt        -- 3s退出次数
    ,count(if(open_time>3000,t1.id))                          as open_3s_cnt        -- 打开时间大于3s的次数
    ,sum(prepare_time)                                        as prepare_time       -- 起播前耗时(由业务层传入)，原单位ms
    ,now()
from  reelshort_perf_detail as t1
group by
        analysis_date
        ,country_id
        ,os_type
        ,app_channel_id
        ,app_version
        ,player_ver
        ,width
        ,is_encrypt
        ,scene
        ,app_lang
        ,app_user_id
        ,story_id
        ,chap_id
        ,chap_order_id
        ,chap_session_id
        ,video_id
        ,clip_id
        ,is_long_video
        ,action
;