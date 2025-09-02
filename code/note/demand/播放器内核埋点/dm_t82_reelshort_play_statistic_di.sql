-- drop Table if  exists dm_reelshort_data.dm_t82_reelshort_play_statistic_di;
-- Create Table if not exists dm_reelshort_data.dm_t82_reelshort_play_statistic_di(
-- id                         bigint auto_increment comment '自增id'
-- ,analysis_date             date                  comment '分析日期'
-- ,country_id                varchar               comment '国家id'
-- ,os_type                   varchar               comment '操作系统类型 （android/ios/macos/windows/...）'
-- ,app_channel_id            varchar               comment '包体外发渠道ID (GoolgePlay=gp  Apple AppStore =appl'
-- ,app_version               varchar               comment 'APP 的应用版本（包体内置版本号 versionname  ）'
-- ,player_ver                varchar               comment '播放器版本'
-- ,resolution                varchar               comment '分辨率'
-- ,is_encrypt                int                   comment '是否加密'
-- ,scene                     varchar               comment '播放场景:scene_type，2为foryou，3普通剧，4互动剧'
-- 
-- 
-- 
-- ,app_lang                  varchar               comment '游戏语言'
-- ,uuid                      varchar               comment '应用中用户ID'
-- -- ,story_id                  varchar               comment '作品id'
-- -- ,chap_id                   varchar               comment '选集id'
-- -- ,chap_order_id             varchar               comment '选集序号id，比如1、2、3'
-- -- ,chap_session_id           varchar               comment '单次播放id，用于区分同一个视频多次播放'
-- -- ,video_id                  varchar               comment '短剧视频id'
-- -- ,clip_id                   varchar               comment '片段id，当scene_type=4互动剧详情时上报'
-- ,is_long_video             int                   comment '短/长视频:0,否；1，是；video_duration<=300为短视频，默认短视频'
-- -- ,action                    varchar               comment '播放行为'
-- ,play_duration             bigint                comment 'play_end时统计视频实际播放时间(单位：秒)'
-- ,player_start_cnt          bigint                comment '播放开始次数'
-- ,player_success_cnt        bigint                comment '播放成功次数'
-- ,player_end_cnt            bigint                comment '播放结束次数'
-- ,open_1s_cnt               bigint                comment '打开时间小于等于1s次数'
-- ,open_2s_cnt               bigint                comment '打开时间小于等于2s次数'
-- ,open_player_success_cnt   bigint                comment '打开播放成功次数'
-- ,buffer_cnt                bigint                comment '卡顿次数'
-- ,buffer_time               bigint                comment '卡顿时长'
-- ,hw_decode_cnt             bigint                comment '硬解次数'
-- ,three_seconds_quit_cnt    bigint                comment '3s退出次数'
-- ,threeseconds_quit_player_end_cnt bigint         comment '3s退出播放结束次数'
-- ,open_3s_cnt               bigint                comment '打开时间大于3s的次数'
-- ,prepare_time              bigint                comment '起播前耗时(由业务层传入):单位ms'
-- ,prepare_player_success_cnt bigint               comment '耗时播放成功次数'
-- ,player_fail_cnt           bigint                comment '播放失败次数'
-- ,buffer_cnts               bigint                comment '缓冲次数'
-- ,player_end_buffer_cnts    bigint                comment '播放结束缓冲次数'
-- ,play_uv                   bigint                comment '播放UV'
-- ,open_time_total           bigint                comment '首帧耗时总量'
-- ,player_end_uv             bigint                comment '播放结束UV'
-- ,create_time               datetime              comment '创建时间'
-- ,PRIMARY KEY (id,analysis_date)
-- )DISTRIBUTE BY HASH(analysis_date,id) PARTITION BY VALUE(DATE_FORMAT(analysis_date,'%Y%m')) LIFECYCLE 120 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='reelshort播放器内核埋点原子指标汇总统计';

-- alter table dm_reelshort_data.dm_t82_reelshort_play_statistic_di
-- add column resolution varchar comment '分辨率'
-- ,add column player_fail_cnt bigint comment '播放失败次数'
-- ,add column mid_open_time bigint comment '首帧耗时中位数'
-- ;
-- chapters_log.dts_project_v_functional_ab_test_user

delete from dm_reelshort_data.dm_t82_reelshort_play_statistic_di where analysis_date='${TX_DATE}';
insert into dm_reelshort_data.dm_t82_reelshort_play_statistic_di
select
    null                                                      as id                  -- 自增id
    ,analysis_date                                                                  -- 分析日期
    ,country_id                                                                     -- 国家id
    ,os_type                                                                        -- 操作系统类型 （android/ios/macos/windows/...）
    ,app_channel_id                                                                 -- 包体外发渠道ID (GoolgePlay=gp  Apple AppStore =apple)
    ,app_version                                                                    -- APP 的应用版本（包体内置版本号 versionname  ）
    ,player_ver                                                                     -- 播放器版本
    -- ,width                                                                          -- 分辨率
    ,case when least(width,height)<=540 then 540
          when least(width,height)>540 and least(width,height)<= 720 then 720
          when least(width,height)>720 and least(width,height)<= 1080 then 1080
          when least(width,height)>1080 then '4k'
     end                                                                            as resolution                 -- 分辨率
    ,is_encrypt                                                                     -- 是否加密
    ,scene                                                                          -- 播放场景:scene_type，2为foryou，3普通剧，4互动剧
    ,app_lang                                                                       -- 游戏语言
    ,uuid                                                                           -- 应用中用户ID
    -- ,story_id                                                                       -- 作品id
    -- ,chap_id                                                                        -- 选集id
    -- ,chap_order_id                                                                  -- 选集序号id，比如1、2、3
    -- ,chap_session_id                                                                -- 单次播放id，用于区分同一个视频多次播放
    -- ,video_id                                                                       -- 短剧视频id
    -- ,clip_id                                                                        -- 片段id，当scene_type=4互动剧详情时上报
    ,if(video_duration<=300,0,1)                              as is_long_video      -- 短/长视频:0,否；1，是；video_duration<=300为短视频，默认短视频
    -- ,action                                                   as action             -- 播放行为
    ,sum(if(action='player_end' and play_duration>0 and play_duration<=10800,play_duration)) as play_duration      -- play_end时统计视频实际播放时间(单位：秒)
    ,count(if(action='player_start',t1.id))                   as player_start_cnt   -- 播放开始次数
    ,count(if(action='player_success',t1.id))                 as player_success_cnt -- 播放成功次数
    ,count(if(action='player_end',t1.id))                     as player_end_cnt     -- 播放结束次数
    ,count(if(open_time<=1000 and action='player_success' and open_time<=1800000,t1.id))                         as open_1s_cnt        -- 打开时间小于等于1s次数
    ,count(if(open_time<=2000 and action='player_success' and open_time<=1800000,t1.id))                         as open_2s_cnt        -- 打开时间小于等于2s次数

    ,count(if(action='player_success' and open_time<=1800000,t1.id)) as open_player_success_cnt -- 打开播放成功次数

    ,count(if(buffer_count>0,t1.id))                          as buffer_cnt         -- 卡顿次数
    ,sum(if(action='player_end' and buffer_time<=1800000, buffer_time)) as buffer_time -- 卡顿时长
    ,count(if(action='player_success' and hw_decode=1,t1.id))                             as hw_decode_cnt      -- 硬解次数
    ,count(if(end_type=1 and load_time>3000 and action='player_end' and load_time<=1800000,t1.id))          as three_seconds_quit_cnt        -- 3s退出次数

    ,count(if(action='player_end' and load_time<=1800000,t1.id))                     as threeseconds_quit_player_end_cnt     -- 3s退出播放结束次数

    ,count(if(open_time>3000 and open_time<=1800000,t1.id))                          as open_3s_cnt        -- 打开时间大于3s的次数
    ,sum(if(action='player_success' and prepare_time<=1800000,prepare_time))         as prepare_time       -- 起播前耗时(由业务层传入)，单位：ms

    ,count(if(action='player_success' and prepare_time<=1800000,t1.id)) as prepare_player_success_cnt -- 耗时播放成功次数

    ,count(if(action='player_fail',t1.id))                    as player_fail_cnt     -- 播放失败次数
    ,sum(buffer_count)                                        as buffer_cnts         -- 缓冲次数

    ,sum(if(action='player_end' and buffer_time<=1800000,buffer_count)) as player_end_buffer_cnts         -- 播放结束缓冲次数

    ,count(distinct if(action='player_end',t1.uuid))        as play_uv                  -- 播放UV

    ,sum(if(action='player_success' and open_time<1800000,open_time)) as open_time_total -- 首帧耗时总量

    ,count(distinct if(action='player_end' and play_duration>0 and play_duration<=10800,t1.uuid)) as player_end_uv  -- 播放结束UV
    -- ,group_concat(distinct if(action='player_success' and open_time<1800000,open_time) order by if(action='player_success' and open_time<1800000,open_time) separator ',') as open_time_list -- 首帧耗时列表
    -- ,SUBSTRING_INDEX(SUBSTRING_INDEX(group_concat(distinct if(action='player_success' and open_time<=1800000,open_time) order by if(action='player_success' and open_time<=1800000,open_time) separator ','),',',ceil((length(group_concat(distinct if(action='player_success' and open_time<=1800000,open_time) order by if(action='player_success' and open_time<=1800000,open_time) asc separator ','))
     -- -length(replace(group_concat(distinct if(action='player_success' and open_time<=1800000,open_time) order by if(action='player_success' and open_time<=1800000,open_time) asc separator ','),',',''))+1)/2)),',',-1) as mid_open_time -- 首帧耗时中位数

    ,now()                                                    as create_time
from  dwd_data.dwd_t82_reelshort_play_detail_di as t1
where analysis_date='${TX_DATE}'
group by
        analysis_date
        ,country_id
        ,os_type
        ,app_channel_id
        ,app_version
        ,player_ver
        ,resolution
        ,is_encrypt
        ,scene
        ,app_lang
        ,uuid
        -- ,story_id
        -- ,chap_id
        -- ,chap_order_id
        -- ,chap_session_id
        -- ,video_id
        -- ,clip_id
        ,is_long_video
        -- ,action
;