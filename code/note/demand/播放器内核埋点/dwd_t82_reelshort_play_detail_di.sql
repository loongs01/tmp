-- drop Table if  exists dwd_data.dwd_t82_reelshort_play_detail_di;
-- Create Table if not exists dwd_data.dwd_t82_reelshort_play_detail_di(
-- id                         bigint                comment '自增id'
-- ,analysis_date             date                  comment '分析日期'
-- ,country_id                varchar               comment '国家id'
-- ,os_type                   varchar               comment '操作系统类型 （android/ios/macos/windows/...）'
-- ,app_channel_id            varchar               comment '包体外发渠道ID (GoolgePlay=gp  Apple AppStore =appl'
-- ,app_version               varchar               comment 'APP 的应用版本（包体内置版本号 versionname  ）'
-- ,player_ver                varchar               comment '播放器版本'
-- ,width                     varchar               comment '视频宽度，player_start填起播分辨率'
-- ,is_encrypt                int                   comment '是否加密:0不加密，1加密，默认0'
-- ,scene                     varchar               comment '播放场景:scene_type，2为foryou，3普通剧，4互动剧'
-- ,app_lang                  varchar               comment '游戏语言'
-- ,uuid                      varchar               comment '应用中用户ID'
-- ,story_id                  varchar               comment '作品id'
-- ,chap_id                   varchar               comment '选集id'
-- ,chap_order_id             varchar               comment '选集序号id，比如1、2、3'
-- ,chap_session_id           varchar               comment '单次播放id，用于区分同一个视频多次播放'
-- ,video_id                  varchar               comment '短剧视频id'
-- ,clip_id                   varchar               comment '片段id，当scene_type=4互动剧详情时上报'
-- ,action                    varchar               comment '播放行为'
-- ,play_duration             bigint                comment 'play_end时统计视频实际播放时间(单位：秒)'
-- ,open_time                 bigint                comment '起播总耗时(内核)，=fail_time - start_time，单位ms'
-- ,buffer_count              int                   comment '缓冲次数'
-- ,buffer_time               bigint                comment '缓冲时长，单位ms'
-- ,hw_decode                 int                   comment '0: 软解，1: 硬解，-1: 未知'
-- ,end_type                  int                   comment '正常退出end_type=0，用户退出end_type=1，起播失败end_type=2，播放出错end_type=3,备注：end_type=1 未起播loading时用户主动退出'
-- ,load_time                 bigint                comment '加载时间，单位ms，end_type=1时上报'
-- ,prepare_time              bigint                comment '起播前耗时(由业务层传入)，单位ms'
-- ,video_duration            bigint                comment '视频时长，单位s'
-- ,create_time               datetime              comment '创建时间'
-- ,PRIMARY KEY (id,analysis_date,uuid)
-- )DISTRIBUTE BY HASH(analysis_date,uuid) PARTITION BY VALUE(DATE_FORMAT(analysis_date,'%Y%m')) LIFECYCLE 7 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='reelshort播放器内核埋点指标统计';

-- alter table dwd_data.dwd_t82_reelshort_play_detail_di
-- add column height bigint comment '视频高度，player_start填起播分辨率'
-- ;
-- ALTER TABLE dwd_data.dwd_t82_reelshort_play_detail_di
-- CHANGE COLUMN widths width_tmp varchar;

-- ALTER TABLE dwd_data.dwd_t82_reelshort_play_detail_di DROP COLUMN width_tmp;


-- ALTER TABLE dwd_data.dwd_t82_reelshort_play_detail_di
-- add COLUMN width bigint  COMMENT '视频宽度，player_start填起播分辨率'
-- after width_tmp ;










-- delete from dwd_data.dwd_t82_reelshort_play_detail_di where analysis_date='${TX_DATE}' or analysis_date<=DATE_SUB('${TX_DATE}',INTERVAL 7 DAY);
delete from dwd_data.dwd_t82_reelshort_play_detail_di where analysis_date='${TX_DATE}';
insert into dwd_data.dwd_t82_reelshort_play_detail_di
select
    id                                                                                     -- 自增id
    ,analysis_date                                                                         -- 分析日期
    ,country_id                                                                            -- 国家id
    ,os_type                                                                               -- 操作系统类型 （android/ios/macos/windows/...）
    ,app_channel_id                                                                        -- 包体外发渠道ID (GoolgePlay=gp  Apple AppStore =apple)
    ,app_version                                                                           -- APP 的应用版本（包体内置版本号 versionname  ）
    ,json_extract(properties,'$.player_ver')                            as player_ver      -- 播放器版本
    -- ,cast(json_extract(properties,'$.width') as bigint)                 as width           -- 视频宽度，player_start填起播分辨率
    ,coalesce(cast(json_extract(properties,'$.encrypt') as int),0)      as is_encrypt      -- 是否加密:0不加密，1加密，默认0
    ,json_extract(properties,'$.scene')                                 as scene           -- 播放场景
    ,app_lang                                                           as app_lang        -- 游戏语言
    ,app_user_id                                                        as uuid            -- 应用中用户ID
    ,json_extract(properties,'$._story_id')                             as story_id        -- 作品id
    ,json_extract(properties,'$._chap_id')                              as chap_id         -- 选集id
    ,json_extract(properties,'$._chap_order_id')                        as chap_order_id   -- 选集序号id，比如1、2、3
    ,json_extract(properties,'$._chap_session_id')                      as chap_session_id -- 单次播放id，用于区分同一个视频多次播放
    ,json_extract(properties,'$.video_id')                              as video_id        -- 短剧视频id
    ,json_extract(properties,'$.clip_id')                               as clip_id         -- 片段id，当scene_type=4互动剧详情时上报
    ,cast(JSON_EXTRACT(properties, '$._action') as varchar)             as action          -- 播放行为
    ,cast(JSON_EXTRACT(properties, '$.play_duration') as bigint)        as play_duration   -- play_end时统计视频实际播放时间(单位：秒)
    ,cast(JSON_EXTRACT(properties, '$.open_time') as bigint)            as open_time       -- 起播总耗时(内核)，=fail_time - start_time，单位ms
    ,cast(JSON_EXTRACT(properties, '$.buffer_count') as int)            as buffer_count    -- 缓冲次数
    ,cast(JSON_EXTRACT(properties, '$.buffer_time') as bigint)          as buffer_time     -- 缓冲时长，单位ms
    ,cast(JSON_EXTRACT(properties, '$.hw_decode') as int)               as hw_decode       -- 0: 软解，1: 硬解，-1: 未知
    ,cast(JSON_EXTRACT(properties, '$.end_type') as int)                as end_type        -- 正常退出end_type=0，用户退出end_type=1，起播失败end_type=2，播放出错end_type=3,备注：end_type=1 未起播loading时用户主动退出
    ,cast(JSON_EXTRACT(properties, '$.load_time') as bigint)            as load_time       -- 加载时间，单位ms，end_type=1时上报
    ,cast(JSON_EXTRACT(properties, '$.prepare_time') as bigint)         as prepare_time    -- 起播前耗时(由业务层传入)，单位ms
    ,cast(JSON_EXTRACT(properties, '$.video_duration') as bigint)       as video_duration  -- 视频时长，单位s
    ,now()
    ,cast(json_extract(properties,'$.height') as bigint)                as height         -- 视频高度，player_start填起播分辨率
    ,cast(json_extract(properties,'$.width') as bigint)                 as width           -- 视频宽度，player_start填起播分辨率
from  chapters_log.reelshort_perf_log as t1
where analysis_date='${TX_DATE}'
-- and cast(JSON_EXTRACT(properties, '$.video_duration') as bigint) is not null
;