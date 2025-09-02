-- Create Table if not exists dwd_data.dim_t99_reelshort_user_lt_info (
--   `id` bigint AUTO_INCREMENT COMMENT '自增id',
--   `uuid` varchar COMMENT '用户id',
--   `country_id` varchar COMMENT '最新的国家id',
--   `channel_id` varchar COMMENT '最新的渠道id',
--   `version` varchar COMMENT '最新的APP的应用版本(包体内置版本号versionname)',
--   `language_id` varchar COMMENT '最新的语言id',
--   `platform` varchar COMMENT '最新的平台id (1-安卓,2-IOS，3-windows,4-macOs)',
--   `cversion` varchar COMMENT '最新的应用游戏版本号',
--   `res_version` varchar COMMENT '最新的应用资源版本号',
--   `os_version` varchar COMMENT '最新的操作系统版本号',
--   `device_id` varchar COMMENT '最新的应用用于标识设备的唯一ID',
--   `ad_id` varchar COMMENT '最新的广告ID:Android=google adid IOS=idfa',
--   `androidid` varchar COMMENT '最新的ANDROIDID',
--   `idfv` varchar COMMENT '最新的IOS-idfv',
--   `user_ip` varchar COMMENT '最新的用户IP地址',
--   `netstatus` varchar COMMENT '最新的事件触发时的网络类型',
--   `analysis_date` date COMMENT '数据日期',
--   `is_login` int COMMENT '是否登陆',
--   `user_type` int COMMENT '用户类型',
--   `is_pay` int COMMENT '是否支付',
--   `is_pay_lt` int COMMENT '生命周期是否支付',
--   `last_login_time` timestamp COMMENT '末次登录时间 ',
--   `last_pay_time` timestamp COMMENT '末次支付时间  ',
--   `last_active_time` timestamp COMMENT '末次活跃时间',
--   `last_pay_amount` int COMMENT '末次支付金额',
--   `last_bind_type` varchar COMMENT '最后一次账号绑定类型，visitor=游客,facebook=facebook账号,gp=googleplay账号,apple=apple账号',
--   `last_bind_time` timestamp COMMENT '最后一次账号绑定时间',
--   `dlink_channel` varchar COMMENT 'deeplink归属渠道（fb、af、aj、system）',
--   `content_type` varchar COMMENT 'novel/audio/comic/visual/chat/',
--   `register_time` timestamp COMMENT '注册时间(用户级别)',
--   `reg_country_id` varchar COMMENT '注册时国家id',
--   `reg_channel_id` varchar COMMENT '注册时渠道id',
--   `reg_version` varchar COMMENT '注册时APP的应用版本(包体内置版本号versionname)',
--   `reg_language_id` varchar COMMENT '注册时语言id',
--   `reg_platform` varchar COMMENT '注册时平台id (1-安卓,2-IOS，3-windows,4-macOs)',
--   `reg_device_id` varchar COMMENT '注册时应用用于标识设备的唯一ID',
--   `af_network_name` varchar COMMENT 'appsflyer归因渠道（媒体来源）',
--   `af_campaign` varchar COMMENT 'af广告投放计划',
--   `af_adset` varchar COMMENT 'af广告组',
--   `af_ad` varchar COMMENT 'af广告',
--   `device_brand` varchar COMMENT '设备品牌',
--   `device_model` varchar COMMENT '设备类型',
--   `first_login_time` timestamp COMMENT '首次登录时间',
--   `first_pay_time` timestamp COMMENT '首次支付时间',
--   `first_active_time` timestamp COMMENT '首次活跃时间 ',
--   `first_pay_amount` int COMMENT '首次支付金额',
--   `first_bind_type` varchar COMMENT '首次账号绑定类型，visitor=游客,facebook=facebook账号,gp=googleplay账号,apple=apple账号',
--   `first_bind_time` timestamp COMMENT '首次账号绑定时间',
--   `install_cnt` int COMMENT '安装次数',
--   `login_cnt` int COMMENT '登陆次数',
--   `pay_amount` int COMMENT '订单金额',
--   `order_cnt` int COMMENT '订单个数',
--   `invaild_pay_amount` int COMMENT '作弊订单金额',
--   `invaild_order_cnt` int COMMENT '作弊订单个数',
--   `pay_show_cnt` int COMMENT '付费面板弹出次数',
--   `pay_start_cnt` int COMMENT '开始支付次数',
--   `pay_end_cnt` int COMMENT '支付结束次数',
--   `pay_failed_cnt` int COMMENT '支付失败次数',
--   `pay_cancel_cnt` int COMMENT '支付取消次数',
--   `pay_delivery_cnt` int COMMENT '支付下发次数',
--   `online_time` int COMMENT '总在线总时长',
--   `main_play_online_time` int COMMENT 'main_play场景在线时长',
--   `main_online_time` int COMMENT 'main场景在线时长',
--   `chap_play_online_time` int COMMENT 'chap_play场景在线时长',
--   `vistor_online_time` int COMMENT '游客在线时长',
--   `bind_gp_online_time` int COMMENT 'google登录在线时长',
--   `bind_fb_online_time` int COMMENT 'facebook登录在线时长',
--   `manual_unlock_01_cnt` int COMMENT '手动耗01解锁次数',
--   `auto_unlock_01_cnt` int COMMENT '自动耗01解锁次数',
--   `manual_unlock_01_exp` int COMMENT '手动解锁01消耗量',
--   `auto_unlock_01_exp` int COMMENT '自动解锁01消耗量',
--   `pay_01_cnt` int COMMENT '付费获得01的次数',
--   `pay_01_get` int COMMENT '付费获得01量',
--   `etl_date` date COMMENT '清洗日期',
--   `first_srv_sub_pay_amount` int DEFAULT '0' COMMENT '首次订阅金额-业务服',
--   `last_srv_sub_pay_amount` int DEFAULT '0' COMMENT '末次订阅金额-业务服',
--   `srv_sub_order_cnt` int DEFAULT '0' COMMENT '订阅订单个数-业务服',
--   `srv_sub_pay_amount` int DEFAULT '0' COMMENT '订阅订单金额-业务服',
--   `cr_days` int COMMENT '上次活跃距今天数',
--   `fcm_push` int DEFAULT '-1' COMMENT 'fcm push权限：1-开启/2-关闭/-1 -未知',
--   ctimezone_offset varchar default '0' comment '设备系统的时区偏移,相对于标准时区',
--   PRIMARY KEY (`etl_date`,`id`,`uuid`)
-- ) DISTRIBUTE BY HASH(`uuid`) PARTITION BY VALUE(`etl_date`) LIFECYCLE 500 INDEX_ALL='Y' STORAGE_POLICY='MIXED' HOT_PARTITION_COUNT=12 ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='v项目dwd层:全量用户码表'
-- ;



delete from  dwd_data.dim_t99_reelshort_user_lt_info where etl_date='${TX_DATE}';
/* direct_batch_load=true*/
insert into dwd_data.dim_t99_reelshort_user_lt_info
select
    null as id
    ,nvl(new.uuid,old.uuid) as uuid -- 用户id
    ,nvl(new.country_id,old.country_id) as country_id -- 国家id
    ,nvl(new.channel_id,old.channel_id) as channel_id -- 渠道id
    ,nvl(new.version,old.version) as version -- APP的应用版本(包体内置版本号versionname)
    ,nvl(new.language_id,old.language_id) as language_id -- 语言id
    ,nvl(new.platform,old.platform) as platform -- 操作系统类型 (android/ios/windows/mac os)
    ,nvl(new.cversion,old.cversion) as cversion -- 应用游戏版本号
    ,nvl(new.res_version,old.res_version) as res_version -- 应用资源版本号
    ,nvl(new.os_version,old.os_version) as os_version -- 操作系统版本号
    ,nvl(new.device_id,old.device_id) as device_id -- 应用用于标识设备的唯一ID
    ,nvl(new.ad_id,old.ad_id) as ad_id -- 广告ID:Android=google adid IOS=idfa
    ,nvl(new.androidid,old.androidid) as androidid -- ANDROIDID
    ,nvl(new.idfv,old.idfv) as idfv -- IOS-idfv
    ,nvl(new.user_ip,old.user_ip) as user_ip -- 用户IP地址
    ,nvl(new.netstatus ,old.netstatus ) as netstatus  -- 事件触发时的网络类型
    ,nvl(new.analysis_date,old.analysis_date) as analysis_date -- 数据日期
    ,nvl(new.is_login,0) as is_login
    ,nvl(new.user_type,0) as user_type
    ,nvl(new.is_pay,0) as is_pay
    ,coalesce(if(new.is_pay=1 , 1 , old.is_pay_lt),0) as is_pay_lt
    ,nvl(new.last_login_time,old.last_login_time) as last_login_time -- 末次登录时间
    ,nvl(new.last_pay_time,old.last_pay_time) as last_pay_time -- 末次支付时间
    ,nvl(new.last_active_time,old.last_active_time) as last_active_time -- 末次活跃时间
    ,nvl(new.last_pay_amount,old.last_pay_amount) as last_pay_amount -- 末次支付金额
    ,nvl(new.last_bind_type,old.last_bind_type) as last_bind_type -- 最后一次账号绑定类型，visitor=游客,facebook=facebook账号,gp=googleplay账号,apple=apple账号
    ,nvl(new.last_bind_time,old.last_bind_time) as last_bind_time -- 最后一次账号绑定时间

    ,nvl(new.dlink_channel,old.dlink_channel) as dlink_channel -- deeplink归属渠道（fb、af、aj、system）
    ,nvl(new.content_type,old.content_type) as content_type -- novel/audio/comic/visual/chat/
    ,nvl(old.register_time,new.register_time) as register_time -- 注册时间(用户级别)
    ,nvl(old.reg_country_id,new.reg_country_id) as reg_country_id
    ,nvl(old.reg_channel_id,new.reg_channel_id) as reg_channel_id
    ,nvl(old.reg_version,new.reg_version) as reg_version
    ,nvl(old.reg_language_id,new.reg_language_id) as reg_language_id
    ,nvl(old.reg_platform,new.reg_platform) as reg_platform
    ,nvl(old.reg_device_id,new.reg_device_id) as reg_device_id
    -- ,nvl(new.af_install_date,old.af_install_date) as af_install_date -- appsflyer安装或重安装日期
    ,nvl(new.af_network_name,old.af_network_name) as af_network_name -- appsflyer归因渠道（媒体来源）
    ,nvl(new.af_campaign,old.af_campaign) as af_campaign
    ,nvl(new.af_adset,old.af_adset) as af_adset
    ,nvl(new.af_ad,old.af_ad) as af_ad
    ,nvl(new.device_brand,old.device_brand) as device_brand
    ,nvl(new.device_model,old.device_model) as device_model
    ,nvl(old.first_login_time,new.first_login_time) as first_login_time -- 首次登录时间
    ,nvl(old.first_pay_time,new.first_pay_time) as first_pay_time -- 首次支付时间
    ,nvl(old.first_active_time,new.first_active_time) as first_active_time -- 首次活跃时间
    ,nvl(old.first_pay_amount,new.first_pay_amount) as first_pay_amount -- 首次支付金额
    ,nvl(old.first_bind_type,new.first_bind_type) as first_bind_type -- 首次账号绑定类型，visitor=游客,facebook=facebook账号,gp=googleplay账号,apple=apple账号
    ,nvl(old.first_bind_time,new.first_bind_time) as first_bind_time -- 首次账号绑定时间
    ,cast(nvl(new.install_cnt,0) + nvl(old.install_cnt,0) as int) as install_cnt -- 安装次数
    ,cast(nvl(new.login_cnt,0) + nvl(old.login_cnt,0) as int) as login_cnt -- 登陆次数
    ,cast(nvl(new.pay_amount,0) + nvl(old.pay_amount,0) as int) as pay_amount -- 订单金额
    ,cast(nvl(new.order_cnt,0) + nvl(old.order_cnt,0) as int) as order_cnt -- 订单个数
    ,cast(nvl(new.invaild_pay_amount,0) + nvl(old.invaild_pay_amount,0) as int) as invaild_pay_amount -- 作弊订单金额
    ,cast(nvl(new.invaild_order_cnt,0) + nvl(old.invaild_order_cnt,0) as int) as invaild_order_cnt -- 作弊订单个数
    ,cast(nvl(new.pay_show_cnt,0) + nvl(old.pay_show_cnt,0) as int) as pay_show_cnt -- 付费面板弹出次数
    ,cast(nvl(new.pay_start_cnt,0) + nvl(old.pay_start_cnt,0) as int) as pay_start_cnt -- 开始支付次数
    ,cast(nvl(new.pay_end_cnt,0) + nvl(old.pay_end_cnt,0) as int) as pay_end_cnt -- 支付结束次数
    ,cast(nvl(new.pay_failed_cnt,0) + nvl(old.pay_failed_cnt,0) as int) as pay_failed_cnt -- 支付失败次数
    ,cast(nvl(new.pay_cancel_cnt,0) + nvl(old.pay_cancel_cnt,0) as int) as pay_cancel_cnt -- 支付取消次数
    ,cast(nvl(new.pay_delivery_cnt,0) + nvl(old.pay_delivery_cnt,0) as int) as pay_delivery_cnt -- 支付下发次数
    ,cast(nvl(new.online_time,0) + nvl(old.online_time,0) as int) as online_time -- 总在线总时长
    ,cast(nvl(new.main_play_online_time,0) + nvl(old.main_play_online_time,0) as int) as main_play_online_time -- main_play场景在线时长
    ,cast(nvl(new.main_online_time,0) + nvl(old.main_online_time,0) as int) as main_online_time -- main场景在线时长
    ,cast(nvl(new.chap_play_online_time,0) + nvl(old.chap_play_online_time,0) as int) as chap_play_online_time -- chap_play场景在线时长
    ,cast(nvl(new.vistor_online_time,0) + nvl(old.vistor_online_time,0) as int) as vistor_online_time -- 游客在线时长
    ,cast(nvl(new.bind_gp_online_time,0) + nvl(old.bind_gp_online_time,0) as int) as bind_gp_online_time -- google登录在线时长
    ,cast(nvl(new.bind_fb_online_time,0) + nvl(old.bind_fb_online_time,0) as int) as bind_fb_online_time -- facebook登录在线时长
    ,cast(nvl(new.manual_unlock_01_cnt,0) + nvl(old.manual_unlock_01_cnt,0) as int) as manual_unlock_01_cnt -- 手动耗01解锁次数
    ,cast(nvl(new.auto_unlock_01_cnt,0) + nvl(old.auto_unlock_01_cnt,0) as int) as auto_unlock_01_cnt -- 自动耗01解锁次数
    ,cast(nvl(new.manual_unlock_01_exp,0) + nvl(old.manual_unlock_01_exp,0) as int) as manual_unlock_01_exp -- 手动解锁01消耗量
    ,cast(nvl(new.auto_unlock_01_exp,0) + nvl(old.auto_unlock_01_exp,0) as int) as auto_unlock_01_exp -- 自动解锁01消耗量
    ,cast(nvl(new.pay_01_cnt,0) + nvl(old.pay_01_cnt,0) as int) as pay_01_cnt -- 付费获得01的次数
    ,cast(nvl(new.pay_01_get,0) + nvl(old.pay_01_get,0) as int) as pay_01_get -- 付费获得01量
    ,'${TX_DATE}' as etl_date
    ,nvl(old.first_srv_sub_pay_amount,new.first_srv_sub_pay_amount) as first_srv_sub_pay_amount -- 首次支付金额
    ,nvl(new.last_srv_sub_pay_amount,old.last_srv_sub_pay_amount) as last_srv_sub_pay_amount -- 末次支付金额
    ,cast(nvl(new.srv_sub_order_cnt,0) + nvl(old.srv_sub_order_cnt,0) as int) as srv_sub_order_cnt -- 订阅订单个数
    ,cast(nvl(new.srv_sub_pay_amount,0) + nvl(old.srv_sub_pay_amount,0) as int) as srv_sub_pay_amount -- 订阅订单金额
    ,cast(coalesce(datediff('${TX_DATE}',date(old.last_active_time)),0) as int) as cr_days
    ,coalesce(new.fcm_push,old.fcm_push,-1) as fcm_push
    ,case when nvl(new.srv_sub_pay_amount,0) + nvl(old.srv_sub_pay_amount,0)>0
          then 2 -- 订阅
          when (new.is_pay=1 or old.is_pay_lt=1)  and  nvl(new.srv_sub_pay_amount,0) + nvl(old.srv_sub_pay_amount,0)=0
          then 1 -- 付费未订阅
     else 0 end as pay_type_lt
     ,nvl(new.ctimezone_offset,old.ctimezone_offset) as  ctimezone_offset
     ,if(nvl(new.srv_sub_pay_amount,0) + nvl(old.srv_sub_pay_amount,0)>0,1,0) as srv_vip_type_lt

    ,if(new.vip_type_lt>0 or old.vip_type_lt>0,1,0) as vip_type_lt -- 买单是否订阅
        ,cast(nvl(new.cli_non_sub_order_cnt,0) + nvl(old.cli_non_sub_order_cnt,0) as int) as cli_non_sub_order_cnt
        ,cast(nvl(new.cli_non_sub_pay_amount,0) + nvl(old.cli_non_sub_pay_amount,0) as int) as cli_non_sub_pay_amount
        ,cast(nvl(new.srv_order_cnt,0) + nvl(old.srv_order_cnt,0) as int) as srv_order_cnt
        ,cast(nvl(new.srv_pay_amount,0) + nvl(old.srv_pay_amount,0) as int) as srv_pay_amount
        ,cast(nvl(new.srv_nosub_order_cnt,0) + nvl(old.srv_nosub_order_cnt,0) as int) as srv_nosub_order_cnt
        ,cast(nvl(new.srv_nosub_pay_amount,0) + nvl(old.srv_nosub_pay_amount,0) as int) as srv_nosub_pay_amount
        ,cast(nvl(new.srv_new_sub_order_cnt,0) + nvl(old.srv_new_sub_order_cnt,0) as int) as srv_new_sub_order_cnt
        ,cast(nvl(new.srv_new_sub_pay_amount,0) + nvl(old.srv_new_sub_pay_amount,0) as int) as srv_new_sub_pay_amount
        ,cast(nvl(new.srv_renew_sub_order_cnt,0) + nvl(old.srv_renew_sub_order_cnt,0) as int) as srv_renew_sub_order_cnt
        ,cast(nvl(new.srv_renew_sub_pay_amount,0) + nvl(old.srv_renew_sub_pay_amount,0) as int) as srv_renew_sub_pay_amount
    ,nvl(new.srv_last_pay_time,old.srv_last_pay_time) as srv_last_pay_time
    ,nvl(new.device_version,old.device_version) as device_version
    ,nvl(new.af_campaign_type,old.af_campaign_type) as af_campaign_type
    ,nvl(new.af_w2a_campaign_mode,old.af_w2a_campaign_mode) as af_w2a_campaign_mode
        ,nvl(new.max_continue_checkin_days,old.max_continue_checkin_days) as max_continue_checkin_days -- 最大连续签到天数
        ,nvl(new.current_continue_checkin_days,0) as current_continue_checkin_days -- 当前连续签到天数
        ,nvl(new.total_checkin_days,old.total_checkin_days) as total_checkin_days -- 累计签到天数
        -- add by lcz
        ,nvl(old.first_visit_landing_page,new.first_visit_landing_page)                   as first_visit_landing_page          -- 首次访问着陆页
        ,nvl(old.first_visit_landing_page_story_id,new.first_visit_landing_page_story_id) as first_visit_landing_page_story_id -- 首次访问着陆页书籍id
        ,nvl(old.first_visit_landing_page_chap_id,new.first_visit_landing_page_chap_id)   as first_visit_landing_page_chap_id  -- 首次访问着陆页章节id

from
(
    select
        t1.uuid
        ,t1.country_id -- 国家id
        ,t1.channel_id -- 渠道id
        ,t1.version -- APP的应用版本(包体内置版本号versionname)
        ,t1.language_id -- 语言id
        ,t1.platform -- 操作系统类型 (android/ios/windows/mac os)
        ,t1.analysis_date -- 数据日期
        ,t1.cversion
        ,t1.res_version
        ,t1.os_version
        ,t1.device_id
        ,t1.ad_id
        ,t1.androidid
        ,t1.idfv
        ,t1.user_ip
        ,t1.netstatus
        ,t1.is_login -- 是否登陆
        ,t1.user_type -- 用户类型
        ,t1.is_pay -- 是否支付
        ,t1.last_login_time -- 末次登录时间
        ,t1.last_pay_time -- 末次支付时间
        ,t1.last_active_time -- 末次活跃时间
        ,cast(t1.last_pay_amount as int) as last_pay_amount -- 末次支付金额
        ,t7.last_bind_type -- 最后一次账号绑定类型，visitor=游客,facebook=facebook账号,gp=googleplay账号,apple=apple账号
        ,t7.last_bind_time -- 最后一次账号绑定时间
        ,t2.dlink_channel -- deeplink归属渠道（fb、af、aj、system）
        ,t2.content_type -- novel/audio/comic/visual/chat/
        ,t1.register_time -- 注册时间(用户级别)
        ,t8.country_id as reg_country_id
        ,t8.channel_id as reg_channel_id
        ,t8.version as reg_version
        ,t8.language_id as reg_language_id
        ,t8.platform as reg_platform
        ,t8.device_id as reg_device_id
        -- ,t1.af_install_date -- appsflyer安装或重安装日期
        ,t1.af_network_name -- appsflyer归因渠道（媒体来源）
        ,t1.af_channel
        ,t1.af_campaign
        ,t1.af_adset
        ,t1.af_ad
        ,t1.device_brand
        ,t1.device_model
        ,t1.first_login_time -- 首次登录时间
        ,t1.first_pay_time -- 首次支付时间
        ,t1.first_active_time -- 首次活跃时间
        ,cast(t1.first_pay_amount as int) as first_pay_amount -- 首次支付金额
        ,t7.first_bind_type -- 首次账号绑定类型，visitor=游客,facebook=facebook账号,gp=googleplay账号,apple=apple账号
        ,t7.first_bind_time -- 首次账号绑定时间
        ,if(t1.register_time is not null,t1.install_cnt,t1.install_cnt-1) as install_cnt
        ,t1.login_cnt -- 登陆次数
        ,cast(t3.pay_amount as int) as pay_amount -- 订单金额
        ,t3.order_cnt -- 订单个数
        ,cast(t3.invaild_pay_amount as int) as invaild_pay_amount -- 作弊订单金额
        ,t3.invaild_order_cnt -- 作弊订单个数
        ,t4.pay_show_cnt -- 付费面板弹出次数
        ,t4.pay_start_cnt -- 开始支付次数
        ,t4.pay_end_cnt -- 支付结束次数
        ,t4.pay_failed_cnt -- 支付失败次数
        ,t4.pay_cancel_cnt -- 支付取消次数
        ,t4.pay_delivery_cnt -- 支付下发次数
        ,t5.online_time -- 总在线总时长
        ,t5.main_play_online_time -- main_play场景在线时长
        ,t5.main_online_time -- main场景在线时长
        ,t5.chap_play_online_time -- chap_play场景在线时长
        ,t5.vistor_online_time -- 游客在线时长
        ,t5.bind_gp_online_time -- google登录在线时长
        ,t5.bind_fb_online_time -- facebook登录在线时长
        ,t6.manual_unlock_01_cnt -- 手动耗01解锁次数
        ,t6.auto_unlock_01_cnt -- 自动耗01解锁次数
        ,t6.manual_unlock_01_exp -- 手动解锁01消耗量
        ,t6.auto_unlock_01_exp -- 自动解锁01消耗量
        ,t6.pay_01_cnt -- 付费获得01的次数
        ,t6.pay_01_get -- 付费获得01量
        ,t1.first_srv_sub_pay_amount
        ,t1.last_srv_sub_pay_amount
        ,t9.srv_sub_order_cnt
        ,t9.srv_sub_pay_amount
        ,t10.fcm_push
        ,t1.ctimezone_offset

        ,vip_type_lt -- 买单是否订阅
        ,cli_non_sub_order_cnt -- 埋点非订阅订单数
        ,cli_non_sub_pay_amount -- 埋点非订阅订单金额
        ,srv_order_cnt -- 业务服订单数
        ,srv_pay_amount -- 业务服付费金额
        ,srv_nosub_order_cnt -- 业务服非订阅订单数
        ,srv_nosub_pay_amount -- 业务服非订阅金额
        ,srv_new_sub_order_cnt -- 业务服普通订阅订单数
        ,srv_new_sub_pay_amount -- 业务服普通订阅金额
        ,srv_renew_sub_order_cnt -- 业务服订阅回调续订订单数
        ,srv_renew_sub_pay_amount -- 业务服订阅回调续订金额
        ,srv_last_pay_time -- 业务服最后付费时间
        ,device_version -- 设备版本
        ,af_campaign_type -- appsflyer投放类型
        ,af_w2a_campaign_mode -- 埋点w2a类型用户 与af_campaign结合使用
        ,max_continue_checkin_days -- 最大连续签到天数
        ,current_continue_checkin_days -- 当前连续签到天数
        ,total_checkin_days -- 累计签到天数
        ,t1.first_visit_landing_page          as first_visit_landing_page          -- 首次访问着陆页
        ,t1.first_visit_landing_page_story_id as first_visit_landing_page_story_id -- 首次访问着陆页书籍id
        ,t1.first_visit_landing_page_chap_id  as first_visit_landing_page_chap_id  -- 首次访问着陆页章节id
    from
    (
        select
            uuid
            ,country_id
            ,channel_id
            ,version
            ,language_id
            ,platform
            ,analysis_date
            ,cversion
            ,res_version
            ,os_version
            ,device_id
            ,ad_id
            ,androidid
            ,idfv
            ,user_ip
            ,netstatus
            ,etl_date
            ,is_login
            ,user_type
            ,is_pay
            ,first_login_time
            ,last_login_time
            ,first_pay_time
            ,last_pay_time
            ,first_active_time
            ,last_active_time
            ,login_cnt
            ,install_cnt
            ,activity_days
            ,first_sku_price as first_pay_amount
            ,last_sku_price  as last_pay_amount
            ,af_network_name
            ,af_channel
            ,af_campaign
            ,af_adset
            ,af_ad
            ,device_brand
            ,device_model
            ,device_version
            ,register_time
            -- ,af_install_date
            ,first_srv_sub_sku_price as first_srv_sub_pay_amount
            ,last_srv_sub_sku_price  as last_srv_sub_pay_amount
        ,case
          when ctimezone_offset in('-12','-13','-14') then '-11'
          when ctimezone_offset in('13','14','15') then '12'
        when ctimezone_offset is null then '0'
          else ctimezone_offset end as ctimezone_offset
        ,af_campaign_type
        ,first_visit_landing_page                          -- 首次访问着陆页
        ,first_visit_landing_page_story_id                 -- 首次访问着陆页书籍id
        ,first_visit_landing_page_chap_id                  -- 首次访问着陆页章节id
         from dwd_data.dwd_t01_reelshort_user_detail_info_di
         where etl_date ='${TX_DATE}'
        )t1
    left join
    (
        select
            uuid
            ,dlink_channel -- deeplink归属渠道（fb、af、aj、system）
            ,content_type -- novel/audio/comic/visual/chat/
        from
        (
            select
                uuid,dlink_channel,content_type
                ,row_number() over(partition by uuid order by ctime desc) rank
            from dwd_data.dwd_t01_reelshort_dlink_event_di
            where etl_date<='${TX_DATE}'
        )
        where rank=1
    ) t2
    on t1.uuid=t2.uuid
    left join
    (
        select
            uuid
            ,cast(sum(if(order_status=1,sku_price,0.0)) as decimal(10,2)) as pay_amount -- 订单金额
            ,cast(count(distinct if(order_status=1,order_id)) as int) as order_cnt -- 订单个数
            ,cast(sum(if(order_status=0,sku_price,0.0)) as decimal(10,2)) as invaild_pay_amount -- 作弊订单金额
            ,cast(count(distinct if(order_status=0,order_id)) as int) as invaild_order_cnt -- 作弊订单个数
     ,cast(count(distinct if(channel_sku not like '%sub%',order_id)) as int) as cli_non_sub_order_cnt
     ,cast(sum(if(channel_sku not like '%sub%',sku_price,0)) as int) as cli_non_sub_pay_amount
        from dwd_data.dwd_t05_reelshort_order_detail_di
        where etl_date='${TX_DATE}' and order_id_rank=1 and order_currency_type='USD'
        group by uuid
    ) t3
    on t1.uuid=t3.uuid
    left join
    (
        select
            uuid
            ,cast(sum(if(pay_status='pay_show',1,0)) as int) as pay_show_cnt -- 付费面板弹出次数
            ,cast(sum(if(pay_status='pay_start',1,0)) as int) as pay_start_cnt -- 开始支付次数
            ,cast(sum(if(pay_status='pay_end',1,0)) as int) as pay_end_cnt -- 支付结束次数
            ,cast(sum(if(pay_status='pay_failed',1,0)) as int) as pay_failed_cnt -- 支付失败次数
            ,cast(sum(if(pay_status='pay_cancel',1,0)) as int) as pay_cancel_cnt -- 支付取消次数
            ,cast(sum(if(pay_status='pay_delivery',1,0)) as int) as pay_delivery_cnt -- 支付下发次数
        from dwd_data.dwd_t05_reelshort_transaction_detail_di
        where etl_date='${TX_DATE}' and order_currency_type='USD'
        group by uuid
    ) t4
    on t1.uuid=t4.uuid
    left join
    (
        select
            uuid
            ,cast(sum(online_time) as bigint) as online_time -- 总在线总时长
            ,cast(sum(if(scene_name='main_play_scene',online_time,0)) as bigint) as main_play_online_time
            ,cast(sum(if(scene_name='main_scene',online_time,0)) as bigint) as main_online_time
            ,cast(sum(if(scene_name='chap_play_scene',online_time,0)) as bigint) as chap_play_online_time
            ,cast(sum(if(app_account_bindtype='vistor',online_time,0)) as bigint) as vistor_online_time -- 游客在线时长
            ,cast(sum(if(app_account_bindtype='gp',online_time,0)) as bigint) as bind_gp_online_time -- google登录在线时长
            ,cast(sum(if(app_account_bindtype='fb',online_time,0)) as bigint) as bind_fb_online_time -- facebook登录在线时长
            ,etl_date
        from dwd_data.dwd_t01_reelshort_user_online_time_di
        where etl_date='${TX_DATE}' and uuid<>'0'
        group by uuid
    ) t5
    on t1.uuid=t5.uuid
    left join
    (
        select
            uuid
            ,cast(sum(if(change_reason='manual_unlock_exp' and vc_id='vc_01',1,0)) as int) as manual_unlock_01_cnt -- 手动耗01解锁次数
            ,cast(sum(if(change_reason='auto_unlock_exp' and vc_id='vc_01',1,0)) as int) as auto_unlock_01_cnt -- 自动耗01解锁次数
            ,cast(sum(if(change_reason='manual_unlock_exp' and vc_id='vc_01',change_amount,0)) as int) as manual_unlock_01_exp -- 手动解锁01消耗量
            ,cast(sum(if(change_reason='auto_unlock_exp' and vc_id='vc_01',change_amount,0)) as int) as auto_unlock_01_exp -- 自动解锁01消耗量
            ,cast(sum(if(change_reason='pay_get' and vc_id='vc_01',1,0)) as int) as pay_01_cnt -- 付费获得01的次数
            ,cast(sum(if(change_reason='pay_get' and vc_id='vc_01',change_amount,0)) as int) as pay_01_get -- 付费获得01量
        from dwd_data.dwd_t06_reelshort_user_currency_get_or_expend_di
        where etl_date='${TX_DATE}'and uuid not in ('227101298')
        group by uuid
    ) t6
    on t1.uuid=t6.uuid
    left join
    (
        select
            uuid
            ,max(if(rankdesc=1,app_account_bindtype)) as last_bind_type -- 最后一次账号绑定类型，visitor=游客,facebook=facebook账号,gp=googleplay账号,apple=apple账号
            ,max(if(rankdesc=1,stime)) as last_bind_time -- 最后一次账号绑定时间
            ,max(if(rank=1,app_account_bindtype)) as first_bind_type -- 首次账号绑定类型，visitor=游客,facebook=facebook账号,gp=googleplay账号,apple=apple账号
            ,max(if(rank=1,stime)) as first_bind_time -- 首次账号绑定时间
        from
        (
            select
                uuid,app_account_bindtype,stime
                ,row_number() over(partition by uuid order by stime ) as rank
                ,row_number() over(partition by uuid order by stime desc ) as rankdesc
            from dwd_data.dwd_t01_reelshort_bindaccount_di
            where etl_date='${TX_DATE}' and action='complete'
        )
        where rankdesc=1 or rank=1
        group by uuid
    ) t7
    on t1.uuid=t7.uuid
    left join (
        select uuid,country_id,channel_id,version,language_id,platform,device_id
        from (select uuid,country_id,channel_id,version,language_id,platform,device_id
                ,row_number() over(partition by uuid order by stime ) as rank
             from dwd_data.dwd_t01_reelshort_user_login_info_di
             where etl_date ='${TX_DATE}'  and stime=register_time and sub_event_name='signup'
         )
        where rank=1
        )t8
on t1.uuid=t8.uuid
left join
    (
     --    select
     --         uuid
  --          ,cast(count(distinct order_id) as int) as srv_sub_order_cnt
  --          ,cast(sum(sku_price) as int) as srv_sub_pay_amount
        -- from dwd_data.dwd_t05_reelshort_srv_order_detail_di
     --    where etl_date='${TX_DATE}' and channel_sku like '%sub%'
     --    group by uuid

    select
          uuid
          ,cast(sum(if(sku rlike 'sub',sku_price,0)) as int) as srv_sub_pay_amount
          ,cast(count(distinct if(sku rlike 'sub',order_id)) as int) as srv_sub_order_cnt

         ,cast(count(distinct order_id) as int) as srv_order_cnt -- 业务服订单数
         ,cast(sum(sku_price) as int) as srv_pay_amount -- 业务服付费金额
         ,cast(count(distinct if(sku not like '%sub%',order_id)) as int) as srv_nosub_order_cnt -- 业务服非订阅订单数
         ,cast(sum(if(sku not like '%sub%',sku_price,0.0)) as int) as srv_nosub_pay_amount -- 业务服非订阅金额
         ,cast(count(distinct if(order_type=1,order_id)) as int) as srv_new_sub_order_cnt -- 业务服普通订阅订单数
         ,cast(sum(if(order_type=1,sku_price,0.0)) as int) as srv_new_sub_pay_amount -- 业务服普通订阅金额
         ,cast(count(distinct if(order_type=2,order_id)) as int) as srv_renew_sub_order_cnt -- 业务服订阅回调续订订单数
         ,cast(sum(if(order_type=2,sku_price,0.0)) as int) as srv_renew_sub_pay_amount -- 业务服订阅回调续订金额
         ,max(end_tm) as srv_last_pay_time -- 业务服最后付费时间
    from
    (
        select
             uid as uuid
            ,product_id as sku
            ,merchant_order_id as order_id
            ,cast(amount*100 as int) as sku_price
            ,cast(case when product_id not like '%sub%' then 0
               when product_id like '%sub%'  and merchant_notify_order_id<>merchant_order_id then 2
               else 1 end as int) as order_type -- 订单类型0普通1订阅2-订阅回调续订
            ,convert_tz(from_unixtime(end_time), 'Asia/Shanghai', 'America/Los_Angeles') as end_tm
            ,cast(row_number() over(partition by uid,merchant_order_id order by end_time) as int) as order_id_rk
        from chapters_log.dts_project_v_payment
        where end_time>=UNIX_TIMESTAMP(convert_tz('${TX_START_TIMESTAMP}','America/Los_Angeles','Asia/Shanghai'))
          and end_time<=UNIX_TIMESTAMP(convert_tz('${TX_END_TIMESTAMP}','America/Los_Angeles','Asia/Shanghai'))
          and channel_id not like 'TEST%'
          and  order_status in (3,4) and is_abnormal=0 and sandbox=0
          and uid not in (124268225,124082749)  -- 测试账号
    )
    where order_id_rk=1
    group by uuid
    ) t9
    on t1.uuid=t9.uuid
left join (
    select uuid ,fcm_push
    from (
    select  uuid
    ,json_extract(properties,'$.fcm_push') as fcm_push
    ,row_number()over(partition by uuid order by ctime desc ) as rank
    from dwd_data.dwd_t02_reelshort_custom_event_di
    where etl_date='${TX_DATE}' and sub_event_name='user_preferrence_conf'
    )
    where rank=1
)t10
on t1.uuid=t10.uuid

left join
(
     select distinct  uuid ,'w2a' as af_w2a_campaign_mode
     from dwd_data.dwd_t01_reelshort_w2a_attribution_di
     where analysis_date='${TX_DATE}'
) t12
on t1.uuid=t12.uuid

left join
(
     select
          uuid
          ,cast(if(vip_type>0,1,0) as int) as vip_type_lt
     from
     (
          select
               uuid
               ,vip_type
               ,row_number() over(partition by uuid order by etl_date desc) as rk
          from
          (
               select
                    uuid
                    ,etl_date
                    ,max(vip_type as int) as vip_type
               from dwd_data.dwd_t01_reelshort_user_info_di
               where etl_date='${TX_DATE}'
               group by uuid
          )
     )
     where rk=1
) t13
on t1.uuid=t13.uuid

    left join
    (
        select
                uuid
                ,max_continue_checkin_days
                ,current_continue_checkin_days
                ,total_checkin_days
        from dwd_data.dwd_t06_reelshort_user_currency_checkin_get_di
        where etl_date='${TX_DATE}'
    ) t14
    on t1.uuid=t14.uuid

) new
 full join
(
    select uuid
          ,country_id
          ,channel_id
          ,version
          ,language_id
          ,platform
          ,cversion
          ,res_version
          ,os_version
          ,device_id
          ,ad_id
          ,androidid
          ,idfv
          ,user_ip
          ,netstatus
          ,analysis_date
          ,is_login
          ,user_type
          ,is_pay
          ,is_pay_lt
          ,last_login_time
          ,last_pay_time
          ,last_active_time
          ,last_pay_amount
          ,last_bind_type
          ,last_bind_time
          ,dlink_channel
          ,content_type
          ,register_time
          ,reg_country_id
          ,reg_channel_id
          ,reg_version
          ,reg_language_id
          ,reg_platform
          ,reg_device_id
          -- ,af_install_date
          ,af_network_name
          ,af_campaign
          ,af_adset
          ,af_ad
          ,device_brand
          ,device_model
          ,first_login_time
          ,first_pay_time
          ,first_active_time
          ,first_pay_amount
          ,first_bind_type
          ,first_bind_time
          ,install_cnt
          ,login_cnt
          ,pay_amount
          ,order_cnt
          ,invaild_pay_amount
          ,invaild_order_cnt
          ,pay_show_cnt
          ,pay_start_cnt
          ,pay_end_cnt
          ,pay_failed_cnt
          ,pay_cancel_cnt
          ,pay_delivery_cnt
          ,online_time
          ,main_play_online_time
          ,main_online_time
          ,chap_play_online_time
          ,vistor_online_time
          ,bind_gp_online_time
          ,bind_fb_online_time
          ,manual_unlock_01_cnt
          ,auto_unlock_01_cnt
          ,manual_unlock_01_exp
          ,auto_unlock_01_exp
          ,pay_01_cnt
          ,pay_01_get
          ,etl_date
          ,first_srv_sub_pay_amount
          ,last_srv_sub_pay_amount
          ,srv_sub_order_cnt
          ,srv_sub_pay_amount
          ,fcm_push
          ,ctimezone_offset
                ,vip_type_lt -- 买单是否订阅
                ,cli_non_sub_order_cnt -- 埋点非订阅订单数
                ,cli_non_sub_pay_amount -- 埋点非订阅订单金额
                ,srv_order_cnt -- 业务服订单数
                ,srv_pay_amount -- 业务服付费金额
                ,srv_nosub_order_cnt -- 业务服非订阅订单数
                ,srv_nosub_pay_amount -- 业务服非订阅金额
                ,srv_new_sub_order_cnt -- 业务服普通订阅订单数
                ,srv_new_sub_pay_amount -- 业务服普通订阅金额
                ,srv_renew_sub_order_cnt -- 业务服订阅回调续订订单数
                ,srv_renew_sub_pay_amount -- 业务服订阅回调续订金额
                ,srv_last_pay_time -- 业务服最后付费时间
                ,device_version -- 设备版本
                ,af_campaign_type -- appsflyer投放类型
                ,af_w2a_campaign_mode -- 埋点w2a类型用户 与af_campaign结合使用
                ,max_continue_checkin_days
                ,current_continue_checkin_days
                ,total_checkin_days
                -- add by lcz
                ,first_visit_landing_page                          -- 首次访问着陆页
                ,first_visit_landing_page_story_id                 -- 首次访问着陆页书籍id
                ,first_visit_landing_page_chap_id                  -- 首次访问着陆页章节id
    from dwd_data.dim_t99_reelshort_user_lt_info
    where etl_date=date_sub('${TX_DATE}',1)
) old
on new.uuid=old.uuid
;

