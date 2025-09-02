--dwd_data.dwd_t01_reelshort_user_info_di

dwd_data.dim_t99_reelshort_user_lt_info
chapters_log.public_event_data  ：app_id='cm1009' and app_channel_id in('AVG20001','AVG10003','1','2','6','11','12','13','14','15','16') and os_type in ('1','2','3','4')


分发平台 ,case  platform  when 1 then 'ANDROID' when 2 then 'IOS' end as sdk_platform
分发渠道 chapters_log.opc_fcm_push_log_cm1009   ,if(t1.task_id=0,'api','opc') as create_channel   --用channel_id

国家 语言  dwd_data.dim_t99_reelshort_user_lt_info    chapters_log.dts_project_v_user_token表也有
是否内购1-是 dw_data.dwd_t04_chapters_opc_detail_di.is_pay

推送类型  ,1notification:通知,2email:邮件,3sms:短信   dts_pub_opt_notification_push_tasks.push_type

新老用户 ：dwd_data.dwd_t01_reelshort_user_info_di     user_type


dw_data.dwd_t04_chapters_opc_detail_di `send_flag` int COMMENT '发送成功标识1-发送成功',

chapters_log.reelshort_event_data_custom    数据日志二级分区   --

是否打开 open_flag
chapters_log.log_avg_data   数据日志二级分区   ：dw_data.dwd_t04_chapters_opc_detail_di


dws_data.dws_t85_spt_user_subscribe_di 业务服订阅订单表
,if(t1.task_id=0,'api','opc') as create_channel

run_id -- 应用每次冷启动启动随机生成 dwd_t02_reelshort_push_stat_di


带srv是业务服数据：dwd_data.dwd_t05_reelshort_srv_order_detail_di
不带srv是api数据：dwd_data.dwd_t05_reelshort_order_detail_di

内购是指买金币包，订阅是vip
付费是包含内购和订阅


订阅=首订+自动续订

走埋点，就只包含内购和首订（不包含自动续订）；走业务服就包含内购+订阅（含自动续订）

-- drop Table if  exists dwd_data.dwd_t04_reelshort_opc_push_detail_di;
Create Table if not exists dwd_data.dwd_t04_reelshort_opc_push_detail_di (
id                               bigint AUTO_INCREMENT    comment '自增id'
,task_id                         varchar                  comment '任务id'
,uuid                            varchar                  comment '用户ID'
,analytics_label                 varchar                  comment '分析标签'
,analysis_date                   date                     comment '分析日期'
,app_id                          varchar                  comment '应用id'
,instance_id                     varchar                  comment '消息发送到的应用的唯一id'
,message_id                      varchar                  comment '消息id'
,create_channel                  varchar                  COMMENT '创建渠道:opc/api'
,platform                        varchar                  comment '分发平台'
,title                           varchar                  comment '通知标题'
,content                         varchar                  comment '通知详情'
,push_type                       varchar                  comment '推送类型:fcm_push_stat:FCM推送,custom_push_stat:自建推送,intent_stat:全屏Intent(Android)'
,planned_quantity                bigint                   comment '计划发送数'
,sent_quantity                   bigint                   comment '已发送数'
,last_send_time_at               varchar                  comment '最后发送时间'
,status                          varchar                  comment '任务状态'
,is_success                      int                      comment '是否发送成功：1-是,0-否'
,accepted_cnt                    bigint                   comment '已发送事件数'
,delivered_cnt                   bigint                   comment '接收事件数'
-- ,show_flag                       int                      comment '展示标识:1-是,0-否'
-- ,click_flag                      int                      comment '是否点击：1-是,0-否'
,show_cnt                        bigint                      comment '展示次数'
,click_cnt                       bigint                      comment '点击次数'
,user_type                       int                      comment '新老用户标记（新用户=1 老用户=2）'
,is_pay_lt                       int                      comment '生命周期内购状态'
,vip_type_lt                     int                      comment '生命周期订阅状态'
,is_login                        int                      comment '是否登陆:1-是,0-否'
,country_id                      varchar                  comment '国家id'
,language_id                     varchar                  comment '语言id'
,channel_id                      varchar                  comment '分发渠道id'
,version                         varchar                  comment 'APP 的应用版本（包体内置版本号 versionname  ）'
,cversion                        varchar                  comment '最新的应用游戏版本号'
,is_play                         int                      comment '是否播放:1-是,0-否'
,play_cnt                        bigint                   comment '播放次数'
,is_pay                          int                      comment '是否内购:1-是,0-否'
,sku_price                       decimal(18, 4)           comment '内购支付订单价格 (单位:分)'
,is_first_subscribe              int                      comment '是否首次订阅：1-是,0-否'
,first_subscribe_sku_price       decimal(18, 4)           comment '首次订阅金额'
,is_subscribe                    int                      comment '是否订阅：1-是,0-否'
,subscribe_price                 decimal(18, 4)           comment '订阅单价'
-- ,is_ad_revenue                   int                      comment '是否广告收入用户：1-是,0-否'
,ad_revenue                      decimal(18, 4)           comment '广告收入'
,sum_online_times                bigint                   comment '用户使用时长:分钟'
-- ,first_subscribe_order_id        varchar                  comment '首次订阅订单号'
,is_intent                       int                      comment '是否全屏intent'
,PRIMARY KEY (analysis_date,id,task_id,uuid)
)DISTRIBUTE BY HASH(uuid) PARTITION BY VALUE(DATE_FORMAT(analysis_date,'%Y%m')) LIFECYCLE 120 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='reelshort Push流转数据';
;

ALTER TABLE dws_data.dws_t88_reelshort_opc_push_analytic_di MODIFY COLUMN push_purchase_amt decimal(38, 4);
--,INDEX idx_dim1 (platform,channel_id)
ALTER TABLE dwd_data.dwd_t04_reelshort_opc_push_detail_di RENAME COLUMN open_flag TO click_flag;

ALTER TABLE dwd_data.dwd_t04_reelshort_opc_push_detail_di RENAME COLUMN push_platform TO platform;
ALTER TABLE dwd_data.dwd_t04_reelshort_opc_push_detail_di RENAME COLUMN lifecycle_purchase_status TO is_pay_lt;
ALTER TABLE dwd_data.dwd_t04_reelshort_opc_push_detail_di RENAME COLUMN lifecycle_subscribe_status TO vip_type_lt;
ALTER TABLE dwd_data.dwd_t04_reelshort_opc_push_detail_di RENAME COLUMN push_channel_id TO channel_id;
ALTER TABLE dwd_data.dwd_t04_reelshort_opc_push_detail_di RENAME COLUMN app_version TO version;
alter table dwd_data.dwd_t04_reelshort_opc_push_detail_di add column first_subscribe_order_id varchar comment '首次订阅订单号'; 
alter table dwd_data.dwd_t04_reelshort_opc_push_detail_di drop column first_subscribe_order_id;

alter table dwd_data.dwd_t04_reelshort_opc_push_detail_di add column is_intent int comment '是否全屏intent'; 
alter table dws_data.dws_t88_reelshort_opc_push_aggregate_di add column is_intent int comment '是否全屏intent'; 

Create Table if not exists dwd_data.dwd_t04_reelshort_opc_push_authority_di (
id                               bigint AUTO_INCREMENT    comment '自增id'
,analysis_date                   date                     comment '分析日期'
,etl_date                        date                     comment '清洗日期'
,platform                        varchar                  comment '分发平台'
,channel_id                      varchar                  comment '分发渠道id'
,country_id                      varchar                  comment '国家id'
,language_id                     varchar                  comment '语言id'
,version                         varchar                  comment 'APP 的应用版本（包体内置版本号 versionname  ）'
,user_type                       int                      comment '新老用户标记（新用户=1 老用户=2）'
,uuid                            varchar                  comment '用户ID'
,fcm_push                        int                      comment 'fcm push权限：1-开启/2-关闭/-1 -未知'
,is_new_open_authority           int                      comment '是否新开启权限：昨天之前关闭，当日开启权限：1-是,0-否'
,is_close_authority              int                      comment '是否关闭权限：昨天之前开启，当日关闭权限：1-是,0-否'
,is_fist_open_authority          int                      comment '是否首次启动开启权限:当日首次启动权限：1-是,0-否'
,is_not_fist_open_authority      int                      comment '是否非首次启动开启权限：1-是,0-否'
,PRIMARY KEY (analysis_date,id,uuid)
)DISTRIBUTE BY HASH(uuid) PARTITION BY VALUE(DATE_FORMAT(analysis_date,'%Y%m')) LIFECYCLE 120 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='reelshort Push流转数据';
;

--
Create Table if not exists dws_data.dws_t88_reelshort_opc_push_di(
analysis_date                    date                     comment '分析日期'
,task_id                         int                      comment '任务id'
,analytics_label                 varchar                  comment '分析标签'
,push_platform                   varchar                  comment '分发平台'
,push_channel_id                 varchar                  comment '分发渠道id'
,country_id                      varchar                  comment '国家id'
,language_id                     varchar                  comment '语言id'
,app_version                     varchar                  comment 'APP 的应用版本（包体内置版本号 versionname  ）'
,user_type                       int                      comment '新老用户标记（新用户=1 老用户=2）'
,lifecycle_purchase_status                 int                      comment '生命周期内购状态'
,lifecycle_subscribe_status                int                      comment '生命周期订阅状态'
,push_type                       varchar                  comment '推送类型,notification:通知,email:邮件,sms:短信'
,sent_cnt                        int                      comment '发送push任务次数'
,sent_uv                         int                      comment '发送push任务用户数'
,success_sent_cnt                int                      comment '成功发送push任务次数'
,success_sent_uv                 int                      comment '成功发送push任务用户数'
,accepted_cnt                    int                      comment '接收push任务次数'
,accepted_uv                     int                      comment '接收push任务用户数'
,show_cnt                        int                      comment '展示push任务次数'
,show_uv                         int                      comment '展示push任务用户数'
,push_play_uv                    int                      comment 'Push转化播放用户数'
,push_purchase_uv                int                      comment 'Push转化内购用户数'
,push_purchase_amt               decimal(18, 4)           comment 'Push转化内购金额'
,push_advertising_uv             int                      comment 'push广告收入用户数'
,push_advertising_amt            decimal(18, 4)           comment 'Push广告收入'
,push_first_subscribe_uv         int                      comment 'Push转化首订用户数'
,push_first_subscribe_amt        decimal(18, 4)           comment 'Push转化首订总收入'
,push_pay_cnt                    int                      comment 'Push转化付费用户数:点击push当次会话内，内购+订阅收入用户数'
,push_total_use_duration         bigint                   comment 'Push转化总使用时长' -- Push转化人均使用时长（分钟）点击push当次会话内，总使用时长/当日点击push用户数
,push_click_uv                   int                      comment '点击push用户数'
,open_authority_uv               int                      comment '权限开启UV:当日权限开启UV'
,new_open_authority_uv           int                      comment '新开启权限UV:昨天之前关闭，当日开启权限UV'
,close_authority_uv              int                      comment '关闭权限UV:昨天之前开启，当日关闭权限UV'
,fist_open_authority_uv          int                      comment '首次启动开启权限UV:当日首次启动权限UV'
,not_fist_open_authority_uv      int                      comment '非首次启动开启权限UV'
,PRIMARY KEY (analysis_date,analytics_label,task_id)
)DISTRIBUTE BY HASH(analysis_date,task_id) PARTITION BY VALUE(DATE_FORMAT(analysis_date,'%Y%m')) LIFECYCLE 120 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='reelshort';








---
select
      null as id -- 自增id
      ,analysis_date     -- 分析日期'
      ,task_id          -- 任务id'
      ,platform         -- 分发平台'
      ,channel_id       -- 分发渠道id'
      ,country_id       -- 国家id'
      ,language_id      -- 语言id'
      ,version          -- APP 的应用版本（包体内置版本号 versionname  ）'
      ,cversion         -- 最新的应用游戏版本号'
      ,user_type        -- 新老用户标记（新用户=1 老用户=2）'
      ,is_pay_lt        -- 生命周期内购状态'
      ,vip_type_lt      -- 生命周期订阅状态'
      ,push_type        -- 推送类型:fcm_push_stat:FCM推送,custom_push_stat:自建推送,intent_stat:全屏Intent(Android)'
      ,count(1)                                                                                 as sent_cnt                      -- 发送push任务次数
      ,count(distinct t1.uuid)                                                                  as sent_uv                       -- 发送push任务用户数
      ,sum(if(t1.is_success=1,1,0))                                                             as success_sent_cnt              -- 成功发送push任务次数
      ,count(distinct if(t1.is_success=1, t1.uuid))                                             as success_sent_uv               -- 成功发送push任务用户数
      ,sum(if(t1.is_success=1,t1.delivered_cnt,0))                                              as accepted_cnt                  -- 接收push任务次数
      ,count(distinct if(t1.is_success=1 and t1.delivered_cnt>=1 ,t1.uuid))                     as accepted_uv                   -- 接收push任务用户数
      ,sum(t1.show_cnt)                                                                         as show_cnt                      -- 展示push任务次数
      ,count(distinct if(t1.show_cnt>=1, t1.uuid))                                              as show_uv                       -- 展示push任务用户数
      ,sum(t1.click_cnt)                                                                        as click_cnt                     -- 点击push任务次数
      ,count(distinct if(t1.click_cnt>=1, t1.uuid))                                             as click_uv                      -- 点击push任务用户数
      ,count(distinct if(t1.is_play=1, t1.uuid))                                                as push_play_uv                  -- Push转化播放用户数
      ,count(distinct if(t1.is_pay=1, t1.uuid))                                                 as push_purchase_uv              -- Push转化内购用户数
      ,cast(sum(t1.sku_price) as decimal(18, 4))                                                as push_purchase_amt             -- Push转化内购金额
      ,count(distinct if(t1.ad_revenue>0, t1.uuid))                                             as push_advertising_uv        -- push广告收入用户数
      ,cast(sum(t1.ad_revenue) as decimal(18, 4))                                               as push_advertising_amt          -- Push广告收入
      ,count(distinct if(t1.is_first_subscribe=1, t1.uuid))                                     as push_first_subscribe_uv       -- Push转化首订用户数
      ,cast(sum(t1.first_subscribe_sku_price) as decimal(18, 4) )                               as push_first_subscribe_amt      -- Push转化首订总收入
      ,count(distinct if(t1.is_pay=1 or t1.is_subscribe=1, t1.uuid))                            as push_pay_cnt               -- Push转化付费用户数:点击push当次会话内，内购+订阅收入用户数
      ,sum(t1.sum_online_times)                                                                 as push_total_use_duration       -- Push转化总使用时长 -- Push转化人均使用时长（分钟）点击push当次会话内，总使用时长/当日点击push用户数
FROM dwd_data.dwd_t04_reelshort_opc_push_detail_di t1
where analysis_date='${TX_DATE}'
group by cube(
        analysis_date
        ,task_id
        ,platform
        ,channel_id
        ,country_id
        ,language_id
        ,version
        ,cversion
        ,user_type
        ,is_pay_lt
        ,vip_type_lt
        ,push_type)
;

-- push 12月变更
-- 展示、点击        修改：推送类型（fcm、自建）