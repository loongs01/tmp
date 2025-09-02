-- drop Table if  exists dws_data.dws_t88_reelshort_opc_push_aggregate_di;
-- Create Table if not exists dws_data.dws_t88_reelshort_opc_push_aggregate_di(
-- id                               bigint AUTO_INCREMENT    comment '自增id'
-- ,analysis_date                   date                     comment '分析日期'
-- ,task_id                         varchar                  comment '任务id'
-- ,platform                        varchar                  comment '分发平台'
-- ,channel_id                      varchar                  comment '分发渠道id'
-- ,country_id                      varchar                  comment '国家id'
-- ,language_id                     varchar                  comment '语言id'
-- ,version                         varchar                  comment 'APP 的应用版本（包体内置版本号 versionname  ）'
-- ,cversion                        varchar                  comment '最新的应用游戏版本号'
-- ,user_type                       int                      comment '新老用户标记（新用户=1 老用户=2）'
-- ,is_pay_lt                       int                      comment '生命周期内购状态'
-- ,vip_type_lt                     int                      comment '生命周期订阅状态'
-- ,push_type                       varchar                  comment '推送类型:fcm_push_stat:FCM推送,custom_push_stat:自建推送,intent_stat:全屏Intent(Android)'
-- ,sent_cnt                        bigint                   comment '发送push任务次数'
-- ,sent_uv                         bigint                   comment '发送push任务用户数'
-- ,success_sent_cnt                bigint                   comment '成功发送push任务次数'
-- ,success_sent_uv                 bigint                   comment '成功发送push任务用户数'
-- ,accepted_cnt                    bigint                   comment '接收push任务次数'
-- ,accepted_uv                     bigint                   comment '接收push任务用户数'
-- ,show_cnt                        bigint                   comment '展示push任务次数'
-- ,show_uv                         bigint                   comment '展示push任务用户数'
-- ,click_cnt                       bigint                   comment '点击push任务次数'
-- ,click_uv                        bigint                   comment '点击push任务用户数'
-- ,push_play_uv                    bigint                   comment 'Push转化播放用户数'
-- ,push_purchase_uv                bigint                   comment 'Push转化内购用户数'
-- ,push_purchase_amt               decimal(18, 4)           comment 'Push转化内购金额'
-- ,push_advertising_uv             bigint                   comment 'push广告收入用户数'
-- ,push_advertising_amt            decimal(18, 4)           comment 'Push广告收入'
-- ,push_first_subscribe_uv         bigint                   comment 'Push转化首订用户数'
-- ,push_first_subscribe_amt        decimal(18, 4)           comment 'Push转化首订总收入'
-- ,push_pay_cnt                    bigint                   comment 'Push转化付费用户数:点击push当次会话内，内购+订阅收入用户数'
-- ,push_total_use_duration         bigint                   comment 'Push转化总使用时长' -- Push转化人均使用时长（分钟）点击push当次会话内，总使用时长/当日点击push用户数
-- ,PRIMARY KEY (id,analysis_date,task_id)
-- )DISTRIBUTE BY HASH(analysis_date,task_id) PARTITION BY VALUE(DATE_FORMAT(analysis_date,'%Y%m')) LIFECYCLE 120 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='reelshort push流转数据汇总表';



 delete from dws_data.dws_t88_reelshort_opc_push_aggregate_di  where analysis_date='${TX_DATE}';
/*+ cte_execution_mode=shared */
insert into dws_data.dws_t88_reelshort_opc_push_aggregate_di
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
      -- ,count(distinct t1.uuid)                                                                  as sent_uv                       -- 发送push任务用户数
	  ,rb_build(cast(split(GROUP_CONCAT(distinct uuid SEPARATOR '#' ),'#') as array(int)))      as sent_uv
      ,sum(if(t1.is_success=1,1,0))                                                             as success_sent_cnt              -- 成功发送push任务次数
      -- ,count(distinct if(t1.is_success=1, t1.uuid))                                             as success_sent_uv               -- 成功发送push任务用户数
	  ,rb_build(cast(split(GROUP_CONCAT(distinct if(t1.is_success=1, t1.uuid) SEPARATOR '#' ),'#') as array(int)))      as success_sent_uv
      ,sum(if(t1.is_success=1,t1.delivered_cnt,0))                                              as accepted_cnt                  -- 接收push任务次数
      -- ,count(distinct if(t1.is_success=1 and t1.delivered_cnt>=1 ,t1.uuid))                     as accepted_uv                   -- 接收push任务用户数
	  ,rb_build(cast(split(GROUP_CONCAT(distinct if(t1.is_success=1 and t1.delivered_cnt>=1 ,t1.uuid) SEPARATOR '#' ),'#') as array(int)))      as accepted_uv
      ,sum(t1.show_cnt)                                                                         as show_cnt                      -- 展示push任务次数
      -- ,count(distinct if(t1.show_cnt>=1, t1.uuid))                                              as show_uv                       -- 展示push任务用户数
	  ,rb_build(cast(split(GROUP_CONCAT(distinct if(t1.show_cnt>=1, t1.uuid) SEPARATOR '#' ),'#') as array(int)))      as show_uv
      ,sum(t1.click_cnt)                                                                        as click_cnt                     -- 点击push任务次数
      -- ,count(distinct if(t1.click_cnt>=1, t1.uuid))                                             as click_uv                      -- 点击push任务用户数
	  ,rb_build(cast(split(GROUP_CONCAT(distinct if(t1.click_cnt>=1, t1.uuid) SEPARATOR '#' ),'#') as array(int)))      as click_uv
      -- ,count(distinct if(t1.is_play=1, t1.uuid))                                                as push_play_uv                  -- Push转化播放用户数
	  ,rb_build(cast(split(GROUP_CONCAT(distinct if(t1.is_play=1, t1.uuid) SEPARATOR '#' ),'#') as array(int)))      as push_play_uv
      -- ,count(distinct if(t1.is_pay=1, t1.uuid))                                                 as push_purchase_uv              -- Push转化内购用户数
	  ,rb_build(cast(split(GROUP_CONCAT(distinct if(t1.is_pay=1, t1.uuid) SEPARATOR '#' ),'#') as array(int)))      as push_purchase_uv
      ,cast(sum(t1.sku_price) as decimal(18, 4))                                                as push_purchase_amt             -- Push转化内购金额
      -- ,count(distinct if(t1.ad_revenue>0, t1.uuid))                                             as push_advertising_uv        -- push广告收入用户数
	  ,rb_build(cast(split(GROUP_CONCAT(distinct if(t1.ad_revenue>0, t1.uuid) SEPARATOR '#' ),'#') as array(int)))      as push_advertising_uv
      ,cast(sum(t1.ad_revenue) as decimal(18, 4))                                               as push_advertising_amt          -- Push广告收入
      -- ,count(distinct if(t1.is_first_subscribe=1, t1.uuid))                                     as push_first_subscribe_uv       -- Push转化首订用户数
	  ,rb_build(cast(split(GROUP_CONCAT(distinct if(t1.is_first_subscribe=1, t1.uuid) SEPARATOR '#' ),'#') as array(int)))      as push_first_subscribe_uv
      ,cast(sum(t1.first_subscribe_sku_price) as decimal(18, 4) )                               as push_first_subscribe_amt      -- Push转化首订总收入
      -- ,count(distinct if(t1.is_pay=1 or t1.is_subscribe=1, t1.uuid))                            as push_pay_cnt               -- Push转化付费用户数:点击push当次会话内，内购+订阅收入用户数
	  ,rb_build(cast(split(GROUP_CONCAT(distinct if(t1.is_pay=1 or t1.is_subscribe=1, t1.uuid) SEPARATOR '#' ),'#') as array(int)))      as push_pay_cnt
      ,sum(t1.sum_online_times)                                                                 as push_total_use_duration       -- Push转化总使用时长 -- Push转化人均使用时长（分钟）点击push当次会话内，总使用时长/当日点击push用户数
FROM dwd_data.dwd_t04_reelshort_opc_push_detail_di t1
where analysis_date='${TX_DATE}'
group by
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
        ,push_type
;