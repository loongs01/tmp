delete from dwd_data.dwd_t04_reelshort_opc_push_detail_di  where analysis_date=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY);
/*+ cte_execution_mode=shared */
insert into  dwd_data.dwd_t04_reelshort_opc_push_detail_di
with push_log_temp as (
   select  t1.app_id
          ,t1.task_id
          ,coalesce(t1.analytics_label,t1.task_id) as analytics_label
          ,if(t1.uuid='',t2.uid,t1.uuid) as uuid
          ,split_part(t1.token,':',1) as instance_id
          ,split_part(t1.message_id,'/',4) as message_id
          ,if(success=1,1,0) as success
          ,analysis_date
          ,min(convert_tz(FROM_UNIXTIME(created_at),'Asia/Shanghai','America/Los_Angeles'))over(partition by app_id,task_id,analytics_label) as start_time
          ,max(convert_tz(FROM_UNIXTIME(created_at),'Asia/Shanghai','America/Los_Angeles'))over(partition by app_id,task_id,analytics_label) as end_time
          ,t2.sdk_platform
          ,t2.platform
   from chapters_log.opc_fcm_push_log_cm1009  t1
   left join (
     select token,uid,sdk_platform,platform
     from(select token
           ,uid
           ,case  platform  when 1 then 'ANDROID' when 2 then 'IOS' end as sdk_platform
           ,platform
           ,row_number()over(partition by token order by lastLogin desc ) as rank
     from chapters_log.dts_project_v_user_token
     where token<>'unknown'
     )
     where rank=1
     )t2
   on t1.token=t2.token
   where t1.app_id = 'cm1009'  -- and t1.success=1
    and  t1.analysis_date=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)
    )
,firebase_temp as (
   select id
        ,message_id
       ,analytics_label
       ,analysis_date
       ,event
       ,sdk_platform
       ,'cm1009' as app_id
       ,instance_id
 from chapters_log.reelshort_google_firebase_data
 where analysis_date=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)
       and project_name='reelshort'
       and event IN ("MESSAGE_ACCEPTED","MESSAGE_DELIVERED")
    )
,push_task_info_temp as (
   select analytics_label
               ,id as task_id
               ,title
               ,content
               ,creator
               ,case when push_type='1' then 'notification'
                     when push_type='2' then 'email'
                     when push_type='3' then 'sms'
                     else push_type
               end as push_type
               ,start_time
               ,end_time
               ,planned_quantity
               ,sent_quantity
               ,last_send_time_at
               ,case when status='1' then 'draft'
                     when status='2' then 'planning'
                     when status='3' then 'complete'
                     when status='4' then 'archive'
                     when status='5' then 'sending'
                     else status
               end as status
              ,row_number()over(partition by id order by  status_updated_at desc  ) as rank
              ,schedule_type
     from chapters_log.dts_pub_opt_notification_push_tasks
    where app_id = 'cm1009'  and date(start_time)=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)
    )
,push_task_info_temp_n2  as (
   select  analytics_label
               ,id as task_id
               ,title
               ,content
               ,creator
               ,case when push_type='1' then 'notification'
                     when push_type='2' then 'email'
                     when push_type='3' then 'sms'
                     else push_type
               end as push_type
               ,case when status='1' then 'draft'
                     when status='2' then 'planning'
                     when status='3' then 'complete'
                     when status='4' then 'archive'
                     when status='5' then 'sending'
                     else status
               end as status
              ,row_number()over(partition by id order by  status_updated_at desc  ) as rank
     from chapters_log.dts_pub_opt_notification_push_tasks
     where app_id = 'cm1009'
    )
select
      null as id -- 自增id
      ,if(t1.task_id>0,t1.task_id,t1.analytics_label)                     as task_id
      ,t1.uuid                                                            as uuid
      ,t1.analytics_label                                                 as analytics_label
      ,t1.analysis_date                                                   as analysis_date
      ,t1.app_id                                                          as app_id
      ,t1.instance_id                                                     as instance_id
      ,t1.message_id                                                      as message_id
      ,if(t1.task_id=0,'api','opc')                                       as create_channel
      ,coalesce(t1.platform,t8.platform,'ALL')                            as platform
      ,coalesce(t2.title,t7.title,t9.title)                               as title
      ,coalesce(t2.content,t7.content,t9.content)                         as content
      ,'fcm_push_stat'                                                    as push_type
      ,coalesce(t2.planned_quantity,0)                                    as planned_quantity            -- 计划发送数
      ,coalesce(t2.sent_quantity,0)                                       as sent_quantity               -- 已发送数
      ,coalesce(t2.last_send_time_at ,t1.end_time)                        as last_send_time_at
      ,coalesce(t2.status,t9.status)                                      as status
      ,t1.success                                                         as is_success
      ,cast(coalesce(t1.accepted_cnt ,0) as int)                          as accepted_cnt
      ,cast(coalesce(t1.delivered_cnt,0) as int)                          as delivered_cnt               -- 接收次数
      ,coalesce(t3.show_cnt,0)                                            as show_cnt
      ,coalesce(t3.click_cnt,0)                                           as click_cnt
      ,coalesce(t4.user_type,0)                                           as user_type
      ,t8.is_pay_lt                                                       as is_pay_lt                   -- 生命周期内购状态 :1-是，0-否
      ,t8.vip_type_lt                                                     as vip_type_lt                 -- 生命周期订阅状态:1-是，0-否
      ,coalesce(t4.is_login,0)                                            as is_login
      ,t8.country_id                                                      as country_id                  -- 国家id
      ,t8.language_id                                                     as language_id                 -- 语言id
      ,t8.channel_id                                                      as channel_id                  -- 渠道id
      ,t8.version                                                         as version                     -- APP的应用版本(包体内置版本号versionname)
      ,t8.cversion                                                        as cversion
      ,coalesce(t10.is_play,0)                                            as is_play                     -- 是否播放
      ,coalesce(t10.play_cnt,0)                                           as play_cnt                    -- 播放次数
      ,coalesce(t10.is_pay,0)                                             as is_pay                      -- 是否内购
      ,coalesce(cast(t10.sku_price as decimal(18, 4)),0)                  as sku_price                   -- 内购支付订单价格
      ,coalesce(t10.is_first_subscribe,0)                                 as is_first_subscribe          -- 是否首次订阅
      ,coalesce(cast(t10.first_subscribe_sku_price as decimal(18, 4)),0)  as first_subscribe_sku_price   -- 首次订阅金额
      ,coalesce(t10.is_subscribe,0)                                       as is_subscribe                -- 是否订阅
      ,coalesce(cast(t10.subscribe_price as decimal(18, 4)),0)            as subscribe_price             -- 订阅单价
      ,coalesce(cast(t10.ad_revenue as decimal(18, 4)),0)                 as ad_revenue                  -- 广告收入
      ,coalesce(cast(t10.sum_online_times as int),0)                      as sum_online_times            -- 用户使用时长:分钟
      ,coalesce(t3.is_intent,0)                                                       as is_intent                   -- 是否全屏intent
from (
      select t1.task_id
            ,t1.uuid
            ,coalesce(t1.analytics_label ,t0.analytics_label) as analytics_label
            ,t1.analysis_date
            ,t1.app_id
            ,t1.instance_id
            ,t1.message_id
            ,coalesce(t1.sdk_platform,t0.sdk_platform) as sdk_platform
            ,coalesce(t1.platform,case t0.sdk_platform when 'ANDROID' then 1 when 'IOS' then 2 end) as platform
            ,t0.accepted_cnt
            ,t0.delivered_cnt
            ,t1.success
            ,t1.start_time
            ,t1.end_time
      from  push_log_temp t1
      left join  (
        SELECT trim(analytics_label) as analytics_label
              ,analysis_date
              ,app_id
              ,instance_id
              ,message_id
              ,sdk_platform
              -- ,case sdk_platform when 'ANDROID' then 1 when 'IOS' then 2 end as platform
              ,count(if(event="MESSAGE_ACCEPTED"  ,id)) as accepted_cnt
              ,count(if(event="MESSAGE_DELIVERED" ,id)) as delivered_cnt
           FROM firebase_temp
           GROUP BY analytics_label,analysis_date,app_id,instance_id,message_id,sdk_platform
         )t0
          on t0.message_id=t1.message_id and t0.app_id=t1.app_id
            and t0.analytics_label=t1.task_id
            and t0.instance_id=t1.instance_id
            and t0.analysis_date=t1.analysis_date
)t1
left join (select * from push_task_info_temp where rank=1 )t2
on t1.task_id=t2.task_id
left join (
            select t.analysis_date
                   ,t.uuid
                   ,t.platform
                   ,t.fcm_message_id                  as message_id
                   ,'fcm_push_stat'                   as sub_event_name
                   ,sum(if(t.action='show' ,1,0))     as show_cnt
                   ,sum(if(t.action='click',1,0))     as click_cnt
                   ,max(if(t0.uuid is not null,1,0))  as is_intent                   -- 是否全屏intent
            from dwd_data.dwd_t02_reelshort_push_stat_di as t
            left join(
                      select distinct uuid,fcm_message_id
                      from dwd_data.dwd_t02_reelshort_push_stat_di as t
                      where fcm_message_id != ''
                      and country_id not in ('50', '98')
                      and analysis_date=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)
                      and sub_event_name='intent_stat'
                    ) as t0
            on t0.uuid=t.uuid and t0.fcm_message_id=t.fcm_message_id
            where t.fcm_message_id != ''
                  and country_id not in ('50', '98')
                  and analysis_date=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)
                  and sub_event_name='fcm_push_stat'
            group by t.analysis_date,t.uuid,t.platform,t.fcm_message_id
)t3
on t1.uuid=t3.uuid  and t1.message_id=t3.message_id and t1.platform=t3.platform
left join (
  select uuid
      ,platform
      ,analysis_date
      ,max(user_type) as user_type
      ,max(is_login) as is_login
      ,max(is_pay) as is_pay
  from dwd_data.dwd_t01_reelshort_user_info_di -- V项目dwd层:当日所有用户表
  where etl_date=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)
  group by uuid ,analysis_date ,platform
 )t4
on t1.analysis_date=t4.analysis_date and t1.uuid=t4.uuid  and t1.platform=t4.platform
left join (
   select uuid
         ,analysis_date
         ,platform
         ,sum(sku_price) as pay_amount
    from dwd_data.dwd_t05_reelshort_order_detail_di
    where etl_date=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)   and order_status=1  and order_id_rank=1
   group by uuid ,analysis_date ,platform
 )t5
on t1.analysis_date=t5.analysis_date and t1.uuid=t5.uuid  and t1.platform=t5.platform
left join (select * from (select
                                t7.analytics_label
                                ,t7.app_id
                                ,t7.title
                                ,t7.content
                                ,row_number() over(partition by analytics_label,app_id order by created_at desc ) as rn
                          from chapters_log.opc_api_push_tasks_info as t7
                         ) as t7
                    where rn=1
) as t7
on t1.analytics_label=t7.analytics_label and t1.app_id=t7.app_id
left join (select
                 uuid
                 ,country_id
                 ,language_id
                 ,platform
                 ,channel_id
                 ,user_type
                 ,version
                 ,cversion
                 ,is_pay_lt       -- 生命周期内购状态
                 ,vip_type_lt     -- 生命周期订阅状态
                 ,srv_vip_type_lt -- 生命周期订阅状态:1-是，0-否
                 ,first_pay_time  -- 首次支付时间
           from dwd_data.dim_t99_reelshort_user_lt_info -- v项目dwd层:全量用户码表
           where etl_date=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)
)t8
on t1.uuid=t8.uuid and t8.platform=t1.platform
left join  (select * from push_task_info_temp_n2 where rank=1 ) t9
on t1.task_id=t9.task_id
-- 获取点击数据
left join (
        select distinct
              t10.analysis_date
              ,t10.uuid
              ,t10.message_id
              ,t10.platform
              ,t10.country_id
              ,t10.language_id
              ,t10.version
              ,if(t11.uuid is not null,1,0) as is_play                         -- 是否播放
              ,t11.play_cnt                                                    -- 播放次数
              ,if(t12.uuid is not null,1,0) as is_pay                          -- 是否内购
              ,t12.sku_price                as sku_price                       -- 内购金额
              ,if(t15.uuid is not null,1,0) as is_first_subscribe              -- 是否首次订阅
              ,coalesce(t15.sku_price,0)    as first_subscribe_sku_price       -- 首次订阅金额
              -- ,if(t13.uuid is not null,1,0) as is_subscribe                    -- 是否订阅
              -- ,coalesce(t13.sku_price,0)    as subscribe_price                 -- 订阅金额
              ,0                            as is_subscribe                    -- 是否订阅
              ,0                            as subscribe_price                 -- 订阅金额
              ,t14.ad_revenue               as ad_revenue                      -- 广告收入
              ,t16.sum_online_times/60      as sum_online_times                -- 用户使用时长:分钟
        from (
              -- 获取点击数据
              select distinct analysis_date
                     ,uuid
                     ,run_id
                     ,fcm_message_id as message_id
                     ,platform
                     ,country_id
                     ,language_id
                     ,version
                     ,stime as click_time
                     ,row_number() over(partition by uuid,run_id order by stime asc ) as rn
              from dwd_data.dwd_t02_reelshort_push_stat_di
              where action = 'click'
                    and fcm_message_id != ''
                    and country_id not in ('50', '98')
                    and etl_date<='${TX_DATE}'
                    and etl_date>=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)
                    and sub_event_name='fcm_push_stat'
                    -- and (run_id!='' or run_id is not null)
              ) t10
        -- 获取播放数据
        left join (
                  select t1.uuid,t1.run_id,count( distinct t1.stime) as play_cnt -- 播放次数
                         -- ,row_number() over(partition by uuid,run_id order by stime asc ) as rn
                  from dwd_data.dwd_t02_reelshort_play_event_di t1
                  where t1.sub_event_name='play_start'
                      and t1.story_id<>''
                      and t1.country_id not in ('50', '98')
                      and t1.chap_id<>''
                      and t1.etl_date<='${TX_DATE}'
                      and t1.etl_date>=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)
                     -- and uuid in (select distinct uuid from click_user_detail)
                     group by t1.uuid,t1.run_id
                  ) as t11
        on t11.uuid = t10.uuid and t11.run_id = t10.run_id and t10.rn=1 and (t11.run_id!='' or t11.run_id is not null)
        -- 获取内购付费数据
        left join (
                   select
                         distinct t1.uuid
                         ,t1.run_id
                         ,t1.sku_price / 100 as sku_price  -- 内购金额
                         -- ,row_number() over(partition by uuid,run_id order by stime asc ) as rn
                         from dwd_data.dwd_t05_reelshort_order_detail_di as t1
                         where t1.order_status = 1
                             and t1.order_id_rank=1
                             and t1.country_id not in ('50', '98')
                             and t1.etl_date<='${TX_DATE}'
                             and t1.etl_date>=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)
                             and t1.channel_sku not like '%sub%'
                   ) as t12
        on t12.uuid = t10.uuid and t12.run_id=t10.run_id and t10.rn=1 and (t12.run_id!='' or t12.run_id is not null)
        -- 获取订阅数据
        -- left join (
                   -- select
                         -- analysis_date
                         -- ,uuid
                         -- ,sku_price
                         -- ,row_number() over (partition by uuid order by pay_time asc) as rn
                   -- from dwd_data.dwd_t05_reelshort_srv_order_detail_di t
                   -- where etl_date=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)
                         -- and order_type in(1,2)
                   -- ) as t13
        -- on t13.uuid = t10.uuid and t13.rn=1
        -- 获取首次订阅数据
        -- left join (select *
                    -- from (select
                                -- analysis_date
                                -- ,uuid
                                -- ,sku_price
                                -- ,row_number() over (partition by uuid order by pay_time asc) as rn
                          -- from dwd_data.dwd_t05_reelshort_srv_order_detail_di t
                          -- where etl_date=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)  and order_type=1
                          -- )
                  -- ) as t15
        -- on t15.uuid = t10.uuid and t15.rn=1
		-- 获取首次订阅数据modify
        left join (
                   select
                         uuid -- 应用中用户ID
                         ,run_id -- 应用每次冷启动启动随机生成
                         -- ,count(distinct cast(nvl(json_extract(properties,'$._channel_orderid'),'') as varchar)) as order_id -- 支付平台订单id (第三方支付平台生成)
                         ,max(sku_price) as sku_price -- 实际支付订单价格 (单位:分)
                   from
                      ( select
                             app_user_id as uuid -- 应用中用户ID
                             ,app_activate_id as run_id -- 应用每次冷启动启动随机生成
                             ,cast(nvl(json_extract(properties,'$._app_orderid'),'') as varchar) as order_id
                             ,cast(nvl(json_extract(properties,'$._order_amount'),0) as int)/100 as sku_price -- 实际支付订单价格 (单位:分)
                        from chapters_log.public_event_data
                        where analysis_date<='${TX_DATE}'
                         and analysis_date>=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)
                         and event_name='m_pay_event'
                         and sub_event_name='pay_complete'
                         and cast(JSON_EXTRACT(properties, '$._channel_sku') as varchar) like '%sub%'
                       )as t
                   inner join
                            (select distinct analysis_date, transaction_id
                             from dwd_data.dwd_t05_reelshort_srv_order_detail_di
                            where etl_date<='${TX_DATE}'
                                  and etl_date>=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)
                                  and pay_status in (3,4)
                                  and merchant_notify_order_id<>''
                                  and merchant_notify_order_id=order_id
                            ) as t1
                    on t1.transaction_id=t.order_id
                   group by uuid,run_id
                  ) as t15
        on t15.uuid = t10.uuid and t15.run_id=t10.run_id and t10.rn=1 and (t15.run_id!='' or t15.run_id is not null)
        -- 获取广告收入数据
        left join (
                    select
                    uuid
                    ,run_id
                    ,sum(cast(ad_revenue as double)) as ad_revenue -- 广告收入
                    from dwd_data.dwd_t07_reelshort_admoney_event_di
                    where etl_date<='${TX_DATE}'
                          and etl_date>=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)
                          and country_id not in ('98', '50')
                          and language_id<>'99'
                          and language_id not like 'zh-%'
                    group by uuid,run_id
                  ) as t14
        on t14.uuid = t10.uuid and t14.run_id = t10.run_id and t10.rn=1  and (t14.run_id!='' or t14.run_id is not null)
        -- 获取用户使用时长:秒
        left join (
                     select
                           uuid
                           ,run_id
                           ,sum(online_time) as sum_online_times
                     from dwd_data.dwd_t01_reelshort_user_online_time_di
                     where etl_date<='${TX_DATE}'
                           and etl_date>=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)
                           and country_id not in ('98', '50')
                           and language_id<>'99'
                           and language_id not like 'zh-%'
                           group by  uuid,run_id
                  ) as t16
        on t16.uuid = t10.uuid and t16.run_id = t10.run_id and t10.rn=1 and (t16.run_id!='' or t16.run_id is not null)
) as t10
on  t10.platform = t1.platform
    and t10.country_id = t8.country_id
    and t10.language_id = t8.language_id
    and t10.version = t8.version
    -- and t10.task_id =t1.task_id
    and t10.uuid = t1.uuid
    and t10.message_id = t1.message_id
-- 获取埋点事件名称：fcm_push_stat:FCM推送,custom_push_stat:自建推送,intent_stat:全屏Intent(Android)
where t1.uuid<>''
;


-- /*+ cte_execution_mode=shared */
insert into  dwd_data.dwd_t04_reelshort_opc_push_detail_di
with reelshort_push_stat as (select t.*
                                    ,cast(JSON_EXTRACT(REPLACE(t.fcm_info,'.','_'), '$.google_c_a_m_l') as varchar) as analytics_label0
                             from dwd_data.dwd_t02_reelshort_push_stat_di as t
                             where fcm_message_id != ''
                             and country_id not in ('50', '98')
                             and etl_date<='${TX_DATE}'
                             and etl_date>=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)
                             and sub_event_name  in('custom_push_stat','intent_stat')
)
select
      null as id -- 自增id
      ,t1.task_id                                                         as task_id
      ,t1.uuid                                                            as uuid
      ,t1.analytics_label                                                 as analytics_label
      ,DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)                              as analysis_date
      ,null                                                               as app_id
      ,null                                                               as instance_id
      ,t1.message_id                                                      as message_id
      ,null                                                               as create_channel
      ,coalesce(t1.platform,t8.platform,'ALL')                            as platform
      ,task.task_name                                                     as title                       -- 任务名称
      ,null                                                               as content
      ,'custom_push_stat'                                                  as push_type
      ,null                                                               as planned_quantity            -- 计划发送数
      ,null                                                               as sent_quantity               -- 已发送数
      ,null                                                               as last_send_time_at
      ,null                                                               as status
      ,null                                                               as is_success
      ,0                          as accepted_cnt
      ,0                          as delivered_cnt               -- 接收次数
      ,coalesce(t1.show_cnt,0)                                            as show_cnt
      ,coalesce(t1.click_cnt,0)                                           as click_cnt
      ,coalesce(t4.user_type,0)                                           as user_type
      ,t8.is_pay_lt                                                       as is_pay_lt                   -- 生命周期内购状态 :1-是，0-否
      ,t8.vip_type_lt                                                     as vip_type_lt                 -- 生命周期订阅状态:1-是，0-否
      ,coalesce(t4.is_login,0)                                            as is_login
      ,t8.country_id                                                      as country_id                  -- 国家id
      ,t8.language_id                                                     as language_id                 -- 语言id
      ,t8.channel_id                                                      as channel_id                  -- 渠道id
      ,t8.version                                                         as version                     -- APP的应用版本(包体内置版本号versionname)
      ,t8.cversion                                                        as cversion
      ,coalesce(t10.is_play,0)                                            as is_play                     -- 是否播放
      ,coalesce(t10.play_cnt,0)                                           as play_cnt                    -- 播放次数
      ,coalesce(t10.is_pay,0)                                             as is_pay                      -- 是否内购
      ,coalesce(cast(t10.sku_price as decimal(18, 4)),0)                  as sku_price                   -- 内购支付订单价格
      ,coalesce(t10.is_first_subscribe,0)                                 as is_first_subscribe          -- 是否首次订阅
      ,coalesce(cast(t10.first_subscribe_sku_price as decimal(18, 4)),0)  as first_subscribe_sku_price   -- 首次订阅金额
      ,coalesce(t10.is_subscribe,0)                                       as is_subscribe                -- 是否订阅
      ,coalesce(cast(t10.subscribe_price as decimal(18, 4)),0)            as subscribe_price             -- 订阅单价
      ,coalesce(cast(t10.ad_revenue as decimal(18, 4)),0)                 as ad_revenue                  -- 广告收入
      ,coalesce(cast(t10.sum_online_times as int),0)                      as sum_online_times            -- 用户使用时长:分钟
      ,t1.is_intent                                                       as is_intent                   -- 是否全屏intent
from
(
    -- 获取展示、点击数据
    select  analysis_date
           ,t1.uuid
           -- ,run_id
           ,analytics_label0 as analytics_label  -- custom_push_stat:自建推送只有message_id
           -- 任务id取值：push_type为custom_push_stat自建，取消息id；
           -- 全屏intent_stat，有分析标签取分析标签【分析标签为fcm_info里goole.c.a.m取值】，无则取消息id
           ,if(sub_event_name='custom_push_stat',fcm_message_id,coalesce(analytics_label0,fcm_message_id)) as task_id
           ,fcm_message_id as message_id
           ,platform
           ,country_id
           ,language_id
           ,version
           -- ,sub_event_name
           ,sum(if(action='show' ,1,0)) as show_cnt
           ,sum(if(action='click',1,0)) as click_cnt
           ,max(if(t0.uuid is not null,1,0)) as is_intent                   -- 是否全屏intent
    from reelshort_push_stat as t1
    left join(
              select
                    uuid
                    ,fcm_message_id as message_id
              from reelshort_push_stat
              where sub_event_name='intent_stat'
              group by
                 uuid
                 ,fcm_message_id
             ) as t0
     on t0.uuid=t1.uuid and t0.message_id=t1.fcm_message_id
    group by analysis_date
             ,t1.uuid
             -- ,run_id
             ,analytics_label
             ,if(sub_event_name='custom_push_stat',fcm_message_id,coalesce(analytics_label0,fcm_message_id))
             ,fcm_message_id
             ,platform
             ,country_id
             ,language_id
             ,version
             -- ,sub_event_name
) t1
-- left join(
          -- select
                -- uuid
                -- ,fcm_message_id as message_id
          -- from reelshort_push_stat
          -- where sub_event_name='intent_stat'
          -- group by
             -- uuid
             -- ,fcm_message_id
         -- ) as t0
-- on t0.uuid=t1.uuid and t0.message_id=t1.message_id
left join (
  select uuid
      ,platform
      ,analysis_date
      ,max(user_type) as user_type
      ,max(is_login) as is_login
      ,max(is_pay) as is_pay
  from dwd_data.dwd_t01_reelshort_user_info_di -- V项目dwd层:当日所有用户表
  where etl_date=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)
  group by uuid ,analysis_date ,platform
 )t4
on t1.analysis_date=t4.analysis_date and t1.uuid=t4.uuid  and t1.platform=t4.platform
left join (select
                 uuid
                 ,country_id
                 ,language_id
                 ,platform
                 ,channel_id
                 ,user_type
                 ,version
                 ,cversion
                 ,is_pay_lt       -- 生命周期内购状态
                 ,vip_type_lt     -- 生命周期订阅状态
                 ,srv_vip_type_lt -- 生命周期订阅状态:1-是，0-否
                 ,first_pay_time  -- 首次支付时间
           from dwd_data.dim_t99_reelshort_user_lt_info -- v项目dwd层:全量用户码表
           where etl_date=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)
)t8
on t1.uuid=t8.uuid and t8.platform=t1.platform
-- 获取点击数据
left join (
        select distinct
              t10.analysis_date
              ,t10.uuid
              ,t10.run_id
              ,t10.message_id
              ,t10.platform
              ,t10.country_id
              ,t10.language_id
              ,t10.version
              ,t10.sub_event_name
              ,if(t11.uuid is not null,1,0) as is_play                         -- 是否播放
              ,t11.play_cnt                                                    -- 播放次数
              ,if(t12.uuid is not null,1,0) as is_pay                          -- 是否内购
              ,t12.sku_price                as sku_price                       -- 内购金额
              ,if(t15.uuid is not null,1,0) as is_first_subscribe              -- 是否首次订阅
              ,coalesce(t15.sku_price,0)    as first_subscribe_sku_price       -- 首次订阅金额
              -- ,if(t13.uuid is not null,1,0) as is_subscribe                    -- 是否订阅
              -- ,coalesce(t13.sku_price,0)    as subscribe_price                 -- 订阅金额
              ,0                            as is_subscribe                    -- 是否订阅
              ,0                            as subscribe_price                 -- 订阅金额
              ,t14.ad_revenue               as ad_revenue                      -- 广告收入
              ,t16.sum_online_times/60      as sum_online_times                -- 用户使用时长:分钟
        from (
              -- 获取点击数据
              select distinct analysis_date
                     ,uuid
                     ,run_id
                     ,fcm_message_id as message_id
                     ,platform
                     ,country_id
                     ,language_id
                     ,version
                     ,stime as click_time
                     ,sub_event_name
                     ,row_number() over(partition by uuid,run_id order by stime asc ) as rn
              from reelshort_push_stat
              where action = 'click'
                    -- and sub_event_name='custom_push_stat'
              ) t10
        -- 获取播放数据
        left join (
                  select t1.uuid,t1.run_id,count( distinct t1.stime) as play_cnt -- 播放次数
                  from dwd_data.dwd_t02_reelshort_play_event_di t1
                  where t1.sub_event_name='play_start'
                      and t1.story_id<>''
                      and t1.country_id not in ('50', '98')
                      and t1.chap_id<>''
                      and t1.etl_date<='${TX_DATE}'
                      and t1.etl_date>=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)
                     -- and uuid in (select distinct uuid from click_user_detail)
                     group by t1.uuid,t1.run_id
                  ) as t11
        on t11.uuid = t10.uuid and t11.run_id = t10.run_id and t10.rn=1 and (t11.run_id!='' or t11.run_id is not null)
        -- 获取内购付费数据
        left join (
                   select
                         distinct t1.uuid
                         ,t1.run_id
                         ,t1.sku_price / 100 as sku_price  -- 内购金额
                         from dwd_data.dwd_t05_reelshort_order_detail_di as t1
                         where t1.order_status = 1
                             and t1.order_id_rank=1
                             and t1.country_id not in ('50', '98')
                             and t1.etl_date<='${TX_DATE}'
                             and t1.etl_date>=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)
                             and t1.channel_sku not like '%sub%'
                   ) as t12
        on t12.uuid = t10.uuid and t12.run_id=t10.run_id and t10.rn=1 and (t12.run_id!='' or t12.run_id is not null)
        -- 获取订阅数据
        -- left join (
                   -- select
                         -- analysis_date
                         -- ,uuid
                         -- ,sku_price
                         -- ,row_number() over (partition by uuid order by pay_time asc) as rn
                   -- from dwd_data.dwd_t05_reelshort_srv_order_detail_di t
                   -- where etl_date=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)
                         -- and order_type in(1,2)
                   -- ) as t13
        -- on t13.uuid = t10.uuid and t13.rn=1
        -- 获取首次订阅数据
        -- left join (select *
                    -- from (select
                                -- analysis_date
                                -- ,uuid
                                -- ,sku_price
                                -- ,row_number() over (partition by uuid order by pay_time asc) as rn
                          -- from dwd_data.dwd_t05_reelshort_srv_order_detail_di t
                          -- where etl_date=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)  and order_type=1
                          -- )
                  -- ) as t15
        -- on t15.uuid = t10.uuid and t15.rn=1
        left join (
                   select
                         uuid -- 应用中用户ID
                         ,run_id -- 应用每次冷启动启动随机生成
                         -- ,count(distinct cast(nvl(json_extract(properties,'$._channel_orderid'),'') as varchar)) as order_id -- 支付平台订单id (第三方支付平台生成)
                         ,max(sku_price) as sku_price -- 实际支付订单价格 (单位:分)
                   from
                      ( select
                             app_user_id as uuid -- 应用中用户ID
                             ,app_activate_id as run_id -- 应用每次冷启动启动随机生成
                             ,cast(nvl(json_extract(properties,'$._app_orderid'),'') as varchar) as order_id
                             ,cast(nvl(json_extract(properties,'$._order_amount'),0) as int)/100 as sku_price -- 实际支付订单价格 (单位:分)
                        from chapters_log.public_event_data
                        where analysis_date<='${TX_DATE}'
                              and analysis_date>=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)
                              and event_name='m_pay_event'
                              and sub_event_name='pay_complete'
                              and cast(JSON_EXTRACT(properties, '$._channel_sku') as varchar) like '%sub%'
                       )as t
                   inner join
                            (select distinct analysis_date, transaction_id
                             from dwd_data.dwd_t05_reelshort_srv_order_detail_di
                            where etl_date<='${TX_DATE}'
                                  and etl_date>=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)
                                  and pay_status in (3,4)
                                  and merchant_notify_order_id<>''
                                  and merchant_notify_order_id=order_id
                            ) as t1
                    on t1.transaction_id=t.order_id
                   group by uuid,run_id
                  ) as t15
        on t15.uuid = t10.uuid and t15.run_id=t10.run_id and t10.rn=1 and (t15.run_id!='' or t15.run_id is not null)
        -- 获取广告收入数据
        left join (
                    select
                          uuid
                          ,run_id
                          ,sum(cast(ad_revenue as double)) as ad_revenue -- 广告收入
                    from dwd_data.dwd_t07_reelshort_admoney_event_di
                    where etl_date<='${TX_DATE}'
                          and etl_date>=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)
                          and country_id not in ('98', '50')
                          and language_id<>'99'
                          and language_id not like 'zh-%'
                    group by uuid,run_id
                  ) as t14
        on t14.uuid = t10.uuid and t14.run_id = t10.run_id and t10.rn=1 and (t14.run_id!='' or t14.run_id is not null)
        -- 获取用户使用时长:秒
        left join (
                     select
                           uuid
                           ,run_id
                           ,sum(online_time) as sum_online_times
                     from dwd_data.dwd_t01_reelshort_user_online_time_di
                     where etl_date<='${TX_DATE}'
                           and etl_date>=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)
                           and country_id not in ('98', '50')
                           and language_id<>'99'
                           and language_id not like 'zh-%'
                           group by  uuid,run_id
                  ) as t16
        on t16.uuid = t10.uuid and t16.run_id = t10.run_id and t10.rn=1 and (t16.run_id!='' or t16.run_id is not null)
) as t10
on  t10.platform = t1.platform
    and t10.country_id = t8.country_id
    and t10.language_id = t8.language_id
    and t10.version = t8.version
    -- and t10.task_id =t1.task_id
    and t10.uuid = t1.uuid
    -- and t10.run_id=t1.run_id
    and t10.message_id = t1.message_id
    -- and t10.sub_event_name=t1.sub_event_name
left join (
           select
           distinct _id as id
           -- ,value
           ,cast(json_extract(value, '$.desc') as varchar) as task_name
           from chapters_log.dts_project_v_cfg_multi_version
           where key in ('manualFcmConfig', 'autoFcmConfig', 'cachePushConfig')
) as task
on task.id=t1.message_id
where t1.uuid<>''
;