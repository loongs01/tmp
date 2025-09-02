 delete from dwd_data.dwd_t04_reelshort_opc_push_detail_di  where analysis_date='${TX_DATE}';
/*+ cte_execution_mode=shared */
insert into  dwd_data.dwd_t04_reelshort_opc_push_detail_di 
with push_log_temp as (
   select  t1.app_id
          ,t1.task_id
          ,coalesce(t1.analytics_label,t1.task_id) as analytics_label
          ,if(t1.uuid='',t2.uid,t1.uuid) as uuid
          ,split_part(t1.token,':',1) as instance_id
          ,split_part(message_id,'/',4) as message_id
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
    and  t1.analysis_date='${TX_DATE}'
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
 where analysis_date='${TX_DATE}' and project_name='reelshort'
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
    where app_id = 'cm1009'  and date(start_time)='${TX_DATE}'
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
,active_user as (
                 select
                       analysis_date
                       ,etl_date
                       ,country_id
                       ,language_id
                       ,channel_id
                       ,user_type
                       ,uuid
                       ,ad_revenue
                       ,sum_online_times
                 from dws_data.dws_t80_reelshort_user_bhv_cnt_detail_di -- reelshort用户当天动作统计类信息详情表
                 where etl_date='${TX_DATE}'
                )
select
      null as id -- 自增id
      ,t1.task_id
      ,t1.uuid
      ,t1.analytics_label
      ,t1.analysis_date
      ,t1.app_id
      ,t1.instance_id
      ,t1.message_id
      -- ,if(t1.task_id=0,'api','opc') as create_channel
      ,coalesce(t1.platform,t8.platform,'ALL') AS platform
      -- ,coalesce(t2.title,t7.title,t9.title) as title
      -- ,coalesce(t2.content,t7.content,t9.content) as content
      -- ,coalesce(t2.creator,t9.creator) as creator
      ,coalesce(t2.push_type,t7.push_type,t9.push_type) as push_type
      -- ,t1.start_time
      -- ,t1.end_time
      ,t2.planned_quantity
      ,t2.sent_quantity
      ,coalesce(t2.last_send_time_at ,t1.end_time) as last_send_time_at
      ,coalesce(t2.status,t9.status) as status
      ,t1.success as is_success
      ,cast(coalesce(t1.accepted_cnt ,0) as int) as accepted_cnt
      ,cast(coalesce(t1.delivered_cnt,0) as int) as delivered_cnt -- 接收次数
      ,coalesce(t3.show_flag,0) as show_flag
      ,coalesce(t3.open_flag,0) as open_flag
      ,coalesce(t4.user_type,0) as user_type
      ,if(t4.is_pay=1 or t8.first_pay_time is not null,1,0) as lifecycle_purchase_status -- 生命周期内购状态 :1-是，0-否
      ,coalesce(t8.srv_vip_type_lt,0)                       as lifecycle_subscribe_status -- 生命周期订阅状态:1-是，0-否
      ,coalesce(t4.is_login,0) as is_login
      -- ,coalesce(t4.is_pay,0) as is_pay
      -- ,coalesce(t6.read_30min_flag,0) as read_30min_flag
      -- ,cast(coalesce(t5.pay_amount,0) as int) as pay_amount
      ,t8.country_id                                     -- 国家id
      ,t8.language_id                                    -- 语言id
      ,t8.channel_id                                     -- 渠道id
      ,t8.version                                        -- APP的应用版本(包体内置版本号versionname)
      -- ,t8.cversion
      -- ,nvl(t2.schedule_type,-1) as schedule_type
      -- ,if(t10.uuid is not null ,1,0)                  -- 是否点击
      -- ,coalesce(t10.click_cnt,0)                      -- 点击次数
      ,coalesce(t10.is_play,0)                           -- 是否播放
      ,coalesce(t10.play_cnt,0)                          -- 播放次数
      ,coalesce(t10.is_pay,0)                            -- 是否内购
      ,coalesce(cast(t10.sku_price as decimal(18, 4)),0)                         -- 内购支付订单价格
      ,coalesce(t10.is_first_subscribe,0)                -- 是否首次订阅
      ,coalesce(cast(t10.first_subscribe_sku_price as decimal(18, 4)),0)         -- 首次订阅金额
      ,coalesce(t10.is_subscribe,0)                      -- 是否订阅
      ,coalesce(cast(t10.subscribe_price as decimal(18, 4)),0)                   -- 订阅单价
      ,coalesce(t10.is_ad_revenue,0)                     -- 是否广告收入用户
      ,coalesce(cast(t10.ad_revenue as decimal(18, 4)),0)                        -- 广告收入
      ,coalesce(cast(t10.sum_online_times as int),0)                  -- 用户使用时长:分钟
from (
      select t1.task_id
            ,t1.uuid
            ,coalesce(t1.analytics_label ,t0.analytics_label) as analytics_label
            ,t1.analysis_date
            ,t1.app_id
            ,t1.instance_id
            ,t1.message_id
            ,coalesce(t1.sdk_platform,t0.sdk_platform) as sdk_platform
            ,t0.accepted_cnt
            ,t0.delivered_cnt
            ,t1.success
            ,t1.start_time
            ,t1.end_time
			,t1.platform
      from  push_log_temp t1
      left join  (
        SELECT trim(analytics_label) as analytics_label
              ,analysis_date
              ,app_id
              ,instance_id
              ,message_id
              ,sdk_platform
              ,count(if(event="MESSAGE_ACCEPTED"  ,id)) as accepted_cnt
              ,count(if(event="MESSAGE_DELIVERED" ,id)) as delivered_cnt
           FROM firebase_temp
           WHERE  event IN ("MESSAGE_ACCEPTED","MESSAGE_DELIVERED")
           GROUP BY analytics_label,analysis_date,app_id,instance_id,message_id,sdk_platform
         )t0
          on t0.message_id=t1.message_id and t0.app_id=t1.app_id  --  and t0.analytics_label=t1.task_id
            and t0.instance_id=t1.instance_id
            and t0.analysis_date=t1.analysis_date
)t1
left join (select * from push_task_info_temp where rank=1 )t2
on t1.task_id=t2.task_id
left join (
           select   analysis_date
              ,app_user_id as uuid
              ,case  os_type  when '1' then 'ANDROID' when '2' then 'IOS' end as sdk_platform
              ,cast(JSON_EXTRACT(properties, '$.fcm_message_id') as varchar) as message_id
              ,max(if(cast(JSON_EXTRACT(properties, '$._action') as varchar)='show' ,1,0)) as show_flag
              ,max(if(cast(JSON_EXTRACT(properties, '$._action') as varchar)='click',1,0)) as open_flag
         from chapters_log.reelshort_event_data_custom
         where analysis_date='${TX_DATE}' and event_name='m_custom_event' and sub_event_name='fcm_push_stat'
         and app_id='cm1009' and app_user_id<>''
         group by analysis_date,app_user_id,sdk_platform ,message_id
)t3
on t1.analysis_date=t3.analysis_date and t1.uuid=t3.uuid  and t1.message_id=t3.message_id
   and t1.sdk_platform=t3.sdk_platform
left join (
  select uuid
      ,case  platform  when 1 then 'ANDROID' when 2 then 'IOS' end as sdk_platform
      ,analysis_date
      ,max(user_type) as user_type
      ,max(is_login) as is_login
      ,max(is_pay) as is_pay
  from dwd_data.dwd_t01_reelshort_user_info_di -- V项目dwd层:当日所有用户表
  where etl_date='${TX_DATE}'
  group by uuid ,analysis_date ,sdk_platform
 )t4
on t1.analysis_date=t4.analysis_date and t1.uuid=t4.uuid  and t1.sdk_platform=t4.sdk_platform
left join (
   select uuid
         ,analysis_date
         ,case  platform  when 1 then 'ANDROID' when 2 then 'IOS' end as sdk_platform
         ,sum(sku_price) as pay_amount
    from dwd_data.dwd_t05_reelshort_order_detail_di
    where etl_date='${TX_DATE}'   and order_status=1  and order_id_rank=1
   group by uuid ,analysis_date ,sdk_platform
 )t5
on t1.analysis_date=t5.analysis_date and t1.uuid=t5.uuid  and t1.sdk_platform=t5.sdk_platform
 -- left join (
 --   select uuid
 --          ,analysis_date
 --          ,case  platform  when 1 then 'ANDROID' when 2 then 'IOS' end as sdk_platform
 --          ,cast(sum(online_time) as int ) as   read_time
 --          ,1 as read_30min_flag
 --   from dwd_data.dwd_t01_reelshort_user_online_time_di
 --   where etl_date='${TX_DATE}' and platform<=2 and scene_id in (5,6)
 --   group by uuid ,analysis_date ,sdk_platform
 --   having read_time>=1800
 -- )t6
 --  ON t1.uuid=t6.uuid and t1.analysis_date=t6.analysis_date and t1.sdk_platform=t6.sdk_platform
left join chapters_log.opc_api_push_tasks_info t7
on t1.analytics_label=t7.analytics_label and t1.app_id=t7.app_id
left join (select
                 uuid
                 ,country_id
                 ,language_id
				 ,platform
                 ,case  platform  when 1 then 'ANDROID' when 2 then 'IOS' end as sdk_platform
                 ,channel_id
                 ,user_type
                 ,version
                 ,cversion
                 ,srv_vip_type_lt -- 生命周期订阅状态:1-是，0-否
                 ,first_pay_time  -- 首次支付时间
           from dwd_data.dim_t99_reelshort_user_lt_info -- v项目dwd层:全量用户码表
           where etl_date='${TX_DATE}'
)t8
on t1.uuid=t8.uuid
left join  (select * from push_task_info_temp_n2 where rank=1 ) t9
on t1.task_id=t9.task_id
-- 获取点击数据
left join (
        select
              t10.uuid
              ,t10.task_id
              ,t10.message_id
              ,t10.sdk_platform
              ,t10.country_id
              ,t10.language_id
              ,t10.version
              -- ,t10.click_cnt                                  -- 点击次数
              ,if(t11.uuid is not null,1,0) as is_play           -- 是否播放
              ,t11.play_cnt                                      -- 播放次数
              ,if(t12.uuid is not null,1,0) as is_pay            -- 是否内购
              ,t12.sku_price                                     -- 内购金额
              ,is_first_subscribe                                -- 是否首次订阅
              ,first_subscribe_sku_price                         -- 首次订阅金额
              ,if(t13.uuid is not null,1,0) as is_subscribe      -- 是否订阅
              ,t13.subscribe_price                               -- 订阅金额
              ,if(t14.ad_revenue>0,1,0) as is_ad_revenue         -- 是否广告收入用户
              ,t14.ad_revenue                                    -- 广告收入
              ,t14.sum_online_times                              -- 用户使用时长:分钟
        from (
              -- 获取点击数据
              select uuid, task_id, message_id
              -- new add
              ,case  platform  when 1 then 'ANDROID' when 2 then 'IOS' end as sdk_platform
              ,country_id
              ,language_id
              ,version
              -- ,count(distinct click_time) as click_cnt
              from dwd_data.dwd_t04_reelshort_opc_click_di
              where analysis_date='${TX_DATE}'
                    and date(click_time)='${TX_DATE}'
              group by
                uuid
                ,task_id
                ,message_id
                ,case  platform  when 1 then 'ANDROID' when 2 then 'IOS' end
                ,country_id
                ,language_id
                ,version
              ) t10
        -- 获取播放数据
        left join (
                  select t1.uuid, count( distinct t1.stime) as play_cnt -- 播放次数
                  from dwd_data.dwd_t02_reelshort_play_event_di t1
                  where t1.sub_event_name='play_start'
                      and t1.story_id<>''
                      and t1.country_id not in ('50', '98')
                      and t1.chap_id<>''
                      and t1.etl_date='${TX_DATE}'
                     -- and uuid in (select distinct uuid from click_user_detail)
                     and date(t1.stime)='${TX_DATE}'
                     group by t1.uuid
                  ) as t11
        on t11.uuid = t10.uuid
        -- 获取内购付费数据
        left join (
                   select
                         distinct t1.uuid
                         ,t1.sku_price / 100 as sku_price                                             -- 内购金额
                         ,if(t2.point is not null,1,0) as is_first_subscribe                          -- 是否首次订阅
                         ,if(t2.point is not null,t1.sku_price / 100,0) as first_subscribe_sku_price  -- 首次订阅金额
                         -- , t1.order_id as order_id
                         from dwd_data.dwd_t05_reelshort_order_detail_di as t1
                         left join  chapters_log.dts_project_v_shopping_price_point t2 -- reelshort sku商品表
                         on t2.type in (9, 10)
                         and t2.point=t1.channel_sku  -- 判断是否首订
                         where t1.order_status = 1
                             and t1.order_id_rank=1
                             and t1.country_id not in ('50', '98')
                             and t1.etl_date='${TX_DATE}'
                             and date(t1.stime)='${TX_DATE}'
                   ) as t12
        on t12.uuid = t10.uuid
        -- 获取订阅数据
        left join (
                   select distinct uid as uuid
                          , amount as subscribe_price -- 订阅单价
                          -- , merchant_order_id as parm7
                   from chapters_log.dts_project_v_payment
                   where is_abnormal=0
                         and order_status in (3)
                         and order_type in (9, 10)
                         and convert_tz(from_unixtime(end_time), 'Asia/Shanghai', 'America/Los_Angeles')='${TX_DATE}'
                   ) t13
        on t13.uuid = t10.uuid
        -- 获取广告收入数据
        left join (select uuid
                          ,sum(ad_revenue) as ad_revenue                  -- 广告收入
                          ,sum(sum_online_times/60) as sum_online_times   -- 用户使用时长:分钟
                   from active_user -- reelshort用户当天动作统计类信息详情表
                   where country_id not in ('98', '50')
                       and language_id<>'99'
                       and language_id not like 'zh-%'
                       and etl_date='${TX_DATE}'
                   group by uuid
                   ) as t14
        on t14.uuid = t10.uuid
) as t10
on  t10.sdk_platform = t8.sdk_platform
    and t10.country_id = t8.country_id
    and t10.language_id = t8.language_id
    and t10.version = t8.version
    and t10.task_id =t1.task_id
    and t10.uuid = t1.uuid
    and t10.message_id = t1.message_id
-- left join active_user as t11
-- on t11.etl_date = '${TX_DATE}'
--    and t11.country_id = t8.country_id
--    and t11.language_id = t8.language_id
--    and t11.channel_id = t8.channel_id
--    and t11.user_type = t8.user_type

where t1.uuid<>''
-- temp
-- and t10.is_pay=1
-- and t10.sum_online_times>0
-- limit 10;




