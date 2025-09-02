 delete from dwd_data.dwd_t01_reelshort_user_detail_info_di where etl_date='${TX_DATE}';
replace into  dwd_data.dwd_t01_reelshort_user_detail_info_di
 select      t1.uuid
                ,t1.analysis_date
            ,t1.analysis_date as etl_date
                ,t1.country_id
                ,t1.channel_id
                ,t1.version
                ,t1.cversion
                ,t1.res_version
                ,t1.language_id
                ,t1.platform
                ,t1.os_version
                ,t1.ctimezone_offset
                ,t1.netstatus
                ,t1.install_id
                ,t1.run_id
                ,t1.device_id
                ,t1.ad_id
                ,t1.androidid
                ,t1.idfv
                ,t1.user_ip
                ,t2.register_time
            ,t2.register_dt
            ,t2.first_login_time
            ,t2.last_login_time
            ,t2.first_pay_time
            ,t2.last_pay_time
            ,t2.first_active_time
            ,t2.last_active_time
            ,t2.login_cnt
            ,t2.install_cnt
            ,t2.activity_days
            ,t2.user_type
            ,t2.is_pay
            ,t2.is_login
            ,t3.first_play_start_story_id
            ,t3.last_play_start_story_id
            ,t3.first_play_complete_story_id
            ,t3.last_play_complete_story_id
            ,t3.first_player_play_start_story_id
            ,t3.last_player_play_start_story_id
            ,t3.first_player_play_end_story_id
            ,t3.last_player_play_end_story_id
            ,t3.first_play_start_ctime
            ,t3.last_play_start_ctime
            ,t3.first_play_complete_ctime
            ,t3.last_play_complete_ctime
            ,t3.first_player_play_start_ctime
            ,t3.last_player_play_start_ctime
            ,t3.first_player_play_end_ctime
            ,t3.last_player_play_end_ctime
            ,t4.first_pay_story_id
            ,t5.first_sku_price
            ,t5.last_sku_price
            ,coalesce(t6.af_network_name,'') as af_network_name
            ,coalesce(t6.af_channel,'') as af_channel
            ,coalesce(t6.af_campaign,'') as af_campaign
            ,coalesce(t6.af_adset,'') as af_adset
            ,coalesce(t6.af_ad ,'') as af_ad
            ,coalesce(t6.af_campaign_type ,'') as af_campaign_type
            ,coalesce(t7.device_version,'') as device_version
            ,coalesce(t7.device_brand,'') as device_brand
            ,coalesce(t7.device_model,'') as device_model
            ,coalesce(t8.register_date,'0001-01-01') as srv_register_date
            ,t3.first_play_start_chap_id
            ,t3.last_play_start_chap_id
            ,t3.first_play_complete_chap_id
            ,t3.last_play_complete_chap_id
            ,t3.first_player_play_start_chap_id
            ,t3.last_player_play_start_chap_id
            ,t3.first_player_play_end_chap_id
            ,t3.last_player_play_end_chap_id
            ,coalesce(t9.account_bind_type,'') as account_bind_type
            ,t1.first_srv_sub_sku_price
            ,t1.last_srv_sub_sku_price
            ,cast(coalesce(t2.vip_type,0) as int) as vip_type
            -- add by lcz
            ,nvl(t10.recent_visit_source,'')                     as recent_visit_source -- 最近一次访问来源
            ,nvl(t10.recent_visit_source_type,'')                as recent_visit_source_type -- 最近一次访问来源类型
            ,nvl(t11.first_visit_landing_page,'') as first_visit_landing_page -- 首次访问着陆页
            ,nvl(t11.story_id,'')                 as first_visit_landing_page_story_id -- 首次访问着陆页书籍id
            ,nvl(t11.chap_id,'')                  as first_visit_landing_page_chap_id  -- 首次访问着陆页章节id
from   (
  select coalesce(t1.uuid,t10.uuid) as uuid
            ,coalesce(t1.analysis_date,'${TX_DATE}') as analysis_date
            ,t1.country_id
            ,t1.channel_id
            ,t1.version
            ,t1.cversion
            ,t1.res_version
            ,t1.language_id
            ,t1.platform
            ,t1.os_version
            ,t1.ctimezone_offset
            ,t1.netstatus
            ,t1.install_id
            ,t1.run_id
            ,t1.device_id
            ,t1.ad_id
            ,t1.androidid
            ,t1.idfv
            ,t1.user_ip
        ,t10.first_srv_sub_sku_price
        ,t10.last_srv_sub_sku_price
  from (
      select *
      from
      (select     *
          ,row_number()over(partition by uuid,analysis_date  order by last_active_time desc ) as rank
       from dwd_data.dwd_t01_reelshort_user_info_di
       where etl_date ='${TX_DATE}'
      )tt1
     where rank=1
   )t1
  full join  (
    --   select uuid
    --         ,etl_date
    --         ,max(case when rank=1 then sku_price end ) as first_srv_sub_sku_price
    --         ,max(case when desc_rank=1 then sku_price end ) as last_srv_sub_sku_price
    -- from
    --   (select uuid
    --          ,etl_date
    --          ,cast(sku_price as int) as sku_price
    --          ,row_number()over(partition by uuid order by end_time asc ) as rank
    --          ,row_number()over(partition by uuid order by end_time desc  ) as desc_rank
    --    from dwd_data.`dwd_t05_reelshort_srv_order_detail_di`
    --    where etl_date ='${TX_DATE}' and channel_sku like '%sub%'
    --   )
    -- where rank=1 or desc_rank=1
    -- group by uuid ,etl_date
    select
          uuid
          ,cast(max(case when rank=1 then sku_price end ) as int) as first_srv_sub_sku_price
          ,cast(max(case when desc_rank=1 then sku_price end ) as int) as last_srv_sub_sku_price
    from
    (
        select
            uid as uuid
            ,cast(amount*100 as int) as sku_price
            ,row_number()over(partition by uid order by end_time asc ) as rank
            ,row_number()over(partition by uid order by end_time desc  ) as desc_rank
        from chapters_log.dts_project_v_payment
        where  end_time>=UNIX_TIMESTAMP(convert_tz('${TX_START_TIMESTAMP}','America/Los_Angeles','Asia/Shanghai'))
          and end_time<=UNIX_TIMESTAMP(convert_tz('${TX_END_TIMESTAMP}','America/Los_Angeles','Asia/Shanghai'))
          and  channel_id not like 'TEST%' and product_id rlike 'sub'
          and  order_status in (3,4) and is_abnormal=0 and sandbox=0
          and uid not in (124268225,124082749)  -- 测试账号
    )
    where rank=1 or desc_rank=1
    group by uuid
)t10
on t1.uuid=t10.uuid
)t1
left join (
        select uuid
         ,analysis_date
         ,min(register_time) as register_time
         ,min(date(register_time)) as register_dt
         ,min(first_login_time) as first_login_time
         ,max(last_login_time) as last_login_time
         ,min(first_pay_time) as first_pay_time
         ,max(last_pay_time) as last_pay_time
         ,min(first_active_time) as first_active_time
         ,max(last_active_time) as last_active_time
         ,min(first_play_start_time) as first_play_start_time
         ,max(last_play_start_time) as last_play_start_time
         ,min(first_play_end_time) as first_play_end_time
         ,max(last_play_end_time) as last_play_end_time
         ,count(distinct case when is_login=1 then  run_id  end ) as login_cnt
         ,count(distinct install_id ) as install_cnt
         ,count(distinct analysis_date) as activity_days
         ,max(user_type) as user_type
         ,max(is_pay) as is_pay
         ,max(is_login) as is_login
         ,max(vip_type) as vip_type
        from dwd_data.dwd_t01_reelshort_user_info_di
        where etl_date ='${TX_DATE}'
        group by uuid ,analysis_date
)t2
on t1.uuid=t2.uuid   and t1.analysis_date=t2.analysis_date
left join (
   select uuid
         ,analysis_date
         ,max(case when rank=1 and  sub_event_name='play_start'   then story_id end)  as first_play_start_story_id
         ,max(case when desc_rank=1 and  sub_event_name='play_start'   then story_id end) as last_play_start_story_id
         ,max(case when rank=1 and  sub_event_name='play_end'   then story_id end) as first_play_complete_story_id
         ,max(case when desc_rank=1 and  sub_event_name='play_end'   then story_id end) as last_play_complete_story_id
         ,max(case when rank=1 and  sub_event_name='play_start' and player_flag='player'  then story_id end)  as first_player_play_start_story_id
         ,max(case when desc_rank=1 and  sub_event_name='play_start' and player_flag='player'  then story_id end) as last_player_play_start_story_id
         ,max(case when rank=1 and  sub_event_name='play_end' and player_flag='player'  then story_id end) as first_player_play_end_story_id
         ,max(case when desc_rank=1 and  sub_event_name='play_end' and player_flag='player'  then story_id end) as last_player_play_end_story_id
         ,max(case when rank=1 and  sub_event_name='play_start'   then min_ctime end)  as first_play_start_ctime
         ,max(case when desc_rank=1 and  sub_event_name='play_start'   then max_ctime end) as last_play_start_ctime
         ,max(case when rank=1 and  sub_event_name='play_end'   then min_ctime end) as first_play_complete_ctime
         ,max(case when desc_rank=1 and  sub_event_name='play_end'   then max_ctime end) as last_play_complete_ctime
         ,max(case when rank=1 and  sub_event_name='play_start' and player_flag='player'  then min_ctime end)  as first_player_play_start_ctime
         ,max(case when desc_rank=1 and  sub_event_name='play_start' and player_flag='player'  then max_ctime end) as last_player_play_start_ctime
         ,max(case when rank=1 and  sub_event_name='play_end' and player_flag='player'  then min_ctime end) as first_player_play_end_ctime
         ,max(case when desc_rank=1 and  sub_event_name='play_end' and player_flag='player'  then max_ctime end) as last_player_play_end_ctime
         ,max(case when rank=1 and  sub_event_name='play_start'   then chap_id end)  as first_play_start_chap_id
         ,max(case when desc_rank=1 and  sub_event_name='play_start'   then chap_id end) as last_play_start_chap_id
         ,max(case when rank=1 and  sub_event_name='play_end'   then chap_id end) as first_play_complete_chap_id
         ,max(case when desc_rank=1 and  sub_event_name='play_end'   then chap_id end) as last_play_complete_chap_id
         ,max(case when rank=1 and  sub_event_name='play_start' and player_flag='player'  then chap_id end)  as first_player_play_start_chap_id
         ,max(case when desc_rank=1 and  sub_event_name='play_start' and player_flag='player'  then chap_id end) as last_player_play_start_chap_id
         ,max(case when rank=1 and  sub_event_name='play_end' and player_flag='player'  then chap_id end) as first_player_play_end_chap_id
         ,max(case when desc_rank=1 and  sub_event_name='play_end' and player_flag='player'  then chap_id end) as last_player_play_end_chap_id
   from (
        select uuid
             ,analysis_date
             ,sub_event_name
             ,player_flag
             ,story_id
             ,chap_id
             ,min_ctime
             ,max_ctime
             ,row_number()over(partition by uuid ,analysis_date,sub_event_name,player_flag order by min_ctime asc ) as rank
             ,row_number()over(partition by uuid ,analysis_date,sub_event_name,player_flag order by max_ctime desc ) as desc_rank
       from(
            select uuid
               ,analysis_date
               ,sub_event_name
               ,if(page_name='player','player',type) as player_flag
               ,story_id
               ,chap_id
               ,max(ctime) as max_ctime
               ,min(ctime) as min_ctime
           from  dwd_data.dwd_t02_reelshort_play_event_di
           where etl_date='${TX_DATE}' and story_id not in ('','0') and sub_event_name in ('play_start','play_end')
           and type in ('begin','complete')
           group by uuid
               ,analysis_date
               ,sub_event_name
               ,if(page_name='player','player',type)
               ,story_id
               ,chap_id
        )t1
    )t
   where rank=1 or desc_rank=1
   group by uuid,analysis_date
)t3
on t1.uuid=t3.uuid and t1.analysis_date=t3.analysis_date
left join (
     select distinct  uuid,first_value(story_id)over(partition by uuid order by ctime asc ) as first_pay_story_id
     from dwd_data.dwd_t05_reelshort_order_detail_di
     where order_status=1 and order_id_rank=1  and story_id not in ('0' ,'')  AND etl_date ='${TX_DATE}'
)t4
on t1.uuid=t4.uuid
left join (
    select uuid
          ,max(case when rank=1 then sku_price end ) as first_sku_price
          ,max(case when desc_rank=1 then sku_price end ) as last_sku_price
  from
    (select uuid
           ,sku_price
           ,row_number()over(partition by uuid order by ctime asc ) as rank
           ,row_number()over(partition by uuid order by ctime desc  ) as desc_rank
     from dwd_data.dwd_t05_reelshort_order_detail_di
     where order_status=1 and order_id_rank=1   AND     etl_date ='${TX_DATE}'
    )
  where rank=1 or desc_rank=1
  group by uuid
)t5
on t1.uuid=t5.uuid
left join  (
  select uuid ,af_network_name,af_channel,af_campaign,af_adset,af_ad  ,af_campaign_type
  from
  (select  uuid
         ,af_network_name
         ,af_channel
         ,campaign as af_campaign
         ,af_adset
         ,af_ad
         ,af_campaign_type
         ,row_number()over(partition by uuid order by analysis_date desc ) as rank
  from dwd_data.dwd_t01_reelshort_appsflyer_device_uuid
 WHERE analysis_date<='${TX_DATE}'
  )t
  where rank=1
) t6
on t1.uuid=t6.uuid
left join (
   select uuid,device_id,device_version,device_brand,device_model
   from dwd_data.dwd_t01_reelshort_device_info
   where updated_at>='${TX_START_TIMESTAMP}' and device_id<>''
)t7
on t1.uuid=t7.uuid and t1.device_id=t7.device_id
left join (
    select  uid as uuid ,min(date(convert_tz(from_unixtime(reg_time), 'Asia/Shanghai', 'America/Los_Angeles'))) as register_date
    from chapters_log.dts_project_v_user
    -- where uid in (select distinct uuid from  dwd_data.dwd_t01_reelshort_user_info_di where etl_date ='${TX_DATE}')
    group by uid
)t8
on t1.uuid=t8.uuid
left join (
  select uuid ,account_bind_type
  from(select uuid
        ,app_account_bindtype as account_bind_type
        ,row_number()over(partition by uuid order by stime desc ) as rank
  from dwd_data.dwd_t01_reelshort_bindaccount_di
  where  etl_date ='${TX_DATE}' and action='complete'
  )
  where rank=1
)t9
on t1.uuid=t9.uuid
-- add by lcz
left join (
           select
               uuid
            ,recent_visit_source -- 最近一次访问来源
            ,recent_visit_source_type -- 最近一次访问来源类型
            ,row_number() over( partition by uuid order by stime asc) as rn
           from
               (select
               uuid
               ,min(stime) as stime
               ,case
                   when referrer_url like '%google%'     then 'google organic'
                   when referrer_url like '%reelshort%' or referrer_url is null  then 'direct'
                   when referrer_url like '%facebook%'   then 'facebook referral'
                   when referrer_url like '%youtube%'    then 'youtube referral'
                   when referrer_url like '%bing%'       then 'bing organic'
                   else 'other'
                end as recent_visit_source -- 最近一次访问来源
               ,case
                   when url like '%campaign_id%' then '付费推广'
                   else '自然量'
                end as recent_visit_source_type -- 最近一次访问来源类型
               from dwd_data.dwd_t01_reelshort_device_start_di as t
               where etl_date='${TX_DATE}'
                     and event_name='m_app_install'
               group by 
                       uuid
                       ,recent_visit_source
                       ,recent_visit_source_type
               union all
               select
               uuid
               ,min(stime) as stime
               ,case
                   when referrer_url like '%google%'     then 'google organic'
                   when referrer_url like '%reelshort%' or referrer_url is null  then 'direct'
                   when referrer_url like '%facebook%'   then 'facebook referral'
                   when referrer_url like '%youtube%'    then 'youtube referral'
                   when referrer_url like '%bing%'       then 'bing organic'
                   else 'other'
                end as recent_visit_source -- 最近一次访问来源
               ,case
                   when url like '%campaign_id%' then '付费推广'
                   else '自然量'
                end as recent_visit_source_type -- 最近一次访问来源类型
               from dwd_data.dwd_t01_reelshort_user_login_info_di as t
               where etl_date='${TX_DATE}'
                     and event_name='m_user_signin'
               group by 
                       uuid
                       ,recent_visit_source
                       ,recent_visit_source_type 
               union all
               select
               uuid
               ,min(stime) as stime
               ,case
                   when referrer_url like '%google%'     then 'google organic'
                   when referrer_url like '%reelshort%' or referrer_url is null  then 'direct'
                   when referrer_url like '%facebook%'   then 'facebook referral'
                   when referrer_url like '%youtube%'    then 'youtube referral'
                   when referrer_url like '%bing%'       then 'bing organic'
                   else 'other'
                end as recent_visit_source -- 最近一次访问来源
               ,case
                   when url like '%campaign_id%' then '付费推广'
                   else '自然量'
                end as recent_visit_source_type -- 最近一次访问来源类型
               from dwd_data.dwd_t02_reelshort_page_enter_exit_di as t
               where etl_date='${TX_DATE}'
                     and event_name='m_page_enter'
               group by 
                       uuid
                       ,recent_visit_source
                       ,recent_visit_source_type   
               ) as t
) as t10
on t10.uuid=t1.uuid and t10.rn=1
left join (
           select
           uuid
           ,SUBSTRING_INDEX(url,'.com/',-1) as first_visit_landing_page -- 首次访问着陆页
           ,story_id                        as story_id -- 书籍id
           ,chap_id                         as chap_id -- 章节id
           ,row_number() over( partition by uuid order by stime asc) as rn
           from dwd_data.dwd_t01_reelshort_device_start_di as t
           where etl_date='${TX_DATE}'
                 and event_name='m_app_install'
) as t11
on t11.uuid=t1.uuid and t11.rn=1
;

