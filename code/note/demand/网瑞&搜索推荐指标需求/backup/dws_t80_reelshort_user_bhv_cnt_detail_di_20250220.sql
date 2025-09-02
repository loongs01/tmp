-- alter table dws_data.`dws_t80_reelshort_user_bhv_cnt_detail_di` add column trans_complete_amt decimal(18,4) comment '订单支付金额';
delete from  dws_data.`dws_t80_reelshort_user_bhv_cnt_detail_di`  where etl_date='${TX_DATE}';

insert into dws_data.`dws_t80_reelshort_user_bhv_cnt_detail_di` 
select  t1.id 
       ,t1.uuid -- '应用中用户ID'
       ,t1.analysis_date --  '分析日期'
       ,t1.etl_date
       ,t1.country_id -- '国家id'
       ,t1.channel_id -- '应用渠道'
       ,t1.version -- 'APP的应用版本(包体内置版本号versionname)'
       ,t1.cversion -- '应用游戏版本号'
       ,t1.res_version -- '应用资源版本号'
       ,t1.language_id -- '游戏语言id'
       ,t1.platform -- '操作系统类型 (android/ios/windows/mac os)'
       ,t1.os_version -- '操作系统版本号'
       ,t1.device_id -- '应用用于标识设备的唯一ID：安卓是androidid，IOS-idfv'
       ,t1.ad_id -- '广告ID:Android=google adid IOS=idfa'
       -- ,t1.androidid -- 'ANDROIDID'
       -- ,t1.idfv -- 'IOS-idfv'
       ,max(t1.user_type)over(partition by t1.uuid,t1.etl_date) as user_type
       ,max(t1.is_pay)over(partition by t1.uuid,t1.etl_date) as is_pay
       ,max(t1.is_login)over(partition by t1.uuid,t1.etl_date) as is_login
       ,coalesce(max(t1.register_time)over(partition by t1.uuid,t1.etl_date),t11.register_time) as register_time
       ,t1.first_login_time
       ,t1.last_login_time
       ,t1.first_pay_time
       ,t1.last_pay_time
       ,t1.first_play_start_time
       ,t1.last_play_start_time
       ,t1.first_play_end_time
       ,t1.last_play_end_time
       ,t1.first_active_time
       ,t1.last_active_time
       ,cast(case when t1.is_login=1 then t1.login_cnt else t1.login_cnt-1 end as int) as login_cnt
       ,cast(case when t1.user_type=1 then t1.install_cnt else t1.install_cnt-1 end as int ) as install_cnt
       ,coalesce(cast(t2.order_cnt as int ),0) as order_cnt
       ,coalesce(cast(t2.invalid_order_cnt as int ),0) as invalid_order_cnt
       ,coalesce(cast(t2.pay_amount as int ),0) as pay_amount
       ,coalesce(cast(t2.invalid_pay_amount as int ),0) as invalid_pay_amount
       ,coalesce(cast(t2.max_pay_amount as int ),0) as max_pay_amount
       ,coalesce(cast(t2.min_pay_amount as int ),0) as min_pay_amount
       ,coalesce(cast(t3.main_scene_times as int ),0)  as main_scene_times
       ,coalesce(cast(t3.main_play_times as int ),0)  as main_play_times
       ,coalesce(cast(t3.chap_play_times as int ),0)  as chap_play_times
       ,coalesce(cast(t3.sum_online_times as int ),0)  as sum_online_times
       ,coalesce(cast(t4.coins_get as int),0) as coins_get
       ,coalesce(cast(t4.coins_exp as int),0) as coins_exp
       ,coalesce(cast(t4.pay_coins_get as int),0) as pay_coins_get
       ,coalesce(cast(t4.pay_coins_exp as int),0) as pay_coins_exp
       ,coalesce(cast(t4.free_coins_get as int),0) as free_coins_get
       ,coalesce(cast(t4.free_coins_exp as int),0) as free_coins_exp
      ,coalesce(cast(t6.trans_show_cnt as int),0) as trans_show_cnt
      ,coalesce(cast(t6.trans_start_cnt as int),0) as trans_start_cnt
      ,coalesce(cast(t6.trans_end_cnt as int),0) as trans_end_cnt
      ,coalesce(cast(t6.trans_complete_cnt as int),0) as trans_complete_cnt
      ,coalesce(cast(t6.trans_failed_cnt as int),0) as trans_failed_cnt
      ,coalesce(cast(t6.trans_cancel_cnt as int),0) as trans_cancel_cnt
      ,t8.u_act_analysis_hour
      ,t8.u_act_analysis_2hour
      ,t8.u_act_analysis_3hour
      ,t8.u_act_analysis_4hour
      ,coalesce(t11.account_bind_type,'') as account_bind_type
      ,nvl(t12.af_network_name,'') as af_network_name
      ,nvl(t12.af_channel,'') as af_channel
      ,nvl(t12.af_campaign_type,'') as af_campaign_type
      ,coalesce(cast(t4.bonus_get as int),0) as bonus_get
      ,coalesce(cast(t4.bonus_exp as int),0) as bonus_exp
      ,coalesce(cast(t4.pay_bonus_get as int),0) as pay_bonus_get
      ,coalesce(cast(t4.pay_bonus_exp as int),0) as pay_bonus_exp
      ,coalesce(cast(t4.free_bonus_get as int),0) as free_bonus_get
      ,coalesce(cast(t4.free_bonus_exp as int),0) as free_bonus_exp
      ,nvl(t12.af_campaign,'') as af_campaign
      ,nvl(t12.af_adset,'') as af_adset
      ,nvl(t12.af_ad,'') as af_ad
      ,case when  t11.lt_last_active_date<=date_sub('${TX_DATE}',90) then 1 else 0 end as reactive_flag
      ,case when  t11.lt_last_active_date<=date_sub('${TX_DATE}',30) then 1 
             else cast(max(t1.user_type)over(partition by t1.uuid,t1.etl_date) as int) 
        end as new_active_flag
      ,coalesce(cast(t5.ad_invoke_cnt as int ),0) as ad_invoke_cnt
      ,coalesce(cast(t5.ad_start_cnt as int ),0) as ad_start_cnt
      ,coalesce(cast(t5.ad_complete_cnt as int ),0) as ad_complete_cnt
      ,coalesce(cast(t5.ad_loadfail_cnt as int ),0) as ad_loadfail_cnt
      ,coalesce(cast(t5.ad_playfail_cnt as int ),0) as ad_playfail_cnt
      ,coalesce(cast(t5.ad_click_cnt as int ),0) as ad_click_cnt
      ,coalesce(cast(t5.ad_impression_cnt as int ),0) as ad_impression_cnt
      ,coalesce(cast(t5.ad_revenue as decimal(18,4) ),0) as ad_revenue 
      ,t13.install_analysis_date 
      ,coalesce(cast(t7.dlink_invoke_cnt as int),0) as dlink_invoke_cnt
      ,coalesce(cast(t7.dlink_complete_cnt as int),0) as dlink_complete_cnt
      ,coalesce(cast(t7.dlink_cancel_cnt as int),0) as dlink_cancel_cnt
      ,coalesce(cast(t7.dlink_timeout_cnt as int),0) as dlink_timeout_cnt
      ,coalesce(cast(t7.dlink_error_cnt as int),0) as dlink_error_cnt
      ,coalesce(cast(t7.dlink_deferred_cnt as int),0) as dlink_deferred_cnt
      ,coalesce(cast(t7.dlink_normal_cnt as int),0) as dlink_normal_cnt
      ,coalesce(cast(t9.srv_order_cnt as int ),0) as srv_order_cnt
      ,coalesce(cast(t9.srv_pay_amount as int ),0) as srv_pay_amount
      ,t13.first_install_date_90d
      ,case when first_install_date_90d is null then 0   -- 安装在90天外，默认
            when max(t1.user_type)over(partition by t1.uuid,t1.etl_date)=1 and first_install_date_90d=t1.analysis_date then 1 -- 首次安装
            when first_install_date_90d<>t1.analysis_date  then 1 -- reinstall安装
            when date(t11.register_time)<=date_sub('${TX_DATE}',90) and first_install_date_90d=t1.analysis_date then 2 -- 重定向
       else 0 end as install_type
      ,coalesce(cast(cli_sub_order_cnt as int ),0) as cli_sub_order_cnt
      ,coalesce(cast(cli_sub_pay_amount as int ),0) as cli_sub_pay_amount
      ,coalesce(cast(srv_sub_order_cnt as int ),0) as srv_sub_order_cnt
      ,coalesce(cast(srv_sub_pay_amount as int ),0) as srv_sub_pay_amount -- 活跃用户的
      ,coalesce(t10.srv_coin_get,0) as srv_coin_get
      ,coalesce(t10.srv_coin_exp,0) as srv_coin_exp
      ,coalesce(t10.srv_pay_bonus_get,0) as srv_pay_bonus_get
      ,coalesce(t10.srv_pay_bonus_exp,0) as srv_pay_bonus_exp
      ,coalesce(t10.srv_free_bonus_get,0) as srv_free_bonus_get
      ,coalesce(t10.srv_free_bonus_exp,0) as srv_free_bonus_exp
      ,coalesce(cast(t2.cli_non_sub_pay_amount as int ),0) as cli_non_sub_pay_amount 
      ,case when  t11.lt_last_active_date<=date_sub('${TX_DATE}',30) then 1 else 0 end as reactive_flag_30d
      ,coalesce(max(t1.vip_type)over(partition by t1.uuid,t1.etl_date) ,0) as vip_type
      ,case when  max(t1.user_type)over(partition by t1.uuid,t1.etl_date)=1 then 1 
            when t11.lt_last_active_date<'${TX_DATE}' AND t11.lt_last_active_date>=date_sub('${TX_DATE}',6)  THEN 2
            when t11.lt_last_active_date<date_sub('${TX_DATE}',6) and t11.lt_last_active_date>=date_sub('${TX_DATE}',29)   then 3
            when t11.lt_last_active_date<=date_sub('${TX_DATE}',30) then 4  
            else -1
      end AS active_user_type
      ,coalesce(cast(t2.cli_non_sub_order_cnt as int ),0) as cli_non_sub_order_cnt 
      ,coalesce(cast(t5.incentive_ad_revenue as decimal(18,4) ),0) as incentive_ad_revenue 
      ,nvl(t14.dlink_story_id,'') as dlink_story_id
      ,nvl(t14.media_type,'') as media_type
	  ,coalesce(cast(t6.trans_complete_amt as decimal(18,4)),0) as trans_complete_amt
from (
select  max(id) as id 
        ,uuid
        ,analysis_date
        ,country_id
        ,channel_id
        ,version
        ,cversion
        ,res_version
        ,language_id
        ,platform
        ,os_version
        ,device_id
        ,ad_id
        -- ,androidid
        -- ,idfv
        ,etl_date
        ,concat_ws('|',country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id) as join_col
        ,max(user_type) as user_type
        ,max(is_pay) as is_pay
        ,max(is_login) as is_login
        ,min(first_login_time) as first_login_time
        ,max(last_login_time) as last_login_time
        ,min(first_pay_time) as first_pay_time
        ,max(last_pay_time) as last_pay_time
        ,min(first_play_start_time) as first_play_start_time
        ,max(last_play_start_time) as last_play_start_time
        ,min(first_play_end_time) as first_play_end_time
        ,max(last_play_end_time) as last_play_end_time
        ,min(first_active_time) as first_active_time
        ,max(last_active_time) as last_active_time
        ,count(distinct run_id ) as login_cnt
        ,count(distinct install_id) as install_cnt
        ,min(register_time) as register_time
        ,max(vip_type) as vip_type
     from dwd_data.dwd_t01_reelshort_user_info_di
     where etl_date='${TX_DATE}' and uuid<>'nil'
     group by  uuid
        ,analysis_date
        ,country_id
        ,channel_id
        ,version
        ,cversion
        ,res_version
        ,language_id
        ,platform
        ,os_version
        ,device_id
        ,ad_id
        -- ,androidid
        -- ,idfv
        ,etl_date
)t1 
left join (
-- pay_get
select 
        uuid
        ,analysis_date
        ,country_id
        ,channel_id
        ,version
        ,cversion
        ,res_version
        ,language_id
        ,platform
        ,os_version
        ,device_id
        ,ad_id
        -- ,androidid
        -- ,idfv
        ,etl_date
        ,concat_ws('|',country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id) as join_col
      ,sum(order_cnt) as order_cnt
      ,sum(invalid_order_cnt) as invalid_order_cnt
      ,sum(pay_amount) as pay_amount
      ,sum(invalid_pay_amount) as invalid_pay_amount
      ,max(case when order_cnt>=1 then sku_price else 0 end  ) as max_pay_amount
      ,min(case when order_cnt>=1 then sku_price else 0 end ) as min_pay_amount
      ,sum(sub_order_cnt) as cli_sub_order_cnt
      ,sum(sub_pay_amount) as cli_sub_pay_amount
      ,sum(pay_amount-sub_pay_amount) as cli_non_sub_pay_amount
      ,cast(sum(order_cnt-sub_order_cnt) as int) as cli_non_sub_order_cnt
from dws_data.dws_t85_reelshort_order_detail_stat_di
where etl_date='${TX_DATE}'
group by  uuid
        ,analysis_date
        ,country_id
        ,channel_id
        ,version
        ,cversion
        ,res_version
        ,language_id
        ,platform
        ,os_version
        ,device_id
        ,ad_id
        -- ,androidid
        -- ,idfv
        ,etl_date
)t2
 ON t1.uuid=t2.uuid and t1.analysis_date=t2.analysis_date and t1.join_col=t2.join_col
left join (
-- heart_get
    select  uuid
        ,analysis_date
        ,country_id
        ,channel_id
        ,version
        ,cversion
        ,res_version
        ,language_id
        ,platform
        ,os_version
        ,device_id
        ,ad_id
        -- ,androidid
        -- ,idfv
        ,etl_date
        ,concat_ws('|',country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id) as join_col
       ,sum(case when scene_name='main_scene' then online_time else 0 end) as main_scene_times
       ,sum(case when scene_name='main_play_scene' then online_time else 0 end) as main_play_times
       ,sum(case when scene_name='chap_play_scene'  then online_time else 0 end ) as chap_play_times
       ,sum(online_time) as sum_online_times
from dwd_data.dwd_t01_reelshort_user_online_time_di
where etl_date='${TX_DATE}' 
group by  uuid
        ,analysis_date
        ,country_id
        ,channel_id
        ,version
        ,cversion
        ,res_version
        ,language_id
        ,platform
        ,os_version
        ,device_id
        ,ad_id
        -- ,androidid
        -- ,idfv
        ,etl_date
)t3
 ON t1.uuid=t3.uuid and t1.analysis_date=t3.analysis_date and t1.join_col=t3.join_col
left join (
-- currency_get  
    select  uuid
        ,analysis_date
        ,country_id
        ,channel_id
        ,version
        ,cversion
        ,res_version
        ,language_id
        ,platform
        ,os_version
        ,device_id
        ,ad_id
        -- ,androidid
        -- ,idfv
        ,etl_date
        ,concat_ws('|',country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id) as join_col
       ,sum(case when vc_id='vc_01' and incre_type='get' then change_amount  else 0 end) as coins_get
       ,sum(case when vc_id='vc_01' and incre_type='exp' then change_amount  else 0 end) as coins_exp
       ,sum(case when vc_id='vc_01_pay' and incre_type='get' then change_amount  else 0 end) as pay_coins_get
       ,sum(case when vc_id='vc_01_pay' and incre_type='exp' then change_amount  else 0 end) as pay_coins_exp
       ,sum(case when vc_id='vc_01_free' and incre_type='get' then change_amount  else 0 end) as free_coins_get
       ,sum(case when vc_id='vc_01_free' and incre_type='exp' then change_amount  else 0 end) as free_coins_exp

       ,sum(case when vc_id='vc_02' and incre_type='get' then change_amount  else 0 end) as bonus_get
       ,sum(case when vc_id='vc_02' and incre_type='exp' then change_amount  else 0 end) as bonus_exp
       ,sum(case when vc_id='vc_02_pay' and incre_type='get' then change_amount  else 0 end) as pay_bonus_get
       ,sum(case when vc_id='vc_02_pay' and incre_type='exp' then change_amount  else 0 end) as pay_bonus_exp
       ,sum(case when vc_id='vc_02_free' and incre_type='get' then change_amount  else 0 end) as free_bonus_get
       ,sum(case when vc_id='vc_02_free' and incre_type='exp' then change_amount  else 0 end) as free_bonus_exp
    from dwd_data.dwd_t06_reelshort_user_currency_get_or_expend_di
    where etl_date='${TX_DATE}'   and incre_type in ('get','exp') 
    group by  uuid
        ,analysis_date
        ,country_id
        ,channel_id
        ,version
        ,cversion
        ,res_version
        ,language_id
        ,platform
        ,os_version
        ,device_id
        ,ad_id
        -- ,androidid
        -- ,idfv
        ,etl_date
)t4
on t1.uuid=t4.uuid and t1.analysis_date=t4.analysis_date and t1.join_col=t4.join_col
left join (
-- ad_get
    select  uuid
        ,analysis_date
        ,country_id
        ,channel_id
        ,version
        ,cversion
        ,res_version
        ,language_id
        ,platform
        ,os_version
        ,device_id
        ,ad_id
        -- ,androidid
        -- ,idfv
        ,etl_date
        ,concat_ws('|',country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id) as join_col
       ,count(case when action='invoke' then adunit_id end ) as ad_invoke_cnt
       ,count(case when action='start' then adunit_id  end ) as ad_start_cnt
       ,count(case when action='complete' then adunit_id  end ) as ad_complete_cnt
       ,count(case when action='loadfail' then adunit_id  end ) as ad_loadfail_cnt
       ,count(case when action='playfail' then adunit_id  end ) as ad_playfail_cnt
       ,count(case when action='click' then adunit_id  end ) as ad_click_cnt
       ,count(case when action='impression' then adunit_id  end ) as ad_impression_cnt
       ,sum(cast(ad_revenue as double)) as ad_revenue 
      ,sum(cast(if(admoney_app_placeid in('10001','20002','10002'),ad_revenue) as double)) as incentive_ad_revenue 
    from dwd_data.dwd_t07_reelshort_admoney_event_di
where etl_date='${TX_DATE}' 
group by  uuid
        ,analysis_date
        ,country_id
        ,channel_id
        ,version
        ,cversion
        ,res_version
        ,language_id
        ,platform
        ,os_version
        ,device_id
        ,ad_id
        -- ,androidid
        -- ,idfv
        ,etl_date
)t5
 ON t1.uuid=t5.uuid and t1.analysis_date=t5.analysis_date and t1.join_col=t5.join_col
left join (
select  uuid
        ,analysis_date
        ,country_id
        ,channel_id
        ,version
        ,cversion
        ,res_version
        ,language_id
        ,platform
        ,os_version
        ,device_id
        ,ad_id
        -- ,androidid
        -- ,idfv
        ,etl_date
        ,concat_ws('|',country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id) as join_col
      ,count(distinct  case when  pay_status='pay_show' then transaction_id end  ) as trans_show_cnt -- 拉起支付sdk开始支付
      ,count(distinct  case when  pay_status='pay_start' then transaction_id end  ) as trans_start_cnt -- 拉起支付sdk开始支付
      ,count(distinct  case when  pay_status='pay_end' then transaction_id end  ) as trans_end_cnt -- 支付完成
      ,count(distinct  case when  pay_status='pay_complete' then transaction_id end  ) as trans_complete_cnt -- 订单验证完成
      ,count(distinct  case when  pay_status='pay_failed' then transaction_id end  ) as trans_failed_cnt -- 支付失败
      ,count(distinct  case when  pay_status='pay_cancel' then transaction_id end  ) as trans_cancel_cnt -- 支付取消
      ,sum(case when  pay_status='pay_complete' then sku_price/100 else 0 end  ) as trans_complete_amt -- 订单支付金额
  from dwd_data.dwd_t05_reelshort_transaction_detail_di
  where etl_date='${TX_DATE}' 
  group by   uuid
        ,analysis_date
        ,country_id
        ,channel_id
        ,version
        ,cversion
        ,res_version
        ,language_id
        ,platform
        ,os_version
        ,device_id
        ,ad_id
        -- ,androidid
        -- ,idfv
        ,etl_date
)t6 
on t1.uuid=t6.uuid    and t1.analysis_date=t6.analysis_date and t1.join_col=t6.join_col
left join (
    -- dlink
    select  uuid
            ,analysis_date
            ,country_id
            ,channel_id
            ,version
            ,cversion
            ,res_version
            ,language_id
            ,platform
            ,os_version
            ,device_id
            ,ad_id
            -- ,androidid
            -- ,idfv
            ,etl_date
            ,concat_ws('|',country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id) as join_col
           ,count(case when action='invoke' then id end ) as dlink_invoke_cnt
           ,count(case when action='complete' then id  end ) as dlink_complete_cnt
           ,count(case when action='cancel' then id  end ) as dlink_cancel_cnt
           ,count(case when action='timeout' then id  end ) as dlink_timeout_cnt
           ,count(case when action='error' then id  end ) as dlink_error_cnt
           ,count(case when dlink_type='deferred' then id  end ) as dlink_deferred_cnt
           ,count(case when dlink_type='normal' then id  end ) as dlink_normal_cnt
        from dwd_data.dwd_t01_reelshort_dlink_event_di
      where etl_date='${TX_DATE}'
      group by  uuid
            ,analysis_date
            ,country_id
            ,channel_id
            ,version
            ,cversion
            ,res_version
            ,language_id
            ,platform
            ,os_version
            ,device_id
            ,ad_id
            -- ,androidid
            -- ,idfv
            ,etl_date
    )t7
    on t1.uuid=t7.uuid    and t1.analysis_date=t7.analysis_date and t1.join_col=t7.join_col

left join (
-- 活跃时间段
    select   uuid
          ,analysis_date
          ,max(case when rank_1h=1 then analysis_hour end)  as u_act_analysis_hour
          ,max(case when rank_2h=1 then analysis_2hour end) as u_act_analysis_2hour
          ,max(case when rank_3h=1 then analysis_3hour end) as u_act_analysis_3hour
          ,max(case when rank_4h=1 then analysis_4hour end) as u_act_analysis_4hour
    from 
    (select 
           uuid
          ,analysis_date
          ,analysis_hour
          ,analysis_2hour
          ,analysis_3hour
          ,analysis_4hour
          ,row_number()over(partition by uuid,analysis_date order by online_time desc ) as rank_1h
          ,row_number()over(partition by uuid,analysis_date order by online_time_2h desc ) as rank_2h
          ,row_number()over(partition by uuid,analysis_date order by online_time_3h desc ) as rank_3h
          ,row_number()over(partition by uuid,analysis_date order by online_time_4h desc ) as rank_4h
    from       
    (select   uuid
          ,analysis_date
          ,analysis_hour
          ,cast(ceiling((cast(analysis_hour as int)+1 )/2 ) as int) as analysis_2hour
          ,cast(ceiling((cast(analysis_hour as int)+1 )/3 ) as int) as analysis_3hour
          ,cast(ceiling((cast(analysis_hour as int)+1) /4 ) as int) as analysis_4hour
          ,sum(online_time)over(partition by uuid,analysis_date,analysis_hour) as online_time
          ,sum(online_time)over(partition by uuid,analysis_date,cast(ceiling((cast(analysis_hour as int)+1 )/2 ) as int) ) as online_time_2h
          ,sum(online_time)over(partition by uuid,analysis_date,cast(ceiling((cast(analysis_hour as int)+1 )/3 ) as int) ) as online_time_3h
          ,sum(online_time)over(partition by uuid,analysis_date,cast(ceiling((cast(analysis_hour as int)+1 )/4 ) as int) ) as online_time_4h
    from dwd_data.dwd_t01_reelshort_user_online_time_di
    where analysis_date='${TX_DATE}' and uuid not in ('','nil')
    )tt1
    )tt2
    group by uuid,analysis_date
)t8
on t1.uuid=t8.uuid and t1.analysis_date=t8.analysis_date 
left join (
-- pay_get
select 
        uuid
        ,analysis_date
        ,country_id
        ,channel_id
        ,version
        ,cversion
        ,res_version
        ,language_id
        ,platform
        ,os_version
        ,device_id
        ,ad_id
        -- ,androidid
        -- ,idfv
        ,etl_date
        ,concat_ws('|',country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id) as join_col
      ,sum(order_cnt) as srv_order_cnt
      ,sum(pay_amount) as srv_pay_amount
      ,sum(sub_order_cnt) as srv_sub_order_cnt
      ,sum(sub_pay_amount) as srv_sub_pay_amount      
from dws_data.dws_t85_reelshort_srv_order_detail_stat_di
where etl_date='${TX_DATE}'
group by  uuid
        ,analysis_date
        ,country_id
        ,channel_id
        ,version
        ,cversion
        ,res_version
        ,language_id
        ,platform
        ,os_version
        ,device_id
        ,ad_id
        -- ,androidid
        -- ,idfv
        ,etl_date
)t9
 ON t1.uuid=t9.uuid and t1.analysis_date=t9.analysis_date and t1.join_col=t9.join_col
left join (
  select uuid 
        ,analysis_date
        ,concat_ws('|',country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id) as join_col
        ,cast(sum(coin_get) as int) as srv_coin_get
        ,cast(sum(coin_exp) as int) as srv_coin_exp
        ,cast(sum(pay_bonus_get) as int) as srv_pay_bonus_get
        ,cast(sum(pay_bonus_exp) as int) as srv_pay_bonus_exp
        ,cast(sum(free_bonus_get) as int) as srv_free_bonus_get
        ,cast(sum(free_bonus_exp) as int) as srv_free_bonus_exp
  from  dwd_data.dwd_t06_reelshort_srv_currency_change_detail_di
  where etl_date='${TX_DATE}'
  group by uuid 
        ,analysis_date
        ,country_id
        ,channel_id
        ,version
        ,cversion
        ,res_version
        ,language_id
        ,platform
        ,os_version
        ,device_id
        ,ad_id
)t10 
 ON t1.uuid=t10.uuid and t1.analysis_date=t10.analysis_date and t1.join_col=t10.join_col 
left join  (
   -- select uuid 
   --  ,3 as account_bind_type
   --  ,min(register_time) as register_time
   --  ,max(analysis_date) as lt_last_active_date
   --  ,max(is_pay) as is_pay_lt
   -- from dwd_data.dwd_t01_reelshort_user_info_di
   -- where etl_date<'${TX_DATE}'
   -- group by uuid 
  select uuid
       ,max(if(etl_date='${TX_DATE}',last_bind_type)) as account_bind_type
       ,min(register_time) as register_time
       ,max(if(etl_date=date_sub('${TX_DATE}',1),date(last_active_time))) as lt_last_active_date
       ,max(is_pay_lt) as is_pay_lt
  from dwd_data.dim_t99_reelshort_user_lt_info
  where etl_date between date_sub('${TX_DATE}',1) and '${TX_DATE}'
  group by uuid 
)t11 
on t1.uuid=t11.uuid
left join
(
  select uuid ,af_network_name,af_channel,af_campaign,af_adset,af_ad,af_campaign_type
  from dwd_data.dwd_t01_reelshort_user_detail_info_di
  where etl_date='${TX_DATE}' 
) t12 
on t12.uuid=t1.uuid
left join (
  select    t1.device_id
            ,t1.platform
            ,t2.uuid
            ,max(if(t1.analysis_date='${TX_DATE}',t1.analysis_date)) as install_analysis_date
            ,min(t1.analysis_date) as first_install_date_90d 
  from(
    select  distinct   device_id,platform,analysis_date
    from dwd_data.dwd_t01_reelshort_device_start_di
    where etl_date between date_sub('${TX_DATE}',89) and '${TX_DATE}' and start_status='1'
    and device_id<>''
  ) t1 
  join (
    select  distinct   device_id,platform,analysis_date,uuid
    from dwd_data.dwd_t01_reelshort_user_login_info_di
    where etl_date between date_sub('${TX_DATE}',89) and '${TX_DATE}'
  ) t2 
  on t1.device_id=t2.device_id and t1.platform=t2.platform and t1.analysis_date=t2.analysis_date
  group by t1.device_id,t1.platform,t2.uuid
)t13 
ON t1.device_id=t13.device_id and t1.platform=t13.platform and t1.uuid=t13.uuid
left join (
      select uuid ,media_type,dlink_story_id
      from (select distinct  uuid 
       ,analysis_date 
       ,media_type
       ,book_id as dlink_story_id
      ,row_number()over(partition by uuid order by  analysis_date desc ) as rank
      from dws_data.dws_t87_reelshort_book_attr_di
      where analysis_date<='${TX_DATE}'
      )
      where rank=1
)t14 
on t1.uuid=t14.uuid
left join (

) as t15
;

