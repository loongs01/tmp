
create or replace MATERIALIZED  view  dw_view.mtv_dws_t80_reelshort_user_bhv_cnt_detail_di_temp
 as
-- REFRESH NEXT now() + interval 5 minute
with  reelshort_user_bhv_cnt_detail_di_temp as (

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
       ,t1.device_id -- '应用用于标识设备的唯一ID'
       ,t1.ad_id -- '广告ID:Android=google adid IOS=idfa'
       -- ,t1.androidid -- 'ANDROIDID'
       -- ,t1.idfv -- 'IOS-idfv'
       ,max(t1.user_type)over(partition by t1.uuid,t1.etl_date) as user_type
       ,max(t1.is_pay)over(partition by t1.uuid,t1.etl_date) as is_pay
       ,max(t1.is_login)over(partition by t1.uuid,t1.etl_date) as is_login
       -- ,max(t1.register_time)over(partition by t1.uuid,t1.etl_date) as register_time
       ,coalesce(max(t1.register_time)over(partition by t1.uuid,t1.etl_date),t11.register_time) as register_time
       ,coalesce(cast(t1.order_cnt as int ),0) as order_cnt
       ,coalesce(cast(t1.pay_amount as int ),0) as pay_amount
       ,coalesce(cast(t1.main_scene_times as int ),0)  as main_scene_times
       ,coalesce(cast(t1.main_play_times as int ),0)  as main_play_times
       ,coalesce(cast(t1.chap_play_times as int ),0)  as chap_play_times
       ,coalesce(cast(t1.sum_online_times as int ),0)  as sum_online_times
       ,coalesce(cast(t1.coins_get as int),0) as coins_get
       ,coalesce(cast(t1.coins_exp as int),0) as coins_exp
       ,coalesce(cast(t1.pay_coins_get as int),0) as pay_coins_get
       ,coalesce(cast(t1.pay_coins_exp as int),0) as pay_coins_exp
       ,coalesce(cast(t1.free_coins_get as int),0) as free_coins_get
       ,coalesce(cast(t1.free_coins_exp as int),0) as free_coins_exp
       ,coalesce(cast(t1.bonus_get as int),0) as bonus_get
      ,coalesce(cast(t1.bonus_exp as int),0) as bonus_exp
      ,nvl(t12.af_network_name,'') as af_network_name
      ,nvl(t12.af_channel,'') as af_channel
      ,nvl(t12.af_campaign_type,'') as af_campaign_type
      ,coalesce(cast(t1.ad_revenue as decimal(18,4) ),0) as ad_revenue
      ,nvl(t12.af_campaign,'') as af_campaign
      ,nvl(t12.af_adset,'') as af_adset
      ,nvl(t12.af_ad,'') as af_ad
      ,case when  t11.lt_last_active_date<=date_sub('${FORMER_TX_DATE}',90) then 1 else 0 end as reactive_flag
      ,case when  t11.lt_last_active_date<=date_sub('${FORMER_TX_DATE}',30) then 1
             else cast(max(t1.user_type)over(partition by t1.uuid,t1.etl_date) as int)
        end as new_active_flag
      ,install_analysis_date
      ,coalesce(t10.srv_coin_get,0) as srv_coin_get
      ,coalesce(t10.srv_coin_exp,0) as srv_coin_exp
      ,coalesce(t10.srv_pay_bonus_get,0) as srv_pay_bonus_get
      ,coalesce(t10.srv_pay_bonus_exp,0) as srv_pay_bonus_exp
      ,coalesce(t10.srv_free_bonus_get,0) as srv_free_bonus_get
      ,coalesce(t10.srv_free_bonus_exp,0) as srv_free_bonus_exp
      ,cast(coalesce(cli_sub_pay_amount,0) as int) as cli_sub_pay_amount
      ,cast(coalesce(cli_non_sub_pay_amount,0) as int) as cli_non_sub_pay_amount
      ,case when  t11.lt_last_active_date<=date_sub('${FORMER_TX_DATE}',30) then 1 else 0 end as reactive_flag_30d
      ,max(t1.vip_type)over(partition by t1.uuid,t1.etl_date)  as vip_type
      ,coalesce(cast(t1.ad_start_cnt as int),0) as ad_start_cnt
      ,coalesce(cast(t1.incentive_ad_revenue as decimal(18,4)),0) as incentive_ad_revenue
      ,coalesce(cast(t1.cli_non_sub_order_cnt as int),0) as cli_non_sub_order_cnt
     ,case when  max(t1.user_type)over(partition by t1.uuid,t1.etl_date)=1 then 1
            when t11.lt_last_active_date<'${FORMER_TX_DATE}' AND t11.lt_last_active_date>=date_sub('${FORMER_TX_DATE}',6)  THEN 2
            when t11.lt_last_active_date<date_sub('${FORMER_TX_DATE}',6) and t11.lt_last_active_date>=date_sub('${FORMER_TX_DATE}',29)   then 3
            when t11.lt_last_active_date<=date_sub('${FORMER_TX_DATE}',30) then 4
            else -1
      end AS active_user_type
     ,cast(case when t1.is_login=1 then t2.login_cnt else t2.login_cnt-1 end as int) as login_cnt
     ,first_login_time

    ,nvl(cast(trans_show_cnt as int),0) as trans_show_cnt
    ,nvl(cast(trans_start_cnt as int),0) as trans_start_cnt
    ,nvl(cast(trans_end_cnt as int),0) as trans_end_cnt
    ,nvl(cast(trans_complete_cnt as int),0) as trans_complete_cnt
    ,nvl(cast(trans_failed_cnt as int),0) as trans_failed_cnt
    ,nvl(cast(trans_cancel_cnt as int),0) as trans_cancel_cnt
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
        ,max(register_time) as register_time
        ,sum(order_cnt ) as order_cnt
        ,sum(pay_amount ) as pay_amount
        ,sum(coins_get) as coins_get
        ,sum(coins_exp) as coins_exp
        ,sum(pay_coins_get) as pay_coins_get
        ,sum(pay_coins_exp) as pay_coins_exp
        ,sum(free_coins_get) as free_coins_get
        ,sum(free_coins_exp) as free_coins_exp
        ,sum(bonus_get) as bonus_get
        ,sum(bonus_exp) as bonus_exp
        ,sum(ad_revenue) as ad_revenue
        ,sum(main_scene_times) as main_scene_times
        ,sum(main_play_times) as main_play_times
        ,sum(chap_play_times) as chap_play_times
        ,sum(sum_online_times) as sum_online_times
        ,sum(sub_pay_amount) as cli_sub_pay_amount
        ,sum(pay_amount-sub_pay_amount) as cli_non_sub_pay_amount
        ,max(vip_type) as vip_type
        ,sum(ad_start_cnt) as ad_start_cnt
        ,sum(incentive_ad_revenue) as incentive_ad_revenue
        ,sum(order_cnt-sub_order_cnt) as cli_non_sub_order_cnt
        ,min(first_login_time) as first_login_time
     from dw_view.vt_dwd_t01_reelshort_user_info_di
     where etl_date between '${FORMER_TX_DATE}'  and '${NOW_TX_DATE}'
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
  select uuid
        ,analysis_date
        ,concat_ws('|',country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id) as join_col
        ,count(distinct run_id ) as login_cnt
        ,count(distinct install_id) as install_cnt
 from  dwd_data.dwd_t01_reelshort_user_login_info_di
 where etl_date between '${FORMER_TX_DATE}'  and '${NOW_TX_DATE}'
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
)t2
 ON t1.uuid=t2.uuid and t1.analysis_date=t2.analysis_date and t1.join_col=t2.join_col
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
  where etl_date between '${FORMER_TX_DATE}'  and '${NOW_TX_DATE}'
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
   select    uuid ,max(analysis_date) as lt_last_active_date,min(register_time) as register_time
   from dwd_data.dim_t99_reelshort_user_lt_info
   where etl_date >= date_sub('${FORMER_TX_DATE}',1)  and  etl_date<'${NOW_TX_DATE}'
   and uuid in (select distinct uuid from dw_view.vt_dwd_t01_reelshort_user_info_di
     where etl_date between '${FORMER_TX_DATE}'  and '${NOW_TX_DATE}'  )
   group by uuid

)t11
on t1.uuid=t11.uuid
left join
(
  select   uuid
         ,analysis_date
         ,af_network_name
         ,af_channel
         ,af_campaign_type
         ,campaign as af_campaign
         ,af_adset
         ,af_ad
   from   dw_view.vt_dwd_t01_appsflyer_device_uuid
   where analysis_date between '${FORMER_TX_DATE}'  and '${NOW_TX_DATE}'
   and app_name ='Reelshort'
) t12
on t12.uuid=t1.uuid  and t1.analysis_date=t12.analysis_date
left join (
  select distinct   device_id,platform,analysis_date as install_analysis_date
  from dw_view.dwd_t01_reelshort_device_start_di
  where etl_date between '${FORMER_TX_DATE}'  and '${NOW_TX_DATE}'  and start_status='1' and device_id<>''
)t13
ON t1.device_id=t13.device_id and t1.analysis_date=t13.install_analysis_date and t1.platform=t13.platform

left join
(
    select
        uuid
        ,analysis_date
        ,concat_ws('|',country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id) as join_col
        ,count(distinct  case when  pay_status='pay_show' then transaction_id end  ) as trans_show_cnt -- 拉起支付sdk开始支付
        ,count(distinct  case when  pay_status='pay_start' then transaction_id end  ) as trans_start_cnt -- 拉起支付sdk开始支付
        ,count(distinct  case when  pay_status='pay_end' then transaction_id end  ) as trans_end_cnt -- 支付完成
        ,count(distinct  case when  pay_status='pay_complete' then transaction_id end  ) as trans_complete_cnt -- 订单验证完成
        ,count(distinct  case when  pay_status='pay_failed' then transaction_id end  ) as trans_failed_cnt -- 支付失败
        ,count(distinct  case when  pay_status='pay_cancel' then transaction_id end  ) as trans_cancel_cnt -- 支付取消
    from dw_view.dwd_t05_reelshort_transaction_detail_di
    where etl_date between '${FORMER_TX_DATE}'  and '${NOW_TX_DATE}'
    group by uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id
) t14
ON t1.uuid=t14.uuid and t1.analysis_date=t14.analysis_date and t1.join_col=t14.join_col


)
select t1.*,nvl(t2.book_id,'') as  dlink_story_id
from reelshort_user_bhv_cnt_detail_di_temp t1
left join (
        select distinct  uuid ,book_id
          from (
                  SELECT  uuid ,book_id
                  ,row_number()over(partition by uuid order by analysis_date desc ) as rank
                  from dw_view.dws_t87_reelshort_book_attr_di
                  where analysis_date <= '${NOW_TX_DATE}'
          )t
          where rank=1
)t2
on t1.uuid=t2.uuid
;

create or replace   view  dw_view.dws_t80_reelshort_user_bhv_cnt_detail_di_v2
    as
select id
       ,uuid
       ,analysis_date
       ,etl_date
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
       ,user_type
       ,is_pay
       ,is_login
       ,register_time
       ,order_cnt
       ,pay_amount
       ,main_scene_times
       ,main_play_times
       ,chap_play_times
       ,sum_online_times
       ,coins_get
       ,coins_exp
       ,pay_coins_get
       ,pay_coins_exp
       ,free_coins_get
       ,free_coins_exp
       ,bonus_get
       ,bonus_exp
       ,af_network_name
       ,af_channel
       ,af_campaign_type
       ,ad_revenue
       ,af_campaign
       ,af_adset
       ,af_ad
       ,reactive_flag
       ,new_active_flag
       ,install_analysis_date
       ,srv_coin_get
       ,srv_coin_exp
       ,srv_pay_bonus_get
       ,srv_pay_bonus_exp
       ,srv_free_bonus_get
       ,srv_free_bonus_exp
       ,cli_sub_pay_amount
       ,cli_non_sub_pay_amount
       ,reactive_flag_30d
       ,vip_type
       ,ad_start_cnt
       ,incentive_ad_revenue
       ,cli_non_sub_order_cnt
       ,active_user_type
       ,login_cnt
       ,first_login_time
       ,trans_show_cnt
       ,trans_start_cnt
       ,trans_end_cnt
       ,trans_complete_cnt
       ,trans_failed_cnt
       ,trans_cancel_cnt
       ,dlink_story_id
from dws_data.dws_t80_reelshort_user_bhv_cnt_detail_di
where etl_date < '${FORMER_TX_DATE}'
      and uuid regexp '^[0-9]+$'
      and uuid<2147483647
union all
select *
from dw_view.mtv_dws_t80_reelshort_user_bhv_cnt_detail_di_temp
where uuid regexp '^[0-9]+$'
      and uuid<2147483647
;
