replace into  dwd_data.dwd_t01_reelshort_user_detail_info_di
select
 uuid
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
 ,ctimezone_offset
 ,netstatus
 ,install_id
 ,run_id
 ,device_id
 ,ad_id
 ,androidid
 ,idfv
 ,user_ip
 ,register_time
 ,register_dt
 ,first_login_time
 ,last_login_time
 ,first_pay_time
 ,last_pay_time
 ,first_active_time
 ,last_active_time
 ,login_cnt
 ,install_cnt
 ,activity_days
 ,user_type
 ,is_pay
 ,is_login
 ,first_play_start_story_id
 ,last_play_start_story_id
 ,first_play_complete_story_id
 ,last_play_complete_story_id
 ,first_player_play_start_story_id
 ,last_player_play_start_story_id
 ,first_player_play_end_story_id
 ,last_player_play_end_story_id
 ,first_play_start_ctime
 ,last_play_start_ctime
 ,first_play_complete_ctime
 ,last_play_complete_ctime
 ,first_player_play_start_ctime
 ,last_player_play_start_ctime
 ,first_player_play_end_ctime
 ,last_player_play_end_ctime
 ,first_pay_story_id
 ,first_sku_price
 ,last_sku_price
 ,af_network_name
 ,af_channel
 ,af_campaign
 ,af_adset
 ,af_ad
 ,af_campaign_type
 ,device_version
 ,device_brand
 ,device_model
 ,srv_register_date
 ,first_play_start_chap_id
 ,last_play_start_chap_id
 ,first_play_complete_chap_id
 ,last_play_complete_chap_id
 ,first_player_play_start_chap_id
 ,last_player_play_start_chap_id
 ,first_player_play_end_chap_id
 ,last_player_play_end_chap_id
 ,account_bind_type
 ,first_srv_sub_sku_price
 ,last_srv_sub_sku_price
 ,vip_type
 ,nvl(recent_visit_source,'')as recent_visit_source -- 最近一次访问来源
 ,nvl(recent_visit_source_type,'')as recent_visit_source_type -- 最近一次访问来源类型
 ,first_visit_landing_page
 ,first_visit_landing_page_story_id
 ,first_visit_landing_page_chap_id
from dwd_data.dwd_t01_reelshort_user_detail_info_di
where etl_date='${TX_DATE}';
;



replace into dws_data.`dws_t80_reelshort_user_bhv_cnt_detail_di`
select
id
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
,user_type
,is_pay
,is_login
,register_time
,first_login_time
,last_login_time
,first_pay_time
,last_pay_time
,first_play_start_time
,last_play_start_time
,first_play_end_time
,last_play_end_time
,first_active_time
,last_active_time
,login_cnt
,install_cnt
,order_cnt
,invalid_order_cnt
,pay_amount
,invalid_pay_amount
,max_pay_amount
,min_pay_amount
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
,trans_show_cnt
,trans_start_cnt
,trans_end_cnt
,trans_complete_cnt
,trans_failed_cnt
,trans_cancel_cnt
,u_act_analysis_hour
,u_act_analysis_2hour
,u_act_analysis_3hour
,u_act_analysis_4hour
,account_bind_type
,af_network_name
,af_channel
,af_campaign_type
,bonus_get
,bonus_exp
,pay_bonus_get
,pay_bonus_exp
,free_bonus_get
,free_bonus_exp
,af_campaign
,af_adset
,af_ad
,reactive_flag
,new_active_flag
,ad_invoke_cnt
,ad_start_cnt
,ad_complete_cnt
,ad_loadfail_cnt
,ad_playfail_cnt
,ad_click_cnt
,ad_impression_cnt
,ad_revenue
,install_analysis_date
,dlink_invoke_cnt
,dlink_complete_cnt
,dlink_cancel_cnt
,dlink_timeout_cnt
,dlink_error_cnt
,dlink_deferred_cnt
,dlink_normal_cnt
,srv_order_cnt
,srv_pay_amount
,first_install_date_90d
,install_type
,cli_sub_order_cnt
,cli_sub_pay_amount
,srv_sub_order_cnt
,srv_sub_pay_amount
,srv_coin_get
,srv_coin_exp
,srv_pay_bonus_get
,srv_pay_bonus_exp
,srv_free_bonus_get
,srv_free_bonus_exp
,cli_non_sub_pay_amount
,reactive_flag_30d
,vip_type
,active_user_type
,cli_non_sub_order_cnt
,incentive_ad_revenue
,dlink_story_id
,media_type
,trans_start_amt
 ,nvl(recent_visit_source,'')as recent_visit_source -- 最近一次访问来源
 ,nvl(recent_visit_source_type,'')as recent_visit_source_type -- 最近一次访问来源类型
,first_visit_landing_page
,first_visit_landing_page_story_id
,first_visit_landing_page_chap_id
from  dws_data.`dws_t80_reelshort_user_bhv_cnt_detail_di`
where etl_date='${TX_DATE}'
;






replace into  dws_data.dws_t85_reelshort_srv_currency_change_statistic_di
select id
      ,uuid
      ,analysis_date
      ,country_id
      ,channel_id
      ,version
      ,language_id
      ,platform
      ,user_type
      ,story_id
      ,coin_type -- 虚拟币类型
      ,coin_expend -- 虚拟币消耗数量
      ,first_visit_landing_page          as first_visit_landing_page          -- 首次访问着陆页
      ,first_visit_landing_page_story_id as first_visit_landing_page_story_id -- 首次访问着陆页书籍id
      ,first_visit_landing_page_chap_id  as first_visit_landing_page_chap_id  -- 首次访问着陆页章节id
       ,nvl(recent_visit_source,'')as recent_visit_source -- 最近一次访问来源
 ,nvl(recent_visit_source_type,'')as recent_visit_source_type -- 最近一次访问来源类型
      ,book_type
from dws_data.dws_t85_reelshort_srv_currency_change_statistic_di where analysis_date='${TX_DATE}'
;




replace into dws_data.dws_t82_reelshort_user_play_data5_detail_di
select
     id
    ,t1.uuid
    ,t1.analysis_date
    ,is_pay
    ,is_login
    ,user_type
    ,vip_type
    ,nvl(t5.af_network_name,'') as af_network_name
    ,af_channel
    ,country_id
    ,channel_id
    ,version
    ,cversion
    ,res_version
    ,language_id
    ,platform

    ,sub_event_name
    ,scene_name
    ,page_name
    ,pre_page_name
    ,action
    ,t1.shelf_id
    ,story_id
    ,referrer_story_id
    ,chap_id
    ,chap_order_id

    ,report_cnt
    ,show_cnt
    ,click_cnt
    ,cover_click_cnt
    ,play_start_cnt -- 播放开始次数
    ,play_complete_cnt -- 播放完成次数
    ,first_report_time
    ,last_report_time
    ,video_id
    ,play_duration
    ,online_times
    -- ,story_ids
    ,t1.analysis_date as etl_date
    ,recent_visit_source                 -- 最近一次访问来源
    ,recent_visit_source_type            -- 最近一次访问来源类型
    ,first_visit_landing_page            -- 首次访问着陆页
    ,first_visit_landing_page_story_id   -- 首次访问着陆页书籍id
    ,first_visit_landing_page_chap_id    -- 首次访问着陆页章节id
    ,shelf_name    
from dws_data.dws_t82_reelshort_user_play_data5_detail_di as t1
left join (
           select distinct etl_date
                  ,uuid
                  ,af_network_name
           from dws_data.dim_t87_reelshort_ads_user_attr_df
           -- where etl_date='${TX_DATE}'
) as t5
on t5.uuid=t1.uuid and t5.etl_date=t1.etl_date
-- where t1.etl_date='${TX_DATE}'
;



replace into dws_data.dws_t82_reelshort_user_exposure_data5_detail_di
select
     id
    ,t1.uuid
    ,t1.analysis_date
    ,is_pay
    ,is_login
    ,user_type
    ,vip_type
    ,nvl(t4.af_network_name,'') as af_network_name
    ,af_channel
    ,country_id
    ,channel_id
    ,version
    ,cversion
    ,res_version
    ,language_id
    ,platform
    ,sub_event_name
    ,scene_name
    ,page_name
    ,pre_page_name
    ,action
    ,t1.shelf_id
    ,story_id
    ,referrer_story_id
    ,chap_id
    ,chap_order_id
    ,report_cnt
    ,show_cnt
    ,click_cnt
    -- ,story_ids
    ,t1.analysis_date as etl_date
    ,shelf_name        -- 书架名称
from dws_data.dws_t82_reelshort_user_exposure_data5_detail_di as t1
left join (
           select distinct etl_date
                  ,uuid
                  ,af_network_name
           from dws_data.dim_t87_reelshort_ads_user_attr_df
           where etl_date='${TX_DATE}'
) as t4
on t4.uuid=t1.uuid and t4.etl_date=t1.etl_date
-- where t1.etl_date='${TX_DATE}'
;





replace into dws_data.dws_t85_reelshort_currency_detail_stat_di
select id 
       ,t1.uuid
       ,t1.analysis_date
       ,t1.etl_date
       ,is_pay
       ,is_login
       ,user_type
       ,t1.country_id
       ,t1.channel_id
       ,t1.version
       ,t1.cversion
       ,t1.res_version
       ,t1.language_id
       ,t1.platform
       ,t1.os_version
       ,t1.device_id
       ,t1.ad_id
       -- ,t1.androidid
       -- ,t1.idfv
       ,t1.scene_name
       ,t1.page_name
       ,t1.story_id
       ,t1.chap_id
       ,t1.chap_order_id
       ,t1.currency_type
       ,t1.incre_type
       ,t1.trigger_id
       ,amount_increment
       ,coalesce(t3.af_network_name,'') as     af_network_name
       ,af_channel
       -- ,coalesce(t2.af_campaign_type,'') as    af_campaign_type
       ,t1.t_book_id
from dws_data.dws_t85_reelshort_currency_detail_stat_di as t1
left join (
           select distinct etl_date
                  ,uuid
                  ,af_network_name
           from dws_data.dim_t87_reelshort_ads_user_attr_df
           -- where etl_date='${TX_DATE}'
) as t3
on t3.uuid=t1.uuid and t3.etl_date=t1.etl_date
-- where t1.etl_date='${TX_DATE}'