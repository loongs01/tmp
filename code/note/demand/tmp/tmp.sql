-- dwd1
select count(1) from dim_data.dim_t99_sku_info where project='magic';
select count(1) from dwd_data.dwd_t01_magic_appsflyer_device_uuid where analysis_date='${TX_DATE}';
select count(1) from dwd_data.dwd_t07_magic_ad_detail_di  where etl_date='${TX_DATE}'; 
select count(1) from dwd_data.dwd_t06_magic_prop_change_detail_di  where etl_date='${TX_DATE}'; 

-- dwd2
select count(1) from  dwd_data.dwd_t02_magic_chat_detail_di where etl_date='${TX_DATE}';
select count(1) from  dwd_data.dwd_t05_magic_order_detail_di where etl_date='${TX_DATE}';
select count(1) from  dwd_data.dwd_t02_magic_phone_message_detail_di where etl_date='${TX_DATE}';

-- dwd3
select count(1) from dim_data.dim_t99_magic_diversion_user_info;
select count(1) from dwd_data.dim_t99_magic_user_lt_info where etl_date='${TX_DATE}';
select count(1) from dwd_data.dwd_t01_magic_user_detail_info_di where analysis_date='${TX_DATE}';
select count(1) from dwd_data.dwd_t06_magic_item_change_detail_di  where etl_date='${TX_DATE}'; 

-- dws
select count(1) from  dws_data.dws_t85_magic_order_detail_stat_di  where etl_date='${TX_DATE}';
select count(1) from  dws_data.dws_t80_magic_user_bhv_cnt_detail_di  where etl_date='${TX_DATE}';
select count(1) from  dws_data.dws_t85_magic_transaction_detail_stat_df  where etl_date='${TX_DATE}';
select count(1) from  dws_data.dws_t84_magic_event_tech_funnel_detail_di where etl_date='${TX_DATE}';

-- dm
select count(1) from  dm_magic_data.dm_t80_magic_daily_stat_df where analysis_date='${TX_DATE}';
select count(1) from  dm_magic_data.dm_t80_magic_month_stat_mf where etl_date='${TX_DATE}';




-- view_dwd
select count(1) from dw_view.dwd_t01_magic_attribution_event_di;

--view_dws
select analysis_date,count(1) from dw_view.dws_t85_magic_order_detail_stat_di 
group by analysis_date order by analysis_date desc;