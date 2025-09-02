-- chapters_log.public_event_data->dwd_data.dwd_t02_reelshort_play_event_di->  

1

select count(1) from dwd_data.dwd_t01_reelshort_api_res_time_stat_di;        --and sub_event_name='api_res_time_stat' 条件不满足 
2

3

select count(1) from dwd_data.dwd_t01_reelshort_user_book_attribution_di;     --where t1.book_id<>'' 条件不满足

select count(1) from dwd_data.dwd_t01_reelshort_appsflyer_device_uuid;        --dwd_data.dwd_t03_appsflyer_uuid_action_di 源表没数
select count(1) from dwd_data.dwd_t02_reelshort_book_ab_record;               --chapters_log.`dts_project_v_composition_ab_test` 源表没数
4

select count(1) from dwd_data.dwd_t06_reelshort_srv_currency_change_detail_di;    -- optime 条件限定
select count(1) from dwd_data.dim_t01_cbs_uuid_distributor;                       -- t3.uuid is null  条件不满足

dws
select count(1) from dws_data.dws_t87_reelshort_book_attribution_di_v2;          -- dwd_data.dwd_t01_reelshort_book_attribution_di_v2 源表没数

select count(1) from dws_data.dws_t88_reelshort_opc_user_tag_detail_di;    -- chapters_log.opc_user_tag_groups 源表只有9月数据20240909-18

---select count(1) from dws_data.dws_t85_reelshort_srv_currency_change_stat_di;  --dwd_data.dwd_t06_reelshort_srv_currency_change_detail_di 源表没数


dm

---select count(1) from dm_reelshort_data.dm_t85_cbs_uuid_revenue_di;            --    dwd_data.dim_t01_cbs_uuid_distributor  源表没数



--- select count(1) from dm_reelshort_data.dm_t85_cbs_uuid_revenue_mf;            --  dwd_data.dim_t01_cbs_uuid_distributor  源表没数
