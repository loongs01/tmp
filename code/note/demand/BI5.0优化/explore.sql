reelshort_derived_content_data.sql                               -- 已完成
-- reelshort_opc_push_di.sql
reelshort_derived_exposure_data.sql                              -- 已完成
-- reelshort_user_active_viscosity_data_m.sql
-- reelshort_user_active_viscosity_data_w.sql
reelshort_user_base_data.sql
reelshort_opera_revenue_detail_data.sql                        ?
reelshort_opera_reten_user_detail_data.sql                     ?
reelshort_opera_revenue_repurchase_data.sql                    ?
-- reelshort_opera_ltv_stat_total.sql
reelshort_opera_reten_reflux_detail_data.sql                   ?
reelshort_opera_reten_detail_user_data.sql                     ?
-- reelshort_opera_ltv_stat_iap.sql
reelshort_opera_reten_detail_reflux_data.sql
reelshort_opera_reten_nu_week_trend_data.sql
-- reelshort_opera_ltv_stat_total_70p.sql
reelshort_opera_reten_nu_month_trend_data.sql
reelshort_opera_reten_nu_detail_data.sql



-- exp_reelshort_target_configs_to_mysql






select
      etl_date as analysis_date
--       ,language_id
--       ,platform
      ,sub_event_name
       ,scene_name
--        ,page_name
--       ,action
      ,count(1) as ct
from dws_data.dws_t82_reelshort_user_play_data5_detail_di as t
where etl_date='${TX_DATE}'
group by 
      etl_date
      ,sub_event_name
       ,scene_name
--        ,page_name
order by ct desc ;

select 
count(1)
from dws_data.dws_t82_reelshort_user_play_data5_detail_di as t
where etl_date='${TX_DATE}'