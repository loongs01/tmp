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
           select distinct
                  uuid
                  ,af_network_name
           from dws_data.dim_t87_reelshort_ads_user_attr_df
           where etl_date='${TX_DATE}'
) as t3
on t3.uuid=t1.uuid
where etl_date='${TX_DATE}'