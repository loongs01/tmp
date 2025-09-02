
delete from   dws_data.dws_t85_reelshort_currency_detail_stat_di  where etl_date='${TX_DATE}';

insert into dws_data.dws_t85_reelshort_currency_detail_stat_di
select id 
       ,t1.uuid
       ,t1.analysis_date
       ,t1.etl_date
       ,coalesce(t2.is_pay,0) as is_pay
       ,coalesce(t2.is_login,0) as is_login
       ,coalesce(t2.user_type,0) as user_type
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
       ,cast(t1.amount_increment as int) as amount_increment
       ,coalesce(t3.af_network_name,'') as     af_network_name
       ,coalesce(t2.af_channel,'') as          af_channel
       -- ,coalesce(t2.af_campaign_type,'') as    af_campaign_type
       ,t1.t_book_id
from 
(select max(id ) as id   
       ,uuid -- '应用中用户ID'
       ,analysis_date -- '分析日期'
       ,country_id -- '国家id'
       ,channel_id -- '应用渠道'
       ,version -- 'APP的应用版本(包体内置版本号versionname)'
       ,cversion -- '应用游戏版本号'
       ,res_version -- '应用资源版本号'
       ,language_id -- '游戏语言id'
       ,platform -- '操作系统类型 (android/ios/windows/mac os)'
       ,os_version -- '操作系统版本号'
       ,device_id -- '应用用于标识设备的唯一ID'
       ,ad_id -- '广告ID:Android=google adid IOS=idfa'
       -- ,androidid -- 'ANDROIDID'
       -- ,idfv -- 'IOS-idfv'
       ,scene_name -- '当前场景名称'
       ,page_name -- '当前页面名称'
       ,story_id -- '书籍id'
       ,chap_id -- '章节id'
       ,chap_order_id -- '章节序列id'
       ,vc_id  as currency_type -- '虚拟币ID'
       ,incre_type
       ,change_reason as trigger_id
       ,sum(change_amount ) as amount_increment
       ,etl_date
       ,t_book_id
from dwd_data.dwd_t06_reelshort_user_currency_get_or_expend_di
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
       -- ,androidid
       -- ,idfv
       ,scene_name
       ,page_name
       ,story_id
       ,chap_id
       ,chap_order_id
       ,vc_id
       ,incre_type
       ,change_reason
       ,t_book_id
)t1
left join (
   select uuid
        ,etl_date 
        ,is_pay
        ,is_login
        ,user_type
        -- ,af_network_name
        ,af_channel
        ,af_campaign
        ,af_adset
        ,af_ad
   from dwd_data.dwd_t01_reelshort_user_detail_info_di
   where etl_date='${TX_DATE}' 
) t2 
on t1.uuid=t2.uuid
left join (
           select distinct
                  uuid
                  ,af_network_name
           from dws_data.dim_t87_reelshort_ads_user_attr_df
           where etl_date='${TX_DATE}'
) as t3
on t3.uuid=t1.uuid
;
