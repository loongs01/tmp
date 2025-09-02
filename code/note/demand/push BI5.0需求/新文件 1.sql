select distinct uid as uuid
                          ,amount as subscribe_price -- 订阅单价
                          ,merchant_order_id as parm7
                          ,convert_tz(from_unixtime(end_time), 'Asia/Shanghai', 'America/Los_Angeles') as stime
                   from chapters_log.dts_project_v_payment
                   where is_abnormal=0


select
      distinct analysis_date
      ,uuid
	  ,sku_price
from dwd_data.dwd_t05_reelshort_srv_order_detail_di t
where etl_date='${TX_DATE}' and order_type in(1,2)


select
      analysis_date
      ,uuid
	  ,sku_price
	  ,row_number() over (partition by uuid order by analysis_date asc) as rn
from dwd_data.dwd_t05_reelshort_srv_order_detail_di t
where order_type=1




select
analysis_date                                                                             as "日期"
,platform                                                                                 as "分发平台"
,count(distinct if(t1.is_play=1, t1.uuid))                                                as "Push转化播放用户数"                  -- Push转化播放用户数
,count(distinct if(t1.click_cnt>=1, t1.uuid))                                             as "点击用户数"                      -- 点击push任务用户数
,count(distinct if(t1.is_play=1, t1.uuid))/count(distinct if(t1.click_cnt>=1, t1.uuid))   as "Push转化播放率UV"
,count(distinct if(t1.ad_revenue>0, t1.uuid))                                             as push_advertising_uv        -- push广告收入用户数
,count(distinct if(t1.ad_revenue>0, t1.uuid))/count(distinct if(t1.click_cnt>=1, t1.uuid))
from dwd_data.dwd_t04_reelshort_opc_push_detail_di t1
-- where platform in('1','2')
group by t1.analysis_date,platform
order by analysis_date,platform
;




