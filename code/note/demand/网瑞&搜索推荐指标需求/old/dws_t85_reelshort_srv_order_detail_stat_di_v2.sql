 -- drop table if exists dws_data.dws_t85_reelshort_srv_order_detail_stat_di_v2;
create table if not exists dws_data.dws_t85_reelshort_srv_order_detail_stat_di_v2 (
       id bigint auto_increment
       ,uuid varchar comment '用户id'
       ,analysis_date date comment '数据日期'
       ,device_id varchar comment '应用用于标识设备的唯一ID'
       ,device_model varchar comment '设备型号'
       ,platform varchar comment '设备平台'
       ,country_id varchar comment '国家地区'
       ,language_id varchar comment '语言id'
       ,channel_id varchar comment '渠道ID'
       ,version varchar comment '客户端版本号,client_ver'
       ,cversion varchar comment '子版本'
       ,res_version varchar comment '应用资源版本号'
       ,os_version varchar comment '操作系统版本号'
       ,ad_id varchar comment '广告ID:Android=google adid IOS=idfa'
       ,channel_sku varchar comment '计费点,product_id'
       ,sku_price int comment '支付金额amount'
       ,paychannel_type varchar comment '支付渠道ID:1001-谷歌支付，2001-苹果支付，3001-paypal'
       ,gid varchar comment '商品id'
       ,gid_type varchar comment '原order_type，商品类型0-6，0=普通商品 1=顶部横幅商品 2=快捷支付商品 3=快捷支付顶部横幅商品 4=VIP免广告权益 5=金币包 6=破冰礼包'
       ,scene_name varchar comment '场景名称'
       ,page_name varchar comment '页面名称'
       ,order_src varchar comment '业务服场景名称'
       ,story_id varchar comment '书籍id'
       ,nth_renewal bigint comment '第n次订阅'
       ,af_network_name varchar comment 'af归因渠道'
       ,is_login int comment '当天是否登陆1-是，0-否'
       ,user_type int comment '是否新用户1-是，0-否'
       ,is_pay int comment '是否当天支付1-是，0-否'
       ,vip_type int comment 'sub_status用户订阅状态:0=未购买  1=生效中 2=已过期'
       ,af_campaign varchar comment 'af广告投放计划'
       ,af_adset varchar comment 'af广告组'
       ,af_ad varchar comment 'af广告'
       ,reactive_flag int comment '90天回流标识'
       ,new_active_flag int comment '回流或新用户注册标识'
       ,reactive_flag_30d int comment '30天回流标识1-是'
       ,is_pay_lt int comment '生命周期是否支付'
       ,vip_type_lt int comment 'sub_status用户订阅状态:0=未购买  1=生效中 2=已过期'
       ,srv_vip_type_lt int comment '生命周期是否订阅'
       ,first_pay_time_lt timestamp comment '生命周期首次支付时间'
       ,srv_order_cnt int comment '订单数(业务服)'
       ,srv_pay_amount int comment '订单金额(业务服)'
       ,srv_coins_get int comment '金币数(业务服)'
       ,srv_bonus_get int comment 'bonus数(业务服)'
       ,srv_subscribe_days int comment 'VIP天数(业务服)'
       ,srv_sub_order_cnt int comment '订阅订单数(业务服)'
       ,srv_sub_pay_amount int comment '订阅订单总金额(业务服)'
       ,srv_renew_sub_order_cnt int comment '续订订单数(业务服)'
       ,srv_renew_sub_pay_amount int comment '续订订单金额(业务服)'
       ,srv_new_sub_order_cnt int comment '新增订阅订单数(业务服)'
       ,srv_new_sub_pay_amount int comment '新增订阅订单金额(业务服)'
       ,srv_auto_renew_sub_pay_amount int comment '自动续订订阅金额(业务服)'
       ,cli_order_cnt int comment '订单数(客户端)'
       ,cli_pay_amount int comment '订单金额(客户端)'
       ,cli_sub_order_cnt int comment '订阅订单数(客户端)'
       ,cli_sub_pay_amount int comment '订阅订单金额(客户端)'
       ,cli_non_sub_order_cnt int comment '纯内购订单数(客户端)'
       ,cli_non_sub_pay_amount int comment '纯内购订单金额(客户端)'
       ,trans_show_cnt int comment '拉起支付sdk开始支付'
       ,trans_start_cnt int comment '拉起支付sdk开始支付'
       ,trans_end_cnt int comment '支付完成'
       ,trans_complete_cnt int comment '订单验证完成'
       ,trans_failed_cnt int comment '支付失败'
       ,trans_cancel_cnt int comment '支付取消'
       ,ad_invoke_cnt int comment '触发广告次数'
       ,ad_start_cnt int comment '开始广告次数'
       ,ad_complete_cnt int comment '完成广告次数'
       ,ad_loadfail_cnt int comment '加载失败的广告次数'
       ,ad_playfail_cnt int comment '播放失败的广告次数'
       ,ad_click_cnt int comment '点击广告次数'
       ,ad_impression_cnt int comment 'impression广告次数'
       ,incentive_ad_revenue decimal(20,10) comment '激励视频广告收入'
       ,ad_revenue decimal(20,10) comment '广告收益'
       ,etl_date date comment '清洗日期'
       ,primary key (id,uuid,etl_date)
) distribute by hash(id)
partition by value(date_format(etl_date, '%y%m')) lifecycle 120 index_all='y'
storage_policy='hot'
comment='reelshort订单汇总表';

delete from dws_data.dws_t85_reelshort_srv_order_detail_stat_di_v2 where etl_date='${TX_DATE}';
insert into dws_data.dws_t85_reelshort_srv_order_detail_stat_di_v2
with srv_norenewal_pay_uuid as
(
      select
            *
      from dwd_data.dwd_t05_reelshort_srv_order_detail_di
      where etl_date='${TX_DATE}' and order_id_rk=1 and (order_type<>2 or merchant_notify_order_id='')
)
,cli_pay_uuid as
(
      select
            *
      from dwd_data.dwd_t05_reelshort_order_detail_di
      where etl_date='${TX_DATE}' and order_id_rank=1
            and order_status=1
)
,srv_renewal_summary as -- 业务服：续订数据
(
      select
            cast(t1.uuid as varchar) as uuid,t1.analysis_date,t1.etl_date,t1.device_id,t1.device_model,t1.platform,t1.country_id,t1.language_id,t1.channel_id,t1.version,t1.cversion
            ,t1.paychannel_type,cast(t1.gid as varchar) as gid,cast(t1.gid_type as varchar) as gid_type,t1.channel_sku,t1.sku_price
            ,t1.res_version,t1.os_version,t1.ad_id
            ,concat_ws('|',t1.etl_date,t1.device_id,t1.platform,t1.country_id,t1.language_id,t1.channel_id,t1.version,t1.cversion,t1.channel_sku,t1.sku_price,t1.res_version,t1.os_version,t1.ad_id,story_id) as join_col
            ,'' as dlink_story_id
            ,nvl(scene_name,'') as scene_name
            ,nvl(page_name,'') as page_name
            ,nvl(order_src,'') as order_src
            ,nvl(story_id,'') as story_id
            -- ,concat_ws('|',etl_date,device_id,platform,country_id,language_id,channel_id,version,cversion,channel_sku,sku_price,res_version,os_version,ad_id,'') as join_col
           ,cast(count(distinct if(order_type in (1,2),merchant_notify_order_id,t1.order_id) ) as int) as srv_order_cnt
           ,cast(sum(t1.sku_price ) as int) as srv_pay_amount
           ,cast(sum(coins) as int) as srv_coins_get
           ,cast(sum(bonus) as int) as srv_bonus_get
           ,cast(sum(subscribe_days) as int) as srv_subscribe_days
           ,cast(count(distinct if(order_type in (1,2)   ,merchant_notify_order_id )) as int) as srv_sub_order_cnt
           ,cast(sum(if(order_type in (1,2)  ,t1.sku_price,0)) as int) as srv_sub_pay_amount

           ,cast(count(distinct if(order_type=2,merchant_notify_order_id )) as int) as srv_renew_sub_order_cnt
           ,cast(sum(if(order_type=2,t1.sku_price,0)) as int) as srv_renew_sub_pay_amount

           ,cast(count(distinct if(order_type=1,merchant_notify_order_id )) as int) as srv_new_sub_order_cnt
           ,cast(sum(if(order_type=1,t1.sku_price,0)) as int) as srv_new_sub_pay_amount

           ,cast(sum(if(order_type=2 and order_source=2,t1.sku_price,0)) as int) as srv_auto_renew_sub_pay_amount
           ,cast(max(vip_type) as int) as vip_type
           ,nth_renewal
      from
      (
          select
               *
               ,if(row_number() over(partition by uuid,order_id order by end_time)=1,1,row_number() over(partition by uuid,order_id order by end_time)-1) as nth_renewal -- 第n次续订
          from dwd_data.dwd_t05_reelshort_srv_order_detail_di
          where etl_date<='${TX_DATE}' and order_id_rk=1
               and order_id in(
                         select distinct order_id
                         from dwd_data.dwd_t05_reelshort_srv_order_detail_di
                         where etl_date='${TX_DATE}' and order_id_rk=1 and order_type<>0
                    )
      ) t1
      left join cli_pay_uuid t2
      on t1.uuid=t2.uuid and t1.merchant_notify_order_id=t2.order_id
      where t1.etl_date='${TX_DATE}' and order_type=2  and merchant_notify_order_id<>''
      group by t1.uuid,t1.analysis_date,t1.etl_date,t1.device_id,t1.device_model,t1.platform,t1.country_id,t1.language_id,t1.channel_id,t1.version,t1.cversion,t1.paychannel_type,t1.gid,t1.gid_type,t1.channel_sku,t1.sku_price,t1.res_version,t1.os_version,t1.ad_id
            ,scene_name,page_name,order_src,story_id
)
,srv_norenewal_pay_summary as -- 业务服：非续订数据
( -- 对首次订阅，但主订单和子订单不同。第n次续订暂定为1
      select
            cast(t1.uuid as varchar) as uuid,t1.analysis_date,t1.etl_date,t1.device_id,t1.device_model,t1.platform,t1.country_id,t1.language_id,t1.channel_id,t1.version,t1.cversion
            ,t1.paychannel_type,cast(t1.gid as varchar) as gid,cast(t1.gid_type as varchar) as gid_type,t1.channel_sku,t1.sku_price
            ,t1.res_version,t1.os_version,t1.ad_id
            ,concat_ws('|',t1.etl_date,t1.device_id,t1.platform,t1.country_id,t1.language_id,t1.channel_id,t1.version,t1.cversion,t1.channel_sku,t1.sku_price,t1.res_version,t1.os_version,t1.ad_id,story_id) as join_col
            ,'' as dlink_story_id
            ,if(t1.channel_sku rlike 'sub',1,0) as nth_renewal
            ,nvl(scene_name,'') as scene_name
            ,nvl(page_name,'') as page_name
            ,nvl(order_src,'') as order_src
            ,nvl(story_id,'') as story_id
            ,cast(count(distinct if(order_type in (1,2),merchant_notify_order_id,t1.order_id) ) as int) as srv_order_cnt
            ,cast(sum(t1.sku_price ) as int) as srv_pay_amount
            ,cast(sum(coins) as int) as srv_coins_get
            ,cast(sum(bonus) as int) as srv_bonus_get
            ,cast(sum(subscribe_days) as int) as srv_subscribe_days
            ,cast(count(distinct if(order_type in (1,2)   ,merchant_notify_order_id )) as int) as srv_sub_order_cnt
            ,cast(sum(if(order_type in (1,2),t1.sku_price,0)) as int) as srv_sub_pay_amount

            ,cast(count(distinct if(order_type=2 and merchant_notify_order_id<>'',merchant_notify_order_id )) as int) as srv_renew_sub_order_cnt
            ,cast(sum(if(order_type=2 and merchant_notify_order_id<>'',t1.sku_price,0)) as int) as srv_renew_sub_pay_amount
            ,cast(count(distinct if(order_type=1 or (merchant_notify_order_id='' and order_type=2),merchant_notify_order_id )) as int) as srv_new_sub_order_cnt
            ,cast(sum(if(order_type=1  or (merchant_notify_order_id='' and order_type=2),t1.sku_price,0)) as int) as srv_new_sub_pay_amount
            ,cast(sum(if(order_type=2 and order_source=2,t1.sku_price,0)) as int) as srv_auto_renew_sub_pay_amount
            ,cast(max(vip_type) as int) as vip_type
      from srv_norenewal_pay_uuid t1
      left join cli_pay_uuid t2
      on t1.uuid=t2.uuid and t1.order_id=t2.order_id
      where t1.etl_date='${TX_DATE}' and order_id_rk=1 
      group by t1.uuid,t1.analysis_date,t1.etl_date,t1.device_id,t1.device_model,t1.platform,t1.country_id,t1.language_id,t1.channel_id,t1.version,t1.cversion,t1.paychannel_type,t1.gid,t1.gid_type,t1.channel_sku,t1.sku_price,t1.res_version,t1.os_version,t1.ad_id
            ,scene_name,page_name,order_src,story_id
)
,cli_renewal_summary as -- 客户端:内购收益
(
      select
            uuid,analysis_date,etl_date,device_id,'' device_model,platform,country_id,language_id,channel_id,version,cversion
            ,'' paychannel_type,'' gid,'' gid_type,channel_sku,sku_price
            ,res_version,os_version,ad_id
            ,scene_name,page_name,order_src,story_id
            ,concat_ws('|',etl_date,device_id,platform,country_id,language_id,channel_id,version,cversion,channel_sku,sku_price,res_version,os_version,ad_id,story_id) as join_col
            ,count(distinct case when order_status=1  and order_id_rank=1 then order_id end) as cli_order_cnt
            ,sum(case when  order_status=1  and order_id_rank=1  then sku_price else 0 end) as cli_pay_amount
            ,count(distinct case when order_status=1  and order_id_rank=1 and channel_sku  like '%sub%' then order_id end) as cli_sub_order_cnt
            ,sum( case when order_status=1  and order_id_rank=1 and channel_sku  like '%sub%' then sku_price  else 0 end) as cli_sub_pay_amount
            ,0 as nth_renewal
      from cli_pay_uuid
      group by uuid,analysis_date,etl_date,device_id,platform,country_id,language_id,channel_id,version,cversion
            ,channel_sku,sku_price
            ,res_version,os_version,ad_id
            ,scene_name,page_name,order_src,story_id
)
,cli_transaction_summary as  -- 客户端:支付流程
(
      select
            uuid,analysis_date,etl_date,device_id,'' device_model,platform,country_id,language_id,channel_id,version,cversion
            ,'' paychannel_type,'' gid,'' gid_type,channel_sku,sku_price
            ,res_version,os_version,ad_id
            ,scene_name,page_name,order_src,story_id
            ,concat_ws('|',etl_date,device_id,platform,country_id,language_id,channel_id,version,cversion,channel_sku,sku_price,res_version,os_version,ad_id,story_id) as join_col
            ,cast(sum(case when pay_status='pay_show' then 1 else 0 end) as int) as trans_show_cnt
            ,cast(sum(case when pay_status='pay_start' then 1 else 0 end) as int) as trans_start_cnt
            ,cast(sum(case when pay_status='pay_end' then 1 else 0 end) as int) as trans_end_cnt
            ,cast(sum(case when pay_status='pay_complete' then 1 else 0 end) as int) as trans_complete_cnt
            ,cast(sum(case when pay_status='pay_failed' then 1 else 0 end) as int) as trans_failed_cnt
            ,cast(sum(case when pay_status='pay_cancel' then 1 else 0 end) as int) as trans_cancel_cnt
            ,0 as nth_renewal
      from dwd_data.dwd_t05_reelshort_transaction_detail_di
      where etl_date='${TX_DATE}' and pay_status in('pay_show','pay_start','pay_end','pay_complete','pay_failed','pay_cancel')
      group by uuid,analysis_date,etl_date,device_id,platform,country_id,language_id,channel_id,version,cversion
            ,channel_sku,sku_price
            ,res_version,os_version,ad_id
            ,scene_name,page_name,order_src,story_id
)
,ad_summary as  -- 客户端:支付流程
(
      select
            uuid,analysis_date,etl_date,device_id,'' device_model,platform,country_id,language_id,channel_id,version,cversion
            ,'' paychannel_type,'' gid,'' gid_type,'ad' as channel_sku,0 sku_price
            ,res_version,os_version,ad_id
            ,'' scene_name,'' page_name,story_id
            ,concat_ws('|',etl_date,device_id,platform,country_id,language_id,channel_id,version,cversion,'','',res_version,os_version,ad_id,story_id) as join_col
                        ,cast(count(case when action='invoke' then adunit_id end ) as int) as ad_invoke_cnt
                        ,cast(count(case when action='start' then adunit_id  end ) as int) as ad_start_cnt
                        ,cast(count(case when action='complete' then adunit_id  end ) as int) as ad_complete_cnt
                        ,cast(count(case when action='loadfail' then adunit_id  end ) as int) as ad_loadfail_cnt
                        ,cast(count(case when action='playfail' then adunit_id  end ) as int) as ad_playfail_cnt
                        ,cast(count(case when action='click' then adunit_id  end ) as int) as ad_click_cnt
                        ,cast(count(case when action='impression' then adunit_id  end ) as int) as ad_impression_cnt
                        ,cast(sum(cast(if(admoney_app_placeid in('10001','20002','10002'),ad_revenue) as double)) as decimal(20,10)) as incentive_ad_revenue
            ,cast(sum(ad_revenue) as decimal(20,10)) as ad_revenue
            ,0 as nth_renewal
            ,'' as order_src
      from dwd_data.dwd_t07_reelshort_admoney_event_di
      where etl_date='${TX_DATE}' and uuid<>'0' and  action in('invoke','start','complete','loadfail','playfail','click','impression')
      group by uuid,analysis_date,etl_date,device_id,platform,country_id,language_id,channel_id,version,cversion
            ,res_version,os_version,ad_id
            ,story_id
)


,summay_data as
(
      select
            coalesce(t1.uuid,t2.uuid,t3.uuid,t4.uuid,t5.uuid) as uuid
            ,coalesce(t1.etl_date,t2.etl_date,t3.etl_date,t4.etl_date,t5.etl_date) as analysis_date
            ,coalesce(t1.device_id,t2.device_id,t3.device_id,t4.device_id,t5.device_id) as device_id
            ,coalesce(t1.platform,t2.platform,t3.platform,t4.platform,t5.platform) as platform
            ,coalesce(t1.country_id,t2.country_id,t3.country_id,t4.country_id,t5.country_id) as country_id
            ,coalesce(t1.language_id,t2.language_id,t3.language_id,t4.language_id,t5.language_id) as language_id
            ,coalesce(t1.channel_id,t2.channel_id,t3.channel_id,t4.channel_id,t5.channel_id) as channel_id
            ,coalesce(t1.version,t2.version,t3.version,t4.version,t5.version) as version
            ,coalesce(t1.cversion,t2.cversion,t3.cversion,t4.cversion,t5.cversion) as cversion
            ,coalesce(t1.channel_sku,t2.channel_sku,t3.channel_sku,t4.channel_sku,t5.channel_sku) as channel_sku
            ,coalesce(t1.sku_price,t2.sku_price,t3.sku_price,t4.sku_price,t5.sku_price) as sku_price
            ,coalesce(t1.res_version,t2.res_version,t3.res_version,t4.res_version,t5.res_version) as res_version
            ,coalesce(t1.os_version,t2.os_version,t3.os_version,t4.os_version,t5.os_version) as os_version
            ,coalesce(t1.ad_id,t2.ad_id,t3.ad_id,t4.ad_id,t5.ad_id) as ad_id
            ,coalesce(t1.device_model,t2.device_model,t3.device_model,t4.device_model,t5.device_model) as device_model
            ,coalesce(t1.paychannel_type,t2.paychannel_type,t3.paychannel_type,t4.paychannel_type,t5.paychannel_type) as paychannel_type
            ,coalesce(t1.gid,t2.gid,t3.gid,t4.gid,t5.gid) as gid
            ,coalesce(t1.gid_type,t2.gid_type,t3.gid_type,t4.gid_type,t5.gid_type) as gid_type
            ,coalesce(t1.scene_name,t2.scene_name,t3.scene_name,t4.scene_name,t5.scene_name) as scene_name
            ,coalesce(t1.page_name,t2.page_name,t3.page_name,t4.page_name,t5.page_name) as page_name
            ,coalesce(t1.order_src,t2.order_src,t3.order_src,t4.order_src,t5.order_src) as order_src
            ,coalesce(t1.story_id,t2.story_id,t3.story_id,t4.story_id,t5.story_id) as story_id
            ,coalesce(t1.nth_renewal,t2.nth_renewal,t3.nth_renewal,t4.nth_renewal,t5.nth_renewal) as nth_renewal

            ,cast(nvl(sum(t3.srv_order_cnt),0)+nvl(sum(t4.srv_order_cnt),0) as int) as srv_order_cnt -- 订单数(业务服)
            ,cast(nvl(sum(t3.srv_pay_amount),0)+nvl(sum(t4.srv_pay_amount),0) as int) as srv_pay_amount -- 订单金额(业务服)
            ,cast(nvl(sum(t3.srv_coins_get),0)+nvl(sum(t4.srv_coins_get),0) as int) as srv_coins_get -- 金币数(业务服)
            ,cast(nvl(sum(t3.srv_bonus_get),0)+nvl(sum(t4.srv_bonus_get),0) as int) as srv_bonus_get -- bonus数(业务服)
            ,cast(nvl(sum(t3.srv_subscribe_days),0)+nvl(sum(t4.srv_subscribe_days),0) as int) as srv_subscribe_days -- VIP天数(业务服)
            ,cast(nvl(sum(t3.srv_sub_order_cnt),0)+nvl(sum(t4.srv_sub_order_cnt),0) as int) as srv_sub_order_cnt -- 订阅订单数(业务服)
            ,cast(nvl(sum(t3.srv_sub_pay_amount),0)+nvl(sum(t4.srv_sub_pay_amount),0) as int) as srv_sub_pay_amount -- 订阅订单总金额(业务服)
            ,cast(nvl(sum(t3.srv_renew_sub_order_cnt),0)+nvl(sum(t4.srv_renew_sub_order_cnt),0) as int) as srv_renew_sub_order_cnt -- 续订订单数(业务服)
            ,cast(nvl(sum(t3.srv_renew_sub_pay_amount),0)+nvl(sum(t4.srv_renew_sub_pay_amount),0) as int) as srv_renew_sub_pay_amount -- 续订订单金额(业务服)
            ,cast(nvl(sum(t3.srv_new_sub_order_cnt),0)+nvl(sum(t4.srv_new_sub_order_cnt),0) as int) as srv_new_sub_order_cnt -- 新增订阅订单数(业务服)
            ,cast(nvl(sum(t3.srv_new_sub_pay_amount),0)+nvl(sum(t4.srv_new_sub_pay_amount),0) as int) as srv_new_sub_pay_amount -- 新增订阅订单金额(业务服)
            ,cast(nvl(sum(t3.srv_auto_renew_sub_pay_amount),0)+nvl(sum(t4.srv_auto_renew_sub_pay_amount),0) as int) as srv_auto_renew_sub_pay_amount -- 自动续订订阅金额(业务服)
            ,nvl(cast(sum(cli_order_cnt) as int),0) as cli_order_cnt -- 订单数(客户端)
            ,nvl(cast(sum(cli_pay_amount) as int),0) as cli_pay_amount -- 订单金额(客户端)
            ,nvl(cast(sum(cli_sub_order_cnt) as int),0) as cli_sub_order_cnt -- 订阅订单数(客户端)
            ,nvl(cast(sum(cli_sub_pay_amount) as int),0) as cli_sub_pay_amount -- 订阅订单金额(客户端)
            ,nvl(cast(sum(trans_show_cnt) as int),0) as trans_show_cnt -- 拉起支付sdk开始支付
            ,nvl(cast(sum(trans_start_cnt) as int),0) as trans_start_cnt -- 拉起支付sdk开始支付
            ,nvl(cast(sum(trans_end_cnt) as int),0) as trans_end_cnt -- 支付完成
            ,nvl(cast(sum(trans_complete_cnt) as int),0) as trans_complete_cnt -- 订单验证完成
            ,nvl(cast(sum(trans_failed_cnt) as int),0) as trans_failed_cnt -- 支付失败
            ,nvl(cast(sum(trans_cancel_cnt) as int),0) as trans_cancel_cnt -- 支付取消
            ,nvl(cast(sum(ad_invoke_cnt) as int),0) as ad_invoke_cnt -- 触发广告次数
            ,nvl(cast(sum(ad_start_cnt) as int),0) as ad_start_cnt -- 开始广告次数
            ,nvl(cast(sum(ad_complete_cnt) as int),0) as ad_complete_cnt -- 完成广告次数
            ,nvl(cast(sum(ad_loadfail_cnt) as int),0) as ad_loadfail_cnt -- 加载失败的广告次数
            ,nvl(cast(sum(ad_playfail_cnt) as int),0) as ad_playfail_cnt -- 播放失败的广告次数
            ,nvl(cast(sum(ad_click_cnt) as int),0) as ad_click_cnt -- 点击广告次数
            ,nvl(cast(sum(ad_impression_cnt) as int),0) as ad_impression_cnt -- impression广告次数
            ,nvl(cast(sum(incentive_ad_revenue) as decimal(20,10)),0.0) as incentive_ad_revenue -- 广告收益
            ,nvl(cast(sum(ad_revenue) as decimal(20,10)),0.0) as ad_revenue -- 广告收益
      from cli_transaction_summary t1
      full join cli_renewal_summary t2
      on t1.uuid=t2.uuid and t1.join_col=t2.join_col
            and t1.device_model=t2.device_model and t1.paychannel_type=t2.paychannel_type and t1.gid=t2.gid and t1.gid_type=t2.gid_type
            and t1.scene_name=t2.scene_name and t1.page_name=t2.page_name and t1.order_src=t2.order_src and t1.story_id=t2.story_id
            and t1.nth_renewal=t2.nth_renewal
      full join srv_norenewal_pay_summary t3
      on t1.uuid=t3.uuid and t1.join_col=t3.join_col
            and t1.device_model=t3.device_model and t1.paychannel_type=t3.paychannel_type and t1.gid=t3.gid and t1.gid_type=t3.gid_type
            and t1.scene_name=t3.scene_name and t1.page_name=t3.page_name and t1.order_src=t3.order_src and t1.story_id=t3.story_id
            and t1.nth_renewal=t3.nth_renewal
      full join srv_renewal_summary t4
      on t1.uuid=t4.uuid and t1.join_col=t4.join_col
            and t1.device_model=t4.device_model and t1.paychannel_type=t4.paychannel_type and t1.gid=t4.gid and t1.gid_type=t4.gid_type
            and t1.scene_name=t4.scene_name and t1.page_name=t4.page_name and t1.order_src=t4.order_src and t1.story_id=t4.story_id
            and t1.nth_renewal=t4.nth_renewal
      full join ad_summary t5
      on t1.uuid=t5.uuid and t1.join_col=t5.join_col
            and t1.device_model=t5.device_model and t1.paychannel_type=t5.paychannel_type and t1.gid=t5.gid and t1.gid_type=t5.gid_type
            -- and t1.scene_name=t5.scene_name and t1.page_name=t5.page_name and t1.story_id=t5.story_id
            and t1.nth_renewal=t5.nth_renewal
      group by
            coalesce(t1.uuid,t2.uuid,t3.uuid,t4.uuid,t5.uuid)
            ,coalesce(t1.etl_date,t2.etl_date,t3.etl_date,t4.etl_date,t5.etl_date)
            ,coalesce(t1.device_id,t2.device_id,t3.device_id,t4.device_id,t5.device_id)
            ,coalesce(t1.platform,t2.platform,t3.platform,t4.platform,t5.platform)
            ,coalesce(t1.country_id,t2.country_id,t3.country_id,t4.country_id,t5.country_id)
            ,coalesce(t1.language_id,t2.language_id,t3.language_id,t4.language_id,t5.language_id)
            ,coalesce(t1.channel_id,t2.channel_id,t3.channel_id,t4.channel_id,t5.channel_id)
            ,coalesce(t1.version,t2.version,t3.version,t4.version,t5.version)
            ,coalesce(t1.cversion,t2.cversion,t3.cversion,t4.cversion,t5.cversion)
            ,coalesce(t1.channel_sku,t2.channel_sku,t3.channel_sku,t4.channel_sku,t5.channel_sku)
            ,coalesce(t1.sku_price,t2.sku_price,t3.sku_price,t4.sku_price,t5.sku_price)
            ,coalesce(t1.res_version,t2.res_version,t3.res_version,t4.res_version,t5.res_version)
            ,coalesce(t1.os_version,t2.os_version,t3.os_version,t4.os_version,t5.os_version)
            ,coalesce(t1.ad_id,t2.ad_id,t3.ad_id,t4.ad_id,t5.ad_id)
            ,coalesce(t1.device_model,t2.device_model,t3.device_model,t4.device_model,t5.device_model)
            ,coalesce(t1.paychannel_type,t2.paychannel_type,t3.paychannel_type,t4.paychannel_type,t5.paychannel_type)
            ,coalesce(t1.gid,t2.gid,t3.gid,t4.gid,t5.gid)
            ,coalesce(t1.gid_type,t2.gid_type,t3.gid_type,t4.gid_type,t5.gid_type)
            ,coalesce(t1.scene_name,t2.scene_name,t3.scene_name,t4.scene_name,t5.scene_name)
            ,coalesce(t1.page_name,t2.page_name,t3.page_name,t4.page_name,t5.page_name)
            ,coalesce(t1.order_src,t2.order_src,t3.order_src,t4.order_src,t5.order_src)
            ,coalesce(t1.story_id,t2.story_id,t3.story_id,t4.story_id,t5.story_id)
            ,coalesce(t1.nth_renewal,t2.nth_renewal,t3.nth_renewal,t4.nth_renewal,t5.nth_renewal)
)

select
      null as id
      ,t1.uuid -- 用户uid
      ,analysis_date
      ,device_id -- 应用用于标识设备的唯一ID
      ,device_model -- 设备型号
      ,platform -- 设备平台
      ,country_id -- 国家地区
      ,language_id -- 语言id
      ,channel_id -- 渠道ID
      ,version -- 客户端版本号,client_ver
      ,cversion -- 子版本
      ,res_version -- 应用资源版本号
      ,os_version -- 操作系统版本号
      ,ad_id -- 广告ID:Android=google adid IOS=idfa
      ,channel_sku -- 计费点,product_id
      ,paychannel_type -- 支付渠道ID:1001-谷歌支付，2001-苹果支付，3001-paypal
      ,gid -- 商品id
      ,gid_type -- 原order_type，商品类型0-6，0=普通商品 1=顶部横幅商品 2=快捷支付商品 3=快捷支付顶部横幅商品 4=VIP免广告权益 5=金币包 6=破冰礼包
      ,scene_name -- 场景名称
      ,page_name -- 页面名称
      ,order_src -- 页面名称
      ,if(length(story_id)=24,story_id,'') as story_id -- 书籍id
      ,nth_renewal  -- 第n次订阅
      ,nvl(t2.af_network_name,'') as af_network_name -- af归因渠道
      ,nvl(t2.is_login,0) as is_login -- 当天是否登陆1-是，0-否
      ,nvl(t2.user_type,0) as user_type -- 是否新用户1-是，0-否
      ,cast(if(srv_order_cnt+cli_order_cnt>0,1,0) as int) as is_pay -- 是否当天支付1-是，0-否
      ,nvl(t3.vip_type,0) as vip_type -- sub_status用户订阅状态:0=未购买  1=生效中 2=已过期
      ,nvl(t2.af_campaign,'') as af_campaign -- af广告投放计划
      ,nvl(t2.af_adset,'') as af_adset -- af广告组
      ,nvl(t2.af_ad,'') as af_ad -- af广告
      ,nvl(t2.reactive_flag,0) as reactive_flag -- 90天回流标识
      ,coalesce(if(t2.user_type=1,1,t2.reactive_flag_30d),0) as new_active_flag -- 回流或新用户注册标识
      ,nvl(t2.reactive_flag_30d,0) as reactive_flag_30d -- 30天回流标识1-是
      ,nvl(t2.is_pay_lt,0) as is_pay_lt -- 生命周期是否支付
      ,nvl(t2.vip_type_lt,0) as vip_type_lt -- sub_status用户订阅状态:0=未购买  1=生效中 2=已过期
      ,nvl(t2.srv_vip_type_lt,0) as srv_vip_type_lt -- 生命周期是否订阅
      ,t2.first_pay_time as first_pay_time_lt -- 生命周期首次支付时间
      ,srv_order_cnt -- 订单数(业务服)
      ,srv_pay_amount -- 订单金额(业务服)
      ,srv_coins_get -- 金币数(业务服)
      ,srv_bonus_get -- bonus数(业务服)
      ,srv_subscribe_days -- VIP天数(业务服)
      ,srv_sub_order_cnt -- 订阅订单数(业务服)
      ,srv_sub_pay_amount -- 订阅订单总金额(业务服)
      ,srv_renew_sub_order_cnt -- 续订订单数(业务服)
      ,srv_renew_sub_pay_amount -- 续订订单金额(业务服)
      ,srv_new_sub_order_cnt -- 新增订阅订单数(业务服)
      ,srv_new_sub_pay_amount -- 新增订阅订单金额(业务服)
      ,srv_auto_renew_sub_pay_amount -- 自动续订订阅金额(业务服)
      ,cli_order_cnt -- 订单数(客户端)
      ,cli_pay_amount -- 订单金额(客户端)
      ,cli_sub_order_cnt -- 订阅订单数(客户端)
      ,cli_sub_pay_amount -- 订阅订单金额(客户端)
      ,cast(cli_order_cnt-cli_sub_order_cnt as int) as  cli_non_sub_order_cnt -- 纯内购订单数(客户端)
      ,cast(cli_pay_amount-cli_sub_pay_amount as int) as cli_non_sub_pay_amount -- 纯内购订单金额(客户端)
      ,trans_show_cnt -- 拉起支付sdk开始支付
      ,trans_start_cnt -- 拉起支付sdk开始支付
      ,trans_end_cnt -- 支付完成
      ,trans_complete_cnt -- 订单验证完成
      ,trans_failed_cnt -- 支付失败
      ,trans_cancel_cnt -- 支付取消

          ,ad_invoke_cnt -- 触发广告次数
          ,ad_start_cnt -- 开始广告次数
          ,ad_complete_cnt -- 完成广告次数
          ,ad_loadfail_cnt -- 加载失败的广告次数
          ,ad_playfail_cnt -- 播放失败的广告次数
          ,ad_click_cnt -- 点击广告次数
          ,ad_impression_cnt -- impression广告次数
          ,incentive_ad_revenue -- 激励视频广告收入
      ,ad_revenue -- 广告收益
      ,'${TX_DATE}' as etl_date -- 清洗日期
      ,cast(sku_price /100 as decimal(10,2)) as sku_price -- 支付金额amount
      ,media_type
      ,dlink_story_id
      ,nvl(t3.recent_visit_source,'')                  as recent_visit_source -- 最近一次访问来源
      ,nvl(t3.recent_visit_source_type,'')             as recent_visit_source_type -- 最近一次访问来源类型
      ,nvl(t2.first_visit_landing_page,'')                as first_visit_landing_page -- 首次访问着陆页
      ,nvl(t2.first_visit_landing_page_story_id,'')       as first_visit_landing_page_story_id -- 首次访问着陆页书籍id
      ,nvl(t2.first_visit_landing_page_chap_id,'')        as first_visit_landing_page_chap_id  -- 首次访问着陆页章节id  
from summay_data t1
left join
(
      select distinct 
            uuid
            ,af_network_name
            ,if(date(last_login_time)='${TX_DATE}',1,0) as is_login
            ,if(date(register_time)='${TX_DATE}',1,0) AS user_type
            ,af_campaign
            ,af_adset
            ,af_ad
            ,if(analysis_date='${TX_DATE}' and cr_days>=89,1,0) as reactive_flag
            ,if(analysis_date='${TX_DATE}' and cr_days>=29,1,0) as reactive_flag_30d
            ,is_pay_lt
            ,vip_type_lt
            ,srv_vip_type_lt
            ,first_pay_time
            ,first_visit_landing_page          -- 首次访问着陆页
            ,first_visit_landing_page_story_id -- 首次访问着陆页书籍id
            ,first_visit_landing_page_chap_id  -- 首次访问着陆页章节id
      from dwd_data.dim_t99_reelshort_user_lt_info
      where etl_date='${TX_DATE}' and uuid in (select distinct uuid from summay_data)
)t2
on t1.uuid=t2.uuid
left join
(
      select
            uuid,vip_type
            ,recent_visit_source -- 最近一次访问来源
            ,recent_visit_source_type -- 最近一次访问来源类型
      from dwd_data.dwd_t01_reelshort_user_detail_info_di
      where etl_date='${TX_DATE}' and uuid in (select distinct uuid from summay_data)
) t3
on t1.uuid=t3.uuid
left join
(
  select uuid,media_type,dlink_story_id
  from 
  (
    select   
      uuid 
      ,media_type
      ,book_id as dlink_story_id
      ,row_number()over(partition by uuid order by  analysis_date desc ) as rank
    from dws_data.dws_t87_reelshort_book_attr_di
    where analysis_date<='${TX_DATE}' and uuid in (select distinct uuid from summay_data)
  )
  where rank=1
) t4
on t1.uuid=t4.uuid
;

replace into dim_data.dim_t99_reelshort_order_src_info
select
      order_src
      ,order_src_name
      ,created_at
      ,updated_at
from ana_data.dim_t99_reelshort_order_src_info
;