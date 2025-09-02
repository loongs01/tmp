 -- drop table if exists dws_data.dws_t85_reelshort_srv_order_stat_bitmap_di_v2;
 -- ALTER TABLE dws_data.dws_t85_reelshort_srv_order_stat_bitmap_di_v2 MODIFY COLUMN cli_start_pay_uv varbinary;
create table if not exists dws_data.dws_t85_reelshort_srv_order_stat_bitmap_di_v2 (
       id bigint auto_increment
       -- ,uuid varchar comment '用户id'
       ,analysis_date date comment '数据日期'
       -- ,device_id varchar comment '应用用于标识设备的唯一ID'
       -- ,device_model varchar comment '设备型号'
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
       -- ,sku_price int comment '支付金额amount'
       ,paychannel_type varchar comment '支付渠道ID:1001-谷歌支付，2001-苹果支付，3001-paypal'
       -- ,gid varchar comment '商品id'
       ,gid_type varchar comment '原order_type，商品类型0-6，0=普通商品 1=顶部横幅商品 2=快捷支付商品 3=快捷支付顶部横幅商品 4=VIP免广告权益 5=金币包 6=破冰礼包'
       ,scene_name varchar comment '场景名称'
       ,page_name varchar comment '页面名称'
       ,order_src varchar comment '业务服场景名称'
       ,story_id varchar comment '书籍id'
       -- ,nth_renewal bigint comment '第n次订阅'
       ,af_network_name varchar comment 'af归因渠道'
       ,is_login int comment '当天是否登陆1-是，0-否'
       ,user_type int comment '是否新用户1-是，0-否'
       ,is_pay int comment '是否当天支付1-是，0-否'
       ,vip_type int comment 'sub_status用户订阅状态:0=未购买  1=生效中 2=已过期'
       ,af_campaign varchar comment 'af广告投放计划'
       ,af_adset varchar comment 'af广告组'
       ,af_ad varchar comment 'af广告'
       -- ,reactive_flag int comment '90天回流标识'
       -- ,new_active_flag int comment '回流或新用户注册标识'
       -- ,reactive_flag_30d int comment '30天回流标识1-是'
       ,cli_is_pay_lt int comment '生命周期是否支付'
       ,cli_vip_type_lt int comment 'sub_status用户订阅状态:0=未购买  1=生效中 2=已过期'
       ,srv_vip_type_lt int comment '生命周期是否订阅'

	   ,total_revenue                    decimal(18,2)             comment '总收入'
       ,cli_non_sub_pay_amount           decimal(18,2)             comment 'IAP收入'
       ,srv_sub_pay_amount               decimal(18,2)             comment 'SUB收入'
       ,ad_revenue                       decimal(18,2)             comment 'IAA收入'
       ,cli_non_sub_pay_uv               varbinary                 comment 'IAP-付费人数'
       -- ,cli_non_sub_arppu                decimal(10,4)             comment 'IAP-ARPPU'
       -- ,cli_non_sub_pay_rate             decimal(10,4)             comment 'IAP-付费率'
       ,cli_non_sub_order_cnt            decimal(18,2)             comment '普通内购订单数 (新增分子指标)'
       ,active_uv                        varbinary                 comment '活跃用户数'
       -- ,avg_cli_nosub_pay_cnt            decimal(10,4)             comment 'IAP-人均付费次数'
       -- ,avg_cli_pay_nosub_pay_cnt        decimal(10,4)             comment 'IAP -付费用户人均付费次数'
       ,srv_new_sub_uv                   varbinary                 comment 'SUB-首次订阅人数'
       ,srv_renew_sub_uv                 varbinary                 comment 'SUB-续订人数'
       ,srv_new_sub_pay_amount           decimal(18,2)             comment 'SUB-首次订阅收入'
       ,srv_renew_sub_pay_amount         decimal(18,2)             comment 'SUB-续订收入'
       ,ad_uv                            varbinary                 comment 'IAA -广告用户数'
       -- ,ad_permeability_rate             decimal(10,4)             comment 'IAA - 广告渗透率'
       ,offerwall_ad_revenue             decimal(18,2)             comment '积分墙广告收入'
       ,offerwall_ad_uv                  int
       ,ad_incentive_uv                  varbinary                 comment 'IAA - 激励视频人数'
       ,incentive_ad_revenue             decimal(18,2)             comment 'IAA - 激励视频收入'
       ,cli_start_pay_cnt                bigint                    comment '支付开始人数'
       ,cli_start_pay_uv                 varbinary                 comment '支付开始次数'
       ,cli_end_pay_cnt                  bigint                    comment '支付成功人数'
       ,cli_end_pay_uv                   varbinary                 comment '支付成功次数'
       ,cli_complete_pay_cnt             bigint                    comment '支付验证通过人数'
       ,cli_complete_pay_uv              varbinary                 comment '支付验证通过次数'
       ,cli_failed_pay_cnt               bigint                    comment '支付失败人数'
       ,cli_failed_pay_uv                varbinary                 comment '支付失败次数'
       ,cli_cancel_pay_cnt               bigint                    comment '支付取消人数'
       ,cli_cancel_pay_uv                varbinary                 comment '支付取消次数'
       ,primary key (id,analysis_date)
) distribute by hash(id)
partition by value(date_format(analysis_date, '%y%m')) lifecycle 120 index_all='y'
storage_policy='hot'
comment='reelshort订单bitmap汇总表';

delete from dws_data.dws_t85_reelshort_srv_order_stat_bitmap_di_v2 where analysis_date='${TX_DATE}';
insert into dws_data.dws_t85_reelshort_srv_order_stat_bitmap_di_v2
select
       null as id
       ,etl_date as analysis_date
       ,platform
       ,country_id
       ,language_id
       ,channel_id
       ,version
       ,cversion
       ,res_version
       ,os_version
       ,ad_id
       ,channel_sku
       ,paychannel_type
       -- ,gid
       ,gid_type
       ,scene_name
       ,page_name
       ,order_src
       ,story_id
       -- ,nth_renewal
       ,af_network_name
       ,is_login
       ,user_type
       ,is_pay
       ,vip_type
       ,af_campaign
       ,af_adset
       ,af_ad
       -- ,reactive_flag
       -- ,new_active_flag
       -- ,reactive_flag_30d
       ,cli_is_pay_lt
       ,cli_vip_type_lt
       ,srv_vip_type_lt
       ,cast(nvl(sum(cli_non_sub_pay_amount),0)/100+nvl(sum(ad_revenue),0)+nvl(sum(srv_sub_pay_amount),0)/100 as decimal(18,2)) as total_revenue -- 总收入 -- 需调整sql字段
       ,cast(sum(cli_non_sub_pay_amount)/100 as decimal(18,2)) as cli_non_sub_pay_amount -- IAP收入
       ,cast(sum(srv_sub_pay_amount)/100 as decimal(18,2)) as srv_sub_pay_amount -- SUB收入 -- 需调整sql字段
       ,cast(sum(ad_revenue) as decimal(18,2)) as ad_revenue -- IAA收入
       ,rb_to_varbinary(rb_build(cast(split(GROUP_CONCAT(distinct if(t.cli_non_sub_pay_amount>0,t.uuid) SEPARATOR '#' ),'#') as array(int)))) as cli_non_sub_pay_uv -- IAP-付费人数
       -- ,if(count(distinct if(cli_non_sub_pay_amount>0,uuid))=0,0,cast(sum(cli_non_sub_pay_amount)/100 / count(distinct if(cli_non_sub_pay_amount>0,uuid)) as decimal(10,4))) as cli_non_sub_arppu -- IAP-ARPPU
       -- ,if(count(distinct uuid)=0,0,cast(count(distinct if(cli_non_sub_pay_amount>0,uuid)) / count(distinct uuid) as decimal(10,4))) as cli_non_sub_pay_rate -- IAP-付费率
       -- 新增分子指标
       ,cast(sum(cli_non_sub_order_cnt) as decimal(18,2)) as cli_non_sub_order_cnt -- 普通内购订单数 (新增分子指标)
       ,rb_to_varbinary(rb_build(cast(split(GROUP_CONCAT(distinct t.uuid SEPARATOR '#' ),'#') as array(int)))) as active_uv -- 活跃用户数

       -- ,if(count(distinct uuid)=0,0,cast(sum(cli_non_sub_order_cnt) / count(distinct uuid) as decimal(10,4))) as avg_cli_nosub_pay_cnt -- IAP-人均付费次数
       -- ,cast(sum(cli_non_sub_order_cnt) / count(distinct if(cli_non_sub_pay_amount>0,uuid)) as decimal(10,4)) as avg_cli_pay_nosub_pay_cnt -- IAP -付费用户人均付费次数 cli_non_sub_order_cnt/cli_non_sub_pay_uv
       ,rb_to_varbinary(rb_build(cast(split(GROUP_CONCAT(distinct if(t.srv_new_sub_order_cnt>0,t.uuid) SEPARATOR '#' ),'#') as array(int)))) as srv_new_sub_uv -- SUB-首次订阅人数 -- 需调整sql字段+sql文件
       ,rb_to_varbinary(rb_build(cast(split(GROUP_CONCAT(distinct if(t.srv_renew_sub_order_cnt>0,t.uuid) SEPARATOR '#' ),'#') as array(int)))) as srv_renew_sub_uv -- SUB-续订人数 -- 需调整sql字段+sql文件
       ,cast(sum(srv_new_sub_pay_amount)/100 as decimal(18,2)) as srv_new_sub_pay_amount -- SUB-首次订阅收入 -- 需调整sql字段+sql文件
       ,cast(sum(srv_renew_sub_pay_amount)/100 as decimal(18,2)) as srv_renew_sub_pay_amount -- SUB-续订收入 -- 需调整sql字段+sql文件
       -- 广告数据明细
       ,rb_to_varbinary(rb_build(cast(split(GROUP_CONCAT(distinct if(t.ad_start_cnt>0,t.uuid) SEPARATOR '#' ),'#') as array(int)))) as ad_uv -- IAA -广告用户数
       -- ,cast(count(distinct if(ad_start_cnt>0,uuid)) / count(distinct uuid) as decimal(10,4)) as ad_permeability_rate -- IAA - 广告渗透率 ad_uv/active_uv
       ,0 offerwall_ad_revenue
       ,0 offerwall_ad_uv
       ,rb_to_varbinary(rb_build(cast(split(GROUP_CONCAT(distinct if(t.incentive_ad_revenue>0,t.uuid) SEPARATOR '#' ),'#') as array(int)))) as ad_incentive_uv -- IAA - 激励视频人数
       ,cast(sum(incentive_ad_revenue) as decimal(18,2)) as incentive_ad_revenue -- IAA - 激励视频收入
       -- 支付漏斗 -- 新增
       ,sum(trans_start_cnt) as cli_start_pay_cnt -- 支付开始人数
       ,rb_to_varbinary(rb_build(cast(split(GROUP_CONCAT(distinct if(t.trans_start_cnt>0,t.uuid) SEPARATOR '#' ),'#') as array(int)))) as cli_start_pay_uv -- 支付开始次数
       ,sum(trans_end_cnt) as cli_end_pay_cnt -- 支付成功人数
       ,rb_to_varbinary(rb_build(cast(split(GROUP_CONCAT(distinct if(t.trans_end_cnt>0,t.uuid) SEPARATOR '#' ),'#') as array(int)))) as cli_end_pay_uv -- 支付成功次数
       ,sum(trans_complete_cnt) as cli_complete_pay_cnt -- 支付验证通过人数
       ,rb_to_varbinary(rb_build(cast(split(GROUP_CONCAT(distinct if(t.trans_complete_cnt>0,t.uuid) SEPARATOR '#' ),'#') as array(int)))) as cli_complete_pay_uv -- 支付验证通过次数
       ,sum(trans_failed_cnt) as cli_failed_pay_cnt -- 支付失败人数
       ,rb_to_varbinary(rb_build(cast(split(GROUP_CONCAT(distinct if(t.trans_failed_cnt>0,t.uuid) SEPARATOR '#' ),'#') as array(int)))) as cli_failed_pay_uv -- 支付失败次数
       ,sum(trans_cancel_cnt) as cli_cancel_pay_cnt -- 支付取消人数
       ,rb_to_varbinary(rb_build(cast(split(GROUP_CONCAT(distinct if(t.trans_cancel_cnt>0,t.uuid) SEPARATOR '#' ),'#') as array(int)))) as cli_cancel_pay_uv -- 支付取消次数
from dws_data.dws_t85_reelshort_srv_order_detail_stat_di_v2 as t
where t.etl_date='${TX_DATE}'
      and t.uuid regexp '^[0-9]+$'
      -- and t.uuid<2147483647
group by
        etl_date
        ,platform
        ,country_id
        ,language_id
        ,channel_id
        ,version
        ,cversion
        ,res_version
        ,os_version
        ,ad_id
        ,channel_sku
        ,paychannel_type
        -- ,gid
        ,gid_type
        ,scene_name
        ,page_name
        ,order_src
        ,story_id
        -- ,nth_renewal
        ,af_network_name
        ,is_login
        ,user_type
        ,is_pay
        ,vip_type
        ,af_campaign
        ,af_adset
        ,af_ad
        -- ,reactive_flag
        -- ,new_active_flag
        -- ,reactive_flag_30d
        ,cli_is_pay_lt
        ,cli_vip_type_lt
        ,srv_vip_type_lt
;

