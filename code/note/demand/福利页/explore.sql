-- 进入ER页面用户数
select
*
from chapters_log.reelshort_event_data_log
where analysis_date='${TX_DATE}'
      and event_name='m_page_enter'
      and cast(nvl(json_extract(properties,'$._page_name'),'') as varchar)='earn_rewards'
limit 10
;
-- select
-- *
-- from chapters_log.reelshort_event_data_custom
-- where analysis_date='${TX_DATE}'
      -- and sub_event_name='profile_page_click'
      -- and cast(nvl(json_extract(properties,'$._action'),'') as varchar)='earn_rewards_click'
-- limit 10
-- ;
select
t.analysis_date
,t.os_type                       -- 分发平台
,t.app_channel_id as  channel_id -- 分发渠道
,t.country_id                    -- 国家id
,t.app_lang as language_id       -- 游戏语言
,t.os_version                    -- APP主版本:操作系统版本号
,t.app_id                        -- 归因投放渠道?
,cast(nvl(json_extract(t.properties,'$._pre_page_name'),'') as varchar) as pre_page_name
,cast(nvl(json_extract(t.properties,'$._story_id'),'') as varchar)      as story_id
,t.app_user_id as uuid
,t.*
from chapters_log.reelshort_event_data_custom as t
where analysis_date='${TX_DATE}'
      and sub_event_name='profile_page_click'
      and cast(nvl(json_extract(properties,'$._action'),'') as varchar)='earn_rewards_click'
--       and cast(nvl(json_extract(t.properties,'$._pre_page_name'),'') as varchar)!=''  -- _pre_page_name为空
limit 10
;

select
*
from chapters_log.reelshort_event_data_custom
where analysis_date='${TX_DATE}'
      and sub_event_name in ('earn_rewards_popup','earn_rewards_popup_v2')
      and cast(nvl(json_extract(properties,'$._action'),'') as varchar) in('go_now','click')
limit 10
;

select
*
from chapters_log.reelshort_event_data_log
where analysis_date='${TX_DATE}'
      and event_name='m_widget_click'
      and cast(nvl(json_extract(properties,'$._element_name'),'') as varchar) in ('earn_rewards_bubble','earn_rewards_hall')
limit 10
;
-- push待确认
action='click'
and fcm_message_id in (select distinct _id from chapters_log.dts_project_v_cfg_multi_version)
where key in ('manualFcmConfig', 'autoFcmConfig', 'cachePushConfig')
and json_extract(`value`, '$.desc') like '%签到提醒-%'


-- 任一任务点击人数
select
*
from chapters_log.reelshort_event_data_custom
where analysis_date='${TX_DATE}'
      and sub_event_name in ('check_in_click')
      and cast(nvl(json_extract(properties,'$._action'),'') as varchar)
--       ='login_reward_click'
      in ('page_show', 'rule_click', 'login_reward_click', 'allow_tracking', 'tracking_claim', 'check_ad_extra_click', 'open_kisskiss', 'open_kiss_claimkissbonus，', 'bind_email', 'follow_whatsapp_gowhatsappGo', 'follow_whatsapp_claimwhatsappbonus，', 'tapjoy_task_gotapjoyGo', 'balance_click（）', 'h5ad_task_go h5Go', 'watch_dramas_go（go）', 'watch_dramas_claim', 'follow_task_gogo', 'follow_task_claim（）', 'topup_task_go（go）', 'topup_task_claim（）', 'collect_task_go（go）', 'collect_task_claim（）', 'push_task_gopush', 'push_task_claimpush')
limit 10
;

-- 广告总参与人数
select
*
from chapters_log.public_event_data
where analysis_date='${TX_DATE}'
      and event_name='m_admoney_event'
      and cast(json_extract(properties,'$._admoney_app_placeid') as int)=40001？
--       (10001,20001,20002,20003,10002,40001)
limit 10
;
select * from chapters_log.ad_monetization_firefly_di  where

"count(distinct x.uuid)
区分广告类型admoney_app_placeid 10001: 福利页激励视频，20001: 等免广告，20002: 章节解锁广告，20003：插屏广告，10002：签到额外广告
数据拼接H5广告，命名40001
chapters_log.ad_monetization_firefly_di
数据拼接积分墙，命名30001
chapters_log.ad_monetization_tapjoy_dev_revenue_di"


-- 广告总收入
select
      uuid
      ,run_id
      ,ad_revenue
      ,cast(ad_revenue as int)
      ,cast(ad_revenue as double) as ad_revenue -- 广告收入
from dwd_data.dwd_t07_reelshort_admoney_event_di
where etl_date='${TX_DATE}'
      and country_id not in ('98', '50')
      and language_id<>'99'
      and language_id not like 'zh-%'
      and action='impression'
      and admoney_app_placeid in (10001,20001,20002,20003,10002)



, h5_ad_rev as (select collect_date, sum(income) as 'h5_ad_rev'
                from(select collect_date, if(channel='T800301', 1, 2) as 'channel_id', income
                     from chapters_log.ad_monetization_firefly_di -- 广告变现数据-渠道firefly
                     where collect_date between '{{进组日期.start}}' and date_add('{{进组日期.end}}', 30)
                     and channel in ('T800301', 'T800402') )
                group by collect_date)
-- 用户
, h5_bonus_get_user as (select etl_date, uuid, sum(free_bonus_get) as h5_bonus_get
                         from dws_data.dws_t85_reelshort_srv_currency_change_stat_di -- V项目dws层:业务服用户虚拟币汇总表
                         where etl_date
                         between '{{进组日期.start}}' and date_add('{{进组日期.end}}', 30)
                         and  trigger_id='24'
                         group by etl_date, uuid)
-- 单价
, h5_bonus_price as (select a.*, b.h5_ad_rev, round(ifnull(b.h5_ad_rev/a.daily_h5_bonus_get, 0), 4) as 'h5_bonus_price'
                     from(select etl_date, sum(h5_bonus_get) as daily_h5_bonus_get
                          from h5_bonus_get_user
                          group by etl_date
                          ) a
                     left join h5_ad_rev b
                     on a.etl_date=b.collect_date)
-- H5广告
h5_bonus_get*h5_bonus_price



select etl_date, publisher_user_ids as 'uuid', sum(publisher_amount) as ""tapjoy_rev""
from chapters_log.ad_monetization_tapjoy_dev_revenue_di -- Tapjoy积分墙广告变现用户级收入数据
where app_name in (""ReelShort"", ""ReelShort - Short Movie & TV"")
and etl_date between '{{进组日期.start}}' and date_add('{{进组日期.end}}', 30)
group by etl_date, uuid



-- 总虚拟币产出人数
select
*
from chapters_log.public_event_data
where analysis_date='${TX_DATE}'
      and event_name='m_currency_change'
      and cast(nvl(json_extract(properties,'$._change_reason'),'') as varchar)!=''
      and cast(nvl(json_extract(properties,'$._page_name'),'') as varchar)!=''
--       and properties like '%Coins%'
limit 10
;


select sum(coins+bonus) from chapters_log.dts_project_v_account

-- 播放次数
select
count(distinct concat(story_id,uuid))
from dwd_data.dwd_t02_reelshort_play_event_di as t
where t.etl_date ='${TX_DATE}'
      and t.event_name ='m_play_event' -- 预定义或自定义事件名称
      and t.sub_event_name='play_start'
-- 播放人数
select
count(distinct uuid)
from dwd_data.dwd_t02_reelshort_play_event_di as t
where t.etl_date ='${TX_DATE}'
      and t.event_name ='m_play_event' -- 预定义或自定义事件名称
      and t.sub_event_name='play_start'