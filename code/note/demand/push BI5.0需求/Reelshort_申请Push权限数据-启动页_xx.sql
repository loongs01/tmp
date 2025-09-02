/*+ cte_execution_mode=shared */

-- https://uv4iolo7hz.feishu.cn/docx/CMpXdqFliowgyhxAFi5c4PoTnKe


with app_start_user as (select distinct analysis_date, device_id, uuid from dwd_data.dwd_t01_reelshort_device_start_di where country_id not in ('50','98') 
    and channel_id in ({{ channel }}) and event_name="m_app_start"
    and etl_date between '{{ analysis_date.start }}' and '{{ analysis_date.end }}')

, active_user as (select distinct a.analysis_date, a.uuid, date_sub(a.analysis_date, 1) as date1 from 
    (select distinct analysis_date, device_id, uuid from dws_data.dws_t80_reelshort_user_bhv_cnt_detail_di where country_id not in ('50','98') 
    and channel_id in ({{ channel }})
    and user_type in ('0')
    and etl_date between '{{ analysis_date.start }}' and '{{ analysis_date.end }}') a inner join app_start_user b on a.analysis_date=b.analysis_date and a.device_id=b.device_id)

-- fcm_push 1=开启，2=关闭	
, fcm_user as (select distinct analysis_date, etl_date, uuid, fcm_push from dwd_data.dim_t99_reelshort_user_lt_info where country_id not in ('50','98') 
    and channel_id in ({{ channel }})
    and etl_date between date_sub('{{ analysis_date.start }}', 1) and '{{ analysis_date.end }}'
    -- and analysis_date between date_sub('{{ analysis_date.start }}', 1) and '{{ analysis_date.end }}'
    )

--  符合条件UV：未授权推送权限，且非首日启动应用的用户数：前一日权限是关闭状态的部分用户   冷启动
, old_close_user as (select t1.* from active_user t1 inner join
        (select etl_date, uuid, fcm_push from fcm_user where fcm_push='2' and etl_date between '{{ analysis_date.start }}' and '{{ analysis_date.end }}') t2 on t1.uuid=t2.uuid and t1.date1=t2.etl_date)

-- 非首次启动开启权限 & 离开福利页开启权限: push_permission_popup  action="click"         "type":1  #弹窗出现场景，定义：1 非首次启动-启动应用引导弹窗；2 福利页-离开福利页推送授权弹窗
, push_permission_user as  (select distinct analysis_date, uuid, `type`, action from dwd_data.dwd_t02_reelshort_custom_event_di 
    where country_id not in ('50','98') 
    and sub_event_name="push_permission_popup" and `type`=1
	and channel_id in ({{ channel }}) and etl_date between '{{ analysis_date.start }}' and '{{ analysis_date.end }}')
    	
, new_open_user as (select distinct b.analysis_date, b.uuid from old_close_user a 
    inner join (select distinct analysis_date, uuid from push_permission_user where action="click") b on a.analysis_date=b.analysis_date and a.uuid=b.uuid
    inner join (select distinct analysis_date, uuid from fcm_user where fcm_push="1") c on a.analysis_date=c.analysis_date and a.uuid=c.uuid)


select t1.analysis_date
, count(distinct t1.uuid) as "符合条件UV"
, count(distinct t2.uuid) as "展示UV"
, count(distinct t2.uuid)/count(distinct t1.uuid)*100 as "展示率"

, count(distinct if(t2.action="close", t2.uuid)) as "点击（关闭）UV"
, count(distinct if(t2.action="close", t2.uuid))/count(distinct t2.uuid)*100 as "点击率（关闭）"

, count(distinct if(t2.action="click", t2.uuid)) as "点击（开启）UV"
, count(distinct if(t2.action="click", t2.uuid))/count(distinct t2.uuid)*100 as "点击率（开启）"

, count(distinct t3.uuid) as "成功开启权限UV"
, count(distinct t3.uuid)/count(distinct t2.uuid)*100 as "开启权限率"

from old_close_user t1 
left join push_permission_user t2 on t1.analysis_date=t2.analysis_date and t1.uuid=t2.uuid
left join new_open_user t3 on t1.analysis_date=t3.analysis_date and t1.uuid=t3.uuid
group by t1.analysis_date
order by t1.analysis_date;











