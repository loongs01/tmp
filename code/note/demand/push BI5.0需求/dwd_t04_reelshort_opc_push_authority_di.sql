 delete from dwd_data.dwd_t04_reelshort_opc_push_authority_di  where analysis_date='${TX_DATE}';
/*+ cte_execution_mode=shared */
insert into  dwd_data.dwd_t04_reelshort_opc_push_authority_di
with active_user as (
                      select
                            analysis_date
                            ,etl_date
                            ,platform
                            ,version
                            ,country_id
                            ,language_id
                            ,channel_id
                            ,uuid
                            ,max(user_type) as user_type
                      from dwd_data.dwd_t01_reelshort_user_info_di -- V项目dwd层:当日所有用户表
                      where etl_date='${TX_DATE}' and uuid<>'nil'
                      group by
                              analysis_date
                              ,etl_date
                              ,platform
                              ,version
                              ,country_id
                              ,language_id
                              ,channel_id
                              ,uuid
                     )
-- fcm_push 1=开启，2=关闭
,fcm_user as (select distinct analysis_date, etl_date, uuid, fcm_push
              from dwd_data.dim_t99_reelshort_user_lt_info -- v项目dwd层:全量用户码表
              where country_id not in ('50','98')
              -- and channel_id in ({{ channel }})
              -- and language_id in ({{ lanuage }})
               and etl_date >=date_sub('${TX_DATE}', 1)
               and etl_date <='${TX_DATE}'
               -- and analysis_date between date_sub('{{ analysis_date.start }}', 1) and '{{ analysis_date.end }}'
               )
-- 新开启权限用户：前一次是关闭状态，当日变为开启状态的这部分用户
-- 关闭权限用户：昨天之前开启，当日关闭权限用户
,new_authority_user as (select t2.*
                   from active_user t1
                   join (select
                         analysis_date
                         ,a.date1
                         ,a.uuid
                         ,a.fcm_push
                         ,if(a.date1=b.etl_date and a.fcm_push='1' and (b.fcm_push='2' or b.uuid is null ),1,0) as is_new_open_authority
                         ,if(a.date1=b.etl_date and a.fcm_push='2' and (b.fcm_push='1'),1,0) as is_close_authority
                         from (select analysis_date, uuid, date_sub(analysis_date, 1)as date1,fcm_push from fcm_user where analysis_date='${TX_DATE}') a
                         left join (select etl_date, uuid, fcm_push from fcm_user) b
                         on a.uuid=b.uuid
                        ) t2
                   on t1.uuid=t2.uuid and t1.analysis_date=t2.analysis_date
                   )
-- 申请权限场景: 取当天最后一次
-- 首次启动开启权限UV: sys_notification_popup  action="allow_click"
-- 预约书架开启权限UV：system_permission_popup action="set_notification"
-- 非首次启动开启权限 & 离开福利页开启权限: push_permission_popup  action="click"   "type":1  #弹窗出现场景，定义：1 非首次启动-启动应用引导弹窗；2 福利页-离开福利页推送授权弹窗

, push_permission_user as (select a.analysis_date, a.uuid, a.ctime, max(sub_event_name) as "sub_event_name",max(action) as action, max(`type`) as "type", max(`scene`) as "scene" from
                    (select distinct analysis_date, uuid, ctime, sub_event_name,action, `type`, cast(json_extract(properties, "$.scene") as string) as "scene" from dwd_data.dwd_t02_reelshort_custom_event_di
                    where country_id not in ('50','98')
                    -- and language_id in ({{ lanuage }})
                    and sub_event_name in ("sys_notification_popup", "system_permission_popup", "push_permission_popup", "bonus_validity_notification_popup")
                    and action in ("allow_click", "set_notification", "click", "enable_now_click", "ok_click")
                    -- and channel_id in ({{ channel }})
                    and etl_date ='${TX_DATE}') a
                inner join
                    (select analysis_date, uuid, max(ctime) ctime from dwd_data.dwd_t02_reelshort_custom_event_di where country_id not in ('50','98')
                    -- and language_id in ({{ lanuage }})
                    and sub_event_name in ("sys_notification_popup", "system_permission_popup", "push_permission_popup", "bonus_validity_notification_popup")
                    and action in ("allow_click", "set_notification", "click", "enable_now_click", "ok_click")
                    -- and channel_id in ({{ channel }})
                    and etl_date ='${TX_DATE}' group by analysis_date, uuid) b
                on a.analysis_date=b.analysis_date and a.uuid=b.uuid and a.ctime=b.ctime group by a.analysis_date, a.uuid, a.ctime)
select
      null as id
      ,t1.analysis_date
      ,t1.etl_date
      ,t1.platform
      ,t1.channel_id
      ,t1.country_id
      ,t1.language_id
      ,t1.version
      ,t1.user_type
      ,t1.uuid
      ,coalesce(t2.fcm_push,0)                                                                            -- cm push权限
      ,coalesce(t2.is_new_open_authority,0)                                                               -- 是否新开启权限
      ,coalesce(t2.is_close_authority,0)                                                                  -- 是否关闭权限
      ,if(t3.sub_event_name="sys_notification_popup" and action='allow_click',1,0)                 as is_fist_open_authority       -- 是否首次启动开启权限
      ,if(t3.sub_event_name="push_permission_popup" and action='click' and t3.type='1',1,0)  as is_not_fist_open_authority   -- 是否非首次启动开启权限
from active_user as t1
left join new_authority_user as t2
on t2.analysis_date=t1.analysis_date and t2.uuid=t1.uuid
left join push_permission_user as t3
on t3.analysis_date=t2.analysis_date and t3.uuid=t2.uuid
;
