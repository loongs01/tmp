# https://uv4iolo7hz.feishu.cn/docx/CMpXdqFliowgyhxAFi5c4PoTnKe

# 第一种：对前一日权限是关闭状态，当日变为开启状态的这部分用户，进行权限开启的归因  以这种为准
# 第二种：对当天首次上报是关闭状态，最后一次上报是开启状态的这部分用户，进行权限开启的归因

# 归因的规则：以当日最后一次触发开启权限场景，并点击了相应按钮为准。比如Bonus有效期记录页中触发申请权限弹窗，并点击了弹窗中的按钮，或者在福利页签到模块点击"开启提醒"


# -*- coding: utf-8 -*-
# import sys 
# print(sys.version)
import pandas as pd

def df_to_redash(df_orig, index_to_col=False):
    import numpy as np
    df = df_orig.copy()
    if index_to_col:
        df.reset_index(inplace=True)
    result = {'columns': [], 'rows': []}
    conversions = [
        {'pandas_type': np.integer, 'redash_type': 'integer',},
        {'pandas_type': np.inexact, 'redash_type': 'float',},
        {'pandas_type': np.datetime64, 'redash_type': 'datetime', 'to_redash': lambda x: x.strftime('%Y-%m-%d %H:%M:%S')},
        {'pandas_type': np.bool_, 'redash_type': 'boolean'},
        {'pandas_type': np.object, 'redash_type': 'string'}
    ]
    labels = []
    for dtype, label in zip(df.dtypes, df.columns):
        for conversion in conversions:
            if issubclass(dtype.type, conversion['pandas_type']):
                result['columns'].append({'name': label, 'friendly_name': label, 'type': conversion['redash_type']})
                labels.append(label)
                func = conversion.get('to_redash')
                if func:
                    df[label] = df[label].apply(func)
                break
    result['rows'] = df[labels].replace({np.nan: None}).to_dict(orient='records')
    return result


def redash_to_df(result, col_to_index=False):
    import pandas as pd
    conversions = [
        {'redash_type': 'datetime', 'to_pandas': lambda x: pd.to_datetime(x, infer_datetime_format=True)},
        {'redash_type': 'date', 'to_pandas': lambda x: pd.to_datetime(x, infer_datetime_format=True)},
    ]
    df = pd.DataFrame.from_dict(result['rows'], orient='columns')
    labels = []
    for column in result['columns']:
        label = column['name']
        labels.append(label)
        for conversion in conversions:
            if conversion['redash_type'] == column['type']:
                func = conversion.get('to_pandas')
                if func:
                    df[label] = df[label].apply(func)
                break
    df = df[labels]
    if col_to_index and labels:
        df.set_index(labels[0], inplace=True)
    return df


if "{{ 聚合周期 }}" == "by day":
    sql="""
        /*+ cte_execution_mode=shared */
        with active_user as (select distinct analysis_date, uuid from dws_data.dws_t80_reelshort_user_bhv_cnt_detail_di where country_id not in ('50','98') 
            and channel_id in ({{ channel }})
            and user_type in ({{ 新老用户 }})
            and language_id in ({{ lanuage }}) 
            and etl_date between '{{ analysis_date.start }}' and '{{ analysis_date.end }}')
        
        -- , fcm_open_user as (select * from (
        --     	select analysis_date, uuid, cast(json_extract(properties, "$.fcm_push") as string) fcm_push, row_number() over(partition by analysis_date, uuid order by ctime desc) rw 
        --     	from dwd_data.dwd_t02_reelshort_custom_event_di where country_id not in ('50','98') and sub_event_name="user_preferrence_conf"
        --     	and channel_id in ({{ channel }}) 
        --      and language_id in ({{ lanuage }}) 
        --     	and etl_date between '{{ analysis_date.start }}' and '{{ analysis_date.end }}' ) 
        -- 	where rw=1 and fcm_push="1")
        	
        -- fcm_push 1=开启，2=关闭	
        , fcm_user as (select distinct analysis_date, etl_date, uuid, fcm_push from dwd_data.dim_t99_reelshort_user_lt_info where country_id not in ('50','98') 
            and channel_id in ({{ channel }})
            and language_id in ({{ lanuage }}) 
            and etl_date between date_sub('{{ analysis_date.start }}', 1) and '{{ analysis_date.end }}'
            -- and analysis_date between date_sub('{{ analysis_date.start }}', 1) and '{{ analysis_date.end }}'
            )
        
        -- 新开启权限用户：前一次是关闭状态，当日变为开启状态的这部分用户
        , new_open_user as (select t2.* from active_user t1 inner join
            (select a.* from 
                (select etl_date, uuid, fcm_push from fcm_user) b 
                right join 
                (select analysis_date, uuid, date_sub(analysis_date, 1) as date1 from fcm_user where fcm_push='1' and analysis_date between '{{ analysis_date.start }}'  and '{{ analysis_date.end }}'  ) a 
                on a.uuid=b.uuid and a.date1=b.etl_date where (b.fcm_push='2' or b.uuid is null) ) t2 
            on t1.uuid=t2.uuid and t1.analysis_date=t2.analysis_date)

        -- 申请权限场景: 取当天最后一次
        -- 首次启动开启权限UV: sys_notification_popup  action="allow_click"
        -- 预约书架开启权限UV：system_permission_popup action="set_notification"
        -- 非首次启动开启权限 & 离开福利页开启权限: push_permission_popup  action="click"   "type":1  #弹窗出现场景，定义：1 非首次启动-启动应用引导弹窗；2 福利页-离开福利页推送授权弹窗
        -- Bonus有效期记录页首次返回开启权限UV: bonus_validity_notification_popup action in ("enable_now_click", "ok_click")  "scene":1  #弹窗出现场景，定义：1 首次离开bonus有效期记录页 ；2 点击右上角铃铛icon
        , push_permission_user as (select a.analysis_date, a.uuid, a.ctime, max(sub_event_name) as "sub_event_name", max(`type`) as "type", max(`scene`) as "scene" from 
                    (select distinct analysis_date, uuid, ctime, sub_event_name, `type`, cast(json_extract(properties, "$.scene") as string) as "scene" from dwd_data.dwd_t02_reelshort_custom_event_di 
                    where country_id not in ('50','98') 
                    and language_id in ({{ lanuage }}) 
                    and sub_event_name in ("sys_notification_popup", "system_permission_popup", "push_permission_popup", "bonus_validity_notification_popup")  
                    and action in ("allow_click", "set_notification", "click", "enable_now_click", "ok_click")
                	and channel_id in ({{ channel }}) and etl_date between '{{ analysis_date.start }}' and '{{ analysis_date.end }}') a
            	inner join
                	(select analysis_date, uuid, max(ctime) ctime from dwd_data.dwd_t02_reelshort_custom_event_di where country_id not in ('50','98') 
                	and language_id in ({{ lanuage }}) 
                    and sub_event_name in ("sys_notification_popup", "system_permission_popup", "push_permission_popup", "bonus_validity_notification_popup")  
                    and action in ("allow_click", "set_notification", "click", "enable_now_click", "ok_click")
                	and channel_id in ({{ channel }}) and etl_date between '{{ analysis_date.start }}' and '{{ analysis_date.end }}' group by analysis_date, uuid) b
            	on a.analysis_date=b.analysis_date and a.uuid=b.uuid and a.ctime=b.ctime group by a.analysis_date, a.uuid, a.ctime)
            	
        -- 签到底部开关开启权限:  m_widget_click {"_chap_order_id":0,"_element_name":"check_in_guide","_page_name":"earn_rewards","_scene_name":"main_scene"}
        , widget_click_user as (select analysis_date, uuid, max(ctime) ctime from dwd_data.dwd_t02_reelshort_widget_click_di where country_id not in ('50','98') 
                and language_id in ({{ lanuage }}) 
                and event_name="m_widget_click" and element_name="check_in_guide"
            	and channel_id in ({{ channel }}) and etl_date between '{{ analysis_date.start }}' and '{{ analysis_date.end }}' group by analysis_date, uuid)
            	
        -- 以上几种场景取最大值
        , push_permission_max as (select a.* from 
            (select * from push_permission_user union select analysis_date, uuid, ctime, "check_in_guide" as "sub_event_name", "" as `type`, "" as "scene" from widget_click_user) a
            left join 
            (select analysis_date, uuid, max(ctime) ctime from 
            (select * from push_permission_user union select analysis_date, uuid, ctime, "check_in_guide" as "sub_event_name", "" as `type`, "" as "scene" from widget_click_user) group by analysis_date, uuid) b
            on a.analysis_date=b.analysis_date and a.uuid=b.uuid and a.ctime=b.ctime)
            
        , push_permission_attr as (select b.analysis_date
            , count(distinct b.uuid) as "新开启权限UV"
            , count(distinct if(c.sub_event_name="sys_notification_popup", c.uuid)) as "首次启动开启权限UV"
            , count(distinct if(c.sub_event_name="push_permission_popup" and c.`type`='1', c.uuid)) as "非首次启动开启权限UV"
            , count(distinct if(c.sub_event_name="system_permission_popup", c.uuid)) as "预约书架开启权限UV"
            , count(distinct if(c.sub_event_name="push_permission_popup" and c.`type`='2', c.uuid)) as "离开福利页开启权限UV"
            , count(distinct if(c.sub_event_name="check_in_guide", c.uuid)) as "签到底部开关开启权限UV"
            , count(distinct if(c.sub_event_name="bonus_validity_notification_popup" and c.scene='1', c.uuid)) as "bonus页面--首次返回开启权限UV"
            , count(distinct if(c.sub_event_name="bonus_validity_notification_popup" and c.scene='2', c.uuid)) as "bonus页面--铃铛icon开启权限UV"
            from 
                new_open_user b 
                left join push_permission_max c on b.analysis_date=c.analysis_date and b.uuid=c.uuid
            group by b.analysis_date)
        
    
        select tab1.*
        , tab2.`新开启权限UV`
        , tab2.`首次启动开启权限UV`
        , tab2.`首次启动开启权限UV`/tab1.`开启权限UV`*100 as "首次启动开启权限占比"
        , tab2.`非首次启动开启权限UV`
        , tab2.`非首次启动开启权限UV`/tab1.`开启权限UV`*100 as "非首次启动开启权限占比"
        , tab2.`预约书架开启权限UV`
        , tab2.`预约书架开启权限UV`/tab1.`开启权限UV`*100 as "预约书架开启权限占比"
        , tab2.`离开福利页开启权限UV`
        , tab2.`离开福利页开启权限UV`/tab1.`开启权限UV`*100 as "离开福利页开启权限占比"
        , tab2.`签到底部开关开启权限UV`
        , tab2.`签到底部开关开启权限UV`/tab1.`开启权限UV`*100 as "签到底部开关开启权限占比"
        , tab2.`bonus页面--首次返回开启权限UV`
        , tab2.`bonus页面--首次返回开启权限UV`/tab1.`开启权限UV`*100 as "bonus页面--首次返回开启权限占比"
        , tab2.`bonus页面--铃铛icon开启权限UV`
        , tab2.`bonus页面--铃铛icon开启权限UV`/tab1.`开启权限UV`*100 as "bonus页面--铃铛icon开启权限占比"
        from
            (select t1.analysis_date
            , count(distinct t1.uuid) as "dau"
            , count(distinct t2.uuid) as "开启权限UV"
            , count(distinct t2.uuid)/count(distinct t1.uuid)*100 as "开启权限UV占比"
            from active_user t1 
            left join (select distinct analysis_date, uuid from fcm_user where fcm_push='1') t2 on t1.analysis_date=t2.analysis_date and t1.uuid=t2.uuid
            group by t1.analysis_date) tab1
        left join push_permission_attr tab2 on tab1.analysis_date=tab2.analysis_date
        order by tab1.analysis_date;
    """


elif "{{ 聚合周期 }}" == "by week":
    sql="""
        /*+ cte_execution_mode=shared */
        with active_user as (select distinct analysis_date, year(analysis_date) dayofyear, month(analysis_date) dayofmonth, date_sub(analysis_date, dayofweek(date_sub(analysis_date, 1))-1) `dayofweek`, uuid 
            from dws_data.dws_t80_reelshort_user_bhv_cnt_detail_di where country_id not in ('50','98') 
            and channel_id in ({{ channel }})
            and user_type in ({{ 新老用户 }})
            and language_id in ({{ lanuage }}) 
            and etl_date between '{{ analysis_date.start }}' and '{{ analysis_date.end }}')
        
        , fcm_user as (select distinct analysis_date, year(analysis_date) dayofyear, month(analysis_date) dayofmonth, date_sub(analysis_date, dayofweek(date_sub(analysis_date, 1))-1) `dayofweek`,
            etl_date, year(etl_date) etldayofyear, month(etl_date) etldayofmonth, date_sub(etl_date, dayofweek(date_sub(etl_date, 1))-1) `etldayofweek`, uuid, fcm_push 
            from dwd_data.dim_t99_reelshort_user_lt_info where country_id not in ('50','98') 
            and channel_id in ({{ channel }})
            and language_id in ({{ lanuage }}) 
            and etl_date between date_sub('{{ analysis_date.start }}', 14) and '{{ analysis_date.end }}'
            -- and analysis_date between date_sub('{{ analysis_date.start }}', 14) and '{{ analysis_date.end }}'
            )
            
        -- 新开启权限用户：前一周最后一次权限是关闭状态，当周最后一次变为开启状态的这部分用户
        , new_open_user as (select t2.* from active_user t1 inner join
            (select a.* from 
                (select * from 
                    (select etl_date, `etldayofweek`, uuid, fcm_push, row_number() over(partition by `etldayofweek`, uuid order by etl_date desc) rw from fcm_user) where rw=1) b
                right join 
                (select * from 
                    (select analysis_date, `dayofweek`, date_sub(`dayofweek`, 7) as dayofweek_1, uuid, fcm_push, row_number() over(partition by `dayofweek`, uuid order by analysis_date desc) rw from fcm_user 
                    where etl_date between '{{ analysis_date.start }}' and '{{ analysis_date.end }}') 
                where rw=1 and fcm_push='1') a
                on a.uuid=b.uuid and a.`dayofweek_1`=b.`etldayofweek` where (b.fcm_push='2' or b.uuid is null) ) t2 
            on t1.uuid=t2.uuid and t1.`dayofweek`=t2.`dayofweek`)

        , push_permission_user as (select a.`dayofweek`, a.uuid, a.ctime, max(sub_event_name) as "sub_event_name", max(`type`) as "type", max(`scene`) as "scene" from 
                    (select distinct analysis_date, date_sub(analysis_date, dayofweek(date_sub(analysis_date, 1))-1) `dayofweek`, uuid, ctime, sub_event_name, `type`, cast(json_extract(properties, "$.scene") as string) as "scene" 
                    from dwd_data.dwd_t02_reelshort_custom_event_di where country_id not in ('50','98') 
                    and language_id in ({{ lanuage }}) 
                    and sub_event_name in ("sys_notification_popup", "system_permission_popup", "push_permission_popup", "bonus_validity_notification_popup")  
                    and action in ("allow_click", "set_notification", "click", "enable_now_click", "ok_click")
                	and channel_id in ({{ channel }}) and etl_date between '{{ analysis_date.start }}' and '{{ analysis_date.end }}') a
            	inner join
                	(select date_sub(analysis_date, dayofweek(date_sub(analysis_date, 1))-1) `dayofweek`, uuid, max(ctime) ctime from dwd_data.dwd_t02_reelshort_custom_event_di where country_id not in ('50','98') 
                    and language_id in ({{ lanuage }}) 
                    and sub_event_name in ("sys_notification_popup", "system_permission_popup", "push_permission_popup", "bonus_validity_notification_popup")  
                    and action in ("allow_click", "set_notification", "click", "enable_now_click", "ok_click")
                	and channel_id in ({{ channel }}) and etl_date between '{{ analysis_date.start }}' and '{{ analysis_date.end }}' group by `dayofweek`, uuid) b
            	on a.`dayofweek`=b.`dayofweek` and a.uuid=b.uuid and a.ctime=b.ctime group by a.`dayofweek`, a.uuid, a.ctime)
            	
        , widget_click_user as (select analysis_date, date_sub(analysis_date, dayofweek(date_sub(analysis_date, 1))-1) `dayofweek`, uuid, max(ctime) ctime 
                from dwd_data.dwd_t02_reelshort_widget_click_di where country_id not in ('50','98') 
                and language_id in ({{ lanuage }}) 
                and event_name="m_widget_click" and element_name="check_in_guide"
            	and channel_id in ({{ channel }}) and etl_date between '{{ analysis_date.start }}' and '{{ analysis_date.end }}' group by `dayofweek`, uuid)
            	
        -- 以上几种场景取最大值
        , push_permission_max as (select a.* from 
            (select * from push_permission_user union select `dayofweek`, uuid, ctime, "check_in_guide" as "sub_event_name", "" as `type`, "" as "scene" from widget_click_user) a
            left join 
            (select `dayofweek`, uuid, max(ctime) ctime from 
            (select * from push_permission_user union select `dayofweek`, uuid, ctime, "check_in_guide" as "sub_event_name", "" as `type`, "" as "scene" from widget_click_user) group by `dayofweek`, uuid) b
            on a.`dayofweek`=b.`dayofweek` and a.uuid=b.uuid and a.ctime=b.ctime)
            
        , push_permission_attr as (select b.`dayofweek`
            , count(distinct b.uuid) as "新开启权限UV"
            , count(distinct if(c.sub_event_name="sys_notification_popup", c.uuid)) as "首次启动开启权限UV"
            , count(distinct if(c.sub_event_name="push_permission_popup" and c.`type`='1', c.uuid)) as "非首次启动开启权限UV"
            , count(distinct if(c.sub_event_name="system_permission_popup", c.uuid)) as "预约书架开启权限UV"
            , count(distinct if(c.sub_event_name="push_permission_popup" and c.`type`='2', c.uuid)) as "离开福利页开启权限UV"
            , count(distinct if(c.sub_event_name="check_in_guide", c.uuid)) as "签到底部开关开启权限UV"
            , count(distinct if(c.sub_event_name="bonus_validity_notification_popup" and c.scene='1', c.uuid)) as "bonus页面--首次返回开启权限UV"
            , count(distinct if(c.sub_event_name="bonus_validity_notification_popup" and c.scene='2', c.uuid)) as "bonus页面--铃铛icon开启权限UV"
            from 
                new_open_user b 
                left join push_permission_max c on b.`dayofweek`=c.`dayofweek` and b.uuid=c.uuid
            group by b.`dayofweek`)
        
    
        select tab1.*
        , tab2.`新开启权限UV`
        , tab2.`首次启动开启权限UV`
        , tab2.`首次启动开启权限UV`/tab1.`开启权限UV`*100 as "首次启动开启权限占比"
        , tab2.`非首次启动开启权限UV`
        , tab2.`非首次启动开启权限UV`/tab1.`开启权限UV`*100 as "非首次启动开启权限占比"
        , tab2.`预约书架开启权限UV`
        , tab2.`预约书架开启权限UV`/tab1.`开启权限UV`*100 as "预约书架开启权限占比"
        , tab2.`离开福利页开启权限UV`
        , tab2.`离开福利页开启权限UV`/tab1.`开启权限UV`*100 as "离开福利页开启权限占比"
        , tab2.`签到底部开关开启权限UV`
        , tab2.`签到底部开关开启权限UV`/tab1.`开启权限UV`*100 as "签到底部开关开启权限占比"
        , tab2.`bonus页面--首次返回开启权限UV`
        , tab2.`bonus页面--首次返回开启权限UV`/tab1.`开启权限UV`*100 as "bonus页面--首次返回开启权限占比"
        , tab2.`bonus页面--铃铛icon开启权限UV`
        , tab2.`bonus页面--铃铛icon开启权限UV`/tab1.`开启权限UV`*100 as "bonus页面--铃铛icon开启权限占比"
        from
            (select t1.`dayofweek` as "analysis_date"
            , count(distinct t1.uuid) as "dau"
            , count(distinct t2.uuid) as "开启权限UV"
            , count(distinct t2.uuid)/count(distinct t1.uuid)*100 as "开启权限UV占比"
            from active_user t1 
            left join (select * from (select analysis_date, `dayofweek`, uuid, fcm_push, row_number() over(partition by `dayofweek`, uuid order by analysis_date desc) rw from fcm_user) where rw=1 and fcm_push='1') t2 
            on t1.`dayofweek`=t2.`dayofweek` and t1.uuid=t2.uuid
            group by t1.`dayofweek`) tab1
        left join push_permission_attr tab2 on tab1.`analysis_date`=tab2.`dayofweek`
        order by tab1.`analysis_date`;
    """


# else:
#     sql="""
#         /*+ cte_execution_mode=shared */
#         with active_user as (select distinct analysis_date, left(analysis_date, 10) `period`, month(analysis_date) `dayofmonth`, uuid from dws_data.dws_t80_reelshort_user_bhv_cnt_detail_di where country_id not in ('50','98') 
#             and channel_id in ({{ channel }})
#             and user_type in ({{ 新老用户 }})
#             and etl_date between '{{ analysis_date.start }}' and '{{ analysis_date.end }}')
        
#         , fcm_user as (select distinct analysis_date, left(analysis_date, 10) `period`, month(analysis_date) `dayofmonth`, etl_date, left(etl_date, 10) `etl_period`, month(etl_date) `etldayofmonth`, uuid, fcm_push 
#             from dwd_data.dim_t99_reelshort_user_lt_info where country_id not in ('50','98') 
#             and channel_id in ({{ channel }})
#             and etl_date between date_sub('{{ analysis_date.start }}', 30) and '{{ analysis_date.end }}'
#             -- and analysis_date between date_sub('{{ analysis_date.start }}', 31) and '{{ analysis_date.end }}'
#             )
        
#         -- 新开启权限用户：前一周最后一次权限是关闭状态，当周最后一次变为开启状态的这部分用户
#         , new_open_user as (select t2.* from active_user t1 inner join
#             (select a.* from 
#                 (select * from 
#                     (select analysis_date, `period`, `dayofmonth`, uuid, fcm_push, row_number() over(partition by `period`, uuid order by analysis_date desc) rw from fcm_user) where rw=1 and fcm_push='1') a 
#                 left join 
#                 (select * from 
#                     (select etl_date, `etl_period`, `etldayofmonth`, uuid, fcm_push, row_number() over(partition by `etl_period`, uuid order by etl_date desc) rw from fcm_user) where rw=1) b 
#                 on a.uuid=b.uuid and date_sub(a.`dayofmonth`, 1)=b.`etldayofmonth` where (b.fcm_push='2' or b.uuid is null) ) t2 
#             on t1.uuid=t2.uuid and t1.`period`=t2.`period`)

#         , push_permission_user as (select a.`period`, a.uuid, a.ctime, max(sub_event_name) as "sub_event_name", max(`type`) as "type", max(`scene`) as "scene" from 
#                     (select distinct analysis_date, left(analysis_date, 10) `period`, uuid, ctime, sub_event_name, `type`, cast(json_extract(properties, "$.scene") as string) as "scene" 
#                     from dwd_data.dwd_t02_reelshort_custom_event_di where country_id not in ('50','98') 
#                     and sub_event_name in ("sys_notification_popup", "system_permission_popup", "push_permission_popup", "bonus_validity_notification_popup")  
#                     and action in ("allow_click", "set_notification", "click", "enable_now_click", "ok_click")
#                 	and channel_id in ({{ channel }}) and etl_date between '{{ analysis_date.start }}' and '{{ analysis_date.end }}') a
#             	inner join
#                 	(select left(analysis_date, 10) `period`, uuid, max(ctime) ctime from dwd_data.dwd_t02_reelshort_custom_event_di where country_id not in ('50','98') 
#                     and sub_event_name in ("sys_notification_popup", "system_permission_popup", "push_permission_popup", "bonus_validity_notification_popup")  
#                     and action in ("allow_click", "set_notification", "click", "enable_now_click", "ok_click")
#                 	and channel_id in ({{ channel }}) and etl_date between '{{ analysis_date.start }}' and '{{ analysis_date.end }}' group by `period`, uuid) b
#             	on a.`period`=b.`period` and a.uuid=b.uuid and a.ctime=b.ctime group by a.`period`, a.uuid, a.ctime)
            	
#         , widget_click_user as (select analysis_date, left(analysis_date, 10) `period`, uuid, max(ctime) ctime 
#                 from dwd_data.dwd_t02_reelshort_widget_click_di where country_id not in ('50','98') and event_name="m_widget_click" and element_name="check_in_guide"
#             	and channel_id in ({{ channel }}) and etl_date between '{{ analysis_date.start }}' and '{{ analysis_date.end }}' group by `period`, uuid)
            	
#         -- 以上几种场景取最大值
#         , push_permission_max as (select a.* from 
#             (select * from push_permission_user union select `period`, uuid, ctime, "check_in_guide" as "sub_event_name", "" as `type`, "" as "scene" from widget_click_user) a
#             left join 
#             (select `period`, uuid, max(ctime) ctime from 
#             (select * from push_permission_user union select `period`, uuid, ctime, "check_in_guide" as "sub_event_name", "" as `type`, "" as "scene" from widget_click_user) group by `period`, uuid) b
#             on a.`period`=b.`period` and a.uuid=b.uuid and a.ctime=b.ctime)
            
#         , push_permission_attr as (select b.`period`
#             , count(distinct b.uuid) as "新开启权限UV"
#             , count(distinct if(c.sub_event_name="sys_notification_popup", c.uuid)) as "首次启动开启权限UV"
#             , count(distinct if(c.sub_event_name="push_permission_popup" and c.`type`='1', c.uuid)) as "非首次启动开启权限UV"
#             , count(distinct if(c.sub_event_name="system_permission_popup", c.uuid)) as "预约书架开启权限UV"
#             , count(distinct if(c.sub_event_name="push_permission_popup" and c.`type`='2', c.uuid)) as "离开福利页开启权限UV"
#             , count(distinct if(c.sub_event_name="check_in_guide", c.uuid)) as "签到底部开关开启权限UV"
#             , count(distinct if(c.sub_event_name="bonus_validity_notification_popup" and c.scene='1', c.uuid)) as "bonus页面--首次返回开启权限UV"
#             , count(distinct if(c.sub_event_name="bonus_validity_notification_popup" and c.scene='2', c.uuid)) as "bonus页面--铃铛icon开启权限UV"
#             from 
#                 new_open_user b 
#                 left join push_permission_max c on b.`period`=c.`period` and b.uuid=c.uuid
#             group by b.`period`)
        
    
#         select tab1.*
#         , tab2.`新开启权限UV`
#         , tab2.`首次启动开启权限UV`
#         , tab2.`首次启动开启权限UV`/tab1.`开启权限UV`*100 as "首次启动开启权限占比"
#         , tab2.`非首次启动开启权限UV`
#         , tab2.`非首次启动开启权限UV`/tab1.`开启权限UV`*100 as "非首次启动开启权限占比"
#         , tab2.`预约书架开启权限UV`
#         , tab2.`预约书架开启权限UV`/tab1.`开启权限UV`*100 as "预约书架开启权限占比"
#         , tab2.`离开福利页开启权限UV`
#         , tab2.`离开福利页开启权限UV`/tab1.`开启权限UV`*100 as "离开福利页开启权限占比"
#         , tab2.`签到底部开关开启权限UV`
#         , tab2.`签到底部开关开启权限UV`/tab1.`开启权限UV`*100 as "签到底部开关开启权限占比"
#         , tab2.`bonus页面--首次返回开启权限UV`
#         , tab2.`bonus页面--首次返回开启权限UV`/tab1.`开启权限UV`*100 as "bonus页面--首次返回开启权限占比"
#         , tab2.`bonus页面--铃铛icon开启权限UV`
#         , tab2.`bonus页面--铃铛icon开启权限UV`/tab1.`开启权限UV`*100 as "bonus页面--铃铛icon开启权限占比"
#         from
#             (select t1.`period` as "analysis_date"
#             , count(distinct t1.uuid) as "dau"
#             , count(distinct t2.uuid) as "开启权限UV"
#             , count(distinct t2.uuid)/count(distinct t1.uuid)*100 as "开启权限UV占比"
#             from active_user t1 
#             left join (select * from (select analysis_date, `period`, uuid, fcm_push, row_number() over(partition by `period`, uuid order by analysis_date desc) rw from fcm_user) where rw=1 and fcm_push='1') t2 
#             on t1.`period`=t2.`period` and t1.uuid=t2.uuid
#             group by t1.`period`) tab1
#         left join push_permission_attr tab2 on tab1.`analysis_date`=tab2.`period`
#         order by tab1.`analysis_date`;
#     """


# print(sql)
df = redash_to_df(execute_query("adb3.0", sql))
result = df_to_redash(df)









