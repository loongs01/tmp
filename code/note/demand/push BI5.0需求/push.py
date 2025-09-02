import pandas as pd

# 将DataFrame在Redash输出
def df_to_redash(df_orig, index_to_col=False):
    import numpy as np
    df = df_orig.copy()
    if index_to_col:
        df.reset_index(inplace=True)
    result = {'columns': [], 'rows': []}
    conversions = [
        {'pandas_type': np.integer, 'redash_type': 'integer', },
        {'pandas_type': np.inexact, 'redash_type': 'float', },
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

# 获取观测周期
get_data_hours = '{{ 观测周期 }}'.split('hour')[0]
get_ad_revenue_days = int(int('{{ 观测周期 }}'.split('hour')[0]) / 24) - 1

# print(get_data_hours, get_ad_revenue_days)

# 获取task_info数据
def get_ByTask_task_info_data(pd = pd):
    sql = '''select
    analytics_label as task_id,
    date(start_time) as start_time
    from dws_data.dws_t88_reelshort_opc_stat_di
    where  analysis_date between '{{ analysis_date.start }}' and '{{ analysis_date.end }}'
    group by analytics_label
    union all
    select '去重汇总' as task_id, '{{ analysis_date.start }}' as start_time;'''
    data = execute_query("adb3.0", sql)
    data = pd.DataFrame(data['rows'])
    return data



    
# 获取发送用户数数据和发送成功的用户数
def get_ByTask_send_users_data(pd = pd):


    s=''
    for i in {{ language_id }}:
        s=s + str(i)
        # print(s)
        if s=='192018':
            break
    # print(s)
    language_id2=s=='192018'
    
    s=''
    for i in {{ country_id }}:
        s=s + str(i)
        if str(i)=='124':
            break
    country_id2=s=='316293124'
    
    s=''
    for i in {{ cversion }}:
        s=s + str(i)
        if str(i)=='1.1.03':
            break
    cversion2=s=='1.8.501.9.101.0.041.1.03'
    
    if country_id2:
        country_id2='-- '
    else:
        country_id2=''    
    
    if language_id2:
        language_id2='-- '
    else:
        language_id2=''    
    
    if cversion2:
        cversion2='-- '
    else:
        cversion2=''

    sql = ''' /*+ cte_execution_mode=shared */ 
    with user_info as ( 
    select distinct uuid 
    from dwd_data.dim_t99_reelshort_user_lt_info 
    where etl_date  between '{{ analysis_date.start }}' and '{{ analysis_date.end }}'
         and platform in ({{ platform }}) 
        {} and country_id in ({{ country_id }}) 
        {} and language_id in ({{ language_id }}) 
        {} and version in ({{ cversion }})
        and country_id not in('98', '50')
        and language_id <>'99'
        and language_id not like 'zh-%'
    ),
    send_user_detail as (
    select  t1.uuid, t1.task_id,t1.message_id,success,sum(send_cnt) as send_cnt
    from(select  analytics_label as task_id
              ,if(t1.uuid='',t2.uid,t1.uuid) as uuid 
              ,split_part(message_id,'/',4) as message_id
              ,success
               ,count(*) as send_cnt 
       from (select analytics_label,uuid,message_id,success,token from chapters_log.opc_fcm_push_log_cm1009
          where app_id = 'cm1009' and  analysis_date between '{{ analysis_date.start }}' and '{{ analysis_date.end }}'
       )  t1 
       left join (
         select token,uid
         from(select token,uid
               ,row_number()over(partition by token order by lastLogin desc ) as rank 
         from chapters_log.dts_project_v_user_token 
         where token<>'unknown' and token in (select distinct token  FROM chapters_log.opc_fcm_push_log_cm1009
            WHERE app_id = 'cm1009' and uuid='' AND analysis_date  between  '{{ analysis_date.start }}' and '{{ analysis_date.end }}')
         )
         where rank=1
         )t2 
       on t1.token=t2.token
        group by analytics_label
              ,if(t1.uuid='',t2.uid,t1.uuid)
              ,split_part(message_id,'/',4)
              ,success
            ) as t1
        inner join user_info as t2 on t2.uuid = t1.uuid
        group by t1.uuid, t1.task_id,t1.message_id,success
    ),
   send_success_user_detail as (
select distinct uuid, task_id,message_id
from send_user_detail
where success=1
   ),


    receive_user_detail as (
        select distinct t1.uuid, t1.task_id, t1.message_id,delivered_cnt
        from send_success_user_detail t1
        inner join
        (select  message_id, analytics_label,count(id) as delivered_cnt 
        from reelshort_google_firebase_data
        where analysis_date between '{{ analysis_date.start }}' and '{{ analysis_date.end }}'
            and project_name='reelshort'
            and event='MESSAGE_DELIVERED'
            group by message_id, analytics_label
        ) t2 on t2.message_id = t1.message_id and t2.analytics_label = t1.task_id
    ),

    click_user_detail as (
        select distinct t1.uuid, t2.task_id, t1.message_id,click_cnt,show_cnt
        from (select  uuid, fcm_message_id as message_id
                ,sum(if(action = 'click',1,0)) as click_cnt
                ,sum(if(action = 'show',1,0)) as show_cnt
            from dwd_data.dwd_t02_reelshort_push_stat_di
            where etl_date between '{{ analysis_date.start }}' and '{{ analysis_date.end }}'
                and action in ('show','click')
                and country_id not in ('50', '98')
                and fcm_message_id != ''
        group by uuid, fcm_message_id
        ) as t1
        inner join send_success_user_detail t2 on t2.uuid = t1.uuid and t2.message_id = t1.message_id
    ),

    send_users_ByTask as (
        select task_id
             ,count(distinct uuid) as send_users
             ,count(distinct if(success=1,uuid)) as send_success_users
             ,sum(send_cnt) as send_cnt
             ,sum( if(success=1,send_cnt)) as send_success_cnt
        from send_user_detail
        group by task_id
        union all
        select '去重汇总' as task_id
            ,count(distinct uuid) as send_users
            ,count(distinct if(success=1,uuid)) as send_success_users
             ,sum(send_cnt) as send_cnt
             ,sum(if(success=1,send_cnt)) as send_success_cnt          
        from send_user_detail
    ),
    receive_users_ByTask as (
        select task_id, count(distinct uuid) as receive_users, sum(delivered_cnt) as receive_cnt
        from receive_user_detail
        group by task_id
        union all
        select '去重汇总' as task_id, count(distinct uuid) as receive_users, sum(delivered_cnt) as receive_cnt
        from receive_user_detail
    ),
    click_users_ByTask as (
        select task_id
        , count(distinct if(click_cnt>=1,uuid)) as click_users
        , sum(click_cnt) as click_cnt
        , count(distinct if(show_cnt>=1,uuid)) as show_users
        , sum(show_cnt) as show_cnt
        from click_user_detail
        group by task_id
        union all
        select '去重汇总' as task_id
        , count(distinct if(click_cnt>=1,uuid)) as click_users
        , sum(click_cnt) as click_cnt
        , count(distinct if(show_cnt>=1,uuid)) as show_users
        , sum(show_cnt) as show_cnt
        from click_user_detail
    )


    select t1.task_id
           ,send_cnt 
           ,send_users
           ,send_success_cnt 
           ,send_success_users
           ,receive_cnt
           ,receive_users
           ,show_cnt
           ,show_users
           ,click_cnt
           ,click_users 
    from send_users_ByTask t1
    left join receive_users_ByTask t2 
    on t1.task_id=t2.task_id
    left join click_users_ByTask t3
    on t1.task_id=t3.task_id
    '''.format(country_id2,language_id2,cversion2)
    
    data = execute_query("adb3.0", sql)
    data = pd.DataFrame(data['rows'])
    return data




# 获取公共数据ByTask

def get_ByTask_common_data(get_ByTask_task_info_data = get_ByTask_task_info_data, get_ByTask_send_users_data = get_ByTask_send_users_data):
    df_ByTask_task_info = get_ByTask_task_info_data()
    df_ByTask_send_users = get_ByTask_send_users_data()
    data = df_ByTask_task_info.merge(df_ByTask_send_users, how = 'left')
    return data


# ==============================================================================================================================================================================



# 获取点击数据后续数据公共SQL
common_sql_ByTask = ''' /*+ cte_execution_mode=shared */ 
with click_user_detail as (
        select distinct uuid, task_id, message_id, click_time
        from dwd_data.dwd_t04_reelshort_opc_click_di 
         where platform in ({{ platform }}) 
        and country_id in ({{ country_id }}) 
        and language_id in ({{ language_id }}) 
        and version in ({{ cversion }})
        and analysis_date between '{{ analysis_date.start }}' and '{{ analysis_date.end }}'
       and date(click_time)  between '{{ analysis_date.start }}' and '{{ analysis_date.end }}'
    ),

'''


# 获取播放数据

def get_ByTask_read_data(get_data_hours = get_data_hours, common_sql_ByTask = common_sql_ByTask, pd = pd):
    sql = common_sql_ByTask + '''
    data_read_ctime_ByTask as (
    select distinct uuid, stime
    from dwd_data.dwd_t02_reelshort_play_event_di
    where sub_event_name='play_start'
        and story_id<>''
        and country_id not in ('50', '98')
        and chap_id<>''
        and etl_date between '{{ analysis_date.start }}' and date_add('{{ analysis_date.end }}',  interval {get_data_hours} hour)
        and uuid in (select distinct uuid from click_user_detail)
    ),

    read_user_detail_ByTask as (
        select  t2.task_id, t2.uuid,count(distinct stime) as read_cnt
        from data_read_ctime_ByTask as t1
        inner join click_user_detail t2 on t1.uuid = t2.uuid and t1.stime between t2.click_time and date_add(t2.click_time, interval {get_data_hours} hour)
        group by  t2.task_id, t2.uuid
    ),
    read_user_detail_ByTask_total as (
        select '去重汇总' as task_id, count(distinct t2.uuid)  as read_users ,count(distinct concat(t2.uuid,'|',stime)) as read_cnt
        from data_read_ctime_ByTask as t1
        inner join click_user_detail t2 on t1.uuid = t2.uuid and t1.stime between t2.click_time and date_add(t2.click_time, interval {get_data_hours} hour)
    ),    
        
    read_users_ByTask_24hour as (
        select task_id, count(distinct uuid) as read_users, sum(read_cnt) as read_cnt
        from read_user_detail_ByTask
        group by task_id
        union all
        select * from read_user_detail_ByTask_total
        # select '去重汇总' as task_id, count(distinct uuid) as read_users, sum(read_cnt) as read_cnt
        # from (select distinct uuid, read_cnt from read_user_detail_ByTask)
        
    )

    select * from read_users_ByTask_24hour;
    '''.format(get_data_hours = get_data_hours)
    data = execute_query("adb3.0", sql)
    data = pd.DataFrame(data['rows'])
    return data






# 获取内购付费数据
def get_ByTask_pay_data(get_data_hours = get_data_hours, common_sql_ByTask = common_sql_ByTask, pd = pd):
    sql = common_sql_ByTask + '''data_pay_ctime_ByTask as (
    select distinct uuid, stime, sku_price / 100 as pay_price, order_id as parm7
    from dwd_data.dwd_t05_reelshort_order_detail_di
    where order_status = 1
        and order_id_rank=1
        and country_id not in ('50', '98')
        and etl_date between '{{ analysis_date.start }}' and date_add('{{ analysis_date.end }}', interval 7 day)
),

pay_data_detail_ByTask_24hour as (
    select distinct t2.task_id, t2.uuid, t1.pay_price, t1.parm7
    from data_pay_ctime_ByTask t1
    inner join (select * from click_user_detail 
    where uuid in (select distinct uuid from data_pay_ctime_ByTask)
    ) t2 on t1.uuid = t2.uuid and t1.stime between t2.click_time and date_add(t2.click_time, interval {get_data_hours} hour)
),

pay_data_ByTask_24hour as (
    select task_id, count(distinct uuid) as pay_users, sum(pay_price) as pay_sum
    from pay_data_detail_ByTask_24hour
    group by task_id
    union all
    select '去重汇总' as task_id, count(distinct uuid) as pay_users, sum(pay_price) as pay_sum
    from
    (select distinct uuid, pay_price, parm7
    from pay_data_detail_ByTask_24hour)
)

select * from pay_data_ByTask_24hour;
'''.format(get_data_hours = get_data_hours)
    data = execute_query("adb3.0", sql)
    data = pd.DataFrame(data['rows'])
    return data

# 获取内购付费数据
def get_ByTask_pay_data_first_dy(get_data_hours = get_data_hours, common_sql_ByTask = common_sql_ByTask, pd = pd):
    sql = common_sql_ByTask + '''data_pay_ctime_ByTask as (
    select distinct uuid, stime, sku_price / 100 as pay_price, order_id as parm7
    from dwd_data.dwd_t05_reelshort_order_detail_di
    where order_status = 1
        and order_id_rank=1
        and country_id not in ('50', '98')
        and channel_sku in (select distinct `point` from dts_project_v_shopping_price_point where type in (9, 10))
        and etl_date between '{{ analysis_date.start }}' and date_add('{{ analysis_date.end }}', interval 7 day)
),

pay_data_detail_ByTask_24hour as (
    select distinct t2.task_id, t2.uuid, t1.pay_price, t1.parm7
    from data_pay_ctime_ByTask t1
    inner join (select * from click_user_detail 
    where uuid in (select distinct uuid from data_pay_ctime_ByTask)
    )  t2 on t1.uuid = t2.uuid and t1.stime between t2.click_time and date_add(t2.click_time, interval {get_data_hours} hour)
),

pay_data_ByTask_24hour as (
    select task_id, count(distinct uuid) as pay_users_fisrt_dy, sum(pay_price) as pay_sum_fisrt_dy
    from pay_data_detail_ByTask_24hour
    group by task_id
    union all
    select '去重汇总' as task_id, count(distinct uuid) as pay_users_fisrt_dy, sum(pay_price) as pay_sum_fisrt_dy
    from
    (select distinct uuid, pay_price, parm7
    from pay_data_detail_ByTask_24hour)
)

select * from pay_data_ByTask_24hour;
'''.format(get_data_hours = get_data_hours)
    data = execute_query("adb3.0", sql)
    data = pd.DataFrame(data['rows'])
    return data
    
# 获取订阅付费数据
def get_ByTask_subscribe_data(get_data_hours = get_data_hours, common_sql_ByTask = common_sql_ByTask, pd = pd):
    sql = common_sql_ByTask + '''data_subscribe_ctime_ByTask as (
    select distinct uid as uuid, convert_tz(from_unixtime(end_time), 'Asia/Shanghai', 'America/Los_Angeles') as stime
        , amount as pay_price, merchant_order_id as parm7
    from chapters_log.dts_project_v_payment
    where is_abnormal=0 
        and order_status in (3) 
        and order_type in (9, 10)
        # and date(convert_tz(from_unixtime(end_time), 'Asia/Shanghai', 'America/Los_Angeles')) between '{{ analysis_date.start }}' and date_add('{{ analysis_date.end }}', interval 7 day)
            and end_time >= UNIX_TIMESTAMP(convert_tz('{{ analysis_date.start }} 00:00:00','America/Los_Angeles','Asia/Shanghai'))
            and end_time <= UNIX_TIMESTAMP(convert_tz(date_add('{{ analysis_date.end }} 23:59:59',interval 7 day),'America/Los_Angeles','Asia/Shanghai'))        
),

subscribe_data_detail_ByTask_24hour as (
    select distinct t2.task_id, t2.uuid, t1.pay_price, t1.parm7
    from data_subscribe_ctime_ByTask t1
    inner join (select * from click_user_detail 
    where uuid in (select distinct uuid from data_subscribe_ctime_ByTask)
    )  t2 on t1.uuid = t2.uuid and t1.stime between t2.click_time and date_add(t2.click_time, interval {get_data_hours} hour)
),

subscribe_data_ByTask_24hour as (
    select task_id, count(distinct uuid) as subscribe_users, sum(pay_price) as subscribe_pay_sum
    from subscribe_data_detail_ByTask_24hour
    group by task_id
    union all
    select '去重汇总' as task_id, count(distinct uuid) as subscribe_users, sum(pay_price) as subscribe_pay_sum
    from
    (select distinct uuid, pay_price, parm7
    from subscribe_data_detail_ByTask_24hour)
)

select * from subscribe_data_ByTask_24hour;'''.format(get_data_hours = get_data_hours)
    data = execute_query("adb3.0", sql)
    data = pd.DataFrame(data['rows'])
    return data

# # 获取广告收入
def get_ByTask_ad_revenue_data(get_ad_revenue_days = get_ad_revenue_days, common_sql_ByTask = common_sql_ByTask, pd = pd):
    sql = common_sql_ByTask + '''ad_revenue_data_detail_ByTask_72hour as (
    select  distinct t2.task_id, t2.uuid, t1.etl_date, t1.ad_revenue,t1.sum_online_times
    from
    (select etl_date, uuid, sum(ad_revenue) as ad_revenue,sum(sum_online_times/60) as sum_online_times
    from dws_data.dws_t80_reelshort_user_bhv_cnt_detail_di
    where 1=1
        and country_id not in ('98', '50')
        and language_id<>'99'
        and language_id not like 'zh-%'
        and etl_date between '{{ analysis_date.start }}' and date_add('{{ analysis_date.end }}', interval {get_ad_revenue_days} day)
        and uuid in (select distinct uuid from click_user_detail)
    group by etl_date, uuid) as t1
    inner join click_user_detail as t2 on t1.uuid = t2.uuid and t1.etl_date between date(t2.click_time) and date_add(date(t2.click_time), interval {get_ad_revenue_days} hour)
),

ad_revenue_data_ByTask_72hour as (
    select task_id, count(distinct if(ad_revenue>0,uuid)) as ad_revenue_users, sum(ad_revenue) as ad_revenue, sum(sum_online_times) as sum_online_times
    from ad_revenue_data_detail_ByTask_72hour
    group by task_id
    union all
    select '去重汇总' as task_id, count(distinct if(ad_revenue>0,uuid)) as ad_revenue_users, sum(ad_revenue) as ad_revenue, sum(sum_online_times) as sum_online_times
    from (select distinct uuid, etl_date, ad_revenue,sum_online_times
    from ad_revenue_data_detail_ByTask_72hour)
)

select * from ad_revenue_data_ByTask_72hour;

'''.format(get_ad_revenue_days = get_ad_revenue_days)
    data = execute_query("adb3.0", sql)
    data = pd.DataFrame(data['rows'])
    return data

# # 获取留存率
def get_ByTask_rr_data(get_ad_revenue_days = get_ad_revenue_days, common_sql_ByTask = common_sql_ByTask, pd = pd):
    sql = common_sql_ByTask + '''rr_data_detail_ByTask_72hour as (
    select  t2.task_id
     , t1.uuid
     , max(if(datediff(t1.etl_date,click_dt)=1,1,0)) as users_1d
     , max(if(datediff(t1.etl_date,click_dt)=3,1,0)) as users_3d
     , max(if(datediff(t1.etl_date,click_dt)=14,1,0)) as users_14d
    from
    (select etl_date, uuid
    from dws_data.dws_t80_reelshort_user_bhv_cnt_detail_di
    where 1=1
        and country_id not in ('98', '50')
        and language_id<>'99'
        and language_id not like 'zh-%'
        and etl_date between '{{ analysis_date.start }}' and date_add('{{ analysis_date.end }}', interval 3 day)
        and uuid in (select distinct uuid from click_user_detail)
    group by etl_date, uuid) as t1
    inner join (select uuid,task_id,min(date(click_time)) as click_dt from click_user_detail
                group by uuid ,task_id
    ) as t2 on t1.uuid = t2.uuid 
    and t1.etl_date between click_dt and date_add(click_dt, interval 3 day)
    group by 1,2
),

rr_data_ByTask_72hour as (
    select task_id
       ,count(distinct if(users_1d=1,uuid)) as users_1d 
       ,count(distinct if(users_3d=1,uuid)) as users_3d
       ,count(distinct if(users_14d=1,uuid)) as users_14d
    from rr_data_detail_ByTask_72hour
    group by task_id
    union all
    select '去重汇总' as task_id
       ,count(distinct if(users_1d=1,uuid)) as users_1d 
       ,count(distinct if(users_3d=1,uuid)) as users_3d
       ,count(distinct if(users_14d=1,uuid)) as users_14d
    from rr_data_detail_ByTask_72hour
)

select * from rr_data_ByTask_72hour;

'''.format(get_ad_revenue_days = get_ad_revenue_days)
    data = execute_query("adb3.0", sql)
    data = pd.DataFrame(data['rows'])
    return data



# 获取全部数据
def get_ByTask_all_data(get_ByTask_common_data = get_ByTask_common_data, get_ByTask_read_data = get_ByTask_read_data, get_ByTask_pay_data = get_ByTask_pay_data, get_ByTask_subscribe_data = get_ByTask_subscribe_data, get_ByTask_ad_revenue_data = get_ByTask_ad_revenue_data, get_ByTask_pay_data_first_dy=get_ByTask_pay_data_first_dy
,get_ByTask_rr_data=get_ByTask_rr_data
):
    common_data = get_ByTask_common_data()
    read_data = get_ByTask_read_data()
    pay_data = get_ByTask_pay_data()
    subscribe_data = get_ByTask_subscribe_data()
    ad_revenue_data = get_ByTask_ad_revenue_data()
    first_dy_data= get_ByTask_pay_data_first_dy()
    rr_data=get_ByTask_rr_data()
    data = common_data.merge(read_data, how = 'left').merge(pay_data, how = 'left').merge(subscribe_data, how = 'left').merge(ad_revenue_data, how = 'left').merge(first_dy_data, how = 'left').merge(rr_data, how = 'left')
    data.ad_revenue.fillna(0, inplace=True)
    data.pay_sum.fillna(0, inplace=True)
    data.subscribe_pay_sum.fillna(0, inplace=True)
    data.pay_sum_fisrt_dy.fillna(0, inplace=True)
    data["总收入总额"]=data["pay_sum"]+data["ad_revenue"] # +data["subscribe_pay_sum"]-data["pay_sum_fisrt_dy"]
    data.rename(columns = {'start_time' : '发送日期', 'task_id' : '分析标签'
       , 'send_cnt' : '已发送次数', 'send_users' : '已发送用户数'
     , 'send_success_cnt' : '成功发送次数', 'send_success_users' : '成功发送用户数'
    , 'receive_cnt' : '接收次数', 'receive_users' : '接收用户数'
    , 'show_cnt' : '展示次数', 'show_users' : '展示用户数'
    , 'click_cnt' : '点击次数', 'click_users' : '点击用户数'
    , 'read_cnt' : '播放次数', 'read_users' : '播放用户数'
    , 'pay_users' : '内购用户数', 'pay_sum' : '内购收入总额'
    , 'subscribe_users' : '订阅用户数', 'subscribe_pay_sum' : '订阅收入总额', 'ad_revenue_users' : '广告收入用户数'
    , 'users_1d' : '留存用户数D1', 'users_3d' : '留存用户数D3', 'users_14d' : '留存用户数D14','sum_online_times':'点击用户应用使用时长（分钟）'
    , 'ad_revenue' : '广告收入总额'}, inplace = True)
    data['内购收入总额-包含首次订阅']=data['内购收入总额']
    data['首次订阅金额']=data['pay_sum_fisrt_dy']
    data['内购收入总额']=data['内购收入总额']+data['订阅收入总额']-data["pay_sum_fisrt_dy"]
    data['成功发送率PV'] = data['成功发送次数'] / data['已发送次数']
    data['成功发送率UV'] = data['成功发送用户数'] / data['已发送用户数']
    data['接收率PV'] = data['接收次数'] / data['成功发送次数']
    data['接收率UV'] = data['接收用户数'] / data['成功发送用户数']
    data['展示率PV'] = data['展示次数'] / data['成功发送次数']
    data['展示率UV'] = data['展示用户数'] / data['成功发送用户数']
    data['点击率（CTR-PV）'] = data['点击次数'] / data['接收次数']
    data['点击率（CTR-UV）'] = data['点击用户数'] / data['接收用户数']
    data['点击发送率'] = data['点击用户数'] / data['成功发送用户数']
    data['播放率PV'] = data['播放次数'] / data['点击次数']
    data['播放率UV'] = data['播放用户数'] / data['点击用户数']
    data['留存率D1'] = data['留存用户数D1'] / data['点击用户数']
    data['留存率D3'] = data['留存用户数D3'] / data['点击用户数']
    data['留存率D14'] = data['留存用户数D14'] / data['点击用户数']
    data['付费率'] = data['内购用户数'] / data['点击用户数']
    data['总ARPU'] = (data['内购收入总额']*0.7+ data['广告收入总额'])/ data['点击用户数']
    data['内购CR'] = data['内购用户数'] / data['点击用户数']
    data['内购ARPPU'] = data['内购收入总额'] / data['内购用户数']
    data['点击用户人均使用时长（分钟）'] = data['点击用户应用使用时长（分钟）'] / data['点击用户数']
    data = data[['分析标签', '发送日期', '已发送次数', '已发送用户数', '成功发送次数', '成功发送用户数', '成功发送率PV', '成功发送率UV','接收次数', '接收用户数', '接收率PV'
    , '接收率UV','展示次数','展示用户数','展示率PV','展示率UV' ,'点击次数', '点击用户数', '点击率（CTR-PV）', '点击率（CTR-UV）', '点击发送率', '播放次数', '播放用户数', '播放率PV', '播放率UV'
    ,'留存率D1','留存率D3','留存率D14', '点击用户应用使用时长（分钟）','点击用户人均使用时长（分钟）','内购用户数', '内购收入总额', '内购CR'
    , '内购ARPPU', '订阅用户数', '订阅收入总额', '广告收入用户数', '广告收入总额', '总收入总额', '付费率', '总ARPU', '首次订阅金额', '内购收入总额-包含首次订阅']]
    data.sort_values(by = '分析标签', inplace = True)
    data['成功发送率PV'] = data['成功发送率PV'].apply(lambda x : str(round(x * 100, 2)) + '%')
    data['成功发送率UV'] = data['成功发送率UV'].apply(lambda x : str(round(x * 100, 2)) + '%')
    data['接收率PV'] = data['接收率PV'].apply(lambda x : str(round(x * 100, 2)) + '%')
    data['接收率UV'] = data['接收率UV'].apply(lambda x : str(round(x * 100, 2)) + '%')
    data['展示率PV'] = data['展示率PV'].apply(lambda x : str(round(x * 100, 2)) + '%')
    data['展示率UV'] = data['展示率UV'].apply(lambda x : str(round(x * 100, 2)) + '%')
    data['点击率（CTR-PV）'] = data['点击率（CTR-PV）'].apply(lambda x : str(round(x * 100, 2)) + '%')
    data['点击率（CTR-UV）'] = data['点击率（CTR-UV）'].apply(lambda x : str(round(x * 100, 2)) + '%')
    data['点击发送率'] = data['点击发送率'].apply(lambda x : str(round(x * 100, 2)) + '%')
    data['播放率PV'] = data['播放率PV'].apply(lambda x : str(round(x * 100, 2)) + '%')
    data['播放率UV'] = data['播放率UV'].apply(lambda x : str(round(x * 100, 2)) + '%')
    data['付费率'] = data['付费率'].apply(lambda x : str(round(x * 100, 2)) + '%')
    data['内购CR'] = data['内购CR'].apply(lambda x : str(round(x * 100, 2)) + '%')
    data['留存率D1'] = data['留存率D1'].apply(lambda x : str(round(x * 100, 2)) + '%')
    data['留存率D3'] = data['留存率D3'].apply(lambda x : str(round(x * 100, 2)) + '%')
    data['留存率D14'] = data['留存率D14'].apply(lambda x : str(round(x * 100, 2)) + '%')
    return data 

# data=get_ByTask_task_info_data()

# result=df_to_redash(data)
result = df_to_redash(get_ByTask_all_data())

