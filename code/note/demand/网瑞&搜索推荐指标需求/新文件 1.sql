select
uuid
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
,count(distinct recent_visit_source) as ct
from
(select
app_user_id as uuid
,country_id
,app_channel_id   as channel_id
,app_version      as version
,app_game_version as cversion
,app_res_version  as res_version
,app_lang         as language_id
,os_type          as platform
,os_version
,device_id
,if(ad_id in ('','0000-0000','00000000-0000-0000-0000-000000000000'),'',ad_id) as ad_id
,case
    when json_extract(properties,'$._referrer_url') regexp 'google'     then 'google organic'
    when json_extract(properties,'$._referrer_url') regexp 'reelshort' or json_extract(properties,'$._referrer_url') is null  then 'direct'
    when json_extract(properties,'$._referrer_url') regexp 'facebook'   then 'facebook referral'
    when json_extract(properties,'$._referrer_url') regexp 'youtube'    then 'youtube referral'
    when json_extract(properties,'$._referrer_url') regexp 'bing'       then 'bing organic'
    else 'other'
 end as recent_visit_source -- 最近一次访问来源
,case
    when json_extract(properties,'$._referrer_url') regexp 'campaign_id' then '付费推广'
    else '自然量'
 end as recent_visit_source_type -- 最近一次访问来源类型
,if(event_name='m_app_install',SUBSTRING_INDEX(replace(cast(nvl(json_extract(properties,'$._url'),'')as varchar),concat('-',SUBSTRING_INDEX(cast(nvl(json_extract(properties,'$._url'),'') as varchar), '-', -1)),''),'.com/',-1)) as first_visit_landing_page -- 首次访问着陆页
,cast(nvl(json_extract(properties,'$._referrer_url'),'') as varchar) as a
,properties
from chapters_log.public_event_data
where analysis_date='${TX_DATE}'
      and event_name in ('m_app_install','m_user_signin')
union all
select
app_user_id as uuid
,country_id
,app_channel_id   as channel_id
,app_version      as version
,app_game_version as cversion
,app_res_version  as res_version
,app_lang         as language_id
,os_type          as platform
,os_version
,device_id
,if(ad_id in ('','0000-0000','00000000-0000-0000-0000-000000000000'),'',ad_id) as ad_id
,case
    when json_extract(properties,'$._referrer_url') regexp 'google'     then 'google organic'
    when json_extract(properties,'$._referrer_url') regexp 'reelshort' or json_extract(properties,'$._referrer_url') is null  then 'direct'
    when json_extract(properties,'$._referrer_url') regexp 'facebook'   then 'facebook referral'
    when json_extract(properties,'$._referrer_url') regexp 'youtube'    then 'youtube referral'
    when json_extract(properties,'$._referrer_url') regexp 'bing'       then 'bing organic'
    else 'other'
 end
,case
    when json_extract(properties,'$._referrer_url') regexp 'campaign_id' then '付费推广'
    else '自然量'
 end as recent_visit_source_type
,null as first_visit_landing_page -- 生命周期内
,cast(nvl(json_extract(properties,'$._referrer_url'),'') as varchar) as a
,properties
from chapters_log.reelshort_event_data_log
where analysis_date='${TX_DATE}'
      and event_name='m_page_enter'
)
group by
country_id
,channel_id
,version
,cversion
,res_version
,language_id
,platform
,os_version
,device_id
,ad_id
having ct>1
order by ct desc
;

--

select
uuid
,first_visit_landing_page -- 首次访问着陆页
,story_id                 -- 书籍id
,chap_id                  -- 章节id
from
    (
    select
    uuid
    ,SUBSTRING_INDEX(url,'.com/',-1) as first_visit_landing_page -- 首次访问着陆页
    ,story_id -- 书籍id
    ,chap_id -- 章节id
    ,row_number() over( partition by uuid order by stime asc) as rn
    from dwd_data.dwd_t01_reelshort_device_start_di as t
    where etl_date='${TX_DATE}'
          and event_name='m_app_install'
    )
where rn=1





select
uuid
,recall_level -- 召回路
from (select
            uuid
            ,recall_level -- 召回路
      FROM dwd_data.dwd_t02_reelshort_play_event_di as t
      where etl_date='${TX_DATE}'
            and event_name='m_play_event'
            and recall_level!=''
      group by
              uuid
              ,recall_level -- 召回路
      union all
      select
            uuid
            ,cast(nvl(json_extract(report_info,'$.recall_level'),'') as varchar) as recall_level
      from dwd_data.dwd_t02_reelshort_item_pv_di
      where etl_date='${TX_DATE}'
            and event_name='m_item_pv'
            and cast(nvl(json_extract(report_info,'$.recall_level'),'') as varchar)!=''
      group by
              uuid
              ,recall_level -- 召回路
) 
group by 
       uuid
       ,recall_level -- 召回路  
order by uuid



dwd_t02_reelshort_search_stat_di

left join (
           select distinct uuid,search_story_ids -- 搜索结果页面中，搜索结果的书籍列表，元素结构是 书籍顺序#书籍ID#匹配规则
           from dwd_data.dwd_t02_reelshort_custom_event_di as t
           where etl_date='${TX_DATE}'
                 and event_name='m_custom_event'
                 and sub_event_name='search_stat'
) as t3
on t3.uuid=t1.uuid
left join (
           select distinct uuid,page_name
                      from dwd_data.dwd_t02_reelshort_page_enter_exit_di as t
                      where etl_date='${TX_DATE}'
                            and event_name='m_page_enter'
                            and page_name='discover'
) as t4
on t4.uuid=t1.uuid
where t1.analysis_date='${TX_DATE}'