/*+cte_execution_mode=shared*/
replace  into dw_view.`vt_dwd_t01_reelshort_user_info_di`
with reelshort_user_info_di_v2 as (
select
    id
    ,uuid
    ,analysis_date
    ,country_id
    ,channel_id
    ,version
    ,cversion
    ,res_version
    ,case -- when t5.language_id is not null then t5.language_id
         when t1.language_id like 'zh%'  or t1.language_id in ('cn')  then 99
         when t1.language_id like 'hi%'  then 20
         when t1.language_id like 'fil%'  then 19         
         when t1.language_id like 'da%'  then 16
         when t1.language_id like 'fi%'  then 15
         when t1.language_id like 'nb%' or t1.language_id like 'no%'  or t1.language_id like 'nn%' then 14
         when t1.language_id like 'sv%'  then 13
         when t1.language_id like 'nl%'  then 12
         when t1.language_id like 'tr%'  then 11
         when t1.language_id like 'pl%'  then 10
         when t1.language_id like 'ja%'  then 9
         when t1.language_id like 'it%'  then 8
         when t1.language_id like 'ko%'  then 7
         when t1.language_id like 'pt%' or t1.language_id='brazil' then 6
         when t1.language_id like 'ru%'  then 5
         when t1.language_id like 'fr%'  then 4
         when t1.language_id like 'de%'  then 3
         when t1.language_id like 'es%'  then 2
         when t1.language_id like 'en%' and t1.language_id<>'en-ID' then 1
         when t1.language_id like 'ar%'  then 17
         when t1.language_id in ('en-ID','in') or t1.language_id like 'id%'  then 18
         when t1.language_id like 'th%' then 98 
         else t1.language_id
    end as language_id 
    ,t1.platform as platform
    ,os_version
    ,device_id
    ,ad_id
    ,androidid
    ,idfv
    ,cast(is_login as int) as is_login
    ,cast(user_type as int) as user_type
    ,cast(is_pay as int) as is_pay
    ,register_time
    ,first_login_time
    ,last_login_time
    ,first_pay_time
    ,last_pay_time
    ,first_invaild_pay_time
    ,last_invaild_pay_time
    ,first_active_time
    ,last_active_time
    ,first_play_start_time
    ,last_play_start_time
    ,first_play_end_time
    ,last_play_end_time
    ,order_cnt 
    ,pay_amount 
    ,coins_get
    ,coins_exp
    ,pay_coins_get
    ,pay_coins_exp
    ,free_coins_get
    ,free_coins_exp
    ,bonus_get
    ,bonus_exp
    ,ad_revenue
    ,main_scene_times
    ,main_play_times
    ,chap_play_times
    ,sum_online_times
    ,analysis_date as etl_date
    ,window_start
    ,window_end
    ,cast(vip_type as int) as vip_type
    ,cast(ad_start_cnt as int) as ad_start_cnt
    ,incentive_ad_revenue
    ,cast(sub_order_cnt as int) as sub_order_cnt
    -- add by lcz
    ,nvl(t2.first_visit_landing_page,'') as first_visit_landing_page -- 首次访问着陆页
    ,nvl(t2.story_id,'')                 as first_visit_landing_page_story_id -- 首次访问着陆页书籍id
    ,nvl(t2.chap_id,'')                  as first_visit_landing_page_chap_id  -- 首次访问着陆页章节id
from
(
    select
        max(id) as id -- 自增id
        ,app_user_id as uuid -- 应用中用户ID
        ,analysis_date -- 分析日期
        ,country_id -- 国家id
        ,app_channel_id as channel_id -- 应用渠道
        ,app_version as version -- APP的应用版本(包体内置版本号versionname)
        ,app_game_version as cversion -- 应用游戏版本号
        ,app_res_version as res_version -- 应用资源版本号
        ,app_lang as language_id -- 游戏语言id  语言表暂时无法处理,数据格式混乱,eg：fr-US 无法确定是英语还是法语
        ,os_type as platform -- 操作系统类型 (android/ios/windows/mac os)
        ,os_version -- 操作系统版本号
        ,device_id -- 应用用于标识设备的唯一ID
        ,if(ad_id in ('','0000-0000','00000000-0000-0000-0000-000000000000'),'',ad_id) as ad_id -- 广告ID:Android=google adid IOS=idfa
        ,androidid -- ANDROIDID
        ,idfv -- IOS-idfv
        ,max(case when event_name='m_user_signin' then 1 else 0 end  ) as is_login 
        ,max(case when event_name='m_user_signin' and sub_event_name='signup' then 1 else 0 end ) as user_type 
        ,max(case when event_name='m_pay_event' and sub_event_name='pay_complete' and (os_type='2' or cast(nvl(json_extract(properties,'$.pay_channel'),1001) as int) not in (1001,1002)) then 1
                  when event_name='m_pay_event' and sub_event_name='pay_complete' and os_type='1' 
                       and cast(nvl(json_extract(properties,'$.pay_channel'),1001) as int)=1001
                       and cast(json_extract(properties,'$._channel_orderid') as varchar) like 'GPA%'
                    then 1
                  when event_name='m_pay_event' and sub_event_name='pay_complete' and os_type='1' 
                       and cast(nvl(json_extract(properties,'$.pay_channel'),1001) as int)=1002
                    then 1
             else 0 end
         ) as is_pay
        ,min(if(event_name='m_user_signin' and sub_event_name='signup',stime)) as register_time 
        ,min(case when event_name='m_user_signin' then stime end) as first_login_time
        ,max(case when event_name='m_user_signin' then stime end)  as last_login_time               
        ,min(case when event_name='m_pay_event' and sub_event_name='pay_complete' and (os_type='2' or cast(nvl(json_extract(properties,'$.pay_channel'),1001) as int) not in (1001,1002)) then stime
                  when event_name='m_pay_event' and sub_event_name='pay_complete' and os_type='1' 
                       and cast(nvl(json_extract(properties,'$.pay_channel'),1001) as int)=1001
                       and cast(json_extract(properties,'$._channel_orderid') as varchar) like 'GPA%'
                    then stime
                  when event_name='m_pay_event' and sub_event_name='pay_complete' and os_type='1' 
                       and cast(nvl(json_extract(properties,'$.pay_channel'),1001) as int)=1002
                    then stime
             end) as first_pay_time
        ,max(case when event_name='m_pay_event' and sub_event_name='pay_complete' and (os_type='2' or cast(nvl(json_extract(properties,'$.pay_channel'),1001) as int) not in (1001,1002)) then stime
                  when event_name='m_pay_event' and sub_event_name='pay_complete' and os_type='1' 
                       and cast(nvl(json_extract(properties,'$.pay_channel'),1001) as int)=1001
                       and cast(json_extract(properties,'$._channel_orderid') as varchar) like 'GPA%'
                    then stime
                  when event_name='m_pay_event' and sub_event_name='pay_complete' and os_type='1' 
                       and cast(nvl(json_extract(properties,'$.pay_channel'),1001) as int)=1002
                    then stime
             end)  as last_pay_time 
        ,min(case when  event_name='m_pay_event' and sub_event_name='pay_complete' and os_type='1' 
                 and cast(nvl(json_extract(properties,'$.pay_channel'),1001) as int)=1001 
                 and cast(json_extract(properties,'$._channel_orderid') as varchar) not like 'GPA%'
             then stime end) as first_invaild_pay_time
        ,max(case when event_name='m_pay_event' and sub_event_name='pay_complete'  and os_type='1' 
                  and cast(nvl(json_extract(properties,'$.pay_channel'),1001) as int)=1001 
                  and cast(json_extract(properties,'$._channel_orderid') as varchar) not like 'GPA%'
             then stime end)  as last_invaild_pay_time 
        ,max(stime) as  last_active_time
        ,min(stime) as  first_active_time
        ,min(case when event_name='m_play_event' and sub_event_name='play_start' then stime end) as first_play_start_time
        ,max(case when event_name='m_play_event' and sub_event_name='play_start' then stime end)  as last_play_start_time
        ,min(case when event_name='m_play_event' and sub_event_name='play_end' then stime end) as first_play_end_time
        ,max(case when event_name='m_play_event' and sub_event_name='play_end' then stime end)  as last_play_end_time

-- pay        
        ,count(distinct case when event_name='m_pay_event' and sub_event_name='pay_complete' 
                         and (os_type='2' or cast(nvl(json_extract(properties,'$.pay_channel'),1001) as int) not in (1001,1002) 
                              or (os_type='1' and cast(json_extract(properties,'$._channel_orderid') as varchar) like 'GPA%' and cast(nvl(json_extract(properties,'$.pay_channel'),1001) as int)=1001) 
                              or (os_type='1' and cast(nvl(json_extract(properties,'$.pay_channel'),1001) as int)=1002) 
                              )
                then json_extract(properties,'$._channel_orderid') end) as order_cnt 
        ,sum(case when event_name='m_pay_event' and sub_event_name='pay_complete' 
                  and (os_type='2' or cast(nvl(json_extract(properties,'$.pay_channel'),1001) as int) not in (1001,1002) 
                       or (os_type='1' and cast(json_extract(properties,'$._channel_orderid') as varchar) like 'GPA%' and cast(nvl(json_extract(properties,'$.pay_channel'),1001) as int)=1001) 
                       or (os_type='1' and cast(nvl(json_extract(properties,'$.pay_channel'),1001) as int)=1002) 
                      )
               then cast(nvl(json_extract(properties,'$._order_amount'),0) as int) else 0  end) as pay_amount 
-- currency 
       ,sum(case when event_name='m_currency_change' and  cast(nvl(json_extract(properties,'$._vc_id'),'') as varchar)='vc_01' and substr(cast(nvl(json_extract(properties,'$._change_reason'),'') as varchar),-3)='get' 
              then cast(nvl(json_extract(properties,'$._change_amount'),0) as int)  else 0 end) as coins_get
       ,sum(case when event_name='m_currency_change' and  cast(nvl(json_extract(properties,'$._vc_id'),'') as varchar)='vc_01' and substr(cast(nvl(json_extract(properties,'$._change_reason'),'') as varchar),-3)='exp' 
              then cast(nvl(json_extract(properties,'$._change_amount'),0) as int)  else 0 end) as coins_exp
       ,sum(case when event_name='m_currency_change' and  cast(nvl(json_extract(properties,'$._vc_id'),'') as varchar)='vc_01_pay' and substr(cast(nvl(json_extract(properties,'$._change_reason'),'') as varchar),-3)='get' 
              then cast(nvl(json_extract(properties,'$._change_amount'),0) as int)  else 0 end) as pay_coins_get
       ,sum(case when event_name='m_currency_change' and  cast(nvl(json_extract(properties,'$._vc_id'),'') as varchar)='vc_01_pay' and substr(cast(nvl(json_extract(properties,'$._change_reason'),'') as varchar),-3)='exp' 
              then cast(nvl(json_extract(properties,'$._change_amount'),0) as int)  else 0 end) as pay_coins_exp
       ,sum(case when event_name='m_currency_change' and  cast(nvl(json_extract(properties,'$._vc_id'),'') as varchar)='vc_01_free' and substr(cast(nvl(json_extract(properties,'$._change_reason'),'') as varchar),-3)='get' 
              then cast(nvl(json_extract(properties,'$._change_amount'),0) as int)  else 0 end) as free_coins_get
       ,sum(case when event_name='m_currency_change' and  cast(nvl(json_extract(properties,'$._vc_id'),'') as varchar)='vc_01_free' and substr(cast(nvl(json_extract(properties,'$._change_reason'),'') as varchar),-3)='exp' 
              then cast(nvl(json_extract(properties,'$._change_amount'),0) as int)  else 0 end) as free_coins_exp
       ,sum(case when event_name='m_currency_change' and  cast(nvl(json_extract(properties,'$._vc_id'),'') as varchar)='vc_02' and substr(cast(nvl(json_extract(properties,'$._change_reason'),'') as varchar),-3)='get' 
              then cast(nvl(json_extract(properties,'$._change_amount'),0) as int)  else 0 end) as bonus_get
       ,sum(case when event_name='m_currency_change' and  cast(nvl(json_extract(properties,'$._vc_id'),'') as varchar)='vc_02' and substr(cast(nvl(json_extract(properties,'$._change_reason'),'') as varchar),-3)='exp' 
              then cast(nvl(json_extract(properties,'$._change_amount'),0) as int)  else 0 end) as bonus_exp
-- ad 
      ,sum(coalesce(case when event_name='m_admoney_event' and  os_type=1 and cast(nvl(json_extract(properties,'$._action'),'') as varchar)='impression'
            then element_at(split_to_map(replace(replace(replace(cast(nvl(json_extract(properties,'$._admoney_impression_callback'),'') as varchar),'{',''),'}',''),', ',','), ',', '='),'valueMicros')/1000000
          when event_name='m_admoney_event' and  os_type=2 and cast(nvl(json_extract(properties,'$._action'),'') as varchar)='impression'  
            then cast(json_extract(cast(nvl(json_extract(properties,'$._admoney_impression_callback'),'') as varchar),'$.value') as double)
         end,0)) as ad_revenue
-- heart 
       ,cast(count(distinct case when event_name='m_app_heart' and cast(nvl(json_extract(properties,'$._scene_name'),'') as varchar)='main_scene' then os_timestamp end)*30 as int) as main_scene_times
       ,cast(count(distinct case when event_name='m_app_heart' and cast(nvl(json_extract(properties,'$._scene_name'),'') as varchar)='main_play_scene' then os_timestamp end)*30 as int) as main_play_times
       ,cast(count(distinct case when event_name='m_app_heart' and cast(nvl(json_extract(properties,'$._scene_name'),'') as varchar)='chap_play_scene'  then os_timestamp end )*30 as int) as chap_play_times
       ,cast(count(distinct case when event_name='m_app_heart' then os_timestamp end )*30 as int) as sum_online_times
       ,date_format(stime,'%Y-%m-%d %H:%i:00') as window_start
       ,date_format(stime,'%Y-%m-%d %H:%i:59') as window_end
       ,max(case when event_name='m_user_signin'  then cast(json_extract(properties,'$.sub_status') as int)   end ) as vip_type   

      ,sum(if(event_name='m_admoney_event' and json_extract(properties,'$._action')='start',1,0)) as ad_start_cnt
      ,sum(coalesce(case 
            when event_name='m_admoney_event' and  os_type=1 and cast(nvl(json_extract(properties,'$._action'),'') as varchar)='impression' and json_extract(properties,'$._admoney_app_placeid') in('10001','20002','10002')
                  then element_at(split_to_map(replace(replace(replace(cast(nvl(json_extract(properties,'$._admoney_impression_callback'),'') as varchar),'{',''),'}',''),', ',','), ',', '='),'valueMicros')/1000000
            when event_name='m_admoney_event' and  os_type=2 and cast(nvl(json_extract(properties,'$._action'),'') as varchar)='impression' and json_extract(properties,'$._admoney_app_placeid') in('10001','20002','10002') 
                  then cast(json_extract(cast(nvl(json_extract(properties,'$._admoney_impression_callback'),'') as varchar),'$.value') as double)
            end,0)) as incentive_ad_revenue
      ,count(distinct case 
          when event_name='m_pay_event' and sub_event_name='pay_complete' and cast(json_extract(properties,'$._channel_sku') as varchar)  rlike 'sub'
                  and (os_type='2' or cast(nvl(json_extract(properties,'$.pay_channel'),1001) as int) not in (1001,1002) 
                          or (os_type='1' and cast(json_extract(properties,'$._channel_orderid') as varchar) like 'GPA%' and cast(nvl(json_extract(properties,'$.pay_channel'),1001) as int)=1001) 
                          or (os_type='1' and cast(nvl(json_extract(properties,'$.pay_channel'),1001) as int)=1002) 
                      )
              then json_extract(properties,'$._channel_orderid') end) as sub_order_cnt 
    from chapters_log.public_event_data
    where analysis_date between '${FORMER_TX_DATE}'  and '${NOW_TX_DATE}'
           and app_id='cm1009' and app_channel_id in('AVG20001','AVG10003','1','2','6','11','12','13','14','15','16','17','18','19') and os_type in ('1','2','3','4')
           and event_name in('m_user_signin','m_app_heart','m_pay_event','m_currency_change','m_play_event','m_admoney_event') and app_user_id not in('','0') 
           and country_id<>'50' and nvl(cast(json_extract(properties,'$._page_name') as varchar),'')<>'external'
           and stime <= '${NOW_TX_MINUTE}' and stime >= '${LAST_TX_MINUTE}' 
           and app_activate_id<>''
           and nvl(cast(json_extract(properties,'$._change_reason') as varchar),'')<>'bind_email_get'
           and app_user_id regexp '^[0-9]+$'=1
           and app_user_id<2147483647
    group by
        app_user_id
        ,analysis_date
        ,country_id
        ,app_channel_id
        ,app_version
        ,app_game_version
        ,app_res_version
        ,app_lang
        ,os_type
        ,os_version
        ,androidid
        ,idfv
        ,device_id
        ,if(ad_id in ('','0000-0000','00000000-0000-0000-0000-000000000000'),'',ad_id)
        ,date_format(stime,'%Y-%m-%d %H:%i:00')
        ,date_format(stime,'%Y-%m-%d %H:%i:59')
) t1

-- left join
-- (
--     select
--         name_en_short
--         ,id as language_id
--     from chapters_log.language_info_v2
-- ) t5
-- on t1.language_id=t5.name_en_short
left join (
           select
           uuid
           ,SUBSTRING_INDEX(url,'.com/',-1) as first_visit_landing_page -- 首次访问着陆页
           ,story_id                        as story_id -- 书籍id
           ,chap_id                         as chap_id -- 章节id
           ,row_number() over( partition by uuid order by stime asc) as rn
           from dwd_data.dwd_t01_reelshort_device_start_di as t
           where etl_date='${TX_DATE}'
                 and event_name='m_app_install'
) as t2
on t2.uuid=t1.uuid and t2.rn=1

)
,reelshort_user_info_di_v2_pay as (
select
    id
    ,uuid
    ,analysis_date
    ,country_id
    ,channel_id
    ,version
    ,cversion
    ,res_version
    ,case -- when t5.language_id is not null then t5.language_id
         when t1.language_id like 'zh%' or t1.language_id in ('cn')  then 99
         when t1.language_id like 'da%'  then 16
         when t1.language_id like 'fi%'  then 15
         when t1.language_id like 'nb%' or t1.language_id like 'no%'  or t1.language_id like 'nn%' then 14
         when t1.language_id like 'sv%'  then 13
         when t1.language_id like 'nl%'  then 12
         when t1.language_id like 'tr%'  then 11
         when t1.language_id like 'pl%'  then 10
         when t1.language_id like 'ja%'  then 9
         when t1.language_id like 'it%'  then 8
         when t1.language_id like 'ko%'  then 7
         when t1.language_id like 'pt%' or t1.language_id='brazil' then 6
         when t1.language_id like 'ru%'  then 5
         when t1.language_id like 'fr%'  then 4
         when t1.language_id like 'de%'  then 3
         when t1.language_id like 'es%'  then 2
         when t1.language_id like 'en%' and t1.language_id<>'en-ID' then 1
         when t1.language_id like 'ar%'  then 17
         when t1.language_id in ('en-ID','in') or t1.language_id like 'id%'  then 18
         when t1.language_id like 'th%' then 98 
         else t1.language_id
    end as language_id 
    ,platform
    ,os_version
    ,device_id
    ,ad_id
    ,androidid
    ,idfv
    ,pay_amount
    ,sub_pay_amount
    ,analysis_date as etl_date
    ,window_start
    ,window_end
from
(
select max(id) as id 
    ,uuid
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
    ,androidid
    ,idfv
    ,sum(pay_amount) as pay_amount
    ,sum(sub_pay_amount) as sub_pay_amount
    ,window_start
    ,window_end
    from (select
        max(id) as id -- 自增id
        ,app_user_id as uuid -- 应用中用户ID
        ,analysis_date -- 分析日期
        ,country_id -- 国家id
        ,app_channel_id as channel_id -- 应用渠道
        ,app_version as version -- APP的应用版本(包体内置版本号versionname)
        ,app_game_version as cversion -- 应用游戏版本号
        ,app_res_version as res_version -- 应用资源版本号
        ,app_lang as language_id -- 游戏语言id  语言表暂时无法处理,数据格式混乱,eg：fr-US 无法确定是英语还是法语
        ,os_type as platform -- 操作系统类型 (android/ios/windows/mac os)
        ,os_version -- 操作系统版本号
        ,device_id -- 应用用于标识设备的唯一ID
        ,if(ad_id in ('','0000-0000','00000000-0000-0000-0000-000000000000'),'',ad_id) as ad_id -- 广告ID:Android=google adid IOS=idfa
        ,androidid -- ANDROIDID
        ,idfv -- IOS-idfv
-- pay  
       ,if(cast(json_extract(properties,'$._channel_sku') as varchar)  like '%sub%', '',cast(json_extract(properties,'$._channel_orderid') as varchar)) as _channel_orderid
        -- ,cast(json_extract(properties,'$._channel_orderid') as varchar)    as    _channel_orderid
        ,max(case when   (os_type='2' or cast(nvl(json_extract(properties,'$.pay_channel'),1001) as int) not in (1001,1002) 
                       or (os_type='1' and cast(json_extract(properties,'$._channel_orderid') as varchar) like 'GPA%' and cast(nvl(json_extract(properties,'$.pay_channel'),1001) as int)=1001) 
                       or (os_type='1' and cast(nvl(json_extract(properties,'$.pay_channel'),1001) as int)=1002) 
                      )
               then cast(nvl(json_extract(properties,'$._order_amount'),0) as int) else 0  end) as pay_amount 

        ,max(case when   (os_type='2' 
                       or (os_type='1' and cast(json_extract(properties,'$._channel_orderid') as varchar) like 'GPA%' and cast(nvl(json_extract(properties,'$.pay_channel'),1001) as int)=1001) 
                       or (os_type='1' and cast(nvl(json_extract(properties,'$.pay_channel'),1001) as int)=1002) 
                      )
                and cast(json_extract(properties,'$._channel_sku') as varchar)  like '%sub%'
               then cast(nvl(json_extract(properties,'$._order_amount'),0) as int) else 0  end) as sub_pay_amount 
       ,date_format(stime,'%Y-%m-%d %H:%i:00') as window_start
       ,date_format(stime,'%Y-%m-%d %H:%i:59') as window_end
    from chapters_log.public_event_data
    where analysis_date between '${FORMER_TX_DATE}'  and '${NOW_TX_DATE}'
           and app_id='cm1009' and app_channel_id in('AVG20001','AVG10003','1','2','6','11','12','13','14','15','16','17','18','19') and os_type in ('1','2','3','4')
           and app_user_id not in('','0') 
           and country_id<>'50'
           and stime <= '${NOW_TX_MINUTE}' and stime >= '${LAST_TX_MINUTE}' 
           and event_name='m_pay_event' and sub_event_name='pay_complete'
    group by
        app_user_id
        ,analysis_date
        ,country_id
        ,app_channel_id
        ,app_version
        ,app_game_version
        ,app_res_version
        ,app_lang
        ,os_type
        ,os_version
        ,androidid
        ,idfv
        ,device_id
        ,if(ad_id in ('','0000-0000','00000000-0000-0000-0000-000000000000'),'',ad_id)
        ,date_format(stime,'%Y-%m-%d %H:%i:00')
        ,date_format(stime,'%Y-%m-%d %H:%i:59')
        ,_channel_orderid
        -- ,cast(json_extract(properties,'$._channel_orderid') as varchar)
      )
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
    ,androidid
    ,idfv
    ,window_start
    ,window_end
) t1
-- left join
-- (
--     select
--         name_en_short
--         ,id as language_id
--     from chapters_log.language_info_v2
-- ) t5
-- on t1.language_id=t5.name_en_short
)

select 
      md5(concat_ws('-',t1.country_id,t1.channel_id,t1.version,t1.cversion,t1.res_version
        ,t1.language_id,t1.platform,t1.os_version,t1.device_id,t1.ad_id,t1.androidid,t1.idfv)
       ) as id 
      ,t1.uuid
      ,t1.analysis_date
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
      ,t1.androidid
      ,t1.idfv
      ,t1.is_login
      ,t1.user_type
      ,t1.is_pay
      ,t1.register_time
      ,t1.first_login_time
      ,t1.last_login_time
      ,t1.first_pay_time
      ,t1.last_pay_time
      ,t1.first_invaild_pay_time
      ,t1.last_invaild_pay_time
      ,t1.first_active_time
      ,t1.last_active_time
      ,t1.first_play_start_time
      ,t1.last_play_start_time
      ,t1.first_play_end_time
      ,t1.last_play_end_time
      ,t1.order_cnt
      ,coalesce(t2.pay_amount,0) as pay_amount
      ,t1.coins_get
      ,t1.coins_exp
      ,t1.pay_coins_get
      ,t1.pay_coins_exp
      ,t1.free_coins_get
      ,t1.free_coins_exp
      ,t1.bonus_get
      ,t1.bonus_exp
      ,t1.ad_revenue
      ,t1.main_scene_times
      ,t1.main_play_times
      ,t1.chap_play_times
      ,t1.sum_online_times
      ,t1.etl_date
      ,t1.window_start
      ,t1.window_end
      ,convert_tz(current_timestamp,'Asia/Shanghai','America/Los_Angeles') as insert_time
      ,coalesce(t2.sub_pay_amount,0) as sub_pay_amount
      ,vip_type
      ,ad_start_cnt
      ,incentive_ad_revenue
      ,sub_order_cnt
      ,t1.first_visit_landing_page -- 首次访问着陆页
      ,t1.first_visit_landing_page_story_id -- 首次访问着陆页书籍id
      ,t1.first_visit_landing_page_chap_id  -- 首次访问着陆页章节id
from reelshort_user_info_di_v2 t1  
left join reelshort_user_info_di_v2_pay t2 
on t1.uuid=t2.uuid 
and t1.analysis_date=t2.analysis_date
and t1.window_start=t2.window_start 
and t1.country_id=t2.country_id
and t1.channel_id=t2.channel_id
and t1.version=t2.version
and t1.cversion=t2.cversion
and t1.res_version=t2.res_version
and t1.language_id=t2.language_id
and t1.platform=t2.platform
and t1.os_version=t2.os_version
and t1.device_id=t2.device_id
and t1.ad_id=t2.ad_id
and t1.androidid=t2.androidid
and t1.idfv=t2.idfv
where t1.uuid not in ( 
 select distinct uuid  from  reelshort_user_info_di_v2
  where platform='2' and version >='2.0.50' and version not like 'v%'
  and uuid not in ( 
      select distinct  app_user_id
      from chapters_log.reelshort_event_data_log
      where analysis_date between '${FORMER_TX_DATE}'  and '${NOW_TX_DATE}' 
      and app_id='cm1009' 
      and os_type='2'
          and event_name in('m_page_enter')
          and country_id<>'50'
          and app_version >='2.0.50'  and app_version not like 'v%'
  )
  )
and t1.coins_exp<10000000
;



delete from  dw_view.`vt_dwd_t01_reelshort_user_info_di`
where etl_date  between '${FORMER_TX_DATE}'  and '${NOW_TX_DATE}'
and platform='2' and version >='2.0.50' and version not like 'v%'
and uuid not in ( 
      select distinct  app_user_id
      from chapters_log.reelshort_event_data_log
      where analysis_date between '${FORMER_TX_DATE}'  and '${NOW_TX_DATE}' and app_id='cm1009' 
      and os_type='2'
          and event_name in('m_page_enter')
          and app_version >='2.0.50'  and app_version not like 'v%'
)
;
