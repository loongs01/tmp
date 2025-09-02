replace into dw_view.vt_dim_t01_reelshort_user_info
 select  uuid
       ,analysis_date
       ,install_analysis_date
       ,reactive_flag
       ,reactive_flag_30d
       ,is_login
       ,new_active_flag
       ,first_login_time
       ,first_login_tm
       ,user_type
       ,lt_last_active_date
       ,case when  max(user_type)over(partition by uuid,analysis_date)=1 then 1
            when lt_last_active_date<'${NOW_TX_DATE}' AND lt_last_active_date>=date_sub('${NOW_TX_DATE}',6)  THEN 2
            when lt_last_active_date<date_sub('${NOW_TX_DATE}',6) and lt_last_active_date>=date_sub('${NOW_TX_DATE}',29)   then 3
            when lt_last_active_date<=date_sub('${NOW_TX_DATE}',30) then 4
            else -1
      end AS active_user_type
      ,register_time
      ,nvl(t1.first_visit_landing_page,'') as first_visit_landing_page -- 首次访问着陆页
      ,nvl(t1.story_id,'')                 as first_visit_landing_page_story_id -- 首次访问着陆页书籍id
      ,nvl(t1.chap_id,'')                  as first_visit_landing_page_chap_id  -- 首次访问着陆页章节id
 from (select   t1.uuid
          ,t1.analysis_date
         ,max(case when  t3.analysis_date<=date_sub('${NOW_TX_DATE}',90) then 1 else 0 end) as reactive_flag
         ,max(case when  t3.analysis_date<=date_sub('${NOW_TX_DATE}',30) then 1 else 0 end) as reactive_flag_30d
         ,max(install_analysis_date) as install_analysis_date
         ,max(is_login) as is_login
         ,max(case when  t3.analysis_date<=date_sub('${NOW_TX_DATE}',30) or user_type=1 then 1 else 0 end) as new_active_flag
         ,min(first_login_time) as first_login_time
         ,min(first_login_tm) as first_login_tm
         ,max(user_type) as user_type
         ,max(t3.analysis_date) as lt_last_active_date
         ,min(coalesce(t1.register_time,t3.register_time)) as register_time
   from (select    uuid,analysis_date,platform,device_id
               ,max(is_login) as is_login
               ,max(user_type) as user_type
               ,min(first_login_time) as first_login_time
               ,min(unix_timestamp(convert_tz(first_login_time,'America/Los_Angeles','Asia/Shanghai'))) as first_login_tm
               ,min(register_time) as register_time
        from dw_view.vt_dwd_t01_reelshort_user_info_di
        where etl_date ='${NOW_TX_DATE}'
        group by    uuid,analysis_date,platform,device_id
   )t1
   left join (
     select distinct   device_id,platform,analysis_date as install_analysis_date
     from dw_view.dwd_t01_reelshort_device_start_di
     where etl_date ='${NOW_TX_DATE}'   and start_status='1'
   )t2
   ON t1.device_id=t2.device_id and t1.analysis_date=t2.install_analysis_date and t1.platform=t2.platform
   left join  (
      select    uuid ,max(analysis_date) as analysis_date,min(register_time) as register_time
      from (
        select uuid ,date(last_active_time) as analysis_date,register_time
        from dwd_data.dim_t99_reelshort_user_lt_info
        where  etl_date=date_sub('${NOW_TX_DATE}',2)
        -- and analysis_date<=date_sub('${NOW_TX_DATE}',30)
        and uuid in (select distinct uuid  from dw_view.vt_dwd_t01_reelshort_user_info_di
                     where etl_date ='${NOW_TX_DATE}')
        union all
        select distinct uuid ,analysis_date,register_time
        from dw_view.vt_dwd_t01_reelshort_user_info_di
        where etl_date=date_sub('${NOW_TX_DATE}',1)
      )
      group by uuid
   )t3
   on t1.uuid=t3.uuid
   group by 1,2
   ) as t
-- add by lcz
left join (
                   select
                      uuid as uuid_t1
                      ,SUBSTRING_INDEX(url,'.com/',-1) as first_visit_landing_page -- 首次访问着陆页
                      ,story_id                        as story_id -- 书籍id
                      ,chap_id                         as chap_id -- 章节id
                      ,row_number() over( partition by uuid order by stime asc) as rn
                      from dwd_data.dwd_t01_reelshort_user_login_info_di as t
                      where etl_date='${NOW_TX_DATE}'
                            and event_name='m_user_signin'
                   ) as t1
on t1.uuid_t1=t.uuid and t1.rn=1
 ;




