select  tmp1.uuid              as uuid
       -- 展示次数  ->APP元素曝光人数->频道页曝光用户数
       ,tmp1.show_cnt          as show_cnt                -- 展示次数  ->APP元素曝光人数->频道页曝光用户数
       -- ,tmp1.click_cnt         as click_cnt               -- 点击次数  ->APP元素点击人数
       ,tmp1.sub_event_name    as sub_event_name          -- 子事件
       ,tmp1.action            as action                  -- 行为
       -- ,tmp2.search_story_ids  as search_story_ids        -- 搜索结果
	   ,tmp3.tab_name         as tab_name                 -- 频道页名称 

from (select
            coalesce(t1.uuid,t2.uuid,t3.uuid,t4.uuid ) as uuid
            ,coalesce(t1.analysis_date,t2.analysis_date,t3.analysis_date,t4.analysis_date ) as analysis_date
            ,cast(coalesce(t3.show_cnt,t4.show_cnt,0.0) as decimal(10,4) )as show_cnt                  -- '展示次数'
            ,cast(coalesce(t2.click_cnt,t3.click_cnt,t4.click_cnt,0.0) as decimal(10,4)) as click_cnt  --  '点击次数'
            ,coalesce(t1.sub_event_name,t2.sub_event_name,t3.sub_event_name,t4.sub_event_name ) as sub_event_name
            ,coalesce(t1.action,t2.action,t3.action,t4.action ) as action
      from (
            select
            null as id,uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform
            ,concat_ws('|',country_id,channel_id,version,cversion,res_version,language_id,platform) as join_col
            ,sub_event_name,scene_name,page_name,pre_page_name,if(sub_event_name in('play_start','play_end'),type,'') as action,cast(shelf_id as varchar) as shelf_id
            ,story_id,if(shelf_id in ('30001', '30003', '30004', '30002', '30005'),referrer_story_id,'') as referrer_story_id,chap_order_id,chap_id
            ,max(video_id) as video_id -- 视频id
            ,cast(count(*) as int) as report_cnt -- 上报次数
            ,cast(sum(if(sub_event_name='cover_click',1,0)) as int) as cover_click_cnt -- 封面点击次数
            ,cast(sum(if(sub_event_name='play_start',1,0)) as int) as play_start_cnt
            ,cast(sum(if(sub_event_name='play_end' and type='complete',1,0)) as int) as play_complete_cnt
            ,min(stime) as first_report_time -- 首次上报时间
            ,max(stime) as last_report_time -- 最后一次上报时间
            -- ,cast(max(case
            --         when sub_event_name='play_end' and t2.id2 is not null and event_duration<0 then 0
            --         when sub_event_name='play_end' and t2.id2 is not null and event_duration<10000 then event_duration
            --         when sub_event_name='play_end' and t2.id2 is not null and event_duration>10000 then chap_total_duration
            --         when sub_event_name='play_end' and t2.id2 is null and event_duration>chap_total_duration then chap_total_duration
            --         else event_duration end) as int) as play_duration -- 播放时长
            ,cast(max(case
            when sub_event_name='play_end' and event_duration<0 then 0
            when sub_event_name='play_end' and t2.id2 is not null and event_duration<500 then event_duration
            when sub_event_name='play_end' and t2.id2 is not null and event_duration>500 then chap_total_duration
            when sub_event_name='play_end' and t2.id2 is null and event_duration>chap_total_duration then chap_total_duration
            else event_duration end) as int) as play_duration
            from dwd_data.dwd_t02_reelshort_play_event_di t1
            left join (select id as id2 from chapters_log.reelshort_book_info where analysis_date='${TX_DATE}' and book_type=2) t2 on t1.story_id=t2.id2
            where etl_date='${TX_DATE}' and sub_event_name in('play_start','play_end','cover_click') and story_id not in ('','0')
            group by uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform
            ,sub_event_name,scene_name,page_name,pre_page_name,if(sub_event_name in('play_start','play_end'),type,''),shelf_id
            ,story_id,referrer_story_id,chap_id,chap_order_id
      ) t1 -- 8521w
      full join (
                 select
                       null as id,uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform
                       ,concat_ws('|',country_id,channel_id,version,cversion,res_version,language_id,platform) as join_col
                       ,sub_event_name
                       ,scene_name
                       ,page_name
                       ,'' pre_page_name
                       ,cast(action_type as varchar) as action
                       ,'' shelf_id
                       ,story_id,chap_id,chap_order_id
                       ,cast(count(*) as int) as report_cnt -- 上报次数
                       ,cast(count(*) as int) as click_cnt -- 点击次数
                       ,min(stime) as first_report_time -- 首次上报时间
                       ,max(stime) as last_report_time -- 最后一次上报时间
                  from dwd_data.dwd_t02_reelshort_user_action_book_di
                  where etl_date='${TX_DATE}' and sub_event_name in ('chap_list_click') -- 'chap_favorite','book_collect',
                       -- and story_id not in ('','0')
                  group by uuid,analysis_date,etl_date,country_id,channel_id,version,cversion,res_version,language_id,platform
                       ,sub_event_name,scene_name,page_name,action_type
                  ,story_id,chap_id,chap_order_id
      ) t2
      on t1.uuid=t2.uuid and t1.analysis_date=t2.analysis_date
      and t1.country_id=t2.country_id and t1.channel_id=t2.channel_id and t1.version=t2.version and t1.cversion=t2.cversion and t1.res_version=t2.res_version and t1.language_id=t2.language_id and t1.platform=t2.platform
      and t1.story_id=t2.story_id and t1.chap_id=t2.chap_id and t1.chap_order_id=t2.chap_order_id
      and t1.sub_event_name=t2.sub_event_name
      and t1.scene_name=t2.scene_name
      and t1.page_name=t2.page_name
      and t1.pre_page_name=t2.pre_page_name
      full join (
                 select
                      null as id,uuid,analysis_date,etl_date,country_id,channel_id,version,cversion,res_version,language_id,platform
                      ,concat_ws('|',country_id,channel_id,version,cversion,res_version,language_id,platform) as join_col
                      ,sub_event_name,scene_name,pre_page_name,shelf_id
                      ,case
                          when sub_event_name='watch_full_drama_popup' and action='show' then 'watch_full_drama_popup_show'
                          when sub_event_name='watch_full_drama_popup' and action='click' then 'watch_full_drama_popup_click'
                          when sub_event_name='story_rec_popup_click' and action='show' then 'story_rec_popup_show'
                          when sub_event_name='story_rec_popup_click' and action='book_click' then 'story_rec_popup_book_click'
                          when sub_event_name='story_rec_popup_click' and action='play_click' then 'story_rec_popup_play_click'
                          when sub_event_name='story_rec_popup_click' and action='book_switch' then 'story_rec_popup_book_switch'
                          when sub_event_name='story_rec_popup_click' and action='close' then 'story_rec_popup_close'
                          when sub_event_name='story_rec_popup_click' and action='complete_play' then 'story_rec_popup_complete_play'
                          else action end as action
                      ,story_id,chap_id,chap_order_id
                      ,cast(count(*) as int) as report_cnt -- 上报次数
                      ,cast(sum(case when sub_event_name='watch_full_drama_popup' and action='show' then 1
                                when sub_event_name='story_rec_popup_click' and action='show' then 1
                                else 0 end
                                ) as int) as show_cnt
                      ,cast(sum(case when sub_event_name='watch_full_drama_popup' and action='click' then 1
                                when sub_event_name='story_rec_popup_click' and action in('book_click','play_click','book_switch','close') then 1
                                else 0 end
                                ) as int) as click_cnt
                      ,min(stime) as first_report_time -- 首次上报时间
                      ,max(stime) as last_report_time -- 最后一次上报时间
                      ,case
                          when sub_event_name='story_rec_popup_click' and nvl(cast(json_extract(properties,'$.rec_scene') as varchar),'')='1' then 'story_rec_popup_click_1'
                          when sub_event_name='story_rec_popup_click' and nvl(cast(json_extract(properties,'$.rec_scene') as varchar),'')='2' then 'story_rec_popup_click_2'
                          when sub_event_name='story_rec_popup_click' and nvl(cast(json_extract(properties,'$.rec_scene') as varchar),'')='3' then 'story_rec_popup_click_3'
                          when sub_event_name='watch_full_drama_popup' then page_name
                          else '' end as page_name
                  from dwd_data.dwd_t02_reelshort_custom_event_di
                  where etl_date='${TX_DATE}' and sub_event_name in ('story_rec_popup_click','watch_full_drama_popup')
                      and story_id not in ('','0')
                  group by uuid,analysis_date,etl_date,country_id,channel_id,version,cversion,res_version,language_id,platform
                      ,sub_event_name,scene_name,pre_page_name,action,shelf_id
                      ,story_id,chap_id,chap_order_id
                      ,case
                          when sub_event_name='story_rec_popup_click' and nvl(cast(json_extract(properties,'$.rec_scene') as varchar),'')='1' then 'story_rec_popup_click_1'
                          when sub_event_name='story_rec_popup_click' and nvl(cast(json_extract(properties,'$.rec_scene') as varchar),'')='2' then 'story_rec_popup_click_2'
                          when sub_event_name='story_rec_popup_click' and nvl(cast(json_extract(properties,'$.rec_scene') as varchar),'')='3' then 'story_rec_popup_click_3'
                          when sub_event_name='watch_full_drama_popup' then page_name
                          else '' end
                      ,case
                          when sub_event_name='watch_full_drama_popup' and action='show' then 'watch_full_drama_popup_show'
                          when sub_event_name='watch_full_drama_popup' and action='click' then 'watch_full_drama_popup_click'
                          when sub_event_name='story_rec_popup_click' and action='show' then 'story_rec_popup_show'
                          when sub_event_name='story_rec_popup_click' and action='book_click' then 'story_rec_popup_book_click'
                          when sub_event_name='story_rec_popup_click' and action='play_click' then 'story_rec_popup_play_click'
                          when sub_event_name='story_rec_popup_click' and action='book_switch' then 'story_rec_popup_book_switch'
                          when sub_event_name='story_rec_popup_click' and action='close' then 'story_rec_popup_close'
                          when sub_event_name='story_rec_popup_click' and action='complete_play' then 'story_rec_popup_complete_play'
                          else action end
      ) t3 -- 379
      on t1.uuid=t3.uuid and t1.analysis_date=t3.analysis_date
      and t1.country_id=t3.country_id and t1.channel_id=t3.channel_id and t1.version=t3.version and t1.cversion=t3.cversion and t1.res_version=t3.res_version and t1.language_id=t3.language_id and t1.platform=t3.platform
      and t1.story_id=t3.story_id and t1.chap_id=t3.chap_id and t1.chap_order_id=t3.chap_order_id
      and t1.sub_event_name=t3.sub_event_name
      and t1.scene_name=t3.scene_name
      and t1.page_name=t3.page_name
      and t1.pre_page_name=t3.pre_page_name
      and t1.action=t3.action
      and t1.shelf_id=t3.shelf_id
      full join (
                 select
                       *
                       ,if(shelf_id2='typing','typing_search_stat',shelf_id2) as shelf_id
                       ,cast(if(action in('default_page_show','result_page_show'),report_cnt,0)/story_ids_size as decimal(10,4)) as show_cnt
                       ,cast(if(action not in('default_page_show','result_page_show'),report_cnt,0)/story_ids_size as decimal(10,4)) as click_cnt
                 from (select
                             null as id,uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform
                             ,sub_event_name
                             ,scene_name
                             ,page_name
                             ,pre_page_name
                             ,action
                             ,shelf_id  shelf_id2
                             ,nvl(split_part(col,'#',2),'') as story_id,chap_id,chap_order_id
                             ,cast(max(if(page_name='search_stat_result' and split_part(col,'#',2) is null,search_null_cnt,report_cnt)) as int) as report_cnt
                             ,story_ids_size
                 --             ,cast(sum(if(action in('default_page_show','result_page_show'),1,0))/size(story_ids) as decimal(10,4)) as show_cnt
                 --             ,cast(sum(if(action in('search_bar_click','tag_click','story_click','search_click','search_click','history_bin_click','history_delete_click'),1,0))/size(story_ids) as decimal(10,4)) as click_cnt
                             ,min(first_report_time) as first_report_time -- 首次上报时间
                             ,max(last_report_time) as last_report_time -- 最后一次上报时间
                             ,0 as play_duration
                             ,0 as online_times
                         from
                         (
                             select
                                 min(id) as id
                                 ,uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform
                                 ,sub_event_name
                                 ,scene_name
                                 ,case
                                     when page_type='default' then 'default_search_stat'
                                     when page_type='result' then 'result_search_stat'
                                     else page_name end as page_name
                                 ,'' as pre_page_name
                                 ,action
                 --                 ,if(search_word_source='typing','typing_search_stat',search_word_source) as shelf_id
                                 ,search_word_source as shelf_id
                                 ,'' chap_id,0 chap_order_id
                                 ,cast(count(*) as int) as report_cnt -- 上报次数
                                 ,min(stime) as first_report_time -- 首次上报时间
                                 ,max(stime) as last_report_time -- 最后一次上报时间
                                 ,if(search_story_ids in('[]',''),search_rec_story_ids,search_story_ids)
                                 ,split(replace(replace(replace(if(search_story_ids in('[]',''),search_rec_story_ids,search_story_ids),'[',''),']',''),'"',''),',') as story_ids
                                 ,size(split(replace(replace(replace(if(search_story_ids in('[]',''),search_rec_story_ids,search_story_ids),'[',''),']',''),'"',''),',')) as story_ids_size
                                 ,cast(count(distinct if(search_story_ids in ('', '[]') and page_type='result' ,ctime)) as int) as search_null_cnt -- 搜索无结果页次数
                             from dwd_data.dwd_t02_reelshort_search_stat_di
                             where etl_date='${TX_DATE}'
                             group by uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform
                                 ,sub_event_name,search_word_source,page_type,action,scene_name
                                 -- ,story_id,chap_id,chap_order_id
                                 ,if(search_story_ids in('[]',''),search_rec_story_ids,search_story_ids)
                         )
                         cross join unnest(story_ids) as tmp(col)
                         group by uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform
                             ,sub_event_name,scene_name,page_name,action,pre_page_name,shelf_id
                             ,nvl(split_part(col,'#',2),''),chap_id,chap_order_id
                 ) as tmp
      ) t4 -- 471
      on t1.uuid=t4.uuid and t1.analysis_date=t4.analysis_date
      and t1.country_id=t4.country_id and t1.channel_id=t4.channel_id and t1.version=t4.version and t1.cversion=t4.cversion and t1.res_version=t4.res_version and t1.language_id=t4.language_id and t1.platform=t4.platform
      and t1.story_id=t4.story_id and t1.chap_id=t4.chap_id and t1.chap_order_id=t4.chap_order_id
      and t1.sub_event_name=t4.sub_event_name
      and t1.scene_name=t4.scene_name
      and t1.page_name=t4.page_name
      and t1.action=t4.action
      and t1.shelf_id=t4.shelf_id
) as tmp1
left join (
           select distinct
                  etl_date as analysis_date
                  ,uuid
                  ,if(search_story_ids='[]',1,0) as search_story_ids -- 搜索结果页面中，搜索结果的书籍列表，元素结构是 书籍顺序#书籍ID#匹配规则
           from dwd_data.dwd_t02_reelshort_search_stat_di as t   -- dwd_data.dwd_t02_reelshort_custom_event_di
           where etl_date='${TX_DATE}'
                 and event_name='m_custom_event'
                 and sub_event_name='search_stat'
) as tmp2
on tmp2.uuid=tmp1.uuid and tmp2.analysis_date=tmp1.analysis_date
left join(
          select distinct
                 -- null as id
                 ,t.analysis_date
                 ,t.uuid
                 ,t.tab_id                         -- 频道页
                 ,nvl(t1.tab_name,'') as tab_name  -- 频道页名称
                 -- ,concat(t.tab_id,t1.tab_name) as tab_id_name -- 频道页id+（中文名）
          from (select distinct
                      etl_date as analysis_date
                      ,uuid
                      ,cast(nvl(json_extract(item_list,'$.sub_page_id'),'') as varchar) as tab_id -- 频道页
                from dwd_data.dwd_t02_reelshort_item_pv_di
                where etl_date='${TX_DATE}'
                and event_name='m_item_pv'
          )as t
          left join dw_view.dts_project_v_hall_v3 as t1
          on t1.tab_id=t.tab_id
) as tmp3
on tmp3.uuid=tmp1.uuid