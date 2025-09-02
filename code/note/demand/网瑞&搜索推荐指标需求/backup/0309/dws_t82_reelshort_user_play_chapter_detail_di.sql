delete from dws_data.dws_t82_reelshort_user_play_chapter_detail_di where etl_date='${TX_DATE}';
/*+ o_cbo_engine=none */
replace into dws_data.dws_t82_reelshort_user_play_chapter_detail_di
select 
	id 
	,t1.uuid 
	,t1.analysis_date
	,t1.etl_date
	,coalesce(t2.is_pay,0) as is_pay
	,coalesce(t2.is_login,0) as is_login
	,coalesce(t2.user_type,0) as user_type
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
	,t1.story_id
	,t1.chap_id
	,t1.chap_order_id
	,max(t1.is_free) as is_free
	,max(t1.chap_total_duration) as chap_total_duration
	,max(t1.video_id) as video_id
	,cast(sum(t1.play_duration) as int) as play_duration 
	,max(t1.max_process) as max_process
	,cast(sum(t1.cover_click_cnt) as int ) as cover_click_cnt 
	,cast(sum(t1.play_enter_cnt ) as int ) as play_enter_cnt  
	,cast(sum(t1.play_start_cnt ) as int ) as play_start_cnt  
	,cast(sum(t1.play_end_cnt ) as int ) as play_end_cnt  
	,cast(sum(t1.player_play_start_cnt ) as int ) as player_play_start_cnt 
	,cast(sum(t1.player_play_end_cnt ) as int ) as player_play_end_cnt
	,cast(sum(t1.play_start_begin_cnt) as int ) as play_start_begin_cnt 
	,cast(sum(t1.play_start_pause_off_cnt) as int ) as play_start_pause_off_cnt 
	,cast(sum(t1.play_end_complete_cnt) as int ) as play_end_complete_cnt 
	,cast(sum(t1.play_end_pause_on_cnt) as int ) as play_end_pause_on_cnt 
	,max(t1.min_play_ctime) as min_play_ctime
	,max(t1.is_auto_unlock) as is_auto_unlock
	,cast(sum(t1.coins_exp)as int ) as coins_exp
	-- ,cast(sum(t1.res_dl_duration)as bigint ) as res_dl_duration
	-- ,cast(sum(t1.res_dl_start_cnt)as int ) as res_dl_start_cnt
	-- ,cast(sum(t1.res_dl_complete_cnt)as int ) as res_dl_complete_cnt
	,cast(sum(t1.favorite_cnt) as int ) as favorite_cnt
	,cast(sum(t1.cancel_favorite_cnt) as int ) as cancel_favorite_cnt
	,cast(sum(t1.collect_cnt) as int ) as collect_cnt
	,cast(sum(t1.cancel_collect_cnt) as int ) as cancel_collect_cnt
	,cast(sum(t1.chap_click_cnt) as int ) as chap_click_cnt
	,coalesce(t2.af_network_name,'') as     af_network_name
	,coalesce(t2.af_channel,'') as          af_channel
	,t1.t_book_id as t_book_id
	,cast(sum(t1.order_cnt) as int ) as order_cnt
	,cast(sum(t1.pay_amount) as int ) as pay_amount
	,cast(sum(t1.sum_online_times) as int ) as sum_online_times
	,cast(sum(t1.main_play_times) as int ) as main_play_times
	,cast(sum(t1.chap_play_times) as int ) as chap_play_times
	,cast(sum(t1.ad_invoke_cnt) as int )  as ad_invoke_cnt
	,cast(sum(t1.ad_start_cnt) as int )  as ad_start_cnt
	,cast(sum(t1.ad_complete_cnt) as int )  as ad_complete_cnt
	,cast(sum(t1.ad_loadfail_cnt) as int )  as ad_loadfail_cnt
	,cast(sum(t1.ad_playfail_cnt) as int )  as ad_playfail_cnt
	,cast(sum(t1.ad_click_cnt) as int )  as ad_click_cnt
	,cast(sum(t1.ad_impression_cnt) as int )  as ad_impression_cnt
	,cast(sum(t1.ad_revenue) as double )  as ad_revenue
	,cast(sum(t1.dlink_complete_cnt) as int )  as dlink_complete_cnt
	,cast(sum(t1.coins_exp_v2)as int ) as coins_exp_v2
	,cast(sum(sub_order_cnt) as int) as sub_order_cnt -- 订阅订单数 
	,cast(sum(sub_pay_amount) as int) as sub_pay_amount -- 订阅订单金额 
	,cast(max(is_banner_exposure) as int) as is_banner_exposure -- 是否banner广告曝光 
	,cast(max(is_discover_exposure) as int) as is_discover_exposure -- 是否在大厅Discover主页曝光 
	,cast(max(is_library_exposure) as int) as is_library_exposure -- 是否在大厅Library主页(My List)曝光 
	,cast(max(is_next_exposure) as int) as is_next_exposure -- 是否在播放器-下一部曝光 
	,cast(sum(t1.40001_play_duration) as int) as 40001_play_duration 
	,coalesce(t2.vip_type,0) as vip_type
	,max(t1.max_play_ctime) as max_play_ctime
from 
(
	select
		coalesce(t1.id ,t2.id,t4.id,t5.id ,t6.id ,t7.id ,t8.id ,t9.id,t10.id,t11.id) as id 
		,coalesce(t1.uuid,t2.uuid,t4.uuid,t5.uuid ,t6.uuid ,t7.uuid ,t8.uuid ,t9.uuid,t10.uuid ,t11.uuid) as uuid 
		,coalesce(t1.analysis_date,t2.analysis_date,t4.analysis_date,t5.analysis_date,t6.analysis_date,t7.analysis_date,t8.analysis_date,t9.analysis_date,t10.analysis_date,t11.analysis_date) as analysis_date
		,coalesce(t1.etl_date ,t2.etl_date,t4.etl_date,t5.etl_date,t6.etl_date,t7.etl_date,t8.etl_date,t9.etl_date,t10.etl_date,t11.etl_date) as etl_date
		,coalesce(t1.country_id,t2.country_id,t4.country_id,t5.country_id ,t6.country_id ,t7.country_id ,t8.country_id ,t9.country_id,t10.country_id ,t11.country_id) as country_id 
		,coalesce(t1.channel_id,t2.channel_id,t4.channel_id,t5.channel_id ,t6.channel_id ,t7.channel_id ,t8.channel_id ,t9.channel_id,t10.channel_id ,t11.channel_id) as channel_id 
		,coalesce(t1.version,t2.version,t4.version,t5.version ,t6.version ,t7.version ,t8.version ,t9.version,t10.version ,t11.version) as version 
		,coalesce(t1.cversion,t2.cversion,t4.cversion,t5.cversion ,t6.cversion ,t7.cversion ,t8.cversion ,t9.cversion,t10.cversion ,t11.cversion) as cversion 
		,coalesce(t1.res_version,t2.res_version,t4.res_version,t5.res_version ,t6.res_version ,t7.res_version ,t8.res_version ,t9.res_version,t10.res_version ,t11.res_version) as res_version 
		,coalesce(t1.language_id,t2.language_id,t4.language_id,t5.language_id ,t6.language_id ,t7.language_id ,t8.language_id ,t9.language_id,t10.language_id ,t11.language_id) as language_id 
		,coalesce(t1.platform,t2.platform,t4.platform,t5.platform ,t6.platform ,t7.platform ,t8.platform ,t9.platform,t10.platform ,t11.platform) as platform 
		,coalesce(t1.os_version,t2.os_version,t4.os_version,t5.os_version ,t6.os_version ,t7.os_version ,t8.os_version ,t9.os_version,t10.os_version ,t11.os_version) as os_version 
		,coalesce(t1.device_id,t2.device_id,t4.device_id,t5.device_id ,t6.device_id ,t7.device_id ,t8.device_id ,t9.device_id,t10.device_id ,t11.device_id) as device_id 
		,coalesce(t1.ad_id,t2.ad_id,t4.ad_id,t5.ad_id ,t6.ad_id ,t7.ad_id ,t8.ad_id ,t9.ad_id,t10.ad_id ,t11.ad_id) as ad_id 
		,coalesce(t1.story_id,t2.story_id,t4.story_id,t5.story_id,t6.story_id,t7.story_id,t8.story_id ,t9.story_id,t10.story_id,t11.story_id) as story_id
		,coalesce(t1.chap_id,t2.chap_id,t4.chap_id,t5.chap_id,t6.chap_id,t7.chap_id,t8.chap_id ,t9.chap_id,t10.chap_id,t11.chap_id) as chap_id
		,coalesce(t1.chap_order_id,t2.chap_order_id,t4.chap_order_id,t5.chap_order_id,t6.chap_order_id,t7.chap_order_id,t8.chap_order_id ,t9.chap_order_id,t10.chap_order_id,t11.chap_order_id) as chap_order_id
		,coalesce(t1.t_book_id,t2.t_book_id,t4.t_book_id,t5.t_book_id,t6.t_book_id,t7.t_book_id,t9.t_book_id,t10.t_book_id,'') as t_book_id 
		,t1.is_free
		,coalesce(t1.chap_total_duration,0) as chap_total_duration
		,coalesce(t1.video_id,'') as video_id
		,coalesce(t1.play_duration,0) as play_duration
		,coalesce(t1.max_process,0) as max_process
		,coalesce(t1.cover_click_cnt ,0) as cover_click_cnt 
		,coalesce(if(t1.play_enter_cnt>0,t1.play_enter_cnt,t1.play_start_cnt)  ,0) as play_enter_cnt 
		,coalesce(t1.play_start_cnt  ,0) as play_start_cnt  
		,coalesce(t1.play_end_cnt  ,0) as play_end_cnt  
		,coalesce(t1.player_play_start_cnt ,0) as player_play_start_cnt
		,coalesce(t1.player_play_end_cnt ,0) as player_play_end_cnt             
		,coalesce(t1.play_start_begin_cnt ,0) as play_start_begin_cnt 
		,coalesce(t1.play_start_pause_off_cnt ,0) as play_start_pause_off_cnt 
		,coalesce(t1.play_end_complete_cnt ,0) as play_end_complete_cnt 
		,coalesce(t1.play_end_pause_on_cnt ,0) as play_end_pause_on_cnt 
		,t1.min_play_ctime
		,t1.max_play_ctime
		,t2.is_auto_unlock
		,coalesce(t2.coins_exp,0) as coins_exp
		-- ,coalesce(t3.res_dl_duration,0) as res_dl_duration
		-- ,coalesce(t3.res_dl_start_cnt,0) as res_dl_start_cnt
		-- ,coalesce(t3.res_dl_complete_cnt,0) as res_dl_complete_cnt
		,coalesce(t4.favorite_cnt,0) as favorite_cnt
		,coalesce(t4.cancel_favorite_cnt,0) as cancel_favorite_cnt
		,coalesce(t4.collect_cnt,0) as collect_cnt
		,coalesce(t4.cancel_collect_cnt,0) as cancel_collect_cnt
		,coalesce(t4.chap_click_cnt,0) as chap_click_cnt
		,coalesce(t5.order_cnt,0) as order_cnt
		,coalesce(t5.pay_amount,0) as pay_amount
		,coalesce(t6.sum_online_times,0) as sum_online_times
		,coalesce(t6.main_play_times,0) as main_play_times
		,coalesce(t6.chap_play_times,0) as chap_play_times
		,coalesce(t7.ad_invoke_cnt,0) as ad_invoke_cnt
		,coalesce(t7.ad_start_cnt,0) as ad_start_cnt
		,coalesce(t7.ad_complete_cnt,0) as ad_complete_cnt
		,coalesce(t7.ad_loadfail_cnt,0) as ad_loadfail_cnt
		,coalesce(t7.ad_playfail_cnt,0) as ad_playfail_cnt
		,coalesce(t7.ad_click_cnt,0) as ad_click_cnt
		,coalesce(t7.ad_impression_cnt,0) as ad_impression_cnt
		,coalesce(t7.ad_revenue,0) as ad_revenue
		,coalesce(t8.dlink_complete_cnt,0) as dlink_complete_cnt
		,coalesce(t9.coins_exp,0) as coins_exp_v2 
		,coalesce(t5.sub_order_cnt,0) as sub_order_cnt -- 订阅订单数 
		,coalesce(t5.sub_pay_amount,0) as sub_pay_amount -- 订阅订单金额 
		,coalesce(t11.is_banner_exposure,0) as is_banner_exposure -- 是否banner广告曝光 
		,coalesce(t10.is_discover_exposure,0) as is_discover_exposure -- 是否在大厅Discover主页曝光 
		,coalesce(t10.is_library_exposure,0) as is_library_exposure -- 是否在大厅Library主页(My List)曝光 
		,coalesce(t10.is_next_exposure,0) as is_next_exposure -- 是否在播放器-下一部曝光 
		,coalesce(t1.40001_play_duration,0) as 40001_play_duration
    from 
    (
		select
			min(id) as id
			,uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id
			,story_id
			,chap_id
			,chap_order_id
			,max(is_free) as is_free
			,max(chap_total_duration) as chap_total_duration
			,max(video_id) as video_id
			,cast(sum(play_duration) as int) as play_duration
			,max(max_process) as max_process
			,sum(cover_click_cnt) as cover_click_cnt 
			,sum(play_enter_cnt) as play_enter_cnt  
			,sum(play_start_cnt) as play_start_cnt  
			,sum(play_end_cnt) as play_end_cnt  
			,sum(play_start_begin_cnt) as play_start_begin_cnt 
			,sum(play_start_pause_off_cnt) as play_start_pause_off_cnt 
			,sum(play_end_complete_cnt) as play_end_complete_cnt 
			,sum(play_end_pause_on_cnt) as play_end_pause_on_cnt 
			,min(min_play_ctime) as min_play_ctime
			,max(max_play_ctime) as max_play_ctime
			,sum(player_play_start_cnt) as player_play_start_cnt
			,sum(player_play_end_cnt) as player_play_end_cnt
			,etl_date
			,t_book_id 
			,cast(sum(40001_play_duration) as int) as 40001_play_duration
		from
		(
			select 
				min(t1.id) as id 
				,uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id
				,story_id
				,chap_id
				,chap_order_id
				,max(is_free) as is_free
				,max(chap_total_duration) as chap_total_duration
				,video_id
				,cast(max(case 
						when sub_event_name='play_end' and event_duration<0 then 0
						when sub_event_name='play_end' and t2.id is not null and event_duration<500 then event_duration
						when sub_event_name='play_end' and t2.id is not null and event_duration>500 then chap_total_duration
						when sub_event_name='play_end' and t2.id is null and event_duration>chap_total_duration then chap_total_duration
						else event_duration end) as int) as play_duration
				,max(process) as max_process
				,count(if(sub_event_name='cover_click',t1.id)) as cover_click_cnt 
				,count(if(sub_event_name='play_enter',t1.id)) as play_enter_cnt  
				,count(if(sub_event_name='play_start',t1.id)) as play_start_cnt  
				,count(if(sub_event_name='play_end',t1.id)) as play_end_cnt  
				,count(if(sub_event_name='play_start' and type='begin',t1.id)) as play_start_begin_cnt 
				,count(if(sub_event_name='play_start' and type='pause_off',t1.id)) as play_start_pause_off_cnt 
				,count(if(sub_event_name='play_end' and type='complete',t1.id)) as play_end_complete_cnt 
				,count(if(sub_event_name='play_end' and type='pause_on',t1.id)) as play_end_pause_on_cnt 
				,min(if(sub_event_name='play_start',ctime)) as min_play_ctime
				,max(if(sub_event_name='play_start',ctime)) as max_play_ctime
				,count(if(sub_event_name='play_start' and page_name='player',t1.id)) as player_play_start_cnt
				,count(if(sub_event_name='play_end' and page_name='player',t1.id)) as player_play_end_cnt
				,etl_date
				,t_book_id 
				,cast(max(case 
						when sub_event_name='play_end' and shelf_id=40001 and event_duration<0 then 0
						when sub_event_name='play_end' and shelf_id=40001 and t2.id is not null and event_duration<500 then event_duration 
						when sub_event_name='play_end' and shelf_id=40001 and t2.id is not null and event_duration>500 then chap_total_duration
						when sub_event_name='play_end' and shelf_id=40001 and t2.id is null and event_duration>chap_total_duration then chap_total_duration
						when sub_event_name='play_end' and shelf_id=40001 and t2.id is null and event_duration<chap_total_duration then event_duration
						else 0 end) as int) as 40001_play_duration
	        from  dwd_data.dwd_t02_reelshort_play_event_di t1 
	        left join (select id from chapters_log.reelshort_book_info where analysis_date='${TX_DATE}' and book_type=2) t2 on t1.story_id=t2.id
	        where etl_date='${TX_DATE}'  and story_id not in ('','0')
	        group by uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id
				,story_id
				,chap_id
				,chap_order_id
				,t_book_id 
				,video_id -- 互动剧同一章节，会有多个video_id
		)
        group by uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id
			,story_id
			,chap_id
			,chap_order_id
			,t_book_id 
    )t1

    full join 
    (
        select 
              min(id) as id 
              ,uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id
              ,story_id
              ,chap_id
              ,chap_order_id
              ,max(is_auto_unlock) as is_auto_unlock
              ,sum(if(unlock_type=1,unlock_cost) ) as coins_exp
              ,etl_date
              ,t_book_id 
        from dwd_data.dwd_t04_reelshort_checkpoint_unlock_di
        where etl_date='${TX_DATE}' and story_id not in ('','0')
        group by  
        	  uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id
              ,story_id
              ,chap_id
              ,chap_order_id
              ,etl_date
              ,t_book_id 
	)t2 
	on t1.uuid=t2.uuid and t1.analysis_date=t2.analysis_date and t1.story_id=t2.story_id 
	and t1.chap_id=t2.chap_id and t1.chap_order_id=t2.chap_order_id
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
	and t1.t_book_id=t2.t_book_id

	-- full join 
	-- (
 --        select 
 --               min(id) as id 
 --               ,uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id
 --               ,story_id
 --               ,chap_id
 --               ,chap_order_id
 --               ,sum(event_duration) as res_dl_duration
 --               ,count(if(load_status='start',id)) as res_dl_start_cnt
 --               ,count(if(load_status='complete',id)) as res_dl_complete_cnt
 --               ,etl_date
 --               ,t_book_id
 --        from dwd_data.dwd_t02_reelshort_res_load_di
 --        where load_status in ('start','complete') and res_type in ('video') and etl_date='${TX_DATE}'
 --        and story_id not in ('','0')
 --        group by 
 --               uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id
 --               ,story_id
 --               ,chap_id
 --               ,chap_order_id
 --               ,etl_date
 --               ,t_book_id
	-- )t3 
	-- on t1.uuid=t3.uuid and t1.analysis_date=t3.analysis_date and t1.story_id=t3.story_id 
	-- and t1.chap_id=t3.chap_id   and t1.chap_order_id=t3.chap_order_id
	-- and t1.country_id=t3.country_id
	-- and t1.channel_id=t3.channel_id
	-- and t1.version=t3.version
	-- and t1.cversion=t3.cversion
	-- and t1.res_version=t3.res_version
	-- and t1.language_id=t3.language_id
	-- and t1.platform=t3.platform
	-- and t1.os_version=t3.os_version
	-- and t1.device_id=t3.device_id
	-- and t1.ad_id=t3.ad_id
	-- and t1.t_book_id=t3.t_book_id

	full join 
	(
           select 
               min(id) as id 
               ,uuid,analysis_date,etl_date,country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id
               ,story_id
               ,chap_id
               ,chap_order_id
               ,t_book_id
               ,count(if(sub_event_name='chap_favorite' and action_type='favorite',chap_id)) as favorite_cnt
               ,count(if(sub_event_name='chap_favorite' and action_type='cancel_favorite',chap_id)) as cancel_favorite_cnt
               ,count(if(sub_event_name='book_collect' and action_type='collect',chap_id)) as collect_cnt
               ,count(if(sub_event_name='book_collect' and action_type='cancel_collect',chap_id)) as cancel_collect_cnt
               ,count(if(sub_event_name='chap_list_click' and action_type='chap_click',chap_id)) as chap_click_cnt
           from dwd_data.dwd_t02_reelshort_user_action_book_di
           where sub_event_name in ('chap_favorite','book_collect','chap_list_click')  and etl_date='${TX_DATE}'
            and story_id not in ('','0')
           group by uuid,analysis_date,etl_date,country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id
               ,story_id
               ,chap_id
               ,chap_order_id
               ,t_book_id
	)t4 
	on t1.uuid=t4.uuid and t1.analysis_date=t4.analysis_date and t1.story_id=t4.story_id 
	and t1.chap_id=t4.chap_id   and t1.chap_order_id=t4.chap_order_id
	and t1.country_id=t4.country_id
	and t1.channel_id=t4.channel_id
	and t1.version=t4.version
	and t1.cversion=t4.cversion
	and t1.res_version=t4.res_version
	and t1.language_id=t4.language_id
	and t1.platform=t4.platform
	and t1.os_version=t4.os_version
	and t1.device_id=t4.device_id
	and t1.ad_id=t4.ad_id
	and t1.t_book_id=t4.t_book_id

    full join 
    (
        select 
        	  min(id) as id
        	  ,uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id
              ,story_id
              ,chap_id
              ,chap_order_id
              ,etl_date
              ,t_book_id 
              ,count(distinct order_id) as order_cnt
              ,sum(sku_price) as pay_amount
              ,count(distinct if(channel_sku like '%sub%',order_id)) as sub_order_cnt
              ,sum(if(channel_sku like '%sub%',sku_price,0)) as sub_pay_amount
        from dwd_data.dwd_t05_reelshort_order_detail_di
        where etl_date='${TX_DATE}'  and story_id not in ('','0')  
        and order_src<>'main_scene_shop' and order_status=1 and order_id_rank=1
        group by  
        	  uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id
              ,story_id
              ,chap_id
              ,chap_order_id
              ,etl_date
              ,t_book_id 
    )t5 
    on t1.uuid=t5.uuid and t1.analysis_date=t5.analysis_date and t1.story_id=t5.story_id 
    and t1.chap_id=t5.chap_id and t1.chap_order_id=t5.chap_order_id
    and t1.country_id=t5.country_id
    and t1.channel_id=t5.channel_id
    and t1.version=t5.version
    and t1.cversion=t5.cversion
    and t1.res_version=t5.res_version
    and t1.language_id=t5.language_id
    and t1.platform=t5.platform
    and t1.os_version=t5.os_version
    and t1.device_id=t5.device_id
    and t1.ad_id=t5.ad_id
    and t1.t_book_id=t5.t_book_id

    full join 
    (
        select 
        	  min(id) as id 
        	  ,uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id
              ,story_id
              ,chap_id
              ,chap_order_id
              ,etl_date
              ,t_book_id 
              ,sum(online_time ) as sum_online_times
              ,sum(if(scene_name='main_play_scene',online_time,0)) as main_play_times
              ,sum(if(scene_name='chap_play_scene',online_time,0)) as chap_play_times
        from dwd_data.dwd_t01_reelshort_user_online_time_di
        where etl_date='${TX_DATE}' and scene_name in ('main_play_scene','chap_play_scene')
         and story_id not in ('','0')
        group by  
        	  uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id
              ,story_id
              ,chap_id
              ,chap_order_id
              ,etl_date
              ,t_book_id 
    )t6 
    on t1.uuid=t6.uuid and t1.analysis_date=t6.analysis_date and t1.story_id=t6.story_id 
        and t1.chap_id=t6.chap_id and t1.chap_order_id=t6.chap_order_id
        and t1.country_id=t6.country_id
        and t1.channel_id=t6.channel_id
        and t1.version=t6.version
        and t1.cversion=t6.cversion
        and t1.res_version=t6.res_version
        and t1.language_id=t6.language_id
        and t1.platform=t6.platform
        and t1.os_version=t6.os_version
        and t1.device_id=t6.device_id
        and t1.ad_id=t6.ad_id
        and t1.t_book_id=t6.t_book_id

    full join 
    (
            select  
            	min(id) as id 
            	,uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id
               ,story_id
               ,chap_id
               ,chap_order_id
               ,etl_date
               ,t_book_id 
               ,count(case when action='invoke' then adunit_id end ) as ad_invoke_cnt
               ,count(case when action='start' then adunit_id  end ) as ad_start_cnt
               ,count(case when action='complete' then adunit_id  end ) as ad_complete_cnt
               ,count(case when action='loadfail' then adunit_id  end ) as ad_loadfail_cnt
               ,count(case when action='playfail' then adunit_id  end ) as ad_playfail_cnt
               ,count(case when action='click' then adunit_id  end ) as ad_click_cnt
               ,count(case when action='impression' then adunit_id  end ) as ad_impression_cnt
               ,sum(cast(ad_revenue as double)) as ad_revenue 
            from dwd_data.dwd_t07_reelshort_admoney_event_di
            where etl_date='${TX_DATE}'  and admoney_app_placeid in (20002,20001) 
             	and story_id not in ('','0')
            group by  
            		uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id
            		,story_id
            		,chap_id
            		,chap_order_id
            		,etl_date
            		,t_book_id 
    )t7
    on t1.uuid=t7.uuid and t1.analysis_date=t7.analysis_date and t1.story_id=t7.story_id 
        and t1.chap_id=t7.chap_id and t1.chap_order_id=t7.chap_order_id
        and t1.country_id=t7.country_id
        and t1.channel_id=t7.channel_id
        and t1.version=t7.version
        and t1.cversion=t7.cversion
        and t1.res_version=t7.res_version
        and t1.language_id=t7.language_id
        and t1.platform=t7.platform
        and t1.os_version=t7.os_version
        and t1.device_id=t7.device_id
        and t1.ad_id=t7.ad_id
        and t1.t_book_id=t7.t_book_id

    full join 
    (
            select  
            	min(id) as id 
            	,uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id
               ,story_id
               ,chap_id
               ,chap_order_id
               ,etl_date
               ,count(case when action='complete' then id  end ) as dlink_complete_cnt
            from dwd_data.dwd_t01_reelshort_dlink_event_di
            where etl_date='${TX_DATE}'   and story_id not in ('','0') and action='complete' 
            group by  
            	  uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id
                  ,story_id
                  ,chap_id
                  ,chap_order_id
                  ,etl_date
    )t8
    on t1.uuid=t8.uuid and t1.analysis_date=t8.analysis_date and t1.story_id=t8.story_id 
        and t1.chap_id=t8.chap_id and t1.chap_order_id=t8.chap_order_id
        and t1.country_id=t8.country_id
        and t1.channel_id=t8.channel_id
        and t1.version=t8.version
        and t1.cversion=t8.cversion
        and t1.res_version=t8.res_version
        and t1.language_id=t8.language_id
        and t1.platform=t8.platform
        and t1.os_version=t8.os_version
        and t1.device_id=t8.device_id
        and t1.ad_id=t8.ad_id

	full join 
	(
         select  
         		min(id) as id 
         		,uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id
               ,story_id
               ,chap_id
               ,chap_order_id
               ,etl_date
               ,t_book_id 
               ,sum(case when incre_type='exp' and vc_id='vc_01' then change_amount end ) as coins_exp
               ,sum(case when incre_type='get' and vc_id='vc_01' then change_amount  end ) as coins_get
               ,sum(case when incre_type='get' and vc_id='vc_02' then change_amount  end ) as bonus_get
            from dwd_data.dwd_t06_reelshort_user_currency_get_or_expend_di
            where etl_date='${TX_DATE}' and story_id not in ('','0')
            group by uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id
                  ,story_id
                  ,chap_id
                  ,chap_order_id
                  ,etl_date
                  ,t_book_id 
    )t9
    on t1.uuid=t9.uuid and t1.analysis_date=t9.analysis_date and t1.story_id=t9.story_id 
        and t1.chap_id=t9.chap_id and t1.chap_order_id=t9.chap_order_id
        and t1.country_id=t9.country_id
        and t1.channel_id=t9.channel_id
        and t1.version=t9.version
        and t1.cversion=t9.cversion
        and t1.res_version=t9.res_version
        and t1.language_id=t9.language_id
        and t1.platform=t9.platform
        and t1.os_version=t9.os_version
        and t1.device_id=t9.device_id
        and t1.ad_id=t9.ad_id
        and t1.t_book_id=t9.t_book_id

	full join 
	(
		select 
			min(id) as id 
			,uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id,etl_date
			,story_id
			,'' as chap_id
			,0 as chap_order_id
			,'' as t_book_id
			,cast(max(if(page_name='discover',1,0)) as int) as is_discover_exposure -- 是否在大厅Discover主页曝光
			,cast(max(if(page_name='library_main',1,0)) as int) as is_library_exposure -- 是否在大厅Library主页(My List)曝光
			,cast(max(if(page_name='player_next_story',1,0)) as int) as is_next_exposure -- 是否在播放器-下一部曝光
		from dwd_data.dwd_t02_reelshort_item_pv_di 
		where etl_date='${TX_DATE}' and story_id<>''
		group by 
			uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id
			,story_id
			-- ,chap_id
			-- ,chap_order_id
			-- ,t_book_id
	)t10
	on 	t1.uuid=t10.uuid 	
		and t1.story_id=t10.story_id 
		and t1.t_book_id=t10.t_book_id
		and t1.chap_id=t10.chap_id 
		and t1.chap_order_id=t10.chap_order_id
		and t1.analysis_date=t10.analysis_date  
		and t1.country_id=t10.country_id and t1.channel_id=t10.channel_id and t1.version=t10.version and t1.cversion=t10.cversion and t1.res_version=t10.res_version and t1.language_id=t10.language_id and t1.platform=t10.platform and t1.os_version=t10.os_version and t1.device_id=t10.device_id and t1.ad_id=t10.ad_id


	full join 
	(
		select 
			min(id) as id 
			,uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id,etl_date
			,book_id as story_id 
			-- ,'' as t_book_id
			,nvl(chap_id,'') as chap_id -- 均为空
			,nvl(chap_order_id,0) as chap_order_id -- 均为0
			,1 as is_banner_exposure -- 是否banner广告曝光
		from dwd_data.dwd_t01_reelshort_iad_track_di 
		where etl_date='${TX_DATE}' and book_id<>'' and action='show' and page_name in('discover','earn_rewards')
		group by 
			uuid,analysis_date,country_id,channel_id,version,cversion,res_version,language_id,platform,os_version,device_id,ad_id
			,book_id
			-- ,chap_id
			-- ,chap_order_id
			-- ,t_book_id
	)t11
	on 	t1.uuid=t11.uuid 	
		and t1.story_id=t11.story_id 
		-- and t1.t_book_id=t11.t_book_id
		and t1.chap_id=t11.chap_id 
		and t1.chap_order_id=t11.chap_order_id
		and t1.analysis_date=t11.analysis_date  
		and t1.country_id=t11.country_id and t1.channel_id=t11.channel_id and t1.version=t11.version and t1.cversion=t11.cversion and t1.res_version=t11.res_version and t1.language_id=t11.language_id and t1.platform=t11.platform and t1.os_version=t11.os_version and t1.device_id=t11.device_id and t1.ad_id=t11.ad_id
) t1 

left join  
(
   select uuid
        ,etl_date 
        ,is_pay
        ,is_login
        ,user_type
        ,af_network_name
        ,af_channel
        ,af_campaign
        ,af_adset
        ,af_ad
        ,vip_type 
   from dwd_data.dwd_t01_reelshort_user_detail_info_di
   where etl_date='${TX_DATE}' 
) t2 
on t1.uuid=t2.uuid
group by t1.uuid 
      ,t1.analysis_date
      ,t1.etl_date
      ,coalesce(t2.is_pay,0)
      ,coalesce(t2.is_login,0)
      ,coalesce(t2.user_type,0)
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
      ,t1.story_id
      ,t1.chap_id
      ,t1.chap_order_id
      ,coalesce(t2.af_network_name,'')
      ,coalesce(t2.af_channel,'')
      ,coalesce(t2.vip_type,0)
;

