-- test  58s ,1m1s,47s
select
      etl_date
      ,user_type
      ,country_id
      ,channel_id
      ,version
      -- ,cversion
      -- ,res_version
      ,language_id
      ,platform
      ,temp_table.col
,cast(sum(if(sub_event_name='play_end',play_duration/60,0)) as decimal(10,2)) as play_duration
,count(distinct uuid) as play_uv
,count(distinct if(sub_event_name='play_start',concat(story_id,uuid))) as play_storys 
,count(distinct if(sub_event_name='play_start',concat(story_id,chap_order_id,uuid))) as play_chaps 
,count(distinct if(play_complete_cnt>0,uuid)) as play_complete_uv 
,sum(play_complete_cnt) as play_complete_cnt 
from  dws_data.dws_t82_reelshort_20250305 as t  -- sub_page_ids -- 频道页array
CROSS JOIN UNNEST(sub_page_ids) as temp_table(col)
where etl_date='${TX_DATE}'
      and temp_table.col!=''
group by 
      etl_date
      ,user_type
      ,country_id
      ,channel_id
      ,version
      -- ,cversion
      -- ,res_version
      ,language_id
      ,platform
      ,temp_table.col
    


-- 17s,13s ,14s
select
      etl_date
      ,user_type
      ,country_id
      ,channel_id
      ,version
      -- ,cversion
      -- ,res_version
      ,language_id
      ,platform
,cast(sum(if(sub_event_name='play_end',play_duration/60,0)) as decimal(10,2)) as play_duration
,count(distinct uuid) as play_uv
,count(distinct if(sub_event_name='play_start',concat(story_id,uuid))) as play_storys 
,count(distinct if(sub_event_name='play_start',concat(story_id,chap_order_id,uuid))) as play_chaps 
,count(distinct if(play_complete_cnt>0,uuid)) as play_complete_uv 
,sum(play_complete_cnt) as play_complete_cnt 
from  dws_data.dws_t82_reelshort_user_play_data5_detail_di as t  -- sub_page_ids -- 频道页array
-- CROSS JOIN UNNEST(sub_page_ids) as temp_table(col)
where etl_date='${TX_DATE}'
--       and temp_table.col!=''
group by 
      etl_date
      ,user_type
      ,country_id
      ,channel_id
      ,version
      -- ,cversion
      -- ,res_version
      ,language_id
      ,platform
    


-- 14s 17s

select
      etl_date
      ,user_type
      ,country_id
      ,channel_id
      ,version
      -- ,cversion
      -- ,res_version
      ,language_id
      ,platform
      ,tab_id
,cast(sum(if(sub_event_name='play_end',play_duration/60,0)) as decimal(10,2)) as play_duration
,count(distinct uuid) as play_uv
,count(distinct if(sub_event_name='play_start',concat(story_id,uuid))) as play_storys 
,count(distinct if(sub_event_name='play_start',concat(story_id,chap_order_id,uuid))) as play_chaps 
,count(distinct if(play_complete_cnt>0,uuid)) as play_complete_uv 
,sum(play_complete_cnt) as play_complete_cnt 
from  dws_data.dws_t82_reelshort_user_play_data5_detail_di as t  -- sub_page_ids -- 频道页array
left join (select uuid as uuid1,tab_id         -- 增加一对多维度关联  数据日增量4118358
from dwd_data.dim_t80_reelshort_user_tab_id_di 
where analysis_date='${TX_DATE}'
      and tab_id!=''
) as t1
on t1.uuid1=t.uuid
where etl_date='${TX_DATE}'
group by 
      etl_date
      ,user_type
      ,country_id
      ,channel_id
      ,version
      -- ,cversion
      -- ,res_version
      ,language_id
      ,platform
      ,tab_id