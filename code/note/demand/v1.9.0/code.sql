select 
story_id
, unlockk_type
, count(uuid, story_id, chap_id) as '剧集解锁pv'
, count(distinct uuid) as '剧集解锁uv' 
from dwd_data.dwd_t04_reelshort_checkpoint_unlock_di
where etl_date between '2025-03-31' and '2025-03-31'
group by story_id, unlockk_type;


select 
story_id
, count(uuid, chap_id) as '剧集广告解锁完成pv'
, count(distinct uuid) as '剧集广告解锁完成uv' 
from dwd_data.dwd_t04_reelshort_checkpoint_unlock_di
where unlock_type='3'
        and etl_date between '2025-03-31' and '2025-03-31'
group by story_id;

------------

select sku
, position
, count(distinct uuid, ctime) as '商品曝光pv'
, count(distinct uuid) as '商品曝光uv' from 
(
        select distinct analysis_date, uuid, ctime, if(position is null or position='', page_name, position) as 'position', substring_index(replace(item, '"', ''), '#', -1) as 'sku' from 
                                        ( (select distinct analysis_date, uuid, page_name, ctime, cast(json_extract(properties, "$.position") as string) as 'position', split(replace(replace(cast(json_extract(properties, "$.item_list") as string), "[", ""), "]", ""),',') as numbers_array  
                                                                        from dwd_data.dwd_t02_reelshort_custom_event_di 
                                                                        where etl_date between '2025-03-31' and '2025-03-31' and page_name not in ('earn_rewards', 'discover', 'for_you')
                                                                        and sub_event_name='page_item_impression')
                                                                        ) 
                                        cross join unnest(numbers_array) as temp_table(item)
union all 
select distinct analysis_date, uuid, ctime, '付费弹窗' as 'position', channel_sku from dwd_data.dwd_t02_reelshort_custom_event_di 
        where etl_date between '2025-03-31' and '2025-03-31' and sub_event_name in ('exclusive_gift_popup', 'pay_popup') and action in ("page_show", "show")
)
where sku not in ('(null)', '') and sku is not null
group by sku, position;


select 
story_id
, count(distinct uuid, ctime) as '剧集广告解锁点击pv'
, count(distinct uuid) as '剧集广告解锁点击uv' 
from dwd_data.dwd_t02_reelshort_custom_event_di
where sub_event_name in ("unlock_panel_click", "unlock_panel_v2_click", "iaa_sub_unlock_panel_click")
                        and action in ("ad_unlock_click", "ad_click")
                        and etl_date between '2025-03-31' and '2025-03-31'
group by story_id;

select 
story_id
, count(distinct uuid, ctime) as '剧集金币解锁点击pv'
, count(distinct uuid) as '剧集金币解锁点击uv' 
from dwd_data.dwd_t02_reelshort_custom_event_di
where sub_event_name in ("unlock_panel_click", "unlock_panel_v2_click", "batch_unlock_panel_v2", "coins_enough_unlock_panel_click", "sub_unlock_panel_click")
                        and action in ("unlock_click", "unlock_now", "batch_unlock_click", "click")
                        and etl_date between '2025-03-31' and '2025-03-31'
group by story_id;

select 
story_id
, count(distinct uuid, ctime) as '互动剧选项卡点击pv'
, count(distinct uuid) as '互动剧选项卡点击uv' 
from dwd_data.dwd_t02_reelshort_custom_event_di
where sub_event_name='option_clip_page_click'
                and action='click_option'
                and etl_date between '2025-03-31' and '2025-03-31'
group by story_id;