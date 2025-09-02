SELECT
       {{group_date_field}}
       {{group_fields}}
       {{target}}
       count(1)over() as total_cnt
       -- ,count(distinct uuid) as chapter_unlock_uv                            -- 剧集解锁用户数
	   ,count(uuid, story_id, chap_id) as chapter_unlock_pv                     -- '剧集解锁pv'
	   ,count(distinct if(unlock_type='3',uuid)) as chapter_ad_unlock_uv        -- '剧集广告解锁完成uv'
	   ,count(if(unlock_type='3',concat(uuid,chap_id))) as chapter_ad_unlock_pv -- '剧集广告解锁完成pv'
FROM dwd_data.dwd_t80_reelshort_user_chapter_unlock_di t1
{{join_oth_dim}}
where 1=1 and analysis_date between  '{{start_date}}' and '{{end_date}}'
       {{where_and}}
{{group_by}}
{{order_by}}
{{limit}}
;