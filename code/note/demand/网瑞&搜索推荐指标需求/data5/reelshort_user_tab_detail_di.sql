SELECT
       {{group_date_field}}
       {{group_fields}}
       {{target}}
       count(1)over() as total_cnt
       -- ,sum(tab_cnt)         as tab_cnt  -- 频道页曝光次数
       -- ,count(distinct uuid) as tab_uv   -- 频道页曝光用户数
FROM dwd_data.dwd_t80_reelshort_user_tab_detail_di t1
{{join_oth_dim}}
where 1=1 and analysis_date between  '{{start_date}}' and '{{end_date}}'
       {{where_and}}
{{group_by}}
{{order_by}}
{{limit}}
;