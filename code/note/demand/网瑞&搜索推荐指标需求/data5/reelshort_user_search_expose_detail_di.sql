SELECT
       {{group_date_field}}
       {{group_fields}}
       {{target}}
       count(1)over() as total_cnt
       -- ,count(distinct t1.uuid) as search_expose_uv -- 搜索入口曝光用户数
FROM dwd_data.dwd_t80_reelshort_user_search_expose_detail_di t1
{{join_oth_dim}}
where 1=1 and analysis_date between  '{{start_date}}' and '{{end_date}}'
       {{where_and}}
{{group_by}}
{{order_by}}
{{limit}}
;