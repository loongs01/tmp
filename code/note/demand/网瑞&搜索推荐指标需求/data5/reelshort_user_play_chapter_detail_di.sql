SELECT
       {{group_date_field}}
       {{group_fields}}
       {{target}}
       count(1)over() as total_cnt
       -- ,sum(trans_start_amt) as trans_start_amt -- 发起支付金额
FROM dws_data.dws_t82_reelshort_user_play_chapter_detail_di t1
{{join_oth_dim}}
where 1=1 and analysis_date between  '{{start_date}}' and '{{end_date}}'
       {{where_and}}
{{group_by}}
{{order_by}}
{{limit}}
;