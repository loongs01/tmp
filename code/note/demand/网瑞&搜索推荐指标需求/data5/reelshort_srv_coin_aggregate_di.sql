SELECT
       {{group_date_field}}
       {{group_fields}}
       {{target}}
       count(1)over() as total_cnt
       -- ,sum(if(coin_expend>0,coin_expend,0)) as expend_coin_cnt -- 总消耗虚拟币个数
       -- ,count(distinct if(coin_expend>0,uuid)) as expend_coin_uv -- 总消耗虚拟币用户
FROM dws_data.dws_t85_reelshort_srv_currency_change_statistic_di t1
{{join_oth_dim}}
where 1=1 and analysis_date between  '{{start_date}}' and '{{end_date}}'
       {{where_and}}
{{group_by}}
{{order_by}}
{{limit}}
;