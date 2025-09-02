/*+cte_execution_mode=shared*/
SELECT
--       {{analysis_date}}
       {{group_date_field}}
       {{group_fields}}
       {{target}}
       count(1)over() as total_cnt
       -- ,RB_OR_CARDINALITY_AGG(rb_build_varbinary(open_authority_uv))          as open_authority_uv                        -- 权限开启UV
       -- ,RB_OR_CARDINALITY_AGG(rb_build_varbinary(new_open_authority_uv))      as new_open_authority_uv                    -- 新开启权限UV
       -- ,RB_OR_CARDINALITY_AGG(rb_build_varbinary(close_authority_uv))         as close_authority_uv                       -- 关闭权限UV
       -- ,RB_OR_CARDINALITY_AGG(rb_build_varbinary(fist_open_authority_uv))     as fist_open_authority_uv                   -- 首次启动开启权限UV
       -- ,RB_OR_CARDINALITY_AGG(rb_build_varbinary(not_fist_open_authority_uv)) as not_fist_open_authority_uv               -- 非首次启动开启权限UV
FROM dws_data.dws_t88_reelshort_opc_push_authority_aggregate_di t1
{{join_oth_dim}}
where 1=1 and analysis_date between  '{{start_date}}' and '{{end_date}}'
       {{where_and}}
-- group by {{analysis_date}}  {{fields}}
{{group_by}}
{{order_by}}
{{limit}}
;