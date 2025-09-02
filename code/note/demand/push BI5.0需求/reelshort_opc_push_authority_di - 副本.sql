/*+cte_execution_mode=shared*/
SELECT
--       {{analysis_date}}
       {{group_date_field}}
       {{group_fields}}
       {{target}}
       count(1)over() as total_cnt
       -- ,count(distinct if(t1.fcm_push=1,t1.uuid))                   as open_authority_uv                                       --权限开启UV
       -- ,count(distinct if(t1.is_new_open_authority=1,t1.uuid))      as new_open_authority_uv                      -- 新开启权限UV
       -- ,count(distinct if(t1.is_close_authority=1,t1.uuid))         as close_authority_uv                            -- 关闭权限UV
       -- ,count(distinct if(t1.is_fist_open_authority=1,t1.uuid))     as fist_open_authority_uv                    -- 首次启动开启权限UV
       -- ,count(distinct if(t1.is_not_fist_open_authority=1,t1.uuid)) as not_fist_open_authority_uv            -- 非首次启动开启权限UV
FROM dwd_data.dwd_t04_reelshort_opc_push_authority_di t1
{{join_oth_dim}}
where 1=1 and analysis_date between  '{{start_date}}' and '{{end_date}}'
       {{where_and}}
-- group by {{analysis_date}}  {{fields}}
{{group_by}}
{{order_by}}
{{limit}}
;