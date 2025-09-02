select
       {{group_date_field}}{{group_fields}}
       {{target}}
       count(1)over() as total_cnt
       -- ,cast(nvl(max(if(register_days=1,trans_start_uv)),0) as decimal(18,4))      as trans_start_retention_uv_1d      -- 次留发起支付用户
       -- ,cast(nvl(max(if(register_days=7,trans_start_uv)),0) as decimal(18,4))      as trans_start_retention_uv_7d      -- 第7留发起支付用户
       -- ,cast(nvl(max(if(register_days=14,trans_start_uv)),0) as decimal(18,4))     as trans_start_retention_uv_14d     -- 第14留发起支付用户
       -- ,cast(nvl(max(if(register_days=30,trans_start_uv)),0) as decimal(18,4))     as trans_start_retention_uv_30d     -- 第30留发起支付用户
       -- ,cast(nvl(max(if(register_days=1,trans_complete_uv)),0) as decimal(18,4))   as trans_complete_retention_uv_1d   -- 次留付费用户
       -- ,cast(nvl(max(if(register_days=7,trans_complete_uv)),0) as decimal(18,4))   as trans_complete_retention_uv_7d   -- 第7留付费用户
       -- ,cast(nvl(max(if(register_days=14,trans_complete_uv)),0) as decimal(18,4))  as trans_complete_retention_uv_14d  -- 第14留付费用户
       -- ,cast(nvl(max(if(register_days=30,trans_complete_uv)),0) as decimal(18,4))  as trans_complete_retention_uv_30d  -- 第30留付费用户
       -- ,cast(nvl(max(if(register_days=1,trans_complete_amt)),0) as decimal(18,4))  as trans_complete_retention_amt_1d  -- 次留付费金额
       -- ,cast(nvl(max(if(register_days=7,trans_complete_amt)),0) as decimal(18,4))  as trans_complete_retention_amt_7d  -- 第7留付费金额
       -- ,cast(nvl(max(if(register_days=14,trans_complete_amt)),0) as decimal(18,4)) as trans_complete_retention_amt_14d -- 第14留付费金额
       -- ,cast(nvl(max(if(register_days=30,trans_complete_amt)),0) as decimal(18,4)) as trans_complete_retention_amt_30d -- 第30留付费金额
       -- ,cast(nvl(max(if(register_days=0,trans_start_amt)),0) as decimal(18,4))     as trans_start_amt                  -- 发起支付金额
from
(
       select
              t1.analysis_date,{{group_fields}}
              datediff(t2.analysis_date,t1.analysis_date) as register_days,
              count(distinct if(t2.trans_start_cnt>0,t2.uuid)) as trans_start_uv,
              count(distinct if(t2.trans_complete_cnt>0,t2.uuid)) as trans_complete_uv,
              sum(if(t2.trans_complete_cnt>0,t2.trans_complete_amt,0)) as trans_complete_amt,
              sum(if(t1.trans_start_cnt>0,t1.trans_start_amt)) as trans_start_amt
       from
       (
              select
                     uuid,
                     sum(trans_start_cnt)    as trans_start_cnt,
                     sum(trans_start_amt)    as trans_start_amt,
                     analysis_date,{{group_fields}}
                     concat_ws(';',{{group_fields_nopvu}}';') as field_ids
              from dw_view.dws_t80_reelshort_user_bhv_cnt_detail_di_v2 t1
              {{join_oth_dim}}
              where etl_date between '{{start_date}}' and '{{end_date}}'
                     {{where_and}}
              group by {{group_fields}}analysis_date,uuid
       )t1
       join
       (
              select
                     uuid,
                     analysis_date,
                     sum(trans_start_cnt)      as trans_start_cnt,
                     sum(trans_complete_cnt)   as  trans_complete_cnt,
                     sum(pay_amount/100)   as trans_complete_amt,
                     concat_ws(';',{{group_fields_nopvu}}';') as field_ids
              from dw_view.dws_t80_reelshort_user_bhv_cnt_detail_di_v2  t1
              {{join_oth_dim}}
              where etl_date between '{{start_date}}' and date_add('{{end_date}}',30)
                    -- and (t1.trans_start_cnt>0 or t1.trans_complete_cnt>0)
                     {{where_and_nopv}}
              group by {{group_fields_nopv}}analysis_date,uuid
       )t2
       on t1.uuid=t2.uuid and t1.field_ids=t2.field_ids
       where t2.analysis_date>=t1.analysis_date
       group by {{group_fields}}t1.analysis_date,datediff(t2.analysis_date,t1.analysis_date)
)
where register_days in(0,1,7,14,30)
group by {{group_fields}}analysis_date
{{order_by}}
{{limit}}