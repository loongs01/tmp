select  
       {{group_date_field}}{{group_fields}}
       {{target}}
      -- cast(nvl(max(if(register_days=1,retention_uv)) / max(if(register_days=0,retention_uv),0.0) as decimal(10,4)) as act_retention_ratio_1d
      -- cast(nvl(max(if(register_days=7,retention_uv)) / max(if(register_days=0,retention_uv),0.0) as decimal(10,4)) as act_retention_ratio_7d
      -- cast(nvl(max(if(register_days=30,retention_uv)) / max(if(register_days=0,retention_uv),0.0) as decimal(10,4)) as act_retention_ratio_30d
       count(1)over() as total_cnt
from  
( 
       select 
              t1.analysis_date,{{group_fields}}
              datediff(t2.analysis_date,t1.analysis_date) as register_days,
              count(distinct t1.uuid) as retention_uv
       from 
       (
              select 
                     uuid,
                     analysis_date,{{group_fields}}
                     concat_ws(';',{{group_fields_nopv}}';') as field_ids
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
                     concat_ws(';',{{group_fields_nopv}}';') as field_ids
              from dw_view.dws_t80_reelshort_user_bhv_cnt_detail_di_v2  t1
              {{join_oth_dim}}
              where etl_date between '{{start_date}}' and date_add('{{end_date}}',30)
                     {{where_and_nopv}}
              group by {{group_fields_nopv}}analysis_date,uuid
       )t2
       on t1.uuid=t2.uuid and t1.field_ids=t2.field_ids
       where t2.analysis_date>=t1.analysis_date 
       group by {{group_fields}}t1.analysis_date,datediff(t2.analysis_date,t1.analysis_date)
)
where register_days in(0,1,7,30)
group by {{group_fields}}analysis_date
{{order_by}}
{{limit}}