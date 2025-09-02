select
       {{group_date_field}}{{group_fields}}
       {{target}}
       count(1)over() as total_cnt
       -- ,cast(nvl(max(if(register_days=1,retention_uv)) / max(if(register_days=0,retention_uv)),0.0) as decimal(18,4)) as shelf_expose_retention_uv_1d -- 书架次留
       -- ,cast(nvl(max(if(register_days=3,retention_uv)) / max(if(register_days=0,retention_uv)),0.0) as decimal(18,4)) as shelf_expose_retention_uv_3d -- 书架3留
       -- ,cast(nvl(max(if(register_days=7,retention_uv)) / max(if(register_days=0,retention_uv)),0.0) as decimal(18,4)) as shelf_expose_retention_uv_7d -- 书架7留
from
(
       select
              t1.analysis_date,{{group_fields}}
              datediff(t2.analysis_date,t1.analysis_date) as register_days,
              count(t1.uuid) as retention_uv
       from
       (
              select
                     uuid,
                     analysis_date,{{group_fields}}
                     concat_ws(';',{{group_fields_nopvu}}';') as field_ids
              from dwd_data.dwd_t80_reelshort_user_shelf_expose_detail_di t1
              {{join_oth_dim}}
              where analysis_date between '{{start_date}}' and '{{end_date}}'
                    -- and shelf_id!=''
                     {{where_and}}
              group by {{group_fields}}analysis_date,uuid
       )t1
       join
       (
              select
                     uuid,
                     analysis_date,
                     concat_ws(';',{{group_fields_nopvu}}';') as field_ids
              from dwd_data.dwd_t80_reelshort_user_shelf_expose_detail_di  t1
              {{join_oth_dim}}
              where analysis_date between '{{start_date}}' and date_add('{{end_date}}',7)
                    -- and shelf_id!=''
                     {{where_and_nopv}}
              group by {{group_fields_nopv}}analysis_date,uuid
       )t2
       on t1.uuid=t2.uuid and t1.field_ids=t2.field_ids
       where t2.analysis_date>=t1.analysis_date
       group by {{group_fields}}t1.analysis_date,datediff(t2.analysis_date,t1.analysis_date)
)
where register_days in(0,1,3,7)
group by {{group_fields}}analysis_date
{{order_by}}
{{limit}}