select
       {{group_date_field}}{{group_fields}}
       {{target}}
       -- sum(exposure_cnt) as exposure_cnt
       -- ,RB_OR_CARDINALITY_AGG(rb_build_varbinary(exposure_uv)) as exposure_uv
       count(1)over() as total_cnt
from dws_data.dws_t82_reelshort_exposure_data5_bitmap_di   t1 {{join_oth_dim}}
where analysis_date between '{{start_date}}' and '{{end_date}}'
       {{where_and}}
{{group_by}}
{{order_by}}
{{limit}}
