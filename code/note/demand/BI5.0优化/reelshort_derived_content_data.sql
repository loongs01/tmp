select
       {{group_date_field}}{{group_fields}}
       {{target}}
       -- RB_OR_CARDINALITY_AGG(rb_build_varbinary(cover_click_uv))    as cover_click_uv
       -- sum(cover_click_cnt)                                         as cover_click_cnt
       -- RB_OR_CARDINALITY_AGG(rb_build_varbinary(show_uv))           as show_uv
       -- sum(show_cnt)                                                as show_cnt
       -- RB_OR_CARDINALITY_AGG(rb_build_varbinary(click_uv))          as click_uv
       -- cast(sum(click_cnt) as int)                                  as click_cnt
       -- sum(play_duration)                                           as play_duration
       -- RB_OR_CARDINALITY_AGG(rb_build_varbinary(play_uv))           as play_uv
       -- sum(play_cnt)                                                as play_cnt
       -- RB_OR_CARDINALITY_AGG(rb_build_varbinary(play_storys))       as play_storys   -- delete
       -- RB_OR_CARDINALITY_AGG(rb_build_varbinary(play_chaps))        as play_chaps    -- delete
       -- RB_OR_CARDINALITY_AGG(rb_build_varbinary(play_complete_uv))  as play_complete_uv
       -- sum(play_complete_cnt)                                       as play_complete_cnt
       count(1)over() as total_cnt
from dws_data.dws_t82_reelshort_play_data5_bitmap_di  t1 {{join_oth_dim}}
where analysis_date between '{{start_date}}' and '{{end_date}}'
       {{where_and}}
{{group_by}}
{{order_by}}
{{limit}}