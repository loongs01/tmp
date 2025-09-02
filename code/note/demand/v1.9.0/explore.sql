数据来源：业务服同步（网端、app端），客户端上报（网端、app端）

-- 上游表
select *
from check_data.metadata_table_relation
where insert_table='dwd_data.dwd_t01_reelshort_user_detail_info_di'
;

-- 下游表
select *
from     check_data.metadata_table_relation_total
where check_table='dwd_data.dwd_t02_reelshort_play_event_di'
and cur_table='dwd_data.dwd_t02_reelshort_play_event_di'
;

--------------
