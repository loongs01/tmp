drop Table if  exists dwd_data.dim_t80_reelshort_user_recall_level_di;
Create Table if not exists dwd_data.dim_t80_reelshort_user_recall_level_di(
id                  bigint AUTO_INCREMENT    comment '自增id'
,analysis_date      date                     comment '分析日期'
,uuid               varchar      comment '用户ID'
,recall_level       varchar      comment '召回路'
,PRIMARY KEY (id,analysis_date,uuid)
)DISTRIBUTE BY HASH(analysis_date,id) PARTITION BY VALUE(DATE_FORMAT(analysis_date, '%Y%m')) LIFECYCLE 120 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='reelshort 用户召回路维度统计表';

-- 召回路
delete from dwd_data.dim_t80_reelshort_user_recall_level_di where analysis_date='${TX_DATE}';
insert into dwd_data.dim_t80_reelshort_user_recall_level_di
select distinct
      null as id
      ,etl_date as analysis_date
      ,uuid
      ,recall_level        -- 召回路
from (select distinct
             etl_date
             ,uuid
             ,recall_level -- 召回路
      FROM dwd_data.dwd_t02_reelshort_play_event_di as t
      where etl_date='${TX_DATE}'
            and event_name='m_play_event'
            -- and recall_level!=''
      union all
      select distinct
             etl_date
             ,uuid
             ,cast(nvl(json_extract(report_info,'$.recall_level'),'') as varchar) as recall_level
      from dwd_data.dwd_t02_reelshort_item_pv_di
      where etl_date='${TX_DATE}'
            and event_name='m_item_pv'
            -- and cast(nvl(json_extract(report_info,'$.recall_level'),'') as varchar)!=''
)
;