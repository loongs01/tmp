-- drop Table if exists dim_data.dim_t99_data5_tag_aggregate_configs_bak;
-- CREATE TABLE if not exists dim_data.dim_t99_data5_tag_aggregate_configs_bak (
  -- `id` bigint AUTO_INCREMENT COMMENT '自增',
  -- `app_id` varchar COMMENT '项目id:cm1001-chapters,cm1003-kiss,cm1009-reelshort',
  -- `name` varchar(255) NOT NULL DEFAULT '' COMMENT '指标名称',
  -- `key` varchar(255) NOT NULL DEFAULT '' COMMENT '指标key',
  -- `value` array<varchar> NOT NULL DEFAULT '[]' COMMENT '特殊维度枚举值',
  -- `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  -- `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
  -- PRIMARY KEY (`id`,key)
-- ) DISTRIBUTE BY HASH(`id`) INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='mapleBi:特殊维度码值汇总表备份';




-- delete from dim_data.dim_t99_data5_tag_aggregate_configs_bak where key='recall_level';
replace into dim_data.dim_t99_data5_tag_aggregate_configs_bak
select
      1 as id
      ,'cm1009' as app_id
      ,'召回路' as name
      ,'recall_level' as key
      ,if(t.recall_level is null or t1.recall_level is null ,COALESCE(t1.recall_level,t.recall_level),array_union(t.recall_level,t1.recall_level))  as value
      ,now() as created_at
      ,now() as updated_at
from (select 1 as key -- 1：历史全量，2：日增量
             ,value as recall_level
--              ,split(group_concat(distinct recall_level SEPARATOR ','),',') as recall_level        -- 召回路
             -- ,1 as id
      from dim_data.dim_t99_data5_tag_aggregate_configs_bak  -- 历史数据
      where key='recall_level'
) as t
right join(select 1 as key
                 ,split(group_concat(distinct recall_level SEPARATOR ','),',') as recall_level        -- 召回路
          from dwd_data.dim_t80_reelshort_user_recall_level_di
          where analysis_date=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)
) as t1
on t1.key=t.key
;
