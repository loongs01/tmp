-- drop Table if  exists dws_data.dws_t88_reelshort_opc_push_authority_aggregate_di;
-- Create Table if not exists dws_data.dws_t88_reelshort_opc_push_authority_aggregate_di (
-- id                               bigint AUTO_INCREMENT    comment '自增id'
-- ,analysis_date                   date                     comment '分析日期'
-- ,platform                        varchar                  comment '分发平台'
-- ,channel_id                      varchar                  comment '分发渠道id'
-- ,country_id                      varchar                  comment '国家id'
-- ,language_id                     varchar                  comment '语言id'
-- ,version                         varchar                  comment 'APP 的应用版本（包体内置版本号 versionname  ）'
-- ,user_type                       int                      comment '新老用户标记（新用户=1 老用户=2）'
-- ,open_authority_uv               varbinary                comment '权限开启UV'
-- ,new_open_authority_uv           varbinary                comment '新开启权限UV'
-- ,close_authority_uv              varbinary                comment '关闭权限UV'
-- ,fist_open_authority_uv          varbinary                comment '首次启动开启权限UV'
-- ,not_fist_open_authority_uv      varbinary                comment '非首次启动开启权限UV'
-- ,PRIMARY KEY (analysis_date,id)
-- )DISTRIBUTE BY HASH(analysis_date) PARTITION BY VALUE(DATE_FORMAT(analysis_date,'%Y%m')) LIFECYCLE 120 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='reelshort Push流转数据';
-- ;

delete from dws_data.dws_t88_reelshort_opc_push_authority_aggregate_di where analysis_date='${TX_DATE}';
/*+ cte_execution_mode=shared */
insert into dws_data.dws_t88_reelshort_opc_push_authority_aggregate_di
select
      null as id -- 自增id
      ,analysis_date     -- 分析日期'
      ,platform         -- 分发平台'
      ,channel_id       -- 分发渠道id'
      ,country_id       -- 国家id'
      ,language_id      -- 语言id'
      ,version          -- APP 的应用版本（包体内置版本号 versionname  ）'
      ,user_type        -- 新老用户标记（新用户=1 老用户=2）'
      ,rb_to_varbinary(rb_build(cast(split(GROUP_CONCAT(distinct if(t1.fcm_push=1,t1.uuid) SEPARATOR '#' ),'#') as array(int))))                    as open_authority_uv                 -- 权限开启UV
      ,rb_to_varbinary(rb_build(cast(split(GROUP_CONCAT(distinct if(t1.is_new_open_authority=1,t1.uuid) SEPARATOR '#' ),'#') as array(int))))       as new_open_authority_uv             -- 新开启权限UV
      ,rb_to_varbinary(rb_build(cast(split(GROUP_CONCAT(distinct if(t1.is_close_authority=1,t1.uuid) SEPARATOR '#' ),'#') as array(int))))          as close_authority_uv                -- 关闭权限UV
      ,rb_to_varbinary(rb_build(cast(split(GROUP_CONCAT(distinct if(t1.is_fist_open_authority=1,t1.uuid) SEPARATOR '#' ),'#') as array(int))))      as fist_open_authority_uv            -- 首次启动开启权限UV
      ,rb_to_varbinary(rb_build(cast(split(GROUP_CONCAT(distinct if(t1.is_not_fist_open_authority=1,t1.uuid) SEPARATOR '#' ),'#') as array(int))))  as not_fist_open_authority_uv        -- 非首次启动开启权限UV
from dwd_data.dwd_t04_reelshort_opc_push_authority_di as t1
where analysis_date='${TX_DATE}'
      and t1.uuid regexp '^[0-9]+$'
      -- and t1.uuid<2147483647
group by
        analysis_date
        ,platform
        ,channel_id
        ,country_id
        ,language_id
        ,version
        ,user_type
;