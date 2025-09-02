-- drop Table if  exists dwd_data.dim_t80_reelshort_user_tab_id_di;
-- Create Table if not exists dwd_data.dim_t80_reelshort_user_tab_id_di(
-- id                  bigint AUTO_INCREMENT    comment '自增id'
-- ,analysis_date      date                     comment '分析日期'
-- ,uuid               varchar      comment '用户ID'
-- ,tab_id             varchar      comment '频道页'
-- ,PRIMARY KEY (id,analysis_date,uuid)
-- )DISTRIBUTE BY HASH(analysis_date,id) PARTITION BY VALUE(DATE_FORMAT(analysis_date, '%Y%m')) LIFECYCLE 120 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='reelshort 用户频道页维度统计表';

-- alter table dwd_data.dim_t80_reelshort_user_tab_id_di
-- add column tab_name varchar comment '频道页名称';





-- 频道页
delete from dwd_data.dim_t80_reelshort_user_tab_id_di where analysis_date='${TX_DATE}';
insert into dwd_data.dim_t80_reelshort_user_tab_id_di
select distinct
       null as id
       ,t.analysis_date
       ,t.uuid
       ,t.tab_id                         -- 频道页
       ,nvl(t1.tab_name,'') as tab_name  -- 频道页名称
       -- ,concat(t.tab_id,t1.tab_name) as tab_id_name -- 频道页id+（中文名）
from (select distinct
            etl_date as analysis_date
            ,uuid
            ,cast(nvl(json_extract(item_list,'$.sub_page_id'),'') as varchar) as tab_id -- 频道页
      from dwd_data.dwd_t02_reelshort_item_pv_di
      where etl_date='${TX_DATE}'
      and event_name='m_item_pv'
)as t
left join dw_view.dts_project_v_hall_v3 as t1
on t1.tab_id=t.tab_id
;