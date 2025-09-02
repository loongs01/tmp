"1.事件：m_custom_event
2.子事件：search_stat
3.派生指标需要增加条件：action和search_story_ids

1.搜索入口点击：_action=search_bar_click
3.搜索结果页曝光：
_action=result_page_show and search_story_id不为空
4.搜索推荐页曝光：_action=default_page_show
5.发起搜索请求：_action=search_click
6.搜索无结果页曝光：
_action=result_page_show and search_story_id为空
7.搜索无结果页点击：埋点暂不支持

------------------
"
"播放人数增加派生条件：
event_name=m_play_event 
 字段is_free=0为付费剧集"
 
 
"APP元素曝光人数
APP元素点击人数"

m_page_enter


搜索入口曝光：1.事件：m_page_enter
2._page_name：discover

事件：m_checkpoint_unlock 


"1.事件名：m_item_pv 增加派生条件：item_list 里的sub_page_id

选项值：频道页id映射表：dw_view.dts_project_v_hall_v3 "

"选项值：频道页id+（中文名）
中文映射表：dw_view.dts_project_v_hall_v3 "

"1.选项值：id+（映射中文名）
2.以下为映射表
dts_project_v_hall_v3_bookshelf 【需要增加的】 
dts_project_v_hall_bookshelf【需要增加的】
dim_t99_reelshort_bookshelf_info【BI现在用的】


select col1, sort_array(collect_list(col2)) as sorted_col2
from your_table
group by col1;

-- udaf
json_arrayagg


select distinct
                  uuid
                  ,cast(nvl(json_extract(item_list,'$.sub_page_id'),'') as varchar) as sub_page_id -- 频道页
           from dwd_data.dwd_t02_reelshort_item_pv_di as t
           where etl_date='${TX_DATE}'
                 and event_name='m_item_pv'
				 
				 
select distinct
uuid
,cast(nvl(json_extract(item_list,'$.sub_page_id'),'') as varchar) as sub_page_id -- 频道页
from dwd_data.dwd_t02_reelshort_item_pv_di as t
where etl_date='${TX_DATE}'
      and event_name='m_item_pv'				 
;	  
	  


select 
t.uuid
,temp_table.col
from 
(select
uuid
,split(GROUP_CONCAT(distinct json_extract(item_list,'$.sub_page_id') SEPARATOR ','),',')  as sub_page_ids -- 频道页
from dwd_data.dwd_t02_reelshort_item_pv_di as t
where etl_date='${TX_DATE}'
      and event_name='m_item_pv'
      and uuid=358734537
group by uuid
) as t   -- sub_page_ids -- 频道页
CROSS JOIN UNNEST(sub_page_ids) as temp_table(col)
where col='-1'
;




select 
-- CONCAT('{"key":"',col,'","value":"',col,'"}')
-- ,CONCAT_ws('','{"key":"',col,'","value":"',col,'"}')
CONCAT('[',group_concat(CONCAT('{"key":"',col,'","value":"',col,'"}') SEPARATOR ','),']')
from (select 
size(array_union(t.recall_level,t.recall_level)) as list_size
,'recall_level' as key
,array_union(t.recall_level,t.recall_level)  as recall_level_list
from (select 1 as key 
             ,split(group_concat(distinct recall_level SEPARATOR ','),',') as recall_level        -- 召回路
      from dwd_data.dim_t80_reelshort_user_recall_level_di
      where analysis_date='${TX_DATE}'
) as t
left join(select 2 as key
                 ,split(group_concat(distinct recall_level SEPARATOR ','),',') as recall_level        -- 召回路
          from dwd_data.dim_t80_reelshort_user_recall_level_di
          where analysis_date=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)
) as t1
on t1.key=t.key
) as t
CROSS JOIN UNNEST(t.recall_level_list) as temp_table(col)
;