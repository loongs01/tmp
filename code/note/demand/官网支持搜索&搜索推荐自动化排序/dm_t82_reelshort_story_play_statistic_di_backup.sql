Create Table if not exists dm_reelshort_data.dm_t82_reelshort_story_play_statistic_di(
 analysis_date              date                     comment '分析日期'
,etl_date                   date                     comment '数据日期'
,language_id                varchar                  comment '用户语言'
,display_tag                varchar                  comment '书籍标签'
,story_id                   varchar                  comment '书籍id'
,lang                       varchar                  comment '书籍语言'
,status                     int                      comment '书籍状态0-未上架，1-已上架，2-已下架'
,story_total_duration       bigint                   comment '书籍播放时长'
,story_play_cnt             bigint                   comment '书籍播放量(次数)'
,PRIMARY KEY (etl_date,story_id,language_id)
)DISTRIBUTE BY HASH(etl_date,story_id) PARTITION BY VALUE(DATE_FORMAT(etl_date, '%Y%m')) LIFECYCLE 120 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='reelshort 书籍每日播放统计';

delete from dm_reelshort_data.dm_t82_reelshort_story_play_statistic_di where etl_date='${TX_DATE}';
replace into dm_reelshort_data.dws_t82_reelshort_user_play_chapter_detail_di
select 
      t.analysis_date                                     -- 分析日期
      ,t.etl_date                                         -- 数据日期
      ,t.language_id                                      -- 用户语言
      ,t.display_tag                                      -- 书籍标签
      ,t.story_id                                         -- 书籍id
      ,t.lang                                             -- 书籍语言
      ,t.status                                           -- 书籍状态0-未上架，1-已上架，2-已下架
      ,sum(play_duration)         as story_total_duration -- 书籍播放时长
      ,sum(user_sroty_play_cnt) as story_play_cnt         -- 书籍播放量(次数)
from (select 
            t.uuid
            ,t.analysis_date
            ,t.etl_date
            ,t.language_id
            ,t1.display_tag
            ,t.story_id
            ,t1.lang
            ,t1.status
            ,cast(max(case 
                                when sub_event_name='play_end' and t1._id is not null and event_duration<10000 then event_duration
                                when sub_event_name='play_end' and t1._id is not null and event_duration>10000 then chap_total_duration
                                when sub_event_name='play_end' and t1._id is null and event_duration>chap_total_duration then chap_total_duration
                                else event_duration end) as int) as play_duration         -- 用户在一件作品里的播放时长
            ,count(distinct t.chap_id)                           as user_sroty_play_cnt   -- 用户在一件作品里的播放次数:每看一集就算一次，有多少集算多少次
            -- ,sum(play_duration) over(partition by t.analysis_date,t.language_id,t.story_id ) as story_total_duration
      from dwd_data.dwd_t02_reelshort_play_event_di as t
      left join (select 
                       t1._id
                       ,t1.t_book_id
                       ,t1.book_id 
                       ,t1.display_tag
                       ,t1.lang
                       ,t1.status
                 from chapters_log.dts_project_v_new_book as t1
                 -- where t1.analysis_date =date'2024-10-27'
                 ) as t1
      on t1._id=t.story_id  -- t1.t_book_id =t.t_book_id
      where t.etl_date =date'2024-10-27'
      and t.event_name ='m_play_event' -- 预定义或自定义事件名称
      and t.shelf_id ='12002'       -- 书架
      group by 
           t.uuid
           ,t.analysis_date
           ,t.etl_date
           ,t.language_id
           ,t1.display_tag
           ,t.story_id
           ,t1.lang
           ,t1.status
    ) as t
group by 
     t.analysis_date
     ,t.etl_date
     ,t.language_id
     ,t.display_tag
     ,t.story_id
     ,t.lang
     ,t.status
order by story_total_duration desc,story_play_count desc 
limit 10
;