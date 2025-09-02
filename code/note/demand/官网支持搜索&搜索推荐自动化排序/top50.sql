-- explain
select * from (select
      t.analysis_date                                      as "分析日期"
      ,t.language_id                                       as "用户语言"
      ,t.display_tag                                       as "书籍标签"
      ,t.story_id                                          as "书籍id"
      ,t.book_title                                        as "书名"
      ,t.lang                                              as "书籍语言"
      ,t.status                                            as "书籍状态0-未上架，1-已上架，2-已下架"
      , story_total_duration                               as "书籍播放时长"
      , story_play_cnt                                     as "书籍播放量(次数)"
      ,dense_rank() over(partition by t.analysis_date order by story_total_duration desc, story_play_cnt desc) as rn
from
(select
      null                       as id                      -- 自增id
      ,t.analysis_date                                      -- 分析日期
      ,t.etl_date                                           -- 数据日期
      ,t.language_id                                        -- 用户语言
      ,t.display_tag                                        -- 书籍标签
      ,t.story_id                                           -- 书籍id
      ,t.book_title
      ,t.lang                                               -- 书籍语言
      ,t.status                                             -- 书籍状态0-未上架，1-已上架，2-已下架
      ,t.publish_at                                         -- 书籍上架时间
      ,sum(play_duration)         as story_total_duration   -- 书籍播放时长
      ,count(distinct t.uuid,t.chap_id)  as story_play_cnt         -- 书籍播放量(次数)
from (select
            t.uuid
            ,t.analysis_date
            ,t.etl_date
            ,t.language_id
            ,t1.display_tag
            ,t.story_id
            ,t1.book_title
            ,t.chap_id
            ,t1.lang
            ,t1.status
            ,max(t1.publish_at) as publish_at
            ,cast(max(case
                                when sub_event_name='play_end' and t1._id is not null and event_duration<10000 then event_duration
                                when sub_event_name='play_end' and t1._id is not null and event_duration>10000 then chap_total_duration
                                when sub_event_name='play_end' and t1._id is null and event_duration>chap_total_duration then chap_total_duration
                                else event_duration end) as int) as play_duration         -- 用户在一件作品里的播放时长
            -- ,count(distinct t.chap_id)                           as user_sroty_play_cnt   -- 用户在一件作品里的播放次数:每看一集就算一次，有多少集算多少
            -- ,sum(play_duration) over(partition by t.analysis_date,t.language_id,t.story_id ) as story_total_duration
      from dwd_data.dwd_t02_reelshort_play_event_di as t
      left join (select
                       t1._id
                       ,t1.t_book_id
                       ,t1.book_id
                       ,t1.book_title
                       ,t1.display_tag
                       ,t1.lang
                       ,t1.status
                       ,FROM_UNIXTIME(cast(t1.publish_at as int))  as publish_at -- 书籍上架时间
                       ,row_number() over(partition by t1._id,t1.t_book_id,t1.book_id,t1.lang order by updated_at desc ) as rn
                 from chapters_log.dts_project_v_new_book as t1
                 ) as t1
      on t1._id=t.story_id and t1.rn=1            -- t1.t_book_id =t.t_book_id
      left join (select
                     distinct uuid
                     ,language_id
                     ,clicked_story_id
                      -- ,search_story_ids -- 搜索结果的书籍列表
                 from dwd_data.dwd_t02_reelshort_search_stat_di as t2
                 where t2.etl_date >=date_sub('${TX_DATE}', 7)
                 and t2.etl_date <='${TX_DATE}'
                 and t2.action='story_click'
                -- and t2.search_story_ids not in ('','[]')
                -- and t2.search_story_ids like  CONCAT('%',t2.clicked_story_id,'%')
                )as t2
      on t2.uuid=t.uuid and t2.language_id=t.language_id and t2.clicked_story_id=t.story_id
      where t.etl_date >=date_sub('${TX_DATE}', 7)
      and t.etl_date <='${TX_DATE}'
      and t.event_name ='m_play_event' -- 预定义或自定义事件名称
      and t.sub_event_name in ('play_enter','play_start','play_end')
      and t2.uuid is not null
      and t.shelf_id ='12002'          -- 书架:搜索结果页面点击书籍跳转播放器
      group by
           t.uuid
           ,t.analysis_date
           ,t.etl_date
           ,t.language_id
           ,t1.display_tag
           ,t.story_id
           ,t1.book_title
           ,t.chap_id
           ,t1.lang
           ,t1.status
    ) as t
group by
     t.analysis_date
     ,t.etl_date
     ,t.language_id
     ,t.display_tag
     ,t.story_id
     ,t.book_title
     ,t.lang
     ,t.status
) as t
) as t

where rn<=50
order by "分析日期" ,rn