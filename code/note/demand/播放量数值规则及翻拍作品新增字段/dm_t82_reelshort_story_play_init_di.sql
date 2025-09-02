-- drop Table if  exists dm_reelshort_data.dm_t82_reelshort_story_play_init_di;
-- Create Table if not exists dm_reelshort_data.dm_t82_reelshort_story_play_init_di(
-- id                          bigint AUTO_INCREMENT    comment '自增id'
-- ,analysis_date              date                     comment '分析日期'
-- ,language_id                varchar                  comment '用户语言'
-- ,dau_average_30d            double                   comment '近30天日均活跃用户数'
-- ,play_cnt_init              int                      comment '初始播放量:上线当天该语言区近30天DAU日均值的10%~12%之间的随机值'
-- ,favorite_cnt_init          int                      comment '初始点赞次数:初始播放量的1%到2%之间的随机值'
-- ,collect_cnt_init           int                      comment '初始收藏量:初始播放量的3%到5%之间的随机值'
-- ,create_time                datetime                 comment '创建时间'
-- ,PRIMARY KEY (id,analysis_date,language_id)
-- )DISTRIBUTE BY HASH(analysis_date,language_id) PARTITION BY VALUE(DATE_FORMAT(analysis_date,'%Y%m')) LIFECYCLE 120 INDEX_ALL='Y' STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}' COMMENT='reelshort每日播放类指标初始值';


delete from dm_reelshort_data.dm_t82_reelshort_story_play_init_di where analysis_date='${TX_DATE}';
insert into dm_reelshort_data.dm_t82_reelshort_story_play_init_di
select
      null                                                                                        as id                  -- 自增id
      ,t.analysis_date                                                                                                   -- 分析日期
      -- ,t.country_id                                                                                                      -- 国家id
      ,t.language_id                                                                                                     -- 用户语言
      ,dau_average_30d                                                                                                   -- 近30天日均活跃用户数
      ,cast(play_cnt_init as int)                                                                 as play_cnt_init       -- 初始播放量:上线当天该语言区近30天DAU日均值的10%~12%之间的随机值
      ,cast(FLOOR(RAND()*(play_cnt_init*0.02-play_cnt_init*0.01 + 1)+play_cnt_init*0.01) as int ) as favorite_cnt_init   -- 初始点赞次数:初始播放量的1%到2%之间的随机值
      ,cast(FLOOR(RAND()*(play_cnt_init*0.05-play_cnt_init*0.03 + 1)+play_cnt_init*0.03) as int)  as collect_cnt_init    -- 初始收藏量:初始播放量的3%到5%之间的随机值
      ,now()                                                                                      as create_time         -- 创建时间
from
    (
     select
     t.analysis_date
     -- ,t.country_id
     ,t.language_id
     ,dau_average_30d                                                                  as dau_average_30d
     ,FLOOR(RAND()*(dau_average_30d*0.12-dau_average_30d*0.1 + 1)+dau_average_30d*0.1) as play_cnt_init -- 初始播放量:上线当天该语言区近30天DAU日均值的10%~12%之间的随机值
     from
         (
          select
          cast('${TX_DATE}' as date) as analysis_date
          -- ,country_id
          ,language_id
          ,count(distinct t.analysis_date,t.uuid)/30                                            as dau_average_30d
          from dws_data.dws_t80_reelshort_user_bhv_cnt_detail_di as t
          where t.analysis_date<='${TX_DATE}'
                and t.analysis_date>=DATE_SUB('${TX_DATE}',INTERVAL 30 DAY)
          group by
                  language_id
         ) as t
    ) as t
;