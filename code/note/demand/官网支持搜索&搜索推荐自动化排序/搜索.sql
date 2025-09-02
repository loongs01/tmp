-- https://uv4iolo7hz.feishu.cn/docx/WlzpdCXRFoQB3YxkJJucNKaCnLe

-- 搜索推荐页
select t1.etl_date
    ,coalesce(user_type,'all') as user_type
    ,count(1) as dau
    ,sum(page_show_uv) as 搜索页曝光uv
    ,sum(foryou_pv+discover_pv) as 搜索页曝光pv
    ,sum(foryou_uv) as foryou入口_搜索页曝光uv
    ,sum(foryou_pv) as foryou入口_搜索页曝光pv
    ,sum(discover_uv) as discover入口_搜索页曝光uv
    ,sum(discover_pv) as discover入口_搜索页曝光pv

    ,sum(history_show_uv) as 历史词曝光uv
    ,sum(history_show_pv) as 历史词曝光pv
    ,sum(history_click_uv) as 历史词点击uv
    ,sum(history_click_uv)/sum(history_show_uv) as 历史词uv点击率
    ,sum(history_click_pv) as 历史词点击pv

    ,sum(rec_tags_show_uv) as 推荐热词曝光uv
    ,sum(rec_tags_show_pv) as 推荐热词曝光pv
    ,sum(rec_tags_click_uv) as 推荐热词点击uv
    ,sum(rec_tags_click_uv)/sum(rec_tags_show_uv) as 推荐热词uv点击率
    ,sum(rec_tags_click_pv) as 推荐热词点击pv
    
    -- 搜索前的推荐作品
    ,sum(rec_story_show_uv) as 推荐作品曝光uv
    ,sum(rec_story_show_pv) as 推荐作品曝光pv
    ,sum(rec_story_click_uv) as 推荐作品点击uv
    ,sum(rec_story_click_uv)/sum(rec_story_show_uv) as 推荐作品uv点击率
    ,sum(rec_story_click_pv) as 推荐作品点击pv

    ,avg(播放剧集数) as 人均推荐作品播放剧集数
    ,avg(播放时长) as 人均推荐作品播放时长

    ,sum(history_click_uv) as 历史词搜索uv
    ,sum(history_click_pv) as 历史词搜索pv

    ,sum(rec_tags_click_uv) as 推荐热词搜索uv
    ,sum(rec_tags_click_pv) as 推荐热词搜索pv

    ,sum(搜索框推荐词搜索uv) as 搜索框推荐词搜索uv
    ,sum(搜索框推荐词搜索pv) as 搜索框推荐词搜索pv

    ,sum(手动输入搜索uv) as 手动输入搜索uv
    ,sum(手动输入搜索pv) as 手动输入搜索pv
    
    ,sum(点击确认搜索uv) as 点击确认搜索uv
    ,sum(点击确认搜索pv) as 点击确认搜索pv

    
    
    ,sum(search_uv) as 搜索人数
    ,sum(search_uv)/sum(page_show_uv) as 搜索uv占比 -- 进入搜索页的用户有多少人会搜索
    -- 有搜索的用户都是通过什么方式搜索的
    ,sum(history_click_uv)/sum(search_uv) as 历史词搜索uv占比
    ,sum(rec_tags_click_uv)/sum(search_uv) as 推荐热词搜索uv占比
    ,sum(搜索框推荐词搜索uv)/sum(search_uv) as 搜索框推荐词搜索uv占比
    ,sum(点击确认搜索uv)/sum(search_uv) as 点击确认搜索uv占比
    ,sum(手动输入搜索uv)/sum(search_uv) as 手动输入搜索uv占比
from(
    select etl_date,uuid,if(user_type=1,'新用户','老用户') as user_type
    from dwd_data.dwd_t01_reelshort_user_detail_info_di
    where etl_date between '2024-10-01' and '2024-10-31'
        and language_id=1   -- 英语
        and user_type in (1,0)
)t1
left join(-- 搜索推荐页
    select etl_date,uuid
        -- 搜索页曝光uv、pv
        ,max(if(action in ('default_page_show'),1,0)) as page_show_uv
        ,sum(if(action in ('default_page_show'),1,0)) as page_show_pv   -- 会重复报
        
        ,max(if(action in ('result_page_show'),1,0)) as search_uv

        ,max(if(action in ('search_bar_click') and page_name='for_you',1,0)) as foryou_uv
        ,max(if(action in ('search_bar_click') and page_name='discover',1,0)) as discover_uv
        ,sum(if(action in ('search_bar_click') and page_name='for_you',1,0)) as foryou_pv
        ,sum(if(action in ('search_bar_click') and page_name='discover',1,0)) as discover_pv
        -- 历史词模块曝光
        ,max(if(action in ('default_page_show') and history_module_show=1,1,0)) as history_show_uv
        ,sum(if(action in ('default_page_show') and history_module_show=1,1,0)) as history_show_pv
        -- 历史词模块点击
        ,max(if(action in ('result_page_show') and search_word_source='history_click_fill',1,0)) as history_click_uv
        ,sum(if(action in ('result_page_show') and search_word_source='history_click_fill',1,0)) as history_click_pv

        -- 推荐热词模块曝光
        ,max(if(action in ('default_page_show') and rec_tags not in ('','[]'),1,0)) as rec_tags_show_uv
        ,sum(if(action in ('default_page_show') and rec_tags not in ('','[]'),1,0)) as rec_tags_show_pv
        -- 推荐热词模块点击
        ,max(if(action in ('result_page_show') and search_word_source='tag_click_fill',1,0)) as rec_tags_click_uv
        ,sum(if(action in ('result_page_show') and search_word_source='tag_click_fill',1,0)) as rec_tags_click_pv

        -- 推荐作品模块曝光
        ,max(if(action in ('default_page_show') and rec_story_ids not in ('','[]'),1,0)) as rec_story_show_uv
        ,sum(if(action in ('default_page_show') and rec_story_ids not in ('','[]'),1,0)) as rec_story_show_pv

        -- 推荐作品模块点击
        -- update：添加page_type='default'，结果页也会有推荐作品
        ,max(if(action in ('story_click') and page_type='default',1,0)) as rec_story_click_uv
        ,sum(if(action in ('story_click') and page_type='default',1,0)) as rec_story_click_pv

        -- 实时输入
        ,max(if(action in ('result_page_show') and search_word_source='typing',1,0)) as 手动输入搜索uv
        ,sum(if(action in ('result_page_show') and search_word_source='typing',1,0)) as 手动输入搜索pv  -- 会重复上报

        -- 手动输入
        ,max(if(action in ('search_click'),1,0)) as 点击确认搜索uv
        ,sum(if(action in ('search_click'),1,0)) as 点击确认搜索pv

        -- 推荐热词模块点击
        ,max(if(action in ('result_page_show') and search_word_source='default_fill',1,0)) as 搜索框推荐词搜索uv
        ,sum(if(action in ('result_page_show') and search_word_source='default_fill',1,0)) as 搜索框推荐词搜索pv


    from dwd_data.dwd_t02_reelshort_search_stat_di
    where etl_date between '2024-10-01' and '2024-10-31'
        and action in ('search_bar_click','default_page_show','result_page_show','story_click','search_click')
    group by 1,2
)t2
on t1.etl_date=t2.etl_date and t1.uuid=t2.uuid
left join(-- 搜索结果页点击推荐作品，统计播放时长
    select t1.etl_date,t1.uuid
        ,sum(chap_cnt) as 播放剧集数
        ,sum(event_duration)/60 as 播放时长
    from(-- 点击推荐作品
        select etl_date,uuid,clicked_story_id
        from dwd_data.dwd_t02_reelshort_search_stat_di
        where etl_date between '2024-10-01' and '2024-10-31'
            and action in ('story_click')
            and page_type='default'
        group by 1,2,3
    )t1
    join(-- 播放时长
        select etl_date,uuid,story_id
            ,sum(least(event_duration,9999)) as event_duration
            ,count(distinct chap_id) as chap_cnt
        from dwd_data.dwd_t02_reelshort_play_event_di
        where etl_date between '2024-10-01' and '2024-10-31'
            and sub_event_name='play_end'
            and event_duration>0
            and pre_page_name='search'
        group by 1,2,3
    )t2
    on t1.etl_date=t2.etl_date and t1.uuid=t2.uuid and t1.clicked_story_id=t2.story_id
    group by 1,2
)t5
on t1.etl_date=t5.etl_date and t1.uuid=t5.uuid
group by grouping sets(
    (t1.etl_date,user_type)
    ,(t1.etl_date)
)
order by 1,2



-- 搜索结果页（有结果）
select t1.etl_date,coalesce(user_type,'all') as user_type
    ,count(1) as dau
    
    ,sum(搜索有结果页uv) as 搜索有结果页uv
    ,sum(搜索有结果页pv) as 搜索有结果页pv

    ,sum(历史词搜索有结果页曝光uv) as 历史词搜索有结果页曝光uv
    ,sum(历史词搜索有结果页曝光pv) as 历史词搜索有结果页曝光pv

    ,sum(推荐热词搜索有结果页曝光uv) as 推荐热词搜索有结果页曝光uv
    ,sum(推荐热词搜索有结果页曝光pv) as 推荐热词搜索有结果页曝光pv

    ,sum(搜索框推荐词搜索有结果页曝光uv) as 搜索框推荐词搜索有结果页曝光uv
    ,sum(搜索框推荐词搜索有结果页曝光pv) as 搜索框推荐词搜索有结果页曝光pv

    ,sum(手动输入搜索有结果页曝光uv) as 手动输入搜索有结果页曝光uv
    ,sum(手动输入搜索有结果页曝光pv) as 手动输入搜索有结果页曝光pv

    ,sum(历史词搜索有结果页点击uv) as 历史词搜索有结果页点击uv
    ,sum(历史词搜索有结果页点击pv) as 历史词搜索有结果页点击pv

    ,sum(推荐热词搜索有结果页点击uv) as 推荐热词搜索有结果页点击uv
    ,sum(推荐热词搜索有结果页点击pv) as 推荐热词搜索有结果页点击pv

    ,sum(搜索框推荐词搜索有结果页点击uv) as 搜索框推荐词搜索有结果页点击uv
    ,sum(搜索框推荐词搜索有结果页点击pv) as 搜索框推荐词搜索有结果页点击pv

    ,sum(手动输入搜索有结果页点击uv) as 手动输入搜索有结果页点击uv
    ,sum(手动输入搜索有结果页点击pv) as 手动输入搜索有结果页点击pv
    
    ,sum(历史词搜索有结果页点击uv)/sum(历史词搜索有结果页曝光uv) as 历史词搜索有结果页点击率
    ,sum(推荐热词搜索有结果页点击uv)/sum(推荐热词搜索有结果页曝光uv) as 推荐热词搜索有结果页点击率
    ,sum(搜索框推荐词搜索有结果页点击uv)/sum(搜索框推荐词搜索有结果页曝光uv) as 搜索框推荐词搜索有结果页点击率
    ,sum(手动输入搜索有结果页点击uv)/sum(手动输入搜索有结果页曝光uv) as 手动输入搜索有结果页点击率

    ,sum(结果页点击uv) as 结果页点击uv
    ,sum(结果页点击pv) as 结果页点击pv

    ,sum(播放剧集数) as 结果页播放剧集数
    ,sum(播放时长) as 结果页播放时长
from(
    select etl_date,uuid,if(user_type=1,'新用户','老用户') as user_type
    from dwd_data.dwd_t01_reelshort_user_detail_info_di
    where etl_date between '2024-10-01' and '2024-10-31'
        and language_id=1   -- 英语
        and user_type in (1,0)
)t1
left join(-- 搜索结果页（有结果）
    select etl_date,uuid
        ,max(if(action in ('result_page_show') and search_story_ids not in ('','[]'),1,0)) as 搜索有结果页uv
        ,sum(if(action in ('result_page_show') and search_story_ids not in ('','[]'),1,0)) as 搜索有结果页pv  -- 会重复上报，手动每输入一个词就上报一次
        -- 曝光
        ,max(if(action in ('result_page_show') and search_story_ids not in ('','[]') and search_word_source='history_click_fill',1,0)) as 历史词搜索有结果页曝光uv
        ,sum(if(action in ('result_page_show') and search_story_ids not in ('','[]') and search_word_source='history_click_fill',1,0)) as 历史词搜索有结果页曝光pv

        ,max(if(action in ('result_page_show') and search_story_ids not in ('','[]') and search_word_source='tag_click_fill',1,0)) as 推荐热词搜索有结果页曝光uv
        ,sum(if(action in ('result_page_show') and search_story_ids not in ('','[]') and search_word_source='tag_click_fill',1,0)) as 推荐热词搜索有结果页曝光pv

        ,max(if(action in ('result_page_show') and search_story_ids not in ('','[]') and search_word_source='default_fill',1,0)) as 搜索框推荐词搜索有结果页曝光uv
        ,sum(if(action in ('result_page_show') and search_story_ids not in ('','[]') and search_word_source='default_fill',1,0)) as 搜索框推荐词搜索有结果页曝光pv

        -- 手动输入的结果页，无法区分是实时输入还是点击确认搜索，点击确认搜索是一个动作，不是搜索结果
        -- 所以其实只要知道有多少人点击确认搜索就可以了
        ,max(if(action in ('result_page_show') and search_story_ids not in ('','[]') and search_word_source='typing',1,0)) as 手动输入搜索有结果页曝光uv
        ,sum(if(action in ('result_page_show') and search_story_ids not in ('','[]') and search_word_source='typing',1,0)) as 手动输入搜索有结果页曝光pv

        -- 点击
        ,max(if(action in ('story_click') and search_story_ids not in ('[]'),1,0)) as 结果页点击uv
        ,sum(if(action in ('story_click') and search_story_ids not in ('[]'),1,0)) as 结果页点击pv

        ,max(if(action in ('story_click') and search_story_ids not in ('[]') and page_type='result',1,0)) as 搜索有结果页点击uv
        ,sum(if(action in ('story_click') and search_story_ids not in ('[]') and page_type='result',1,0)) as 搜索有结果页点击pv

        ,max(if(action in ('story_click') and search_story_ids not in ('[]') and search_word_source='history_click_fill',1,0)) as 历史词搜索有结果页点击uv
        ,sum(if(action in ('story_click') and search_story_ids not in ('[]') and search_word_source='history_click_fill',1,0)) as 历史词搜索有结果页点击pv

        ,max(if(action in ('story_click') and search_story_ids not in ('[]') and search_word_source='tag_click_fill',1,0)) as 推荐热词搜索有结果页点击uv
        ,sum(if(action in ('story_click') and search_story_ids not in ('[]') and search_word_source='tag_click_fill',1,0)) as 推荐热词搜索有结果页点击pv

        ,max(if(action in ('story_click') and search_story_ids not in ('[]') and search_word_source='default_fill',1,0)) as 搜索框推荐词搜索有结果页点击uv
        ,sum(if(action in ('story_click') and search_story_ids not in ('[]') and search_word_source='default_fill',1,0)) as 搜索框推荐词搜索有结果页点击pv

        ,max(if(action in ('story_click') and search_story_ids not in ('[]') and search_word_source='typing',1,0)) as 手动输入搜索有结果页点击uv
        ,sum(if(action in ('story_click') and search_story_ids not in ('[]') and search_word_source='typing',1,0)) as 手动输入搜索有结果页点击pv
    from dwd_data.dwd_t02_reelshort_search_stat_di
    where etl_date between '2024-10-01' and '2024-10-31'
        and action in ('result_page_show','story_click')
        -- and search_story_ids not in ('','[]')
    group by 1,2
    
)t3
on t1.etl_date=t3.etl_date and t1.uuid=t3.uuid
left join(
    select t1.etl_date,t1.uuid
        ,sum(chap_cnt) as 播放剧集数
        ,sum(event_duration)/60 as 播放时长
    from(-- 点击推荐作品
        select etl_date,uuid,clicked_story_id
        from dwd_data.dwd_t02_reelshort_search_stat_di
        where etl_date between '2024-10-01' and '2024-10-31'
            and action in ('story_click')
            and page_type='result'
            and search_story_ids not in ('[]')
        group by 1,2,3
    )t1
    left join(-- 播放时长
        select etl_date,uuid,story_id
            ,sum(least(event_duration,9999)) as event_duration
            ,count(distinct chap_id) as chap_cnt
        from dwd_data.dwd_t02_reelshort_play_event_di
        where etl_date between '2024-10-01' and '2024-10-31'
            and sub_event_name='play_end'
            and event_duration>0
            and pre_page_name='search'
        group by 1,2,3
    )t2
    on t1.etl_date=t2.etl_date and t1.uuid=t2.uuid and t1.clicked_story_id=t2.story_id
    group by 1,2
)t4
on t1.etl_date=t4.etl_date and t1.uuid=t4.uuid
group by grouping sets(
    (t1.etl_date,user_type)
    ,(t1.etl_date)
)





-- 搜索结果页（没有结果）
select t1.etl_date,coalesce(user_type,'all') as user_type
    ,count(1) as dau

    -- 搜索结果页（没有结果）
    ,sum(搜索无结果页结果uv) as 搜索无结果页uv
    ,sum(搜索无结果页结果pv) as 搜索无结果页pv
    
    ,sum(搜索无结果页无推荐uv) as 搜索无结果页无推荐uv
    ,sum(搜索无结果页无推荐pv) as 搜索无结果页无推荐pv
    
    ,sum(搜索无结果页有推荐uv) as 搜索无结果页有推荐uv
    ,sum(搜索无结果页有推荐pv) as 搜索无结果页有推荐pv
    

    ,sum(历史词搜索无结果页曝光uv) as 历史词搜索无结果页曝光uv
    ,sum(历史词搜索无结果页曝光pv) as 历史词搜索无结果页曝光pv

    ,sum(推荐热词搜索无结果页曝光uv) as 推荐热词搜索无结果页曝光uv
    ,sum(推荐热词搜索无结果页曝光pv) as 推荐热词搜索无结果页曝光pv

    ,sum(搜索框推荐词搜索无结果页曝光uv) as 搜索框推荐词搜索无结果页曝光uv
    ,sum(搜索框推荐词搜索无结果页曝光pv) as 搜索框推荐词搜索无结果页曝光pv

    ,sum(手动输入搜索无结果页曝光uv) as 手动输入搜索无结果页曝光uv
    ,sum(手动输入搜索无结果页曝光pv) as 手动输入搜索无结果页曝光pv
    
    -- 点击的是推荐作品，但是跟搜索前的推荐作品不一样
    ,sum(历史词搜索无结果页点击uv) as 历史词搜索无结果页点击uv
    ,sum(历史词搜索无结果页点击pv) as 历史词搜索无结果页点击pv
    -- 热词为什么会无结果
    ,sum(推荐热词搜索无结果页点击uv) as 推荐热词搜索无结果页点击uv
    ,sum(推荐热词搜索无结果页点击pv) as 推荐热词搜索无结果页点击pv
    -- 搜索框推荐词为什么会无结果
    ,sum(搜索框推荐词搜索无结果页点击uv) as 搜索框推荐词搜索无结果页点击uv
    ,sum(搜索框推荐词搜索无结果页点击pv) as 搜索框推荐词搜索无结果页点击pv
    -- 大部分无结果是手动输入
    ,sum(手动输入搜索无结果页点击uv) as 手动输入搜索无结果页点击uv
    ,sum(手动输入搜索无结果页点击pv) as 手动输入搜索无结果页点击pv

    ,sum(无结果页点击uv) as 无结果页点击uv
    ,sum(播放剧集数) as 无结果页播放剧集数
    ,sum(播放时长) as 无结果页播放时长

from(
    select etl_date,uuid,if(user_type=1,'新用户','老用户') as user_type
    from dwd_data.dwd_t01_reelshort_user_detail_info_di
    where etl_date between '2024-10-01' and '2024-10-31'
        and language_id=1   -- 英语
        and user_type in (1,0)
)t1
left join(-- 搜索结果页（没有结果）
    select etl_date,uuid
        ,max(if(action in ('result_page_show'),1,0)) as 搜索无结果页结果uv
        ,sum(if(action in ('result_page_show'),1,0)) as 搜索无结果页结果pv

        ,max(if(action in ('story_click') and search_story_ids='[]',1,0)) as 无结果页点击uv

        ,max(if(action in ('result_page_show') and search_rec_story_ids in ('','[]'),1,0)) as 搜索无结果页无推荐uv
        ,sum(if(action in ('result_page_show') and search_rec_story_ids in ('','[]'),1,0)) as 搜索无结果页无推荐pv

        ,max(if(action in ('result_page_show') and search_rec_story_ids not in ('','[]'),1,0)) as 搜索无结果页有推荐uv
        ,sum(if(action in ('result_page_show') and search_rec_story_ids not in ('','[]'),1,0)) as 搜索无结果页有推荐pv
        -- 曝光
        ,max(if(action in ('result_page_show') and search_word_source='history_click_fill',1,0)) as 历史词搜索无结果页曝光uv
        ,sum(if(action in ('result_page_show') and search_word_source='history_click_fill',1,0)) as 历史词搜索无结果页曝光pv

        ,max(if(action in ('result_page_show') and search_word_source='tag_click_fill',1,0)) as 推荐热词搜索无结果页曝光uv
        ,sum(if(action in ('result_page_show') and search_word_source='tag_click_fill',1,0)) as 推荐热词搜索无结果页曝光pv

        ,max(if(action in ('result_page_show') and search_word_source='default_fill',1,0)) as 搜索框推荐词搜索无结果页曝光uv
        ,sum(if(action in ('result_page_show') and search_word_source='default_fill',1,0)) as 搜索框推荐词搜索无结果页曝光pv

        ,max(if(action in ('result_page_show') and search_word_source='typing',1,0)) as 手动输入搜索无结果页曝光uv
        ,sum(if(action in ('result_page_show') and search_word_source='typing',1,0)) as 手动输入搜索无结果页曝光pv

        -- 点击
        ,max(if(action in ('story_click') and search_story_ids='[]' and search_word_source='history_click_fill',1,0)) as 历史词搜索无结果页点击uv
        ,sum(if(action in ('story_click') and search_story_ids='[]' and search_word_source='history_click_fill',1,0)) as 历史词搜索无结果页点击pv

        ,max(if(action in ('story_click') and search_story_ids='[]' and search_word_source='tag_click_fill',1,0)) as 推荐热词搜索无结果页点击uv
        ,sum(if(action in ('story_click') and search_story_ids='[]' and search_word_source='tag_click_fill',1,0)) as 推荐热词搜索无结果页点击pv

        ,max(if(action in ('story_click') and search_story_ids='[]' and search_word_source='default_fill',1,0)) as 搜索框推荐词搜索无结果页点击uv
        ,sum(if(action in ('story_click') and search_story_ids='[]' and search_word_source='default_fill',1,0)) as 搜索框推荐词搜索无结果页点击pv

        ,max(if(action in ('story_click') and search_story_ids='[]' and search_word_source='typing',1,0)) as 手动输入搜索无结果页点击uv
        ,sum(if(action in ('story_click') and search_story_ids='[]' and search_word_source='typing',1,0)) as 手动输入搜索无结果页点击pv
    from dwd_data.dwd_t02_reelshort_search_stat_di
    where etl_date between '2024-10-01' and '2024-10-31'
        and action in ('result_page_show','story_click')
        and search_story_ids in ('','[]')
        -- and search_rec_story_ids not in ('','[]')
    group by 1,2
)t4
on t1.etl_date=t4.etl_date and t1.uuid=t4.uuid
left join(-- 搜索结果页（没有结果）播放
    select t1.etl_date,t1.uuid
        ,sum(chap_cnt) as 播放剧集数
        ,sum(event_duration)/60 as 播放时长
    from(-- 点击推荐作品
        select etl_date,uuid,clicked_story_id
        from dwd_data.dwd_t02_reelshort_search_stat_di
        where etl_date between '2024-10-01' and '2024-10-31'
            and action in ('story_click')
            and page_type='result'
            and search_story_ids in ('[]')
            and search_rec_story_ids not in ('[]','')
        group by 1,2,3
    )t1
    left join(-- 播放时长
        select etl_date,uuid,story_id
            ,sum(least(event_duration,9999)) as event_duration
            ,count(distinct chap_id) as chap_cnt
        from dwd_data.dwd_t02_reelshort_play_event_di
        where etl_date between '2024-10-01' and '2024-10-31'
            and sub_event_name='play_end'
            and event_duration>0
            and pre_page_name='search'
        group by 1,2,3
    )t2
    on t1.etl_date=t2.etl_date and t1.uuid=t2.uuid and t1.clicked_story_id=t2.story_id
    group by 1,2
)t5
on t1.etl_date=t5.etl_date and t1.uuid=t5.uuid
group by grouping sets(
    (t1.etl_date,user_type)
    ,(t1.etl_date)
)





-- 从搜索页进入播放器
select t1.etl_date
    ,coalesce(user_type,'all') as user_type
    ,count(1) as 播放uv
    ,sum(event_duration)/60 as `播放时长（分钟）`
    ,sum(chap_cnt) as 播放剧集
from(
    select etl_date,uuid,if(user_type=1,'新用户','老用户') as user_type
    from dwd_data.dwd_t01_reelshort_user_detail_info_di
    where etl_date between '2024-06-01' and '2024-06-30'
        and language_id=1   -- 英语
        and user_type in (1,0)
)t1
join(
    select etl_date,uuid
        ,sum(least(event_duration,9999)) as event_duration
        ,count(distinct chap_id) as chap_cnt
    from dwd_data.dwd_t02_reelshort_play_event_di
    where etl_date between '2024-06-01' and '2024-06-30'
        and sub_event_name='play_end'
        and event_duration>0
        and pre_page_name='search'
    group by 1,2
)t2
on t1.etl_date=t2.etl_date and t1.uuid=t2.uuid
join(
    select etl_date,uuid
    from dwd_data.dwd_t02_reelshort_search_stat_di
    where etl_date between '2024-06-01' and '2024-06-30'
        and action in ('story_click')
    group by 1,2
)t3
on t1.etl_date=t3.etl_date and t1.uuid=t3.uuid
group by grouping sets(
    (t1.etl_date,user_type)
    ,(t1.etl_date)
)
order by 1,2