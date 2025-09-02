-- -- drop table dwd_data.dwd_t02_reelshort_item_pv_di;
Create Table if not exists dwd_data.dwd_t02_reelshort_item_pv_di (
    id bigint comment '自增id'
    ,event_name varchar comment '预定义或自定义事件名称'
    ,sub_event_name varchar comment '子事件名称,默认传空字符串(业务自定义）'
    ,package_name varchar comment '包名(Android为packageName,IOS为bundleID）'
    ,uuid varchar comment '应用中用户ID'
    ,analysis_date date comment '分析日期'
    ,analysis_hour varchar comment '分析小时'
    ,country_id varchar comment '国家id'
    ,channel_id varchar comment '应用渠道(1:AVG10003,2:AVG20001)'
    ,version varchar comment 'APP的应用版本(包体内置版本号versionname)'
    ,cversion varchar comment '应用游戏版本号'
    ,res_version varchar comment '应用资源版本号'
    ,language_id varchar comment '游戏语言id'
    ,platform varchar comment '平台id (1-安卓,2-IOS，3-windows,4-macOs)'
    ,os_version varchar comment '操作系统版本号'
    ,ctimezone_offset varchar comment '设备系统的时区偏移,相对于标准时区'
    ,ctime bigint comment '设备系统当前时间戳(秒级)'
    ,stime timestamp comment '服务器接收时间'
    ,netstatus int comment '事件触发时的网络类型  未知=0 wifi=1 蜂窝网络2G=2  蜂窝网络3G=3 蜂窝网络4G=4 蜂窝网络5G=5'
    ,install_id varchar comment '应用安装后第一次启动随机生成'
    ,run_id varchar comment '应用每次冷启动启动随机生成'
    ,device_id varchar comment '应用用于标识设备的唯一ID'
    ,ad_id varchar comment '广告ID:Android=google adid IOS=idfa'
    ,androidid varchar comment 'ANDROIDID'
    ,idfv varchar comment 'IOS-idfv'
    ,user_ip varchar comment '用户IP地址'
    -- ,properties varchar comment '可变参数json'

    ,scene_name varchar comment '当前场景名称(业务自定义）'
    ,page_name varchar comment '当前页面名称(业务自定义）'
    ,item_type varchar comment 'item_type'
    ,item_list varchar comment 'item_list列表元素，元素格式 序号#story_id  例如：["1#abcdefg", "2#abcdefg"]'
        ,story_id varchar comment '书籍id（由item_list拆开）'
        ,story_order int comment '书籍id在item_list中的顺序（由item_list拆开）'
        ,cornermark_id varchar comment '书籍在书架角标id(17期)'
        ,shelf_id varchar comment '书架id（14期）'
        ,sub_page_id varchar comment '书架在大厅的频道tab子页面id（19期）'
        ,shelf_rank varchar comment '书架在大厅的顺序（17期）'
        ,tab_name varchar comment '排行榜书架tab名（17期）'
        ,tab_period varchar comment '排行榜书架时间周期（17期）'
        ,enter_type varchar comment '进入方式（17期）'
    ,etl_date date comment '清洗日期'
    ,primary key (etl_date,id,uuid,story_id)
) DISTRIBUTE BY HASH(uuid) PARTITION BY VALUE(DATE_FORMAT(etl_date, '%Y%m')) LIFECYCLE 60 
    INDEX_ALL='Y' STORAGE_POLICY='COLD' COMMENT='V项目dwd层：pv统计信息表';

-- delete from  dwd_data.dwd_t02_reelshort_item_pv_di where etl_date='${TX_DATE}';
/*+ INSERT_SELECT_TIMEOUT=1800000, direct_batch_load=true,filter_not_pushdown_columns=[chapters_log.reelshort_event_data_log:country_id|properties]*/
replace into dwd_data.dwd_t02_reelshort_item_pv_di
select
    id
    ,event_name
    ,sub_event_name
    ,package_name
    ,uuid
    ,analysis_date
    ,analysis_hour
    ,country_id
    ,channel_id
    ,version
    ,cversion
    ,res_version
    ,language_id
    ,platform
    ,os_version
    ,ctimezone_offset
    ,ctime
    ,stime
    ,netstatus
    ,install_id
    ,run_id
    ,device_id
    ,ad_id
    ,androidid
    ,idfv
    ,user_ip

    ,scene_name
    ,page_name
    ,item_type
    ,item_list
    ,case when is_new_version=1 then json_extract(item_list_new,'$.story_id') else split_part(replace(item_list,'&','#'),'#',2) end as story_id
    ,case when is_new_version=1 then json_extract(item_list_new,'$.story_rank') else split_part(replace(item_list,'&','#'),'#',1) end as story_order
    ,case
        when is_new_version=1 then json_extract(item_list_new,'$.cornermark_id')
        when size(regexp_matches(item_list,'&','g'))>2 -- 书籍顺序&书籍id&cornermark_id #书架id&shelf_rank&sub_page_id #tab_name&tab_period #enter_type(19期（Android v1.9.01  IOS v1.9.00）)
            then split_part(replace(item_list,'&','#'),'#',3)
        when size(regexp_matches(item_list,'&','g'))=2 -- 书籍顺序#书籍id$cornermark_id#书架id&shelf_rank (17期（Android v1.7.02  IOS v1.7.00）)
            then split_part(split_part(item_list,'#',2),'&',2)
        else '' end as cornermark_id -- 书籍在书架角标id(17期)
    ,case
        when is_new_version=1 then json_extract(item_list_new,'$.shelf_id')
        when size(regexp_matches(item_list,'&','g'))>2 -- 书籍顺序&书籍id&cornermark_id #书架id&shelf_rank&sub_page_id #tab_name&tab_period #enter_type(19期（Android v1.9.01  IOS v1.9.00）)
            then split_part(split_part(item_list,'#',2),'&',1)
        when size(regexp_matches(item_list,'&','g'))=0 and size(regexp_matches(item_list,'#','g'))>2 -- 书籍顺序#书籍id#cornermark_id#书架id#tab_nam#enter_type (17期（Android v1.7.02  IOS v1.7.00）)
            then split_part(item_list,'#',3)
        when size(regexp_matches(item_list,'&','g'))=2  -- 书籍顺序#书籍id$cornermark_id#书架id&shelf_rank (17期（Android v1.7.02  IOS v1.7.00）)
            then split_part(replace(item_list,'&','#'),'#',4)
        when size(regexp_matches(item_list,'&','g'))=0 and size(regexp_matches(item_list,'#','g'))=2 -- 书籍顺序#书籍id#书架id(14期（Android v1.3.00  IOS v1.3.00）)
            then split_part(item_list,'#',3)
        else '' end as shelf_id -- 书架id（14期）
    ,case
        when is_new_version=1 then json_extract(item_list_new,'$.sub_page_id')
        when size(regexp_matches(item_list,'&','g'))>2 -- 书籍顺序&书籍id&cornermark_id #书架id&shelf_rank&sub_page_id #tab_name&tab_period #enter_type(19期（Android v1.9.01  IOS v1.9.00）)
            then split_part(split_part(item_list,'&',5),'#',1)
        else '' end as sub_page_id -- 书架在大厅的频道tab子页面id（19期）
    ,case
        when is_new_version=1 then json_extract(item_list_new,'$.shelf_rank')
        when size(regexp_matches(item_list,'&','g'))>2 -- 书籍顺序&书籍id&cornermark_id #书架id&shelf_rank&sub_page_id #tab_name&tab_period #enter_type(19期（Android v1.9.01  IOS v1.9.00）)
            then split_part(item_list,'&',4)
        when size(regexp_matches(item_list,'&','g'))=2 -- 书籍顺序&书籍id&cornermark_id #书架id&shelf_rank&sub_page_id #tab_name&tab_period #enter_type(19期（Android v1.9.01  IOS v1.9.00）)
            then split_part(split_part(item_list,'&',3),'#',1)
        else '' end as shelf_rank
    ,case
        when is_new_version=1 then json_extract(item_list_new,'$.ranking_tab_name')
        when size(regexp_matches(item_list,'&','g'))>2 -- 书籍顺序&书籍id&cornermark_id #书架id&shelf_rank&sub_page_id #tab_name&tab_period #enter_type(19期（Android v1.9.01  IOS v1.9.00）)
            then split_part(split_part(item_list,'#',3),'&',1)
        when size(regexp_matches(item_list,'&','g'))=0 and size(regexp_matches(item_list,'#','g'))>2 -- 书籍顺序#书籍id#cornermark_id#书架id#tab_nam#enter_type (17期（Android v1.7.02  IOS v1.7.00）)
            then split_part(item_list,'#',4)
        when size(regexp_matches(item_list,'&','g'))=2 and size(regexp_matches(item_list,'#','g'))>=4 and split_part(item_list,'#',4) regexp '[A-Za-z]' -- 书籍顺序#书籍id$cornermark_id#书架id&shelf_rank #tab_name#enter_type (17期（Android v1.7.02  IOS v1.7.00）)
            then split_part(item_list,'#',4)
        else '' end as tab_name
    ,case
        when is_new_version=1 then json_extract(item_list_new,'$.ranking_tab_period')
        when size(regexp_matches(item_list,'&','g'))>2 -- 书籍顺序&书籍id&cornermark_id #书架id&shelf_rank&sub_page_id #tab_name&tab_period #enter_type(19期（Android v1.9.01  IOS v1.9.00）)
            then split_part(split_part(item_list,'&',6),'#',1)
        when size(regexp_matches(item_list,'&','g'))=2 and size(regexp_matches(item_list,'#','g'))>=4 and split_part(item_list,'#',4) regexp '[A-Za-z]' and split_part(item_list,'#',5) regexp '^[0-9]+$' -- 书籍顺序#书籍id$cornermark_id#书架id&shelf_rank #tab_name#enter_type (17期（Android v1.7.02  IOS v1.7.00）)
            then split_part(item_list,'#',5)
        else '' end as tab_period -- 排行榜书架时间周期（17期）
    ,case
        when is_new_version=1 then json_extract(item_list_new,'$.enter_type')
        when size(regexp_matches(item_list,'&','g'))>2 -- 书籍顺序&书籍id&cornermark_id #书架id&shelf_rank&sub_page_id #tab_name&tab_period #enter_type(19期（Android v1.9.01  IOS v1.9.00）)
            then split_part(split_part(item_list,'&',6),'#',1)
        when size(regexp_matches(item_list,'&','g'))=0 and size(regexp_matches(item_list,'#','g'))>2 -- 书籍顺序#书籍id#cornermark_id#书架id#tab_nam#enter_type (17期（Android v1.7.02  IOS v1.7.00）)
            then split_part(item_list,'#',5)
        when size(regexp_matches(item_list,'&','g'))=2 and size(regexp_matches(item_list,'#','g'))=3 and split_part(item_list,'#',4) regexp '^[0-9]+$' -- 书籍顺序#书籍id$cornermark_id#书架id&shelf_rank（#enter_type） (17期（Android v1.7.02  IOS v1.7.00）)
            then split_part(item_list,'#',4)
        when size(regexp_matches(item_list,'&','g'))=2 and size(regexp_matches(item_list,'#','g'))>=5 and split_part(item_list,'#',4) regexp '[A-Za-z]' and split_part(item_list,'#',5) regexp '^[0-9]+$' -- 书籍顺序#书籍id$cornermark_id#书架id&shelf_rank #tab_name#enter_type (17期（Android v1.7.02  IOS v1.7.00）)
            then split_part(item_list,'#',6)
        else '' end as enter_type -- 进入方式（17期）
    ,'${TX_DATE}' as etl_date

    ,cast(case when is_new_version=1 then json_extract(item_list_new,'$.report_info') else '' end as varchar) as report_info --  推荐相关数据
    ,cast(case when is_new_version=1 then json_extract(item_list_new,'$.report_info.is_continue_watching') else '' end as varchar) as is_continue_watching --  九宫格书架上报，是否为continue_watching
    ,cast(case when is_new_version=1 then json_extract(item_list_new,'$.story_value') else '' end as varchar) as story_value -- 上报目前展现作品热力值(1:)/播放量(2:)
    ,cast(case when is_new_version=1 then json_extract(item_list_new,'$.pic_url') else '' end as varchar) as pic_url -- 上报当前配置封面图片url
    ,cast(case when is_new_version=1 then json_extract(item_list_new,'$.gif_url') else '' end as varchar) as gif_url -- 配置gif的url
    ,cast(case when is_new_version=1 then json_extract(item_list_new,'$.story_rank_order') else '' end as varchar) as story_rank_order -- 当前排行内顺序
    ,cast(case when is_new_version=1 then json_extract(item_list_new,'$.tag_list') else '' end as varchar) as tag_list -- 
from 
(
    select *
    ,case when is_new_version=1 then replace(replace(item_list,'"{','{'),'}"','}')
    end as item_list_new
    from
    (
        select
        id -- 自增id
        ,event_name -- 预定义或自定义事件名称
        ,sub_event_name -- 子事件名称,默认传空字符串(业务自定义）
        ,package_name -- 包名(Android为packageName,IOS为bundleID）
        ,app_user_id as uuid -- 应用中用户ID
        ,analysis_date -- 分析日期
        ,analysis_hour -- 分析小时
        ,country_id -- 国家id
        ,app_channel_id as channel_id -- 应用渠道
        ,app_version as version -- APP的应用版本(包体内置版本号versionname)
        ,app_game_version as cversion -- 应用游戏版本号
        ,app_res_version as res_version -- 应用资源版本号
        ,case
             when app_lang like 'zh%' or app_lang in ('cn')  then 99
             when app_lang like 'hi%'  then 20
             when app_lang like 'fil%'  then 19
             when app_lang like 'da%'  then 16
             when app_lang like 'fi%'  then 15
             when app_lang like 'nb%'  then 14
             when app_lang like 'sv%'  then 13
             when app_lang like 'nl%'  then 12
             when app_lang like 'tr%'  then 11
             when app_lang like 'pl%'  then 10
             when app_lang like 'ja%'  then 9
             when app_lang like 'it%'  then 8
             when app_lang like 'ko%'  then 7
             when app_lang like 'pt%'  then 6
             when app_lang like 'ru%'  then 5
             when app_lang like 'fr%'  then 4
             when app_lang like 'de%'  then 3
             when app_lang like 'es%'  then 2
             when app_lang like 'en%' and app_lang<>'en-ID' then 1
             when app_lang like 'ar%'  then 17
             when app_lang in ('en-ID','in')  then 18
             when app_lang like 'th%' then 98
             else app_lang
        end as language_id   -- 游戏语言id  语言表暂时无法处理,数据格式混乱,eg：fr-US 无法确定是英语还是法语
        ,os_type as platform -- 操作系统类型 (android/ios/windows/mac os)
        ,os_version -- 操作系统版本号
        ,split_part(split_part(replace(replace(replace(replace(replace(os_timezone_offset,'+0',''),'+',''),'-0','-'),'=',''),'utc_offset',''),':',1),'.',1) as ctimezone_offset
        ,os_timestamp as ctime -- 设备系统当前时间戳(秒级)
        ,stime -- 服务器接收时间
        ,cast(device_network_type as int) as netstatus -- 事件触发时的网络类型  未知=0 wifi=1 蜂窝网络2G=2  蜂窝网络3G=3 蜂窝网络4G=4 蜂窝网络5G=5
        ,app_install_id as install_id -- 应用安装后第一次启动随机生成
        ,app_activate_id as run_id -- 应用每次冷启动启动随机生成
        ,device_id -- 应用用于标识设备的唯一ID
        ,if(ad_id in ('','0000-0000','00000000-0000-0000-0000-000000000000'),'',ad_id) as ad_id  -- 广告ID:Android=google adid IOS=idfa
        ,androidid -- ANDROIDID
        ,idfv -- IOS-idfv
        ,user_ip -- 用户IP地址
        -- ,properties -- 可变参数json
        ,cast(nvl(json_extract(properties,'$._scene_name'),'') as varchar) as scene_name -- 当前场景名称(业务自定义）
        ,cast(nvl(json_extract(properties,'$._page_name'),'') as varchar) as page_name -- 当前页面名称(业务自定义）
        ,cast(nvl(json_extract(properties,'$._item_type'),'') as varchar) as item_type -- item_type

         ,case when json_extract(properties,'$.item_list') rlike "nul" then split('',',')
             when left(json_extract(properties,'$.item_list'),5) not like '%{%'
                then split(replace(replace(replace(json_extract(properties,'$.item_list'),'[','') ,']',''),char(34),''),',')
             when left(json_extract(properties,'$.item_list'),5) rlike '{'
                then split(replace(replace(replace(replace(replace(cast(json_extract(properties,'$.item_list') as varchar),'\\',''),'}","{','}###{'),'},{','}###{'),'[',''),']',''),'###')
             else split('',',') end  as numbers_array
        ,if(left(json_extract(properties,'$.item_list'),5) rlike '{',1,0) as is_new_version

        ,cast(nvl(json_extract(properties, '$.sub_page_id'),'') as varchar) as  sub_page_id -- 大厅频道tab子页面id
        from chapters_log.reelshort_event_data_log
        where analysis_date='${TX_DATE}'
        and app_id='cm1009'
        and event_name='m_item_pv'
        -- and app_channel_id in('AVG20001','AVG10003','1','2','6','11','12','13','14','15','16','17','18','19','20','21') and os_type in ('1','2','3','4')
        -- and country_id<>'50'
        -- and properties rlike '#'
        and properties not like '%nul%'
        and id not in (select distinct id from dwd_data.dwd_t02_reelshort_item_pv_di where etl_date='${TX_DATE}' )
    )t
    cross join unnest(numbers_array) as temp_table(item_list)
)tt
;