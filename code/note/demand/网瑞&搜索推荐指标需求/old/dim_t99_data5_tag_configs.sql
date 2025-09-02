
replace into dim_data.dim_t99_data5_tag_configs
with dim_t99_tag_configs_target_enum as (
       select 
              1 as id,'scene_name' as key,'场景' name
              ,concat('[',group_concat(concat('{"key":"',scene_name,'","value":"',concat(scene_name_cn,'(',scene_name,')'),'"}') order by scene_name  separator ',') ,']') as value 
       from  dim_data.dim_t99_reelshort_scene_info

       union all 
       select 
              2 as id,'page_name' as key,'页面' name
              , concat('[',group_concat(concat('{"key":"',page_name,'","value":"',concat(page_name_cn,'(',page_name,')'),'"}') order by page_name  separator ',') ,']') as value 
       from 
       (      
                     select page_name,replace(page_name_cn,' ','') as page_name_cn from dim_data.dim_t99_reelshort_page_info
                     union select page_name,replace(page_name_cn,' ','') as page_name_cn from ana_data.dim_t99_reelshort_page_info
       )

       union all 
       select 
              3 as id,'pre_page_name' as key,'上一页面' name
              ,concat('[',group_concat(concat('{"key":"',pre_page_name,'","value":"',concat(pre_page_name_cn,'(',pre_page_name,')'),'"}') order by pre_page_name  separator ',') ,']') as value 
       from 
       (      
                     select pre_page_name,pre_page_name_cn from dim_data.dim_t99_reelshort_pre_page_name_info
                     union select page_name,page_name_cn from ana_data.dim_t99_reelshort_page_info
       )
       union all 
       -- select 
       --        4 as id,'shelf_id' as key,'书架' name
       --        ,concat('[',group_concat(concat('{"key":"',shelf_id,'","value":"',concat(shelf_name_cn,'(',shelf_id,')'),'"}') order by shelf_id  separator ',') ,']') as value 
       -- from dim_data.dim_t99_reelshort_bookshelf_info
       select 
              4 as id,'shelf_name' as key,'书架' name
              ,concat('[',group_concat(distinct concat('{"key":"',nvl(shelf_name_cn,''),'","value":"',nvl(shelf_name_cn,''),'"}') order by concat('{"key":"',nvl(shelf_name_cn,''),'","value":"',nvl(shelf_name_cn,''),'"}') desc  separator ',') ,']') as value
        from 
        (
            select
                shelf_id
                ,shelf_name_cn
                ,row_number() over(partition by shelf_id order by priority) as rk 
            from
        (
                select shelf_id,shelf_name_cn,1 as priority from dim_data.dim_t99_reelshort_bookshelf_info
                union all
                select shelf_id,shelf_name_cn,1.1 as priority from ana_data.dim_t99_reelshort_bookshelf_info
                union all
                select id
                       ,case when bookshelf_name is null then case when ui_style=1  then '基础布局'
                                                                 when ui_style=2  then '基础布局排行'
                                                                 when ui_style=3  then '继续观看'
                                                                 when ui_style=4  then '列表式布局'
                                                                 when ui_style=5  then '推荐布局'
                                                                 when ui_style=6  then '预告书架'
                                                                 when ui_style=7  then '即将上架'
                                                                 when ui_style=8  then '限时免费布局'
                                                                 when ui_style=9  then 'APP引流'
                                                                 when ui_style=10 then '排行榜'
                                                                 when ui_style=11 then '自动播放'
                                                                 when ui_style=12 then '活动横幅布局'
                                                                 when ui_style=13 then '滚动布局'
                                                                 when ui_style=15 then '三列布局'
                                                                 when ui_style=16 then '两列布局'
                                                                 when ui_style=17 then '中横幅'
                                                                 when ui_style=18 then '九宫格瀑布流'
                                                                 when ui_style=19 then '双列瀑布流'
                                                                 when ui_style=20 then 'Hot'
                                                            end
                           else bookshelf_name
                       end as shelf_name
                       ,2 as priority from chapters_log.dts_project_v_hall_v3_bookshelf
                union all
                select id
                       ,case when bookshelf_name is null then case when ui_style=1  then '基础布局'
                                                                  when ui_style=2  then '基础布局排行'
                                                                  when ui_style=3  then '继续观看'
                                                                  when ui_style=4  then '列表式布局'
                                                                  when ui_style=5  then '推荐布局'
                                                                  when ui_style=6  then '预告书架'
                                                                  when ui_style=7  then '即将上架'
                                                                  when ui_style=8  then '限时免费布局'
                                                                  when ui_style=9  then 'APP引流'
                                                                  when ui_style=10 then '排行榜'
                                                                  when ui_style=11 then '自动播放'
                                                                  when ui_style=12 then '活动横幅布局'
                                                                  when ui_style=13 then '滚动布局'
                                                                  when ui_style=15 then '三列布局'
                                                                  when ui_style=16 then '两列布局'
                                                                  when ui_style=17 then '中横幅'
                                                                  when ui_style=18 then '九宫格瀑布流'
                                                                  when ui_style=19 then '双列瀑布流'
                                                                  when ui_style=20 then 'Hot'
                                                             end
                            else bookshelf_name
                       end as shelf_name
                       ,3 as priority from chapters_log.dts_project_v_hall_bookshelf
            )
        )
        where rk=1
        
       union all 
       select 
              5 as id,'action' as key,'组件' name
              ,concat('[',group_concat( concat('{"key":"',action,'","value":"',value,'"}') order by action  separator ',') ,']') as value 
       from 
       (select distinct action,action_cn,concat(action_cn,'(',action,')') as value from dim_data.dim_t99_reelshort_action_info order by action)

       union all 
       select 
              6 as id,'sub_event_name' as key,'子事件' name
              ,concat('[',group_concat( concat('{"key":"',sub_event_name,'","value":"',concat(sub_event_name_cn,'(',sub_event_name,')'),'"}')  order by sub_event_name separator ',') ,']') as value 
       from dim_data.dim_t99_reelshort_sub_event_info

       union all
       select
              7 as id,'referrer_story_id' as key,'来源剧目id' name
              ,concat('[',group_concat(distinct concat('{"key":"',story_id,'","value":"',concat(story_id_cn,'(',story_id,')'),'"}')   separator ',') ,']') as value 
       from
       (
           select t1._id as story_id,concat('[',name_cn,']',REPLACE(REPLACE(book_title, CHAR(10), ' '), CHAR(13), ' ')) as story_id_cn 
           from chapters_log.dts_project_v_new_book t1 
           left join chapters_log.language_info_v2 t2 on t1.lang=t2.name_en_short 
           union all select '',''
       )

       union all
       select
              8 as id,'story_id' as key,'剧目id' name
              ,concat('[',group_concat(distinct concat('{"key":"',story_id,'","value":"',concat(story_id_cn,'(',story_id,')'),'"}')   separator ',') ,']') as value 
       from
       (
           select t1._id as story_id,concat('[',name_cn,']',REPLACE(REPLACE(book_title, CHAR(10), ' '), CHAR(13), ' ')) as story_id_cn 
           from chapters_log.dts_project_v_new_book t1 
           left join chapters_log.language_info_v2 t2 on t1.lang=t2.name_en_short 
           union all select '',''
       )

       union all select 9 as id,'platform' as key,'分发平台' as name,'[{"key":1,"value":"Android"},{"key":2,"value":"IOS"},{"key":3,"value":"Windows"},{"key":4,"value":"MacOS"},{"key":5,"value":"PC/MAC"}]' as value

       union all 
       select 
              10 as id,'channel_id' as key,'分发渠道' as name
              ,concat('[',group_concat(distinct concat('{"key":"',id,'","value":"',concat(remark,'(',name,')'),'"}')   separator ',') ,']') as value 
       from chapters_log.channel_info

       union all
       select
              11 as id,'country_id' as key,'国家id' as name
              ,concat('[',group_concat( concat('{"key":"',country_id,'","value":"',name_en,'"}') order by uv desc separator ',') ,']') as value 
       from 
       (
              select
                     group_concat(distinct id) as country_id,
                     tier as name_en,
                     case   when tier = 'T1' then 9999999 
                            when tier = 'T2' then 9999998 
                            when tier = 'T3' then 9999997 
                            end as uv 
                     from chapters_log.country_info 
              where tier in ( 'T1', 'T2', 'T3' ) 
              group by tier 
              union all
              select
                     t2.id as country_id,
                     t2.name_en,
                     uv 
              from 
              (
                     select 
                            country_id,count(distinct uuid) uv 
                     from dws_data.dws_t80_reelshort_user_bhv_cnt_detail_di 
                     where etl_date>=date_sub(date(now()),3) group by country_id
              ) t1
              right join chapters_log.country_info t2 
              on t1.country_id = t2.id 
              group by t2.id,t2.name_en
              order by uv desc
       )

       union all 
       select
              12 as id,'language_id' as key,'语言id' name
              ,concat('[',group_concat(concat('{"key":"',id,'","value":"',value,'"}') order by id separator ',') ,']') as value 
       from 
       (select distinct id,name_cn,name_en,concat(name_cn,'(',name_en,')') as value from chapters_log.language_info_v2 order by id)

       union all 
       select
              13 as id,'version' as key,'APP主版本' name
              ,concat('[',group_concat(concat('{"key":"',version,'","value":"',version,'"}') order by version separator ',') ,']') as value 
       from 
       (select distinct name as version from chapters_log.dts_project_v_version order by version)

       union all 
       select
              14 as id,'af_network_name' as key,'归因投放渠道' name
              ,concat('[',group_concat(concat('{"key":"',t2.af_network_name,'","value":"',t2.af_network_name,'"}') order by uv desc,t2.af_network_name separator ',') ,']') as value 
       from 
       (
              select 
                     af_network_name,count(distinct uuid) uv 
              from dws_data.dws_t80_reelshort_user_bhv_cnt_detail_di 
              where etl_date>=date_sub(date(now()),3) group by af_network_name
       ) t1
       right join (select distinct af_network_name from dim_data.dim_t99_appsflyer_network_name where app_name='Reelshort' order by af_network_name union all select '') t2 -- 取自appsflyer表
       on t1.af_network_name = t2.af_network_name

       union all 
       select
              15 as id,'dlink_story_id' as key,'用户归因书籍' name
              ,concat('[',group_concat(concat('{"key":"',t2.dlink_story_id,'","value":"',t2.dlink_story_id,'"}') order by uv desc,t2.dlink_story_id separator ',') ,']') as value 
       from 
       (
              select 
                     dlink_story_id,count(distinct uuid) uv 
              from dws_data.dws_t80_reelshort_user_bhv_cnt_detail_di 
              where etl_date>=date_sub(date(now()),3) group by dlink_story_id
       ) t1
       right join (select dlink_story_id,deeplink_story from dim_data.dim_t99_reelshort_dlink_story_info) t2 
       on t1.dlink_story_id = t2.dlink_story_id

       union all select 16 as id,'user_type' as key,'新老用户' as name,'[{"key":1,"value":"新用户"},{"key":0,"value":"老用户"}]' as value
       union all select 17 as id,'is_pay' as key,'当天内购状态' as name,'[{"key":1,"value":"当天内购"},{"key":0,"value":"当天未内购"}]' as value
       union all select 18 as id,'vip_type' as key,'当天订阅状态' as name,'[{"key":0,"value":"非订阅用户"},{"key":1,"value":"订阅用户"},{"key":2,"value":"订阅过期"}]' as value
       union all select 19 as id,'is_pay_period' as key,'当期内购状态' as name,'[{"key":1,"value":"当天内购"},{"key":0,"value":"当天未内购"}]' as value
       union all select 20 as id,'vip_type_period' as key,'当期订阅状态' as name,'[{"key":1,"value":"当期订阅"},{"key":0,"value":"当期未订阅"}]' as value
       union all select 21 as id,'is_pay_lt' as key,'生命周期内购状态' as name,'[{"key":1,"value":"生命周期内购"},{"key":0,"value":"生命周期未内购"}]' as value
       union all select 22 as id,'vip_type_lt' as key,'生命周期订阅状态' as name,'[{"key":1,"value":"生命周期订阅"},{"key":0,"value":"生命周期未订阅"}]' as value

       -- union all 
       -- select
       --    23 as id,'campaign_id' as key,'投放计划' name
       --    ,concat('[',group_concat(concat('{"key":"',id,'","value":"',concat(name,'(',id,')'),'"}') order by updated_at desc separator ',') ,']') as value 
       -- from
       -- (
       --  select campaign_id as id,campaign_name as name,updated_at,row_number()over(partition by campaign_id order by updated_at desc  ) as rank 
       --  from dim_data.dim_t99_adjust_ad_info where campaign_id not in ('','0')
       -- ) 
       -- where rank=1

       -- union all 
       -- select
       --    24 as id,'ad_group_id' as key,'投放广告组' name
       --    ,concat('[',group_concat(concat('{"key":"',id,'","value":"',concat(name,'(',id,')'),'"}') order by updated_at desc separator ',') ,']') as value 
       -- from
       -- (
       --  select ad_group_id as id,ad_group_name as name,updated_at,row_number()over(partition by ad_group_id order by updated_at desc  ) as rank 
       --  from dim_data.dim_t99_adjust_ad_info where ad_group_id not in ('','0')
       -- ) 
       -- where rank=1

       -- union all 
       -- select
       --    25 as id,'ad_id' as key,'投放广告' name
       --    ,concat('[',group_concat(concat('{"key":"',id,'","value":"',concat(name,'(',id,')'),'"}') order by updated_at desc separator ',') ,']') as value 
       -- from
       -- (
       --  select ad_id as id,ad_name as name,updated_at,row_number()over(partition by ad_id order by updated_at desc  ) as rank 
       --  from dim_data.dim_t99_adjust_ad_info where ad_id not in ('','0')
       -- ) 
       -- where rank=1

       -- 1-新增、2-持续活跃(上一次活跃在1-6天前)、3-召回(上一次活跃在7-29天前)、4-回流(上一次活跃在30天前)
       union all select 26 as id,'active_user_type' as key,'活跃用户分类' as name,'[{"key":1,"value":"新增用户"},{"key":2,"value":"持续活跃用户"},{"key":3,"value":"召回用户"},{"key":4,"value":"回流用户"}]' as value

       union all 
       select
              27 as id,'channel_sku' as key,'平台计费点' name
              ,concat('[',group_concat(concat('{"key":"',channel_sku,'","value":"',channel_sku,'"}') order by channel_sku separator ',') ,']') as value 
       from 
       (
           select
               channel_sku
               ,if(sku_name rlike '赠送' and num>1,SUBSTRING_INDEX(sku_name,'-',2),sku_name) as sku_name
           from
           (
               select 
                   channel_sku,sku_name
                   ,row_number() over(partition by channel_sku order by sku_name) as rk 
                   ,count(*) over(partition by channel_sku) as num
               from 
               (
                   select
                       distinct 
                       t1.channel_sku
                       ,nvl(t2.sku_name,t1.sku_name) as sku_name
                   from 
                   (
                       select point as channel_sku,title as sku_name from chapters_log.dts_project_v_shopping_price_point -- 新sku表
                       union 
                       select distinct product_id as channel_sku,product_id as sku_name from chapters_log.dts_project_v_store -- 老sku表
                   ) t1 
                   left join dmd_data.dim_t99_reelshort_channel_sku_info_tmp t2 -- excel补数
                   on t1.channel_sku=t2.channel_sku
               )
           )
           where rk=1
           order by channel_sku
       )

       union all 
       select
              28 as id,'task_id' as key,'push任务id' name
              ,concat('[',group_concat(concat('{"key":"',id,'","value":"',concat(title,'(',id,')'),'"}') order by id separator ',') ,']') as value 
       from 
       (select distinct id,title from chapters_log.dts_pub_opt_notification_push_tasks order by id)

       union all 
       select
               29 as id,'gid' as key,'商品ID' name
               ,concat('[',group_concat(concat('{"key":"',t2.gid,'","value":"',concat(t2.gname,'(',t2.gid,')'),'"}') order by srv_pay_amount desc separator ',') ,']') as value 
       from 
       (
               select 
               gid,sum(srv_pay_amount) as srv_pay_amount
               from dws_data.dws_t85_reelshort_srv_order_detail_stat_di_v2 
               where etl_date>=date_sub(date(now()),3) 
               group by gid
       ) t1
       right join 
       (
               -- create view dw_view.dim_t99_reelshort_gid_info as 
               select 
               distinct coalesce(t2.gid,t1.gid) as gid
               ,coalesce(t2.gname,t1.gname) as gname
               from 
               (select gid,gname from chapters_log.dts_project_v_store) t1
               full join 
               (select id as gid,title as gname from chapters_log.dts_project_v_shopping_good) t2 on t1.gid = t2.gid

               union all
               select '',''
       ) t2 
       on t1.gid = t2.gid 

       union all select 30 as id,'nth_renewal' as key,'续订周期' as name,'[{"key":1,"value":"1"},{"key":2,"value":"2"},{"key":3,"value":"3"},{"key":4,"value":"4"},{"key":5,"value":"5"},{"key":6,"value":"6"},{"key":7,"value":"7"},{"key":8,"value":"8"},{"key":9,"value":"9"},{"key":10,"value":"10"},{"key":11,"value":"11"},{"key":12,"value":"12"}]' as value
       
       union all 
       select
              31 as id,'sku_price' as key,'商品价格' name
              ,concat('[',group_concat(concat('{"key":"',sku_price,'","value":"',sku_price,'"}') order by sku_price  separator ',') ,']') as value 
       from 
       (select distinct sku_price as sku_price from dws_data.dws_t85_reelshort_srv_order_detail_stat_di_v2)

       union all 
       select
              32 as id,'order_src' as key,'付费场景' name
              ,concat('[',group_concat(concat('{"key":"',order_src,'","value":"',value,'"}') order by order_src  separator ',') ,']') as value 
       from 
       (
              select order_src,order_src_name,concat(order_src_name,'(',order_src,')') as value 
              from 
              (
                     select order_src,order_src_name from dim_data.dim_t99_reelshort_order_src_info 
                     union select order_src,order_src_name from ana_data.dim_t99_reelshort_order_src_info 
              )
       ) -- ana_data.dim_t99_reelshort_order_src_info定时更新
     -- add by lcz
     union all
     select
           33 as id,'recall_level' as key,'召回路' as name
           ,CONCAT('[',group_concat(if(col='',CONCAT('{"key":"',"",'","value":"','其他','"}'),CONCAT('{"key":"',col,'","value":"',col,'"}')) order by col desc SEPARATOR ','),']') as value
     from (
           select
                 value as recall_level_list
           from dim_data.dim_t99_data5_tag_aggregate_configs_bak where key='recall_level'
     ) as t
     CROSS JOIN UNNEST(t.recall_level_list) as temp_table(col)
     union all
     select
           34 as id,'tab_name' as key,'频道页' as name
           ,CONCAT('[',group_concat(distinct CONCAT('{"key":"',t.tab_name,'","value":"',t.tab_name,'"}') order by CONCAT('{"key":"',t.tab_name,'","value":"',t.tab_name,'"}') desc SEPARATOR ','),']') as value
     from dw_view.dts_project_v_hall_v3 as t
     union all 
     select 35 as id,'is_free' as key,'是否付费' as name,'[{"key":0,"value":"付费剧集"},{"key":1,"value":"免费剧集"},{"key":"","value":"其他"}]' as value
     union all
     select 36 as id,'search_story_ids' as key,'搜索结果' as name,'[{"key":0,"value":"非空"},{"key":1,"value":"空"},{"key":"","value":"其他"}]' as value
     union all
     select 37 as id,'page_type' as key,'搜索结果' as name,'[{"key":"has_result","value":"has_result"},{"key":"no_result","value":"no_result"},{"key":"","value":"其他"}]' as value
)

select 
       id
       ,'cm1009' as app_id
       ,1 as category
       ,'' as source_table
       ,name
       ,key
       ,'' as comment
       ,'string'as target_type
       ,'option' as select_type
       ,value as target_enum
       ,cast(1 as tinyint) as status
       ,now() as created_at
       ,now() as updated_at
       ,'["IN","NOT IN"]' as target_operator
from dim_t99_tag_configs_target_enum
;

