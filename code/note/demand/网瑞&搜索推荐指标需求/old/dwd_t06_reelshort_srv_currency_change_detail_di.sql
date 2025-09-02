-- Create Table if not exists dwd_data.dwd_t06_reelshort_srv_currency_change_detail_di (
--      _id varchar comment '源表mongodb主键'
--     ,uuid varchar comment '应用中用户ID'
--     ,analysis_date date comment '分析日期'
--     ,analysis_hour varchar comment '分析小时'
--     ,country_id varchar comment '国家id'
--     ,channel_id varchar comment '应用渠道(1:AVG10003,2:AVG20001)'
--     ,version varchar comment 'APP的应用版本(包体内置版本号versionname)'
--     ,cversion varchar comment '应用游戏版本号'
--     ,res_version varchar comment '应用资源版本号'
--     ,language_id varchar comment '游戏语言id'
--     ,platform varchar comment '平台id (1-安卓,2-IOS，3-windows,4-macOs)'
--     ,os_version varchar comment '操作系统版本号'
--     ,optime bigint comment '系统操作时间'
--     ,created_at varchar comment '入库时间戳'
--     ,device_id varchar comment '应用用于标识设备的唯一ID'
--     ,ad_id varchar comment '广告ID:Android=google adid IOS=idfa'
--     ,af_network_name varchar comment 'af归因渠道'
--     ,is_login int comment '是否登陆1-是'
--     ,user_type int comment '是否新用户1-是'
--     ,is_pay int comment '是否付费1-是'
--     ,change_reason varchar comment '变化原因（业务自定义）'
--     ,story_id varchar comment '书籍id'
--     ,chap_id varchar comment '章节id'
--     ,chap_order_id int comment '章节序列id'
--     ,book_title varchar comment '书籍名称'
--     ,coin_get int comment '获得金币'
--     ,coin_exp int comment '消耗金币'
--     ,coin_former int comment '金币变化前数量'
--     ,coin_latter int comment '金币变化后数量'
--     ,pay_bonus_get int comment '获得付费bonus'
--     ,pay_bonus_exp int comment '消耗付费bonus'
--     ,pay_bonus_former int comment '付费bonus变化前数量'
--     ,pay_bonus_latter int comment '付费bonus变化后数量'
--     ,free_bonus_get int comment '获得免费bonus'
--     ,free_bonus_exp int comment '消耗免费bonus'
--     ,free_bonus_former int comment '免费bonus变化前数量'
--     ,free_bonus_latter int comment '免费bonus变化后数量'
--     ,etl_date date comment '清洗日期'
--     ,primary key (etl_date,_id,uuid)
-- ) DISTRIBUTE BY HASH(uuid,etl_date) PARTITION BY VALUE(DATE_FORMAT(etl_date, '%Y%m')) LIFECYCLE 60 
--     INDEX_ALL='Y' STORAGE_POLICY='COLD' COMMENT='V项目dwd层:业务服用户虚拟币变化表';


-- delete from dwd_data.dwd_t06_reelshort_srv_currency_change_detail_di where etl_date='${TX_DATE}';
/*+LEFT_TO_RIGHT_ENABLED=true,CASCADES_OPTIMIZER_ENABLED=false*/
replace into dwd_data.dwd_t06_reelshort_srv_currency_change_detail_di
select _id
      ,t1.uuid 
      ,t1.analysis_date
      ,t1.analysis_hour
      ,coalesce(t3.country_id,'') as country_id
      ,coalesce(t2.id,'') as channel_id 
      ,t1.version 
      ,coalesce(t3.cversion,'') as cversion
      ,coalesce(t3.res_version,'') as res_version
      ,coalesce(t3.language_id,'') as language_id
      ,coalesce(t3.platform,'') as platform
      ,coalesce(t3.os_version,'') as os_version
      ,t1.optime
      ,t1.created_at
      ,t1.device_id 
      ,t3.ad_id      
      ,t3.af_network_name
      ,coalesce(t3.is_login,0) as is_login
      ,coalesce(t3.user_type,0) as user_type
      ,max(coalesce(if(change_reason in (3,11,16) and coin_get>0  ,1),t3.is_pay,0))over(partition by t1.analysis_date,t1.uuid) as is_pay
      ,t1.change_reason
      ,t1.story_id
      ,t1.chap_id
      ,t1.chap_order_id
      ,t1.book_title
      ,t1.coin_get
      ,t1.coin_exp
      ,t1.coin_former
      ,t1.coin_latter
      ,if(t1.opt_pb=0 ,t1.num_pb,0) as pay_bonus_get
      ,if(t1.opt_pb=1 ,t1.num_pb,0) as pay_bonus_exp
      ,t1.old_pb as pay_bonus_former      
      ,t1.new_pb as pay_bonus_latter
      ,if(t1.opt_fb=0 ,t1.num_fb,0) as free_bonus_get
      ,if(t1.opt_fb=1 ,t1.num_fb,0) as free_bonus_exp
      ,t1.old_fb as free_bonus_former
      ,t1.new_fb as free_bonus_latter
      ,t1.analysis_date as etl_date
      ,t3.first_visit_landing_page          as first_visit_landing_page          -- 首次访问着陆页
      ,t3.first_visit_landing_page_story_id as first_visit_landing_page_story_id -- 首次访问着陆页书籍id
      ,t3.first_visit_landing_page_chap_id  as first_visit_landing_page_chap_id  -- 首次访问着陆页章节id
      ,t4.recent_visit_source               as recent_visit_source               -- 最近一次访问来源
      ,t4.recent_visit_source_type          as recent_visit_source_type          -- 最近一次访问来源类型
      ,t5.book_type
      -- ,case when t1.coin_get>0 then 'IAPconins'
            -- when if(t1.opt_pb=0 ,t1.num_pb,0)>0 then '付费bonus'
            -- when if(t1.opt_fb=0 ,t1.num_fb,0)>0 then '免费bonus'
       -- end as coin_type -- 虚拟币类型  
	    ,case when t1.coin_get>0 then coin_get
         -- when if(t1.opt_pb=0 ,t1.num_pb,0)>0 then if(t1.opt_pb=0 ,t1.num_pb,0)
         -- when if(t1.opt_fb=0 ,t1.num_fb,0)>0 then if(t1.opt_fb=0 ,t1.num_fb,0)
        end as coin_type -- 消耗虚拟币  
	   
	   
	   
	   
from (select _id
      ,uid as uuid 
      ,DATE(convert_tz(FROM_UNIXTIME(optime),'Asia/Shanghai','America/Los_Angeles'))  as analysis_date
      ,hour(convert_tz(FROM_UNIXTIME(optime),'Asia/Shanghai','America/Los_Angeles'))  as analysis_hour
      ,channel_id
      ,optime 
      ,convert_tz(replace(substr(created_at,1,19),'T',' '),'UTC','America/Los_Angeles') AS created_at
      ,client_ver as version 
      ,dev_id as device_id 
      ,type  as change_reason
      ,cast(case when type=4 then json_extract(remark,'$.book_id')  end as varchar) as story_id
      ,cast(case when type=4 then json_extract(remark,'$.chapter_id') end as varchar) as chap_id
      ,cast(case when type=4 then json_extract(remark,'$.chapter_index') end as varchar) as chap_order_id
      ,cast(case when type=4 then json_extract(remark,'$.book_title') end as varchar) as book_title
      ,if(opt=0 , num,0) as coin_get
      ,if(opt=1 , num,0) as coin_exp
      ,cast(old as int) as coin_former
      ,cast(new as int) as coin_latter
      ,if ( bonus_change_log like '[{%' , CAST(json_extract(element_at(CAST(bonus_change_log AS array<string>),1) , '$.opt') as int) ,  CAST(json_extract( element_at(CAST(element_at(CAST(bonus_change_log AS array<string>),1) as array<string>),2) ,'$.Value') as int))as opt_pb
      ,if ( bonus_change_log like '[{%' , CAST(json_extract(element_at(CAST(bonus_change_log AS array<string>),1) , '$.num') as int) ,  CAST(json_extract( element_at(CAST(element_at(CAST(bonus_change_log AS array<string>),1) as array<string>),3) ,'$.Value') as int))as num_pb
      ,if ( bonus_change_log like '[{%' , CAST(json_extract(element_at(CAST(bonus_change_log AS array<string>),1) , '$.new') as int) ,  CAST(json_extract( element_at(CAST(element_at(CAST(bonus_change_log AS array<string>),1) as array<string>),5) ,'$.Value') as int))as new_pb
      ,if ( bonus_change_log like '[{%' , CAST(json_extract(element_at(CAST(bonus_change_log AS array<string>),1) , '$.old') as int) ,  CAST(json_extract( element_at(CAST(element_at(CAST(bonus_change_log AS array<string>),1) as array<string>),4) ,'$.Value') as int))as old_pb
      ,if ( bonus_change_log like '[{%' , CAST(json_extract(element_at(CAST(bonus_change_log AS array<string>),2) , '$.opt') as int) ,  CAST(json_extract( element_at(CAST(element_at(CAST(bonus_change_log AS array<string>),2) as array<string>),2) ,'$.Value') as int))as opt_fb
      ,if ( bonus_change_log like '[{%' , CAST(json_extract(element_at(CAST(bonus_change_log AS array<string>),2) , '$.num') as int) ,  CAST(json_extract( element_at(CAST(element_at(CAST(bonus_change_log AS array<string>),2) as array<string>),3) ,'$.Value') as int))as num_fb
      ,if ( bonus_change_log like '[{%' , CAST(json_extract(element_at(CAST(bonus_change_log AS array<string>),2) , '$.new') as int) ,  CAST(json_extract( element_at(CAST(element_at(CAST(bonus_change_log AS array<string>),2) as array<string>),5) ,'$.Value') as int))as new_fb
      ,if ( bonus_change_log like '[{%' , CAST(json_extract(element_at(CAST(bonus_change_log AS array<string>),2) , '$.old') as int) ,  CAST(json_extract( element_at(CAST(element_at(CAST(bonus_change_log AS array<string>),2) as array<string>),4) ,'$.Value') as int))as old_fb
from chapters_log.dts_project_v_account_log
-- WHERE  DATE(convert_tz(FROM_UNIXTIME(optime),'Asia/Shanghai','America/Los_Angeles'))='${TX_DATE}'
where 
    optime>=UNIX_TIMESTAMP(convert_tz('${TX_START_TIMESTAMP}','America/Los_Angeles','Asia/Shanghai'))
and optime<=UNIX_TIMESTAMP(convert_tz('${TX_END_TIMESTAMP}','America/Los_Angeles','Asia/Shanghai'))
-- and channel_id in  ('AVG10003','AVG20001','WEB41001') 
and uid not in (
      -- SELECT distinct uid FROM chapters_log.dts_project_v_account_log WHERE channel_id  in ('TEST19999','TEST20000','TEST41001')
      -- and optime<=UNIX_TIMESTAMP(convert_tz('${TX_END_TIMESTAMP}','America/Los_Angeles','Asia/Shanghai'))
      select distinct uid from chapters_log.dts_project_v_user where channel_id like 'TEST%'
           and replace(substr(created_at,1,19),'T',' ')<convert_tz('${TX_END_TIMESTAMP}','America/Los_Angeles','Asia/Shanghai')
     )
and uid not in ('120699603','10058440','123604418')
) t1 
left join (select uuid
                  ,platform
                  ,country_id
                  -- ,channel_id
                  ,language_id
                  -- ,version
                  ,cversion
                  ,af_network_name
                  ,is_login
                  ,user_type  
                  ,is_pay
                  ,res_version
                  ,os_version
                  ,ad_id
                  ,etl_date
                  ,first_visit_landing_page          -- 首次访问着陆页
                  ,first_visit_landing_page_story_id -- 首次访问着陆页书籍id
                  ,first_visit_landing_page_chap_id  -- 首次访问着陆页章节id
          from dwd_data.dim_t99_reelshort_user_lt_info
          where etl_date='${TX_DATE}' 
)t3
on t1.uuid=t3.uuid and t1.analysis_date=t3.etl_date
left join chapters_log.channel_info t2 
on t1.channel_id=t2.name
left join(
         select distinct etl_date 
               ,uuid
               ,recent_visit_source -- 最近一次访问来源
               ,recent_visit_source_type -- 最近一次访问来源类型
         from dwd_data.dwd_t01_reelshort_user_detail_info_di
         where etl_date='${TX_DATE}'
) as t4 
on t4.uuid=t1.uuid and t1.analysis_date=t4.etl_date
left join (
           select distinct id
           ,book_type 
            from dim_data.dim_t99_reelshort_book_info 
) as t5
on t5.id=t1.story_id
;