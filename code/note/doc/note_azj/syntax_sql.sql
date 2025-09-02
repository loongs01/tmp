
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class Main {
    public static void main(String[] args) {
        LocalDate yesterday = LocalDate.now().minusDays(1);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        String formattedDate = yesterday.format(formatter);
        System.out.println(formattedDate);
    }
}







select now()
,date_format(now(),'yyyy-MM-dd')                        --返回值类型string
,to_date(paymentduedate)
--,datediff(current_timestamp(), current_date(),'dd')    ---报错（s1,s2类型不一致）
,datediff(current_timestamp(), now(),'dd')
FROM azods.ods_loverent_liquidation_repayment_schedule_da
where dt = '20240320'
limit 10
;


--------------
select
date_format(CURRENT_TIMESTAMP(),'yyyy-MM-dd')
,add_months(date_format(CURRENT_TIMESTAMP(),'yyyy-MM-dd'), -6)
,date_add(date_format(CURRENT_TIMESTAMP(),'yyyy-MM-dd'),-20)
,date_sub(date_format(CURRENT_TIMESTAMP(),'yyyy-MM-dd'),-20)
,CURRENT_TIMESTAMP()
,CURRENT_DATE()
from azods_dev.ods_zhongan_log_xflowcloud_xcx_dev_di_rt_view
 where dt='20240301'
 limit 10;

select
CURRENT_TIMESTAMP()
,now()
,add_months(date_format(now(),'yyyy-MM-dd'), -6)
,date_sub(date_format(CURRENT_TIMESTAMP(),'yyyy-MM-dd'),180)
,regexp_replace(date_format(now(),'yyyy-MM-dd'),'-','')     ------regexp
from azods_dev.ods_zhongan_log_xflowcloud_xcx_dev_di_rt_view
 where dt>'20240323'
 limit 10;


select
CURRENT_TIMESTAMP()
,now()
,add_months(date_format(now(),'yyyy-MM-dd'), -6)
,date_sub(date_format(CURRENT_TIMESTAMP(),'yyyy-MM-dd'),180)
,regexp_replace(date_format(now(),'yyyy-MM-dd'),'-','')
,regexp_extract(date_format(now(),'yyyy-MM-dd'),'([0-9]+)',0)
,regexp_extract(date_format(now(),'yyyy-MM-dd'),'([0-9]+)([0-9]+)([0-9]+)',2)
,regexp_extract_all(date_format(now(),'yyyy-MM-dd'),'([^-]+)',0)
from azods_dev.ods_zhongan_log_xflowcloud_xcx_dev_di_rt_view
 where dt>'20240323'
 limit 10

------------

select
CONCAT(null,'b','c') AS tag_value
from
 azads.ads_azj_tag_wechat_usr_rfm_td a
  where a.dt='20240411'
  limit 1;





select
dt
,create_timestamp
,to_timestamp(create_timestamp)
,to_char(to_timestamp(create_timestamp), 'YYYYMMDD')
,to_char(to_timestamp(create_timestamp), 'YYYYMMDDHH24MI') ---
from "holoads".ads_azj_tag_userid_mapping_5mi
where dt like '2024%'
and to_char(to_timestamp(create_timestamp), 'YYYYMMDD') like '2024%'

order by to_char(to_timestamp(create_timestamp), 'YYYYMMDD') desc


,to_char(to_timestamp(create_timestamp), 'YYYYMMDD HH24:MI')






select
to_char(rr1.applytime,'yyyy-mm-dd')
,count(1)
FROM    azods.ods_loverent_aliorder_new_rent_record_da AS rr1
WHERE   rr1.dt='20240423'

and to_char(rr1.applytime,'yyyy-mm-dd')>'2024-04-07'
group by to_char(rr1.applytime,'yyyy-mm-dd')





mysql
--
--设置变量
 set @row_number = 0;

SELECT
    t.*,
    @row_number := IF(@merchant_code = t.merchant_code, @row_number + 1, 1) AS serial_number,
    @merchant_code := t.merchant_code
FROM
    (SELECT * FROM ads_azj_activity_auto_goods_info_da ORDER BY merchant_code, apply_num_30d) as t;




   select @row_number from ads_azj_activity_auto_goods_info_da


 --变量直接赋值
 SELECT
    t.*,
    @row_number := IF(@merchant_code = t.merchant_code, @row_number + 1, 1) AS serial_number,
    @merchant_code := t.merchant_code
FROM
    (SELECT * FROM ads_azj_activity_auto_goods_info_da ORDER BY merchant_code, apply_num_30d) as t;



--正确写法：等同于row_number

 set @row_number = 0;    ----要赋值才准确
 set  @model_code ='a';  ---分组字段，要赋值才准确

SELECT
    t.*,
    @row_number := IF(@model_code = t.model_code, @row_number + 1, 1) AS serial_number,
    @model_code := t.model_code
FROM
    (SELECT * FROM ads_azj_activity_auto_goods_info_da ORDER BY model_code, apply_num_30d) as t;
                                                           model_code:分组字段  apply_num_30d：分组排序字段



   select @row_number ,@model_code ,a.*from ads_azj_activity_auto_goods_info_da a



mysql分组生成序列号
在MySQL中，可以使用变量来创建一个组内的序列号。以下是一个示例代码，它演示了如何为每个组生成一个递增的序列号：
SET @row_number := 0;
SET @group_id := NULL;

SELECT
    t.*,
    CASE
        WHEN @group_id = t.group_id THEN @row_number := @row_number + 1
        ELSE @row_number := 1
    END AS sequence_number,
    @group_id := t.group_id
FROM
    (SELECT group_id, other_column FROM your_table ORDER BY group_id, some_order_column) t;

在这个例子中，your_table 是你的数据表名，group_id 是你想要基于其进行分组的列，other_column 是该组中的其他列，some_order_column 是在同一组内部进行排序的列。

请根据你的具体需求调整表名、列名和排序方式。










    <if test="discountTypeList != null and discountTypeList.size()>0">
            and (
            <foreach collection="discountTypeList" index="index" item="item" separator="," open="(" close=")">
                <if test="index != 0">
                    or
                </if>
                <if test="item == 1">
                    is_down_payment_rent_coupon=1
                </if>
                <if test="item == 2">
                    is_rental_repayment_coupon=1
                </if>
                <if test="item == 3">
                    is_buyout_coupon=1
                </if>
                <if test="item == 5">
                    is_extend_return_coupon=1
                </if>
                <if test="item == 6">
                    is_total_rent_coupon=1
                </if>
            </foreach>
            )
        </if>


-- mysql
select concat('a',cast(CEILING(RAND()*90000+10000) as char))


delete from ads_azj_activity_auto_goods_info_time_config_da where dt<='${bizdate_7}' or dt='${bizdate}';
insert into ads_azj_activity_auto_goods_info_time_config_da (tb_desc,dt)
select '${bizdate}日期配置' as tb_desc,'${bizdate}' as dt
 on duplicate key update tb_desc='${bizdate}日期配置';


--azj
select
    t.table_name,
    t.table_comment,
    c.column_name,
    c.column_type,
    c.column_comment
from
    information_schema. tables t,
    information_schema. columns c
where
    c.table_name = t.table_name
and t.table_name = 'pbcc_report_basics';


--cms
select
    t.table_schema,
    t.table_name,
    t.table_comment,
    c.column_name,
    c.column_type,
    c.column_comment
from
    information_schema. tables t,
    information_schema. columns c
where t.table_schema='dws_data'
and c.table_name = t.table_name
and t.table_name = 'dws_t80_reelshort_user_bhv_cnt_detail_di';



select
    t.table_schema,
    t.table_name,
    t.table_comment,
    c.column_name,
    c.column_type,
    c.column_comment
from
    information_schema. tables t
    ,information_schema. columns c
where t.table_schema='dws_data'
and c.table_schema='dws_data'
and c.table_name = t.table_name
and t.table_name = 'dws_t80_reelshort_user_bhv_cnt_detail_di'
and c.table_name = 'dws_t80_reelshort_user_bhv_cnt_detail_di';



select
    t.table_schema,
    t.table_name,
    t.table_comment,
    c.column_name,
    c.column_type,
    c.column_comment
from
    information_schema. tables t
    ,information_schema. columns c
where t.table_schema=c.table_schema
and c.table_name = t.table_name
-- and t.table_schema='dws_data'
-- and t.table_name = 'dws_t80_reelshort_user_bhv_cnt_detail_di'
and c.column_comment like '%平台%'
limit 10




-- 要查看 MySQL 服务器的默认字符集和排序规则，可以使用以下命令：
SHOW VARIABLES LIKE 'character_set_server';
SHOW VARIABLES LIKE 'collation_server'; -- 查看排序规则

-- 查询MySQL的INT类型是有符号（SIGNED）或无符号（UNSIGNED）声明
SELECT 
    COLUMN_NAME,
    COLUMN_TYPE,
    IS_NULLABLE,
    COLUMN_DEFAULT,
    EXTRA,
    -- 使用CASE语句来检查COLUMN_TYPE中是否包含'UNSIGNED'
    CASE 
        WHEN COLUMN_TYPE LIKE '%UNSIGNED%' THEN 'UNSIGNED'
        ELSE 'SIGNED'
    END AS SIGNED_OR_UNSIGNED
FROM 
    INFORMATION_SCHEMA.COLUMNS
WHERE 
    TABLE_SCHEMA = 'dws_data' -- 替换为你的数据库名
    AND TABLE_NAME = 'dws_t82_reelshort_user_play_data5_detail_di'   -- 替换为你的表名
    AND DATA_TYPE = 'int';





select * from information_schema.PROCESSLIST where info like '%abnormal_orders%'


-- azj
select
t.table_schema          as '数据库'
,t.table_name           as '表名'
,t.table_type           as '表类型'
,t.table_rows           as '数据量'
,t.auto_increment       as '自增主键'
,t.create_time          as '创建时间'
,t.update_time          as '更新时间'
,t.table_comment        as '表注释'
-- ,t.data_length
,concat(truncate(data_length/1024/1024,2),'M')  as '表大小：单位M'
,t.check_time          as '表的检查时间'
from
    information_schema.tables t
where
t.table_schema = 'loverent_warehouse'
-- and t.update_time is not null
order by t.create_time desc,t.update_time desc
;



MySQL information_schema.table 查询表数据量
要查询MySQL中每个表的数据量，可以使用information_schema.tables表来获取。以下是一个示例SQL查询，它会返回所有表的表名和行数：
SELECT
    table_schema AS 'Database',
    table_name AS 'Table',
    table_rows AS 'Rows'
FROM
    information_schema.tables
WHERE
    table_schema = 'your_database_name'; -- 替换为你的数据库名

请注意，table_rows列提供的行数是一个估计值，对于非InnoDB表格来说可能不太准确。对于InnoDB表，你可以使用以下SQL命令来获取更准确的行数：

SELECT
    CONCAT('SELECT ''', table_name, ''' AS table_name, COUNT(*) AS count FROM `', table_name, '`;')
FROM
    information_schema.tables
WHERE
    table_schema = 'your_database_name'; -- 替换为你的数据库名
这将为每个表生成一个计数查询，你可以将这些查询复制并运行，以获取每个表的确切行数。



--mysql

select
count(1)
,max(createtime)
,min(createtime)
from loverent_warehouse.purchase_order_recv
group by SUBSTRING_INDEX(createtime,'-',1)
;
select
SUBSTRING_INDEX(createtime,'-',-1)
from loverent_warehouse.purchase_order_recv
limit 3

-- 2025
select 
count(1)
from 
(
select
cast(nvl(json_extract(properties,'$._url'),'') as varchar) as a
,SUBSTRING_INDEX(replace(cast(nvl(json_extract(properties,'$._url'),'')as varchar),concat('-',SUBSTRING_INDEX(cast(nvl(json_extract(properties,'$._url'),'') as varchar), '-', -1)),''),'.com/',-1)
,SUBSTRING_INDEX(cast(nvl(json_extract(properties,'$._url'),'')as varchar),'.com/',-1)
from chapters_log.public_event_data
where analysis_date='${TX_DATE}'
      and event_name in ('m_app_install','m_user_signin')
      and length(json_extract(properties,'$._url'))>0
--        and cast(nvl(json_extract(properties,'$._url'),'')as varchar) not like '%.com%'
group by 
cast(nvl(json_extract(properties,'$._url'),'') as varchar)
,SUBSTRING_INDEX(replace(cast(nvl(json_extract(properties,'$._url'),'')as varchar),concat('-',SUBSTRING_INDEX(cast(nvl(json_extract(properties,'$._url'),'') as varchar), '-', -1)),''),'.com/',-1)


)


select
 SUBSTRING_INDEX(a, '/', 3)
 ,a
from 
(select
cast(nvl(json_extract(properties,'$._referrer_url'),'') as varchar) as a
,properties
from chapters_log.public_event_data
where analysis_date='${TX_DATE}'
      and event_name in ('m_app_install','m_user_signin')
      and json_extract(properties,'$._referrer_url') not regexp 'google|direct|facebook|youtube|bing'
)
-- group by SUBSTRING_INDEX(a, '/', 3)
limit 300
;


-- 2025
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
length(group_concat(distinct if(action='player_success' and open_time<1800000,open_time) order by if(action='player_success' and open_time<1800000,open_time) asc separator ',')) as open_time_list -- 首帧耗时的中位数
,length(replace(group_concat(distinct if(action='player_success' and open_time<1800000,open_time) order by if(action='player_success' and open_time<1800000,open_time) asc separator ','),',',''))    --这里，我们通过计算原始字符串长度和去除所有逗号后的字符串长度之差，然后加1来得到元素数量
from  dwd_data.dwd_t82_reelshort_play_detail_di as t1
where analysis_date='${TX_DATE}'
group by
        analysis_date
        ,country_id
        ,os_type
limit 10
;



select
group_concat(distinct if(action='player_success' and open_time<1800000,open_time) order by if(action='player_success' and open_time<1800000,open_time) separator ',') as open_time_list -- 首帧耗时的中位数
,split(group_concat(distinct if(action='player_success' and open_time<1800000,open_time) order by if(action='player_success' and open_time<1800000,open_time) separator ','),',')
,SUBSTRING_INDEX(group_concat(distinct if(action='player_success' and open_time<1800000,open_time) order by if(action='player_success' and open_time<1800000,open_time) separator ','),',',1)


,SUBSTRING_INDEX(SUBSTRING_INDEX(group_concat(distinct if(action='player_success' and open_time<1800000,open_time) order by if(action='player_success' and open_time<1800000,open_time) separator ','),',',FLOOR(length(group_concat(distinct if(action='player_success' and open_time<1800000,open_time) order by if(action='player_success' and open_time<1800000,open_time) asc separator ','))
-length(replace(group_concat(distinct if(action='player_success' and open_time<1800000,open_time) order by if(action='player_success' and open_time<1800000,open_time) asc separator ','),',',''))/2)),',',-1)
,FLOOR(length(group_concat(distinct if(action='player_success' and open_time<1800000,open_time) order by if(action='player_success' and open_time<1800000,open_time) asc separator ','))
-length(replace(group_concat(distinct if(action='player_success' and open_time<1800000,open_time) order by if(action='player_success' and open_time<1800000,open_time) asc separator ','),',',''))/2)
from  dwd_data.dwd_t82_reelshort_play_detail_di as t1
where analysis_date='${TX_DATE}'
group by
        analysis_date
        ,country_id
        ,os_type
limit 10
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




select 
array_union(t.recall_level,t1.recall_level)
,if(t.recall_level is null or t1.recall_level is null ,COALESCE(t1.recall_level,t1.recall_level),array_union(t.recall_level,t1.recall_level))  as value
from (select 1 as key -- 1：历史全量，2：日增量
             ,value as recall_level
--              ,split(group_concat(distinct recall_level SEPARATOR ','),',') as recall_level        -- 召回路
      from dim_data.dim_t99_data5_tag_aggregate_configs_bak  -- 历史数据
--       where analysis_date='${TX_DATE}'
) as t
right join(select 1 as key
                 ,split(group_concat(distinct recall_level SEPARATOR ','),',') as recall_level        -- 召回路
          from dwd_data.dim_t80_reelshort_user_recall_level_di
          where analysis_date=DATE_SUB('${TX_DATE}',INTERVAL 1 DAY)
) as t1
on t1.key=t.key


-- select array_union(split('1,2',','),null)


