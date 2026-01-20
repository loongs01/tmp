truncate table ods.ods_sap_erp_kna1_df;



ALTER TABLE dim.dim_product_info_df RENAME dim_product_info_df_bak;



-- alter table ods.ods_sap_erp_likp_df modify column fkdiv string;

ALTER TABLE ods.ods_sap_erp_vbak_df COMMENT 'ods_sap_erp_销售凭证:抬头数据';

select current_version();




set global enable_group_execution=false;

命令说明
set global：表示这是一个全局级别的配置修改，会对整个集群生效（所有 FE 节点）。

enable_group_execution：是 StarRocks 的一个查询执行优化参数。

= false：表示关闭该功能。

功能作用
组执行（Group Execution） 是 StarRocks 的查询优化技术，主要特点：

将多个相似的查询分组合并执行，减少重复计算

提高并发查询的吞吐量

特别适合 OLAP 场景中多个相似查询同时到达的情况
可以使用 SHOW VARIABLES LIKE 'enable_group_execution'; 查看当前值


ALTER TABLE ods.ods_oa_view_hrmresource_df 
MODIFY COLUMN `adsjgs` varchar(65533) NULL COMMENT 'AD同步上级公司',
MODIFY COLUMN `adgs` varchar(65533) NULL COMMENT 'AD同步公司',
MODIFY COLUMN `adbm` varchar(65533) NULL COMMENT 'AD同步部门';




-- db2
SELECT 
    t.TABSCHEMA AS table_schema,
    t.TABNAME AS table_name,
    t.REMARKS AS table_comment,  -- 表注释可能直接在这里
    c.COLNAME AS column_name,
    c.TYPENAME AS column_type,
    c.REMARKS AS column_comment,  -- 列注释可能直接在这里
    c.COLNO AS ordinal_position
FROM 
    SYSCAT.TABLES t
    JOIN SYSCAT.COLUMNS c ON t.TABSCHEMA = c.TABSCHEMA AND t.TABNAME = c.TABNAME
WHERE 
     t.TABSCHEMA = 'SAPPRD'
     AND t.TABNAME = 'KNA1'
--    c.REMARKS LIKE '%平台%'  
ORDER BY c.COLNO asc



-- 查看表的列结构信息
SELECT 
    COLNAME AS "列名",
    TYPENAME AS "数据类型",
    LENGTH AS "长度",
    SCALE AS "小数位",
    CASE NULLS 
        WHEN 'Y' THEN '允许' 
        ELSE '不允许' 
    END AS "空值",
    COALESCE(DEFAULT, '无') AS "默认值",
    CASE 
        WHEN KEYSEQ > 0 THEN '是(' || KEYSEQ || ')'
        ELSE '否'
    END AS "主键"
-- SELECT *
FROM SYSCAT.COLUMNS 
WHERE TABSCHEMA = 'SAPPRD' 
  AND TABNAME = 'KNA1'
ORDER BY COLNO;



若sap源数据采集过程，日期格式数据，遇到逆算日期问题：
 逆算日期 = 99999999 - YYYYMMDD，如99999999 - 20010101 = 79799168 做实际存储日期，
可以在数据同步时做下转换处理：DATE(TO_DATE(CHAR((99999999 - CAST(GDATU AS INTEGER))), 'YYYYMMDD'))

-- db2数据库
select 
     gdatu
     ,(99999999 - cast(gdatu as integer)) as new_gdatu
     ,date(to_date(char((99999999 - cast(gdatu as integer))), 'yyyymmdd')) as new_gdatu_date
from tcurr
