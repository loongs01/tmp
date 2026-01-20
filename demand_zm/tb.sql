alter table `ods`.`ods_sap_erp_vbak_df` comment 'ods_sap_erp_销售凭证:抬头数据';


alter table `ods`.`ods_sap_erp_likp_df` modify column fkdiv string;



 
建表方式
        参考sql: 生成SQL
        参考基础表结构: https://u0vocx8xrmg.feishu.cn/sheets/N6KysSIynhNS0ftZPNrcLU15nDh?sheet=pSYbFS
        开发规范：https://u0vocx8xrmg.feishu.cn/docx/ElL7d87tfopxU3xWX4ic3brDniX
1、表编码
前缀：ods_系统简称_
后缀：_df（全量）
2、识别主键
如果源系统有明确主键：PRIMARY KEY(`mandt`)
如果没有：DUPLICATE KEY ( `id` ) 
3、表注释  comment 'ods_系统简称_来源表名'
4、最后新增创建时间字段
`insert_dt` datetime default current_timestamp comment '数仓数据更新时间'
5、副本数
测试环境："replication_num"="1"
6、所有表名、字段统一小写
7、erp数据过滤：MANDT='800'
8、导入前语句（全量）
truncate table ods.ods_sap_
9、记录数据量，如果数据量大，考虑做增量