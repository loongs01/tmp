源表表结构如下：

表名	Field	短文本
TCURF	MANDT	客户端
TCURF	KURST	汇率类型
TCURF	FCURR	从货币
TCURF	TCURR	最终货币
TCURF	GDATU	输入开始有效的日期
TCURF	FFACT	"来自"货币单位的比率
TCURF	TFACT	"到" 货币单位汇率
TCURF	ABWCT	可选汇率类型
TCURF	ABWGA	可选汇率类型的日期有效






根据上面源表字段注释内容
为如下starrocks建表语句，字段类型varchar类型统一改为string，DECIMAL类型统一改为decimal(38, 3)类型，补充表字段注释，不存在的字段注释为空
并去掉not null 约束，字段数量、字段名称严格保持不变，字段顺序保持不变,
末尾增加字段`insert_dt` datetime default current_timestamp comment '数仓数据更新时间'
如果存在字段命名中存在/，如：/BEV2/ED_AETIM，统一修改成下划线_,如BEV2_ED_AETIM，
表字段统一改为小写
primary key为前5个字段


create table `ods`.`ods_sap_erp_tcurf_df`  (
`MANDT` varchar(27) NOT NULL,
`KURST` varchar(36) NOT NULL,
`FCURR` varchar(45) NOT NULL,
`TCURR` varchar(45) NOT NULL,
`GDATU` varchar(72) NOT NULL,
`FFACT` DECIMAL(9, 0) NOT NULL,
`TFACT` DECIMAL(9, 0) NOT NULL,
`ABWCT` varchar(36) NOT NULL,
`ABWGA` varchar(72) NOT NULL,
`insert_dt` datetime default current_timestamp comment '数仓数据更新时间'
)
primary key(`mandt`)
comment 'ods_sap_erp_汇率转换因子'
distributed by hash(`mandt`)
properties (
    "replication_num" = "1"
);