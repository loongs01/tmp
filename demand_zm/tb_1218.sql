
CREATE TABLE IF NOT EXISTS dim.dim_exchange_rate_di (
    dt INT COMMENT '日期（YYYYMMDD）',
    rate_type VARCHAR(50) COMMENT '汇率类型',
    from_ccy VARCHAR(50) COMMENT '从货币',
    to_ccy VARCHAR(50) COMMENT '最终货币',
    start_date VARCHAR(50) COMMENT '汇率起始日期',
    raw_rate DECIMAL(27,8) COMMENT '汇率（未转换因子）',
    from_unit_rate DECIMAL(27,8) COMMENT '来自货币单位的比率',
    to_unit_rate DECIMAL(27,8) COMMENT '到货币单位汇率',
    final_rate DECIMAL(27,8) COMMENT '汇率',
    insert_dt DATETIME COMMENT '数仓数据更新时间'
) ENGINE=OLAP
PRIMARY KEY(dt, rate_type, from_ccy, to_ccy)
COMMENT 'dim_汇率'
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(rate_type, from_ccy, to_ccy)
PROPERTIES (
    "compression" = "LZ4",
    "enable_persistent_index" = "true",
    "fast_schema_evolution" = "true",
    "replicated_storage" = "true",
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "32"
    -- 注意：移除了 time_format 参数
);


DB2
10.1.1.39 5912
数据库名：prd
账号：db2user/db2user


STARROCKS_CONFIG = {
    "host": "10.2.8.36",
    "port": 9030,
    "user": "root",
    "password": "iPXE83EEZSrOBUfe",
    "database": "test",
    "charset": "utf8"
}

开发一个python脚本同步DB2 SAPPRD库一个表到starrocks库ods库