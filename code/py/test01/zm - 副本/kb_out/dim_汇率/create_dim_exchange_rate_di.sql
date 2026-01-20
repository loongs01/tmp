CREATE TABLE IF NOT EXISTS `dim_exchange_rate_di` (
  `dt` VARCHAR(50) COMMENT '日期（YYYYMMDD）',
  `rate_type` VARCHAR(50) COMMENT '汇率类型',
  `from_ccy` VARCHAR(50) COMMENT '从货币',
  `to_ccy` VARCHAR(50) COMMENT '最终货币',
  `start_date` VARCHAR(50) COMMENT '汇率起始日期',
  `raw_rate` DECIMAL(27,8) COMMENT '汇率（未转换因子）',
  `from_unit_rate` DECIMAL(27,8) COMMENT '来自货币单位的比率',
  `by` VARCHAR(256),
  `to_unit_rate` DECIMAL(27,8) COMMENT '到 货币单位汇率',
  `final_rate` DECIMAL(27,8) COMMENT '汇率',
  `insert_dt` DATETIME COMMENT '数仓数据更新时间'
) ENGINE=OLAP
PRIMARY KEY(`dt`, `rate_type`, `from_ccy`, `to_ccy`, `start_date`)
PARTITION BY RANGE(`dt`) ()
DISTRIBUTED BY HASH(`dt`) BUCKETS 10;