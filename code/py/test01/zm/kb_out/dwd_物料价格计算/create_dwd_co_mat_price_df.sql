CREATE TABLE IF NOT EXISTS `dwd_co_mat_price_df` (
  `mat_code` VARCHAR(100) COMMENT '物料编码',
  `factory_code` VARCHAR(100) COMMENT '工厂编码',
  `mat_name` VARCHAR(100) COMMENT '物料名称',
  `basic_unit` VARCHAR(100) COMMENT '基本计量单位',
  `mat_type_code` VARCHAR(100) COMMENT '物料类型编码',
  `mat_type_name` VARCHAR(100) COMMENT '物料类型名称',
  `mat_pur_type` VARCHAR(100) COMMENT '物料采购类型',
  `special_pur_type` VARCHAR(100) COMMENT '特殊采购类型',
  `price_unit` DECIMAL(27,8) COMMENT '价格单位',
  `moq_unit` DECIMAL(27,8) COMMENT 'MOQ',
  `max_unit_price` DECIMAL(27,8) COMMENT '最高价单个物料单价',
  `CNY` VARCHAR(256),
  `min_unit_price` DECIMAL(27,8) COMMENT '最低价单个物料单价',
  `latest_unit_price` DECIMAL(27,8) COMMENT '最近价单个物料单价',
  `std_unit_price` DECIMAL(27,8) COMMENT '标准价单个物料单价',
  `moving_avg_unit_price` DECIMAL(27,8) COMMENT '移动加权平均价单个物料单价',
  `po_est_unit_price` DECIMAL(27,8) COMMENT '采购预估单个物料单价',
  `MOQ` VARCHAR(256),
  `purchase_org_code` VARCHAR(256),
  `factory` VARCHAR(256),
  `dim` VARCHAR(256),
  `date` VARCHAR(256) COMMENT '补录价格有效期',
  `insert_dt` DATETIME COMMENT '数仓数据更新时间'
) ENGINE=OLAP
PRIMARY KEY(`mat_code`, `factory_code`)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(`mat_code`) BUCKETS 10;