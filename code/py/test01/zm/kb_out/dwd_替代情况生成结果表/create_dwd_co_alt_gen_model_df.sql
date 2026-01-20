CREATE TABLE IF NOT EXISTS `dwd_co_alt_gen_model_df` (
  `dt` VARCHAR(100) COMMENT '日期（分区字段）',
  `pci_bom_code` VARCHAR(100) COMMENT 'PCI-BOM编码',
  `replace_rule_code` VARCHAR(500) COMMENT '替代规则编码',
  `replace_mat_group_code` VARCHAR(1000) COMMENT '替代物料组合编码',
  `idnrk_s` VARCHAR(256),
  `PCI` VARCHAR(256),
  `original_rule_code` VARCHAR(500) COMMENT '原始物料编码',
  `original_mat_flag` VARCHAR(100) COMMENT '原始物料标识',
  `light_mat_code` VARCHAR(100) COMMENT '灯珠物料编码',
  `ic_mat_code` VARCHAR(100) COMMENT '恒流IC物料编码',
  `power_mat_code` VARCHAR(100) COMMENT '电源物料编码',
  `card_mat_code` VARCHAR(100) COMMENT '接收卡物料编码',
  `insert_dt` DATETIME COMMENT '数仓数据更新时间'
) ENGINE=OLAP
PRIMARY KEY(`pci_bom_code`, `replace_rule_code`)
PARTITION BY RANGE(`dt`) ()
DISTRIBUTED BY HASH(`pci_bom_code`) BUCKETS 10;