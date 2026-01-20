CREATE TABLE IF NOT EXISTS `dim_product_info_df` (
  `pci_code` VARCHAR(100) COMMENT 'PCI编码',
  `pci_bom_code` VARCHAR(100) COMMENT 'PCI-BOM编码',
  `pci_label_start_date` DATETIME COMMENT 'PCI标签开始时间',
  `pci_tag` VARCHAR(100) COMMENT 'PCI标签',
  `product_catalog_code` VARCHAR(100) COMMENT '关联产业目录树编码',
  `industry_code` VARCHAR(100) COMMENT '产业编码',
  `industry_name` VARCHAR(100) COMMENT '产业名称',
  `product_line_code` VARCHAR(100) COMMENT '产品线编码',
  `product_line_name` VARCHAR(100) COMMENT '产品线名称',
  `product_group_code` VARCHAR(100) COMMENT '产品族编码',
  `product_group_name` VARCHAR(100) COMMENT '产品族名称',
  `product_series_code` VARCHAR(100) COMMENT '产品系列编码',
  `product_series_name` VARCHAR(100) COMMENT '产品系列名称',
  `product_mcm_type` VARCHAR(100) COMMENT '产品类型（模组整机）',
  `scene_type` VARCHAR(100) COMMENT '场景类型',
  `offering_code` VARCHAR(100) COMMENT 'Offering编码',
  `offering` VARCHAR(100) COMMENT 'Offering描述',
  `pci_short_desc` VARCHAR(100) COMMENT 'PCI短描述',
  `pitch_mm` DECIMAL(27,8) COMMENT '间距',
  `led_grade` VARCHAR(100) COMMENT '灯档次',
  `length_mm` DECIMAL(27,8) COMMENT '长',
  `width_mm` DECIMAL(27,8) COMMENT '宽',
  `product_area` DECIMAL(27,8) COMMENT '产品面积',
  `warranty_max_y` VARCHAR(100) COMMENT '质保上限'
) ENGINE=OLAP
PRIMARY KEY(`pci_code`, `pci_bom_code`, `pci_label_start_date`)
DISTRIBUTED BY HASH(`pci_code`) BUCKETS 10;