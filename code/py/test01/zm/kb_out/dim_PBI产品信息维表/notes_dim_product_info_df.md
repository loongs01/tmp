# dim_PBI产品信息维表 开发说明

## 目标表

- 英文表名: `dim_product_info_df`
- 中文含义: dim_PBI产品信息维表
- 用途: dimension
- 建议业务主键: `Unnamed: 2`

## 数据来源表

```
来源系统 英文表名 中文含义 表别名 备注 产品主数据流转现状：PBI→PDM→SAP
pbi ods_pbi_view_unilumin_pci_df ods_pbi_PCI信息 pci
pbi ods_pbi_view_unilumin_product_catalog_df ods_pbi_产业目录树 cat
```

## 模型逻辑概述（原文）

```
序号 英文表名 中文含义 应用场景 备注
1 dim_product_info_df dim_PBI产品信息维表
筛选条件
表关联条件
以pci表为主表，关联cat表
pci.product_catalog_code=cat.identifier_no
```

## 目标表字段结构（解析记忆）

| 字段编码 | 字段类型 | 主键 | 非空 | 字段说明 | 来源表 | 来源字段 | 计算逻辑 |
|----------|----------|------|------|----------|--------|----------|----------|
| `pci_code` | VARCHAR(100) | Y |  | PCI编码 |  |  |  |
| `pci_bom_code` | VARCHAR(100) | Y |  | PCI-BOM编码 |  |  |  |
| `pci_label_start_date` | DATETIME | Y |  | PCI标签开始时间 |  |  |  |
| `pci_tag` | VARCHAR(100) |  |  | PCI标签 |  |  |  |
| `product_catalog_code` | VARCHAR(100) |  |  | 关联产业目录树编码 |  |  |  |
| `industry_code` | VARCHAR(100) |  |  | 产业编码 |  |  |  |
| `industry_name` | VARCHAR(100) |  |  | 产业名称 |  |  |  |
| `product_line_code` | VARCHAR(100) |  |  | 产品线编码 | L3 | 5 |  |
| `product_line_name` | VARCHAR(100) |  |  | 产品线名称 |  |  |  |
| `product_group_code` | VARCHAR(100) |  |  | 产品族编码 |  |  |  |
| `product_group_name` | VARCHAR(100) |  |  | 产品族名称 |  |  |  |
| `product_series_code` | VARCHAR(100) |  |  | 产品系列编码 | L3 | 5 |  |
| `product_series_name` | VARCHAR(100) |  |  | 产品系列名称 |  |  |  |
| `product_mcm_type` | VARCHAR(100) |  |  | 产品类型（模组整机） |  |  |  |
| `scene_type` | VARCHAR(100) |  |  | 场景类型 |  |  |  |
| `offering_code` | VARCHAR(100) |  |  | Offering编码 |  |  |  |
| `offering` | VARCHAR(100) |  |  | Offering描述 |  |  |  |
| `pci_short_desc` | VARCHAR(100) |  |  | PCI短描述 |  |  |  |
| `pitch_mm` | DECIMAL(27,8) |  |  | 间距 |  |  |  |
| `led_grade` | VARCHAR(100) |  |  | 灯档次 |  |  |  |
| `length_mm` | DECIMAL(27,8) |  |  | 长 |  |  |  |
| `width_mm` | DECIMAL(27,8) |  |  | 宽 |  |  |  |
| `product_area` | DECIMAL(27,8) |  |  | 产品面积 |  |  | length_mm*width_mm |
| `warranty_max_y` | VARCHAR(100) |  |  | 质保上限 |  |  |  |

## 模型逻辑详情（原文备忘）

```
模型标准 pbi
序号 字段说明 字段编码 字段类型 维度/度量 主键 值域 非空 获取方式 来源表 来源字段 来源字段名称 来源字段类型 来源字段说明 计算逻辑
1 PCI编码 pci_code varchar(100) Y 直接获取 pbi pci_code PCI编码
2 PCI-BOM编码 pci_bom_code varchar(100) Y 直接获取 pbi cip_code PCI-标准BOM编码
3 PCI标签开始时间 pci_label_start_date date Y 直接获取 pbi official_effective_time 正式生效时间
4 PCI标签 pci_tag varchar(100) 直接获取 pbi sales_status 销售状态
5 关联产业目录树编码 product_catalog_code varchar(100) 直接获取 pbi product_catalog_code 关联产业目录树编码
6 产业编码 industry_code varchar(100) 直接获取 cat industry_code 产业编码
7 产业名称 industry_name varchar(100) 直接获取 cat industry_name 行业名称（产业中文描述）
8 产品线编码 product_line_code varchar(100) 直接获取 cat technology_domain_code 技术领域编码（产品线编码） 软件从L3和L3.5判断：自研软件
9 产品线名称 product_line_name varchar(100) 直接获取 cat technology_domain_name 技术领域名称（产品线中文描述）
10 产品族编码 product_group_code varchar(100) 直接获取 cat technology_family_code 技术族编码（产品族编码）
11 产品族名称 product_group_name varchar(100) 直接获取 cat technology_family_name 技术族名称（产品族中文描述）
12 产品系列编码 product_series_code varchar(100) 直接获取 cat series_code 系列编码（产品系列编码） 软件从L3和L3.5判断：software
13 产品系列名称 product_series_name varchar(100) 直接获取 cat series_name 系列名称（产品系列中文描述）
14 产品类型（模组整机） product_mcm_type varchar(100) 直接获取 pbi product_template 产品模板
15 场景类型 scene_type varchar(100) 直接获取 pbi scene_type 场景类型
16 Offering编码 offering_code varchar(100) 直接获取 pbi offering_code offering编码
17 Offering描述 offering varchar(100) 直接获取 pbi offering_model 所属Offering型号
18 PCI短描述 pci_short_desc varchar(100) 直接获取 pbi pci_short_desc PCI短描述
20 间距 pitch_mm decimal(27,8) 直接获取 pbi spacing 间距（mm）
21 灯档次 led_grade varchar(100) 直接获取 pbi led_config LED配置
22 产品尺寸：长 length_mm decimal(27,8) 直接获取 pbi product_length 产品尺寸：长
23 产品尺寸：宽 width_mm decimal(27,8) 直接获取 pbi product_width 产品尺寸：宽
24 产品面积 product_area decimal(27,8) 计算 length_mm*width_mm
25 质保上限 warranty_max_y varchar(100) 直接获取 pbi warranty_upper_limit 质保上限
```

## 建议开发步骤

- 解析数据来源表/模型逻辑概述/模型逻辑详情三段内容，锁定目标表与字段。
- 校验字段类型、主键、分区列，生成 StarRocks DDL（可选执行）。
- 生成 INSERT SELECT 逻辑，按业务逻辑完成关联、去重、计算。
- 落地中间结果或目标表，补充数据质量校验与唯一性检查。
- 补充自动化脚本与回填验证（行数、键唯一性、NULL 分布）。
