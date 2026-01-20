# dwd_出货明细模型 开发说明

## 目标表

- 英文表名: `dwd_sd_shipment_detail_df`
- 中文含义: dwd_出货明细模型
- 用途: table
- 建议业务主键: `Unnamed: 2`

## 模型逻辑概述（原文）

```
序号 英文表名 中文含义 应用场景 备注
1 dwd_sd_shipment_detail_df dwd_出货明细模型 发货单关联销售订单的数据，
未添加业绩拆分但包含手工业绩，用字段【场景标签】=“I手工业绩”标识；且对子公司客户以及康利子客户打上标签 表关联条件看以下单元格第一次出现的关联条件 1. 以发货行表为主表，关联发货头表；再关联销售订单行表，再关联销售订单头表
2. 对于销售订单/出货长文本，SAP以RFC形式给出，中台提供销售订单号VBAK-【VBELN】/交货单号LIPS-【VBELN】
--------------------------
最终筛选：
1.国际：销售组织=1010\6030\6010
2.国内：销售组织=1000\8500\8600\6020\7000\8300\7200\8200且分销渠道=10(显示)\2开头(照明)
注：对应公司代码：1000/6000/7000/7200/8200/8300/8500/8600
1. 前端查询日期BY实际交货日期
筛选条件
1. 排除销售订单行拒绝(VBAP-【ABGRU】<>"")的数据
2. 排除工厂VBAP-【WERKS】=1030{洲明科技景观照明事业部}
3. 筛选订单类型VBAK-【AUART】<>ZMOR/ZB2B/ZMO1/ZMRE/ZMHH/ZM05/ZM07/ZMCE/ZMDR  注：ZMCE/ZMDR（只有金额没有数量）
4. 排除合同编号（VBAK-BSTNK）为P开头，且订单类型（VBAK-AUART）为ZMRE的数据
表关联条件
1、表关联条件看以下单元格第一次出现的关联条件
```

## 目标表字段结构（解析记忆）

| 字段编码 | 字段类型 | 主键 | 非空 | 字段说明 | 来源表 | 来源字段 | 计算逻辑 |
|----------|----------|------|------|----------|--------|----------|----------|
| `delivery_order_num` | VARCHAR(100) |  |  | 交货单号 | LIPS | VBELN |  |
| `delivery_order_item_num` | VARCHAR(100) |  |  | 交货单行号 |  |  |  |
| `order_num` | VARCHAR(100) |  |  | 销售订单号 | LIPS | VGBEL |  |
| `order_item_num` | VARCHAR(100) |  |  | 销售订单行号 | LIPS | VGPOS |  |
| `manu_sales_order_num` | VARCHAR(100) |  |  | 制造公司销售订单号 |  |  |  |
| `order_num_transfer` | VARCHAR(100) |  |  | 销售订单号_含抛转 |  |  |  |
| `contract_num` | VARCHAR(100) |  |  | 合同号（客户采购订单编号） |  |  |  |
| `contract_m_num` | VARCHAR(100) |  |  | 母合同号 |  |  |  |
| `order_type_code` | VARCHAR(100) |  |  | 订单类型编码 |  |  |  |
| `order_type_name` | VARCHAR(255) |  |  | new 订单类型名称 |  |  |  |
| `shipment_date` | DATETIME |  |  | 出货日期 |  |  |  |
| `request_del_date` | DATETIME |  |  | 请求交货日期 |  |  |  |
| `approval_date` | DATETIME |  |  | 订单首次审批日期 |  |  |  |
| `create_date` | DATETIME |  |  | new 订单创建日期 |  |  |  |
| `latest_warehousing_date` | DATETIME |  |  | 最新入库日期 |  |  |  |
| `MSEG` | VARCHAR(256) |  |  |  |  |  |  |
| `ON` | VARCHAR(256) |  |  | MSEG | MSEG | MAT_KDAUF |  |
| `BWART` | VARCHAR(256) |  |  |  |  |  |  |
| `ZSD068VA` | VARCHAR(256) |  |  |  |  |  |  |
| `ZSD124VA` | VARCHAR(256) |  |  |  |  |  |  |
| `ON` | VARCHAR(256) |  |  | MSEG | MSEG | MAT_KDAUF |  |
| `BWART` | VARCHAR(256) |  |  |  |  |  |  |
| `MSEG` | VARCHAR(256) |  |  |  |  |  |  |
| `BWART` | VARCHAR(256) |  |  |  |  |  |  |
| `mat_code` | VARCHAR(100) |  |  | 物料编码 |  |  |  |
| `mat_desc` | VARCHAR(255) |  |  | 物料描述 | MAKT | MATNR |  |
| `industry_code` | VARCHAR(100) |  |  | 产业编码 |  |  |  |
| `industry_name` | VARCHAR(255) |  |  | 产业描述 |  |  |  |
| `product_line_code` | VARCHAR(100) |  |  | 产品线编码 |  |  |  |
| `product_line_name` | VARCHAR(255) |  |  | 产品线描述 |  |  |  |
| `product_group_code` | VARCHAR(100) |  |  | 产品族编码 |  |  |  |
| `product_group_name` | VARCHAR(255) |  |  | 产品族描述 |  |  |  |
| `product_series_code` | VARCHAR(100) |  |  | 产品系列编码 |  |  |  |
| `product_series_name` | VARCHAR(255) |  |  | 产品系列描述 |  |  |  |
| `product_area` | DECIMAL(27,8) |  |  | 产品单位面积 |  |  |  |
| `product_type` | VARCHAR(100) |  |  | 产品类型 |  |  |  |
| `industry_type` | VARCHAR(100) |  |  | 行业类型 |  |  |  |
| `special_stock_flag` | VARCHAR(100) |  |  | 特殊库存标识 |  |  |  |
| `delivery_address` | VARCHAR(255) |  |  | 交货地址 | ods | ods_sap_erp_zhone_text_get_vbbk_di |  |
| `special_stock_name` | VARCHAR(100) |  |  | 特殊库存标识名称 |  |  |  |
| `factory_code` | VARCHAR(100) |  |  | 工厂编码 |  |  |  |
| `factory_name` | VARCHAR(255) |  |  | 工厂名称 |  |  |  |
| `company_code` | VARCHAR(100) |  |  | 公司代码 |  |  |  |
| `company_name` | VARCHAR(255) |  |  | 公司名称 |  |  |  |
| `customer_code` | VARCHAR(100) |  |  | 客户编码 |  |  |  |
| `customer_name` | VARCHAR(255) |  |  | 客户名称 |  |  |  |
| `sales_person_num` | VARCHAR(100) |  |  | new 业务员编码 |  |  |  |
| `sales_person_name` | VARCHAR(255) |  |  | 业务员姓名 |  |  |  |
| `sales_dept_code` | VARCHAR(100) |  |  | new 销售部门编码 |  |  |  |
| `sales_dept_name` | VARCHAR(255) |  |  | 销售部门名称 |  |  |  |
| `employee_dept_code` | VARCHAR(100) |  |  | 雇员部门编码 |  |  |  |
| `employee_dept_name` | VARCHAR(255) |  |  | 雇员部门名称 |  |  |  |
| `sales_org_code` | VARCHAR(100) |  |  | new 销售组织编码 |  |  |  |
| `sales_org_name` | VARCHAR(255) |  |  | 销售组织名称 |  |  |  |
| `sales_group_code` | VARCHAR(100) |  |  | new 销售组 |  |  |  |
| `sales_group_name` | VARCHAR(255) |  |  | 销售组名称 |  |  |  |
| `sales_channel_code` | VARCHAR(100) |  |  | new 分销渠道编码 |  |  |  |
| `sales_channel_name` | VARCHAR(255) |  |  | 分销渠道名称 |  |  |  |
| `sales_unit_name` | VARCHAR(255) |  |  | new 销售战区名称 |  |  |  |
| `sales_area_name` | VARCHAR(255) |  |  | new 销售大区名称 |  |  |  |
| `sales_region_name` | VARCHAR(255) |  |  | new 销售区域名称 |  |  |  |
| `customer_country_code` | VARCHAR(100) |  |  | 客户所属国家编码 |  |  |  |
| `customer_country_name` | VARCHAR(255) |  |  | 客户所属国家名称 |  |  |  |
| `freight_forwarder` | VARCHAR(100) |  |  | 货代 | LIKP | VBELN |  |
| `transport_method` | VARCHAR(100) |  |  | 运输方式 |  |  |  |
| `payment_method` | VARCHAR(100) |  |  | 付款方式 |  |  |  |
| `embezzle_spot_info` | VARCHAR(100) |  |  | 挪用现货信息 |  |  |  |
| `is_need_inspection` | VARCHAR(100) |  |  | 是否需要验收 |  |  |  |
| `is_inspected` | VARCHAR(100) |  |  | 是否已验收 |  |  |  |
| `LK` | VARCHAR(256) |  |  | LIKP |  |  |  |
| `distinct` | VARCHAR(256) |  |  | select |  |  |  |
| `from` | VARCHAR(256) |  |  |  |  |  |  |
| `JOIN` | VARCHAR(256) |  |  | LEFT | V1 | VBELN |  |
| `JOIN` | VARCHAR(256) |  |  | LEFT | V2 | SFAKN |  |
| `IS` | VARCHAR(256) |  | Y | SFAKN | V2 | SFAKN |  |
| `LEFT` | VARCHAR(256) | Y |  | )T | T | LEFT |  |
| `inspection_order_num` | VARCHAR(100) |  |  | 验收单号 |  |  |  |
| `distinct` | VARCHAR(256) |  |  | select |  |  |  |
| `from` | VARCHAR(256) |  |  |  |  |  |  |
| `JOIN` | VARCHAR(256) |  |  | LEFT | V1 | VBELN |  |
| `JOIN` | VARCHAR(256) |  |  | LEFT | V2 | SFAKN |  |
| `IS` | VARCHAR(256) |  | Y | SFAKN | V2 | SFAKN |  |
| `LEFT` | VARCHAR(256) |  |  | )T | T | LEFT |  |
| `VBELN` | VARCHAR(256) |  |  |  |  |  |  |
| `inspection_invoice_date` | DATETIME |  |  | 验收开票日期 |  |  |  |
| `trade_terms` | VARCHAR(500) |  |  | 贸易条款 |  |  |  |
| `warranty_period` | VARCHAR(100) |  |  | 质保期 |  |  |  |
| `project_text` | VARCHAR(500) |  |  | 项目文本 | ods | ods_sap_erp_zhone_text_get_vbbk_di |  |
| `currency` | VARCHAR(100) |  |  | 销售订单行币种 |  |  |  |
| `cny_exchange_rate` | DECIMAL(27,8) |  |  | new 汇率（to_CNY） | TCURR | FCURR |  |
| `usd_exchange_rate` | DECIMAL(27,8) |  |  | new 汇率（to_USD） | TCURR | FCURR |  |
| `M` | VARCHAR(256) |  |  |  |  |  |  |
| `freight_amt` | DECIMAL(27,8) |  |  | new 订单运费金额-原币 |  |  |  |
| `order_qty` | DECIMAL(27,8) |  |  | new 订单数量 |  |  |  |
| `product_area` | DECIMAL(27,8) |  |  | new 单位面积（平方米） |  |  |  |
| `order_area` | DECIMAL(27,8) |  |  | new 订单面积 |  |  |  |
| `order_tax_amt` | DECIMAL(27,8) |  |  | new 订单金额（含运费）_含税-原币 |  |  |  |
| `order_tax_cny_amt` | DECIMAL(27,8) |  |  | new 订单金额（含运费）_含税-CNY |  |  | dwd_订单明细模型 / 【订单金额（含运费）_含税-原币】*【汇率（to_CNY）】 |
| `order_tax_usd_amt` | DECIMAL(27,8) |  |  | new 订单金额（含运费）_含税-USD |  |  | dwd_订单明细模型 / 【订单金额（含运费）_含税-原币】*【汇率（to_USD）】 |
| `order_notax_amt` | DECIMAL(27,8) |  |  | new 订单金额（含运费）_不含税-原币 |  |  |  |
| `order_notax_cny_amt` | DECIMAL(27,8) |  |  | new 订单金额（含运费）_不含税-CNY |  |  | dwd_订单明细模型 / 【订单金额（含运费）_不含税-原币】*【汇率（to_CNY）】 |
| `order_notax_usd_amt` | DECIMAL(27,8) |  |  | new 订单金额（含运费）_不含税-USD |  |  | dwd_订单明细模型 / 【订单金额（含运费）_不含税-原币】*【汇率（to_USD）】 |
| `shipment_qty` | DECIMAL(27,8) |  |  | new 出货数量 |  |  | LIPS |
| `VBFA` | VARCHAR(256) |  |  |  |  |  |  |
| `VBUP` | VARCHAR(256) |  |  |  |  |  |  |
| `RFMNG` | VARCHAR(256) |  |  |  |  |  |  |
| `WBSTA` | VARCHAR(256) |  |  |  |  |  |  |
| `ON` | VARCHAR(256) |  |  | VBFA | VBFA | VBELV |  |
| `VBUP` | VARCHAR(256) |  |  | join | VBUP | VBELN |  |
| `VBFA` | VARCHAR(256) |  |  |  |  |  |  |
| `AND` | VARCHAR(256) |  |  |  |  |  |  |
| `AND` | VARCHAR(256) |  |  |  |  |  |  |
| `LIPS` | VARCHAR(256) |  |  |  |  |  |  |
| `shipment_area` | DECIMAL(27,8) |  |  | new 出货面积 |  |  | 当前表计算 / / / 计算：【出货数量】*【单位面积】 |
| `shipment_tax_amt` | DECIMAL(27,8) |  |  | new 出货金额（含运费）_含税-原币 |  |  | 当前表计算 / / / （【订单金额（含运费）_含税-原币】/【订单数量】）*【出货数量】 KONV:MWST 销项税额 |
| `shipment_tax_cny_amt` | DECIMAL(27,8) |  |  | new 出货金额（含运费）_含税-CNY |  |  | 当前表计算 / / / 【出货金额（含运费）_含税-原币】*【汇率（to_CNY）】 |
| `shipment_tax_usd_amt` | DECIMAL(27,8) |  |  | new 出货金额（含运费）_含税-USD |  |  | 当前表计算 / / / 【出货金额（含运费）_含税-原币】*【汇率（to_USD）】 |
| `shipment_notax_amt` | DECIMAL(27,8) |  |  | new 出货金额（含运费）_不含税-原币 |  |  | 当前表计算 / / / （【订单金额（含运费）_不含税-原币】/【订单数量】）*【交货数量】 |
| `shipment_notax_cny_amt` | DECIMAL(27,8) |  |  | new 出货金额（含运费）_不含税-CNY |  |  | 当前表计算 / / / 【出货金额（含运费）_不含税-原币】*【汇率（to_CNY）】 |
| `shipment_notax_usd_amt` | DECIMAL(27,8) |  |  | new 出货金额（含运费）_不含税-USD |  |  | 当前表计算 / / / 【出货金额（含运费）_不含税-原币】*【汇率（to_USD）】 |
| `unshipment_qty` | DECIMAL(27,8) |  |  | new 未出货数量 |  |  | 当前表计算 / / / 【订单数量】-【出货数量】 |
| `unshipment_area` | DECIMAL(27,8) |  |  | new 未出货面积 |  |  | 当前表计算 / / / 【订单面积】-【出货面积】 |
| `unshipment_tax_amt` | DECIMAL(27,8) |  |  | new 未出货金额（含运费）_含税-原币 |  |  | 当前表计算 / / / 【订单金额（含运费）_含税-原币】-【出货金额（含运费）_含税-原币】 |
| `unshipment_tax_cny_amt` | DECIMAL(27,8) |  |  | new 未出货金额（含运费）_含税-CNY |  |  | 当前表计算 / / / 【订单金额（含运费）_含税-CNY】-【出货金额（含运费）_含税-CNY】 |
| `unshipment_tax_usd_amt` | DECIMAL(27,8) |  |  | new 未出货金额（含运费）_含税-USD |  |  | 当前表计算 / / / 【订单金额（含运费）_含税-USD】-【出货金额（含运费）_含税-USD】 |
| `unshipment_notax_amt` | DECIMAL(27,8) |  |  | new 未出货金额（含运费）_不含税-原币 |  |  | 当前表计算 / / / 【订单金额（含运费）_不含税-原币】-【出货金额（含运费）_不含税-原币】 |
| `unshipment_notax_cny_amt` | DECIMAL(27,8) |  |  | new 未出货金额（含运费）_不含税-CNY |  |  | 当前表计算 / / / 【订单金额（含运费）_不含税-CNY】-【出货金额（含运费）_不含税-CNY】 |
| `unshipment_notax_usd_amt` | DECIMAL(27,8) |  |  | new 未出货金额（含运费）_不含税-USD |  |  | 当前表计算 / / / 【订单金额（含运费）_不含税-USD】-【出货金额（含运费）_不含税-USD】 |
| `scenario_tag` | VARCHAR(100) |  |  | new 场景标签 |  |  |  |
| `ZSD_SUB` | VARCHAR(256) |  |  |  |  |  |  |
| `ZSD_SUB` | VARCHAR(256) |  |  |  |  |  |  |
| `setleaf` | VARCHAR(256) |  |  |  |  |  |  |
| `vkorg` | VARCHAR(256) |  |  | 若某销售订单存在 |  |  |  |
| `SELECT` | VARCHAR(256) |  |  |  |  |  |  |
| `vb` | VARCHAR(256) |  |  |  |  |  |  |
| `COUNT` | VARCHAR(256) |  |  |  |  |  |  |
| `zs` | VARCHAR(256) |  |  |  |  |  |  |
| `vb` | VARCHAR(256) |  |  |  |  |  |  |
| `FROM` | VARCHAR(256) |  |  |  |  |  |  |
| `JOIN` | VARCHAR(256) |  |  | LEFT | ods | ods_sap_erp_zsd_sub_df |  |
| `IS` | VARCHAR(256) |  | Y | vbeln | zs | vbeln |  |
| `AND` | VARCHAR(256) |  |  |  |  |  |  |
| `insert_dt` | DATETIME |  |  | 数仓数据更新时间 |  |  |  |
| `source_system` | VARCHAR(100) |  |  | 来源系统 |  |  |  |

## 模型逻辑详情（原文备忘）

```
模型标准 系统（独立逻辑，可来源于多系统）
序号 字段备注 字段名称 字段编码 字段类型 维度/度量 主键 值域 非空 获取方式 来源表 来源字段 来源字段说明 数据类型 值列表 关联条件 计算逻辑 备注1 备注2
交货单号 delivery_order_num varchar(100) 维度 直接获取 LIKP VBELN 交货单号 LIPS~LIKP ON LIPS.VBELN = LIKP.VBELN
交货单行号 delivery_order_item_num varchar(100) 维度 直接获取 LIPS POSNR 交货单行号 主表
销售订单号 order_num varchar(100) 维度 直接获取 LIPS LIPS.VGBEL 销售凭证编号 LIPS~订单明细模型order：LIPS.VGPOS=order.order_item_num AND LIPS.VGBEL=order.order_num
销售订单行号 order_item_num varchar(100) 维度 直接获取 LIPS LIPS.VGPOS 销售订单行号 /
制造公司销售订单号 manu_sales_order_num varchar(100) 维度 直接获取 dwd_订单明细模型 制造公司销售订单号 /
销售订单号_含抛转 order_num_transfer varchar(100) 直接获取 / 销售订单号
制造公司销售订单号 / 若【制造公司销售订单号】有值 ，则值=【制造公司销售订单号】否则 值=【销售公司销售订单号】
合同号（客户采购订单编号） contract_num varchar(100) 维度 直接获取 dwd_订单明细模型 采购订单编号 /
母合同号 contract_m_num varchar(100) 维度 直接获取 dwd_订单明细模型 母合同号 /
订单类型编码 order_type_code varchar(100) 维度 直接获取 dwd_订单明细模型 订单类型 /
new 订单类型名称 order_type_name varchar(255) 维度 直接获取 dwd_订单明细模型 订单类型描述 /
出货日期 shipment_date date 维度 直接获取 LIKP WADAT_IST 实际发货日期 / 以此为查询条件
请求交货日期 request_del_date date 维度 直接获取 dwd_订单明细模型 请求交货日期 /
订单首次审批日期 approval_date date 维度 直接获取 dwd_订单明细模型 订单首次审批日期 /
new 订单创建日期 create_date date 维度 直接获取 dwd_订单明细模型 创建日期 /
最新入库日期 latest_warehousing_date date 维度 直接获取 当前表
MSEG MSEG.BUDAT_MKPF 过账日期 MSEG-【BUDAT_MKPF】
注：有抛转则按制造公司销售订单取入库日期，无则按原单取入库日期
        当前表~MSEG ON MSEG.MAT_KDAUF =当前表.【销售订单号_含抛转】
筛【BWART】=in("101","413"),取销售订单最新的入库日期 筛【BWART】=in("101","413"),取销售订单最新的入库日期 MSEG-【BUDAT_MKPF】
注：有抛转则按制造公司销售订单取入库日期，无则按原单取入库日期
抛转：
（~ZSD068VA 销售订单号与订单抛转信息ZSD068VA-【OVBELN】关联
~ZSD124VA 销售订单号与渠道新抛转信息ZSD124VA-【OVBELN】关联）
        ~MSEG ON MSEG.MAT_KDAUF = 以上两张表的结果.IVBELN 
筛【BWART】="101",取销售订单最新的入库日期
无抛转：
dwd_订单明细模型~MSEG关联条件：通过销售订单号关联→宽表.销售订单编号 = MSEG.MAT_KDAUF 
筛【BWART】="101"
物料编码 mat_code varchar(100) 维度 直接获取 LIPS MATNR 物料编码 /
物料描述 mat_desc varchar(255) 维度 直接获取 MAKT MAKTX 物料描述 ~MAKT ON MAKT.MATNR = LIPS.MATNR AND SPRAS = 1(为1是中文) -- 取物料描述
产业编码 industry_code varchar(100) 维度 直接获取 dwd_订单明细模型 产业编码 /
产业描述 industry_name varchar(255) 维度 直接获取 dwd_订单明细模型 产业描述 /
产品线编码 product_line_code varchar(100) 维度 直接获取 dwd_订单明细模型 产品线编码 /
产品线描述 product_line_name varchar(255) 维度 直接获取 dwd_订单明细模型 产品线描述 /
产品族编码 product_group_code varchar(100) 维度 直接获取 dwd_订单明细模型 产品族编码 /
产品族描述 product_group_name varchar(255) 维度 直接获取 dwd_订单明细模型 产品族描述 /
产品系列编码 product_series_code varchar(100) 维度 直接获取 dwd_订单明细模型 产品系列编码 /
产品系列描述 product_series_name varchar(255) 维度 直接获取 dwd_订单明细模型 产品系列描述 /
产品单位面积 product_area decimal(27,8) 度量 直接获取 dwd_订单明细模型 产品单位面积 /
产品类型 product_type varchar(100) 维度 直接获取 dwd_订单明细模型 产品类型 /
行业类型 industry_type varchar(100) 维度 直接获取 dwd_订单明细模型 行业类型 /
特殊库存标识 special_stock_flag varchar(100) 维度 直接获取 dwd_订单明细模型 特殊库存标识 /
交货地址 delivery_address varchar(255) 维度 直接获取 交货单 RFC text 【RFC】~vbeln 关联【交货单号】 交货单抬头长文本-收货地址字段ods.ods_sap_erp_zhone_text_get_vbbk_di where tdid ='Y002'
特殊库存标识名称 special_stock_name varchar(100) 维度 直接获取 dwd_订单明细模型 特殊库存标识描述 /
工厂编码 factory_code varchar(100) 维度 直接获取 dwd_订单明细模型 工厂编码 /
工厂名称 factory_name varchar(255) 维度 直接获取 dwd_订单明细模型 工厂名称 /
公司代码 company_code varchar(100) 维度 直接获取 dwd_订单明细模型 公司代码 /
公司名称 company_name varchar(255) 维度 直接获取 dwd_订单明细模型 公司名称 /
客户编码 customer_code varchar(100) 维度 直接获取 dwd_订单明细模型 客户编码 /
客户名称 customer_name varchar(255) 维度 直接获取 dwd_订单明细模型 客户名称 /
new 业务员编码 sales_person_num varchar(100) 维度 直接获取 dwd_订单明细模型 人员编码 /
业务员姓名 sales_person_name varchar(255) 维度 直接获取 dwd_订单明细模型 姓+名 /
new 销售部门编码 sales_dept_code varchar(100) 维度 直接获取 dwd_订单明细模型 销售部门编码 /
销售部门名称 sales_dept_name varchar(255) 维度 直接获取 dwd_订单明细模型 部门描述 /
雇员部门编码 employee_dept_code varchar(100) 维度 直接获取 dwd_订单明细模型 雇员部门编号 /
雇员部门名称 employee_dept_name varchar(255) 维度 直接获取 dwd_订单明细模型 雇员部门 /
new 销售组织编码 sales_org_code varchar(100) 维度 直接获取 dwd_订单明细模型 销售组织编码 /
销售组织名称 sales_org_name varchar(255) 维度 直接获取 dwd_订单明细模型 销售组织描述 /
new 销售组 sales_group_code varchar(100) 维度 直接获取 dwd_订单明细模型 销售组编码 /
销售组名称 sales_group_name varchar(255) 维度 直接获取 dwd_订单明细模型 销售组描述 /
new 分销渠道编码 sales_channel_code varchar(100) 维度 直接获取 dwd_订单明细模型 分销渠道编码 /
分销渠道名称 sales_channel_name varchar(255) 维度 直接获取 dwd_订单明细模型 分销渠道描述 /
new 销售战区名称 sales_unit_name varchar(255) 维度 直接获取 dwd_订单明细模型 销售战区 / 国际26战区，国内32省份
new 销售大区名称 sales_area_name varchar(255) 维度 直接获取 dwd_订单明细模型 销售大区 / 国际8区域，国内6区域
new 销售区域名称 sales_region_name varchar(255) 维度 直接获取 dwd_订单明细模型 销售区域 / 区分国内，国外
客户所属国家编码 customer_country_code varchar(100) 维度 直接获取 dwd_订单明细模型 客户所属国家 /
客户所属国家名称 customer_country_name varchar(255) 维度 直接获取 dwd_订单明细模型 客户所属国家名称 /
货代 freight_forwarder varchar(100) 维度 直接获取 ZSD_CLXX_GJ HD 货代 ~ZSD_CLXX_GJ ON LIKP.VBELN = ZSD_CLXX_GJ.VBELN -- 取国际货代【HD】 仅国际有值
运输方式 transport_method varchar(100) 维度 ZSD_CLXX_GJ YSFS 运输方式 /
付款方式 payment_method varchar(100) 维度 自定义 dwd_订单明细模型 付款方式 /
挪用现货信息 embezzle_spot_info varchar(100) 维度 直接获取 dwd_订单明细模型 挪用现货信息 /
是否需要验收 is_need_inspection varchar(100) 维度 直接获取 dwd_订单明细模型 是否需要验收 / 结论：不用生产任务单的是否验收兜底
是否已验收 is_inspected varchar(100) 维度 自定义 / / 关联筛选：
1.排除过账状态为E{未过账被取消}；
2.排除已取消的票据（冲销场景）

LIKP LK LEFT JOIN (
select distinct VGBEL 
from VBRP 
LEFT JOIN VBRK V1 ON V1.VBELN = VBRP.VBELN  
LEFT JOIN VBRK V2 ON V2.SFAKN = VBRP.VBELN 
WHERE V2.SFAKN IS NOT NULL AND  V1.RFBSK <>"E"
)T LEFT JOIN LK.VBELN=VBRP.VGBEL 如果交货单号可以找到对应验收单号，则值=Y{已验收} 否则=N{未验收}
验收单号 inspection_order_num varchar(100) 维度 直接获取 VBRK VBELN LIKP LK LEFT JOIN (
select distinct VGBEL 
from VBRP 
LEFT JOIN VBRK V1 ON V1.VBELN = VBRP.VBELN  
LEFT JOIN VBRK V2 ON V2.SFAKN = VBRP.VBELN 
WHERE V2.SFAKN IS NOT NULL AND  V1.RFBSK <>"E"
)T LEFT JOIN LK.VBELN=VBRP.VGBEL
取VBRK.VBELN
验收开票日期 inspection_invoice_date date 维度 VBRK FKDAT
贸易条款 trade_terms varchar(500) 维度 直接获取 dwd_订单明细模型 贸易条款 /
质保期 warranty_period varchar(100) 维度 直接获取 dwd_订单明细模型 质保期 /
项目文本 project_text varchar(500) 维度 直接获取 销售订单 RFC _text 【RFC】~vbeln 关联【销售订单号】 逻辑：销售订单抬头长文本-项目名称字段ods.ods_sap_erp_zhone_text_get_vbbk_di where tdid ='0001'
销售订单行币种 currency varchar(100) 维度 直接获取 dwd_订单明细模型 币种 /
new 汇率（to_CNY） cny_exchange_rate decimal(27,8) 维度 直接获取 dim_汇率 UKURS 汇率 关联 ：当前表【币种】=TCURR.FCURR AND TUCRR.TCURR="CNY" AND dwd_订单明细模型.创建日期=汇率表【日期】的汇率
new 汇率（to_USD） usd_exchange_rate decimal(27,8) 维度 直接获取 dim_汇率 UKURS 汇率 关联 ：当前表【币种】=TCURR.FCURR AND TUCRR.TCURR="USD" AND dwd_订单明细模型.创建日期=汇率表【日期】的汇率 对于荷兰{公司代码5300}：取汇率类型=“EURX”的汇率（SAP当前逻辑，之后考虑转M）
其余情况：取汇率类型=“M”的汇率
new 订单运费金额-原币 freight_amt decimal(27,8) 度量 直接获取 dwd_订单明细模型 运费
new 订单数量 order_qty decimal(27,8) 度量 直接获取 dwd_订单明细模型 订单行数量 /
new 单位面积（平方米） product_area decimal(27,8) 度量 直接获取 dwd_订单明细模型 产品单位面积 /
new 订单面积 order_area decimal(27,8) 度量 直接获取 dwd_订单明细模型 订单总面积 /
new 订单金额（含运费）_含税-原币 order_tax_amt decimal(27,8) 度量 直接获取 dwd_订单明细模型 订单行项目金额（含运费）_含税 / /
new 订单金额（含运费）_含税-CNY order_tax_cny_amt decimal(27,8) 度量 计算 dwd_订单明细模型 / 【订单金额（含运费）_含税-原币】*【汇率（to_CNY）】
new 订单金额（含运费）_含税-USD order_tax_usd_amt decimal(27,8) 度量 计算 dwd_订单明细模型 / 【订单金额（含运费）_含税-原币】*【汇率（to_USD）】
new 订单金额（含运费）_不含税-原币 order_notax_amt decimal(27,8) 度量 直接获取 dwd_订单明细模型 订单行项目金额（含运费）_不含税 / /
new 订单金额（含运费）_不含税-CNY order_notax_cny_amt decimal(27,8) 度量 计算 dwd_订单明细模型 / 【订单金额（含运费）_不含税-原币】*【汇率（to_CNY）】
new 订单金额（含运费）_不含税-USD order_notax_usd_amt decimal(27,8) 度量 计算 dwd_订单明细模型 / 【订单金额（含运费）_不含税-原币】*【汇率（to_USD）】
new 出货数量 shipment_qty decimal(27,8) 度量 计算 LIPS
VBFA
VBUP LFIMG
RFMNG
WBSTA 数量
数量
取过账状态 国内/国际=国际:
~VBFA ON VBFA.VBELV = VBAP.VBELN ON VBFA.POSNV = VBAP.POSNR 
        ~join VBUP ON VBUP.VBELN= VBFA.VBELN AND VBUP.POSNR= VBFA.POSNR AND VBUP-【WBSTA】='C' AND VBFA-【VBTYP_N】= 'J' AND VBFA-【VBTYP_V】='C'--销售状态表：已交货过账  --销售凭证流：取状态J交货，C订单
即：
销售凭证流-【子层凭证类别】VBFA-【VBTYP_N】= 'J' "只取交货
AND 销售凭证流-【先前凭证类别】VBFA-【VBTYP_V】='C'"订单
AND 销售凭证：项目状态-【货物移动】VBUP-【WBSTA】='C'. "已交货过账
国内/国际=国内：
LIPS-【LFIMG】 若：订单类型 ZMRE（退货）/ZMHH（换货）/ZMCE（贷项凭证） 值=-1*值； 业务场景：国际的订单常有分批出货的需求，会存在实际交货单已整单打出且交付给物流/仓库，结果临时客户要修改分批的情况；或者国际大订单有多个货柜需要连续装柜2-3天这种，需实际需求分批过账或者整单过账。
new 出货面积 shipment_area decimal(27,8) 度量 计算 当前表计算 / / / 计算：【出货数量】*【单位面积】
new 出货金额（含运费）_含税-原币 shipment_tax_amt decimal(27,8) 度量 计算 当前表计算 / / / （【订单金额（含运费）_含税-原币】/【订单数量】）*【出货数量】 KONV:MWST 销项税额
new 出货金额（含运费）_含税-CNY shipment_tax_cny_amt decimal(27,8) 度量 计算 当前表计算 / / / 【出货金额（含运费）_含税-原币】*【汇率（to_CNY）】
new 出货金额（含运费）_含税-USD shipment_tax_usd_amt decimal(27,8) 度量 计算 当前表计算 / / / 【出货金额（含运费）_含税-原币】*【汇率（to_USD）】
new 出货金额（含运费）_不含税-原币 shipment_notax_amt decimal(27,8) 度量 计算 当前表计算 / / / （【订单金额（含运费）_不含税-原币】/【订单数量】）*【交货数量】
new 出货金额（含运费）_不含税-CNY shipment_notax_cny_amt decimal(27,8) 度量 计算 当前表计算 / / / 【出货金额（含运费）_不含税-原币】*【汇率（to_CNY）】
new 出货金额（含运费）_不含税-USD shipment_notax_usd_amt decimal(27,8) 度量 计算 当前表计算 / / / 【出货金额（含运费）_不含税-原币】*【汇率（to_USD）】
new 未出货数量 unshipment_qty decimal(27,8) 度量 计算 当前表计算 / / / 【订单数量】-【出货数量】
new 未出货面积 unshipment_area decimal(27,8) 度量 计算 当前表计算 / / / 【订单面积】-【出货面积】
new 未出货金额（含运费）_含税-原币 unshipment_tax_amt decimal(27,8) 度量 计算 当前表计算 / / / 【订单金额（含运费）_含税-原币】-【出货金额（含运费）_含税-原币】
new 未出货金额（含运费）_含税-CNY unshipment_tax_cny_amt decimal(27,8) 度量 计算 当前表计算 / / / 【订单金额（含运费）_含税-CNY】-【出货金额（含运费）_含税-CNY】
new 未出货金额（含运费）_含税-USD unshipment_tax_usd_amt decimal(27,8) 度量 计算 当前表计算 / / / 【订单金额（含运费）_含税-USD】-【出货金额（含运费）_含税-USD】
new 未出货金额（含运费）_不含税-原币 unshipment_notax_amt decimal(27,8) 度量 计算 当前表计算 / / / 【订单金额（含运费）_不含税-原币】-【出货金额（含运费）_不含税-原币】
new 未出货金额（含运费）_不含税-CNY unshipment_notax_cny_amt decimal(27,8) 度量 计算 当前表计算 / / / 【订单金额（含运费）_不含税-CNY】-【出货金额（含运费）_不含税-CNY】
new 未出货金额（含运费）_不含税-USD unshipment_notax_usd_amt decimal(27,8) 度量 计算 当前表计算 / / / 【订单金额（含运费）_不含税-USD】-【出货金额（含运费）_不含税-USD】
new 场景标签 scenario_tag varchar(100) 维度 自定义 SETLEAF
ZSD_SUB ~SETLEAF 客户编码关联
~ZSD_SUB 销售订单号关联 1. 数据来自ZLAMP_SALES，则 【场景标签】= “I手工业绩”；
2. 剔除客户为国内子公司/国外子公司数据，（取表：setleaf 用字段setname = 'ZKUNNR_INNER_SUBCOM'取值，用字段setname = 'ZKUNNR_OUTSIDE_SUBCOM'取值，如果订单数据的客户编码KUNNR在RANGE表存在，则【场景标签】="D-关联交易"。
3. 对子客的康利数据打标签
以销售订单号关联：
若某销售订单存在 vkorg 组织= '2000'{广东洲明内销} AND kunnr 客户号= '0000106034' ：则把  vkorg 组织= '2000' 替换为  vkorg 组织= '1010'{洲明外销}，并打【场景标签】=“R-子客”；
-- 标签上D表示删除，I表示插入，R表示替换 数据-订单类型
康利处理参考逻辑：
SELECT 
-- *
-- vb.vbeln,
-- COUNT(*)
zs.vbeln
,vb.*
FROM ods.ods_sap_erp_vbak_df vb
LEFT JOIN (SELECT DISTINCT vbeln FROM ods.ods_sap_erp_zsd_sub_df) zs ON vb.vbeln = zs.vbeln 
WHERE zs.vbeln IS NOT NULL
AND vb.vkorg = '2000' AND vb.kunnr = '0000106034'
数仓数据更新时间 insert_dt datetime 审计 自定义 当前表 / now()
来源系统 source_system varchar(100) 审计 自定义 当前表 / 值=SAP
```

## 建议开发步骤

- 解析数据来源表/模型逻辑概述/模型逻辑详情三段内容，锁定目标表与字段。
- 校验字段类型、主键、分区列，生成 StarRocks DDL（可选执行）。
- 生成 INSERT SELECT 逻辑，按业务逻辑完成关联、去重、计算。
- 落地中间结果或目标表，补充数据质量校验与唯一性检查。
- 补充自动化脚本与回填验证（行数、键唯一性、NULL 分布）。
