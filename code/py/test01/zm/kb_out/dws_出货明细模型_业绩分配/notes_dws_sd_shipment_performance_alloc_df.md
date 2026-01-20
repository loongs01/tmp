# dws_出货明细模型_业绩分配 开发说明

## 目标表

- 英文表名: `dws_sd_shipment_performance_alloc_df`
- 中文含义: dws_出货模型（含业绩拆分）
- 用途: table
- 建议业务主键: `Unnamed: 3`

## 模型逻辑概述（原文）

```
序号 英文表名 中文含义 应用场景 备注
1 dws_sd_shipment_performance_alloc_df dws_出货模型（含业绩拆分） 用于出货数据业绩拆分后的分析 1. 因为涉及到业绩拆分，此时与业绩拆分表关联时会发散，这是正常的
2. 出货模型与业绩分配表用销售订单号关联
筛选条件
1删除 场景标签以“D”开头的数据
表关联条件
关联条件和过滤条件：
dws_接单业绩模型.order_no=dwd_业绩分配比例.order_no and dws_接单业绩模型.contract_no=dwd_业绩分配比例.contract_no
```

## 目标表字段结构（解析记忆）

| 字段编码 | 字段类型 | 主键 | 非空 | 字段说明 | 来源表 | 来源字段 | 计算逻辑 |
|----------|----------|------|------|----------|--------|----------|----------|
| `delivery_order_num` | VARCHAR(100) |  |  | 交货单号 |  |  |  |
| `delivery_order_item_num` | VARCHAR(100) |  |  | 交货单行号 |  |  |  |
| `order_num` | VARCHAR(100) |  |  | 销售订单号 |  |  |  |
| `order_item_num` | VARCHAR(100) |  |  | 销售订单行号 |  |  |  |
| `contract_num` | VARCHAR(100) |  |  | 合同号（客户采购订单编号） |  |  |  |
| `contract_m_num` | VARCHAR(100) |  |  | 母合同号 |  |  |  |
| `order_type_code` | VARCHAR(100) |  |  | 订单类型编码 |  |  |  |
| `order_type_name` | VARCHAR(255) |  |  | 订单类型名称 |  |  |  |
| `shipment_date` | DATETIME |  |  | 出货日期 |  |  |  |
| `request_del_date` | DATETIME |  |  | 请求交货日期 |  |  |  |
| `approval_date` | DATETIME |  |  | 订单首次审批日期 |  |  |  |
| `create_date` | DATETIME |  |  | 订单创建日期 |  |  |  |
| `latest_warehousing_date` | DATETIME |  |  | 最新入库日期 |  |  |  |
| `mat_code` | VARCHAR(100) |  |  | 物料编码 |  |  |  |
| `mat_desc` | VARCHAR(255) |  |  | 物料描述 |  |  |  |
| `product_type` | VARCHAR(100) |  |  | 产品类型 |  |  |  |
| `industry_type` | VARCHAR(100) |  |  | 行业类型 |  |  |  |
| `industry_code` | VARCHAR(100) |  |  | 产业编码 |  |  |  |
| `industry_name` | VARCHAR(255) |  |  | 产业描述 |  |  |  |
| `product_line_code` | VARCHAR(100) |  |  | 产品线编码 |  |  |  |
| `product_line_name` | VARCHAR(255) |  |  | 产品线描述 |  |  |  |
| `product_group_code` | VARCHAR(100) |  |  | 产品族编码 |  |  |  |
| `product_group_name` | VARCHAR(255) |  |  | 产品族描述 |  |  |  |
| `product_series_code` | VARCHAR(100) |  |  | 产品系列编码 |  |  |  |
| `product_series_name` | VARCHAR(255) |  |  | 产品系列描述 |  |  |  |
| `product_area` | DECIMAL(27,8) |  |  | 产品单位面积 |  |  |  |
| `special_stock_flag` | VARCHAR(100) |  |  | 特殊库存标识 |  |  |  |
| `special_stock_name` | VARCHAR(100) |  |  | 特殊库存标识名称 |  |  |  |
| `delivery_address` | VARCHAR(255) |  |  | 交货地址 |  |  |  |
| `factory_code` | VARCHAR(100) |  |  | 工厂编码 |  |  |  |
| `factory_name` | VARCHAR(255) |  |  | 工厂名称 |  |  |  |
| `company_code` | VARCHAR(100) |  |  | 公司代码 |  |  |  |
| `company_name` | VARCHAR(255) |  |  | 公司名称 |  |  |  |
| `customer_code` | VARCHAR(100) |  |  | 客户编码 |  |  |  |
| `customer_name` | VARCHAR(255) |  |  | 客户名称 |  |  |  |
| `sales_person_num` | VARCHAR(100) |  |  | 业务员编码 |  |  |  |
| `b` | VARCHAR(256) |  |  |  |  |  |  |
| `sales_person_name` | VARCHAR(255) |  |  | 业务员姓名 |  |  |  |
| `b` | VARCHAR(256) |  |  |  |  |  |  |
| `sales_dept_code` | VARCHAR(100) |  |  | 销售部门编码 |  |  |  |
| `b` | VARCHAR(256) |  |  |  |  |  |  |
| `sales_dept_name` | VARCHAR(255) |  |  | 销售部门名称 |  |  |  |
| `b` | VARCHAR(256) |  |  |  |  |  |  |
| `employee_dept_code` | VARCHAR(100) |  |  | 雇员部门编码 |  |  |  |
| `b` | VARCHAR(256) |  |  |  |  |  |  |
| `employee_dept_name` | VARCHAR(255) |  |  | 雇员部门描述 |  |  |  |
| `b` | VARCHAR(256) |  |  |  |  |  |  |
| `sales_org_code` | VARCHAR(100) |  |  | 销售组织编码 |  |  |  |
| `sales_org_name` | VARCHAR(255) |  |  | 销售组织名称 |  |  |  |
| `sales_group_code` | VARCHAR(100) |  |  | 销售组编码 |  |  |  |
| `b` | VARCHAR(256) |  |  |  |  |  |  |
| `sales_group_name` | VARCHAR(255) |  |  | 销售组名称 |  |  |  |
| `b` | VARCHAR(256) |  |  |  |  |  |  |
| `sales_channel_code` | VARCHAR(100) |  |  | 分销渠道编码 |  |  |  |
| `sales_channel_name` | VARCHAR(255) |  |  | 分销渠道名称 |  |  |  |
| `sales_unit_name` | VARCHAR(255) |  |  | 销售战区名称 |  |  |  |
| `b` | VARCHAR(256) |  |  |  |  |  |  |
| `sales_area_name` | VARCHAR(255) |  |  | 销售大区名称 |  |  |  |
| `b` | VARCHAR(256) |  |  |  |  |  |  |
| `sales_region_name` | VARCHAR(255) |  |  | 销售区域名称 |  |  |  |
| `b` | VARCHAR(256) |  |  |  |  |  |  |
| `customer_country_code` | VARCHAR(100) |  |  | 客户所属国家编码 |  |  |  |
| `customer_country_name` | VARCHAR(255) |  |  | 客户所属国家名称 |  |  |  |
| `freight_forwarder` | VARCHAR(100) |  |  | 货代 |  |  |  |
| `transport_method` | VARCHAR(100) |  |  | 运输方式 |  |  |  |
| `payment_method` | VARCHAR(100) |  |  | 付款方式 |  |  |  |
| `is_need_inspection` | VARCHAR(100) |  |  | 是否需要验收 |  |  |  |
| `is_inspected` | VARCHAR(100) |  |  | 是否已验收 |  |  |  |
| `inspection_order_num` | VARCHAR(100) |  |  | 验收单号 |  |  |  |
| `inspection_invoice_date` | DATETIME |  |  | 验收开票日期 |  |  |  |
| `trade_terms` | VARCHAR(500) |  |  | 贸易条款 |  |  |  |
| `manu_sales_order_num` | VARCHAR(100) |  |  | 制造公司销售订单号 |  |  |  |
| `warranty_period` | VARCHAR(100) |  |  | 质保期 |  |  |  |
| `project_text` | VARCHAR(500) |  |  | 项目文本 |  |  |  |
| `currency` | VARCHAR(100) |  |  | 销售订单行币种 |  |  |  |
| `cny_exchange_rate` | DECIMAL(27,8) |  |  | 汇率（to_CNY） |  |  |  |
| `usd_exchange_rate` | DECIMAL(27,8) |  |  | 汇率（to_USD） |  |  |  |
| `sales_percent` | DECIMAL(3,8) |  |  | new 业绩分配比例 |  |  |  |
| `shipment_qty` | DECIMAL(27,8) |  |  | new 出货数量 |  |  | a 出货数量 / 【交货数量】*【业绩分配比例】/100 含业绩分配 |
| `shipment_area` | DECIMAL(27,8) |  |  | new 出货面积 |  |  | a 出货面积 / 【出货面积】*【业绩分配比例】/100 含业绩分配 |
| `shipment_tax_amt` | DECIMAL(27,8) |  |  | new 出货金额（含运费）_含税-原币 |  |  | a 出货金额（含运费）_含税-原币 / 【出货金额（含运费）_含税-原币】*【业绩分配比例】/100 含业绩分配 |
| `shipment_tax_cny_amt` | DECIMAL(27,8) |  |  | new 出货金额（含运费）_含税-CNY |  |  | a 出货金额（含运费）_含税-CNY / 【出货金额（含运费）_含税-CNY】*【业绩分配比例】/100 含业绩分配 |
| `shipment_tax_usd_amt` | DECIMAL(27,8) |  |  | new 出货金额（含运费）_含税-USD |  |  | a 出货金额（含运费）_含税-USD / 【出货金额（含运费）_含税-USD】*【业绩分配比例】/100 含业绩分配 |
| `shipment_notax_amt` | DECIMAL(27,8) |  |  | new 出货金额（含运费）_不含税-原币 |  |  | a 出货金额（含运费）_不含税-原币 / 【出货金额（含运费）_不含税-原币】*【业绩分配比例】/100 含业绩分配 |
| `shipment_notax_cny_amt` | DECIMAL(27,8) |  |  | new 出货金额（含运费）_不含税-CNY |  |  | a 出货金额（含运费）_不含税-CNY / 【出货金额（含运费）_不含税-CNY】*【业绩分配比例】/100 含业绩分配 |
| `shipment_notax_usd_amt` | DECIMAL(27,8) |  |  | new 出货金额（含运费）_不含税-USD |  |  | a 出货金额（含运费）_不含税-USD / 【出货金额（含运费）_不含税-USD】*【业绩分配比例】/100 含业绩分配 |
| `scenario_tag` | VARCHAR(100) |  |  | 场景标签 |  |  |  |
| `insert_dt` | DATETIME |  |  | 数仓数据更新时间 |  |  |  |
| `source_system` | VARCHAR(100) |  |  | 来源系统 |  |  |  |

## 模型逻辑详情（原文备忘）

```
模型标准 系统（独立逻辑，可来源于多系统）
序号 字段备注 字段名称 字段编码 字段类型 维度/度量 主键 值域 非空 获取方式 来源表 来源字段 来源字段说明 数据类型 值列表 关联条件 计算逻辑 备注1 备注2
交货单号 delivery_order_num varchar(100) 维度 直接获取 a 交货单号
交货单行号 delivery_order_item_num varchar(100) 维度 直接获取 a 交货单行号
销售订单号 order_num varchar(100) 维度 直接获取 a 销售订单号
销售订单行号 order_item_num varchar(100) 维度 直接获取 a 销售订单行号
合同号（客户采购订单编号） contract_num varchar(100) 维度 直接获取 a 合同号/订单号
母合同号 contract_m_num varchar(100) 维度 直接获取 a 母合同号
订单类型编码 order_type_code varchar(100) 维度 直接获取 a 订单类型
订单类型名称 order_type_name varchar(255) 维度 直接获取 a 订单类型描述
出货日期 shipment_date date 维度 直接获取 a 出货日期
请求交货日期 request_del_date date 维度 直接获取 a 请求交货日期
订单首次审批日期 approval_date date 维度 直接获取 a 订单审批日期
订单创建日期 create_date date 维度 直接获取 a 订单创建日期
最新入库日期 latest_warehousing_date date 维度 直接获取 a 最新入库日期
物料编码 mat_code varchar(100) 维度 直接获取 a 物料编码
物料描述 mat_desc varchar(255) 维度 直接获取 a 物料描述
产品类型 product_type varchar(100) 维度 直接获取 a 产品类型
行业类型 industry_type varchar(100) 维度 直接获取 a 行业类型
产业编码 industry_code varchar(100) 维度 直接获取 a 产业编码
产业描述 industry_name varchar(255) 维度 直接获取 a 产业描述
产品线编码 product_line_code varchar(100) 维度 直接获取 a 产品线编码
产品线描述 product_line_name varchar(255) 维度 直接获取 a 产品线描述
产品族编码 product_group_code varchar(100) 维度 直接获取 a 产品族编码
产品族描述 product_group_name varchar(255) 维度 直接获取 a 产品族描述
产品系列编码 product_series_code varchar(100) 维度 直接获取 a 产品系列编码
产品系列描述 product_series_name varchar(255) 维度 直接获取 a 产品系列描述
产品单位面积 product_area decimal(27,8) 维度 直接获取 a 产品单位面积
特殊库存标识 special_stock_flag varchar(100) 维度 直接获取 a 特殊库存标识
特殊库存标识名称 special_stock_name varchar(100) 维度 直接获取 a 特殊库存标识描述 /
交货地址 delivery_address varchar(255) 维度 直接获取 a 交货地址
工厂编码 factory_code varchar(100) 维度 直接获取 a 工厂编码
工厂名称 factory_name varchar(255) 维度 直接获取 a 工厂名称
公司代码 company_code varchar(100) 维度 直接获取 a 公司代码
公司名称 company_name varchar(255) 维度 直接获取 a 公司名称
客户编码 customer_code varchar(100) 维度 直接获取 a 客户编码
客户名称 customer_name varchar(255) 维度 直接获取 a 客户名称
业务员编码 sales_person_num varchar(100) 维度 直接获取 a
b 人员编码 关联条件：订单号+合同号
关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的业务员编码；若关联不上则保持dwd_订单明细模型数据不变 含业绩分配
业务员姓名 sales_person_name varchar(255) 维度 直接获取 a
b 姓+名 关联条件：订单号+合同号
关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的业务员描述；若关联不上则保持dwd_订单明细模型数据不变 含业绩分配
销售部门编码 sales_dept_code varchar(100) 维度 直接获取 a
b 销售部门编码 关联条件：订单号+合同号
关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的销售部门编码；若关联不上则保持dwd_订单明细模型数据不变 含业绩分配
销售部门名称 sales_dept_name varchar(255) 维度 直接获取 a
b 销售部门描述 关联条件：订单号
关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的销售部门描述；若关联不上则保持dwd_订单明细模型数据不变 含业绩分配
雇员部门编码 employee_dept_code varchar(100) 维度 直接获取 a
b 雇员部门编号 关联条件：订单号
关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的雇员部门编码；若关联不上则保持dwd_订单明细模型数据不变 含业绩分配
雇员部门描述 employee_dept_name varchar(255) 直接获取 a
b 雇员部门 关联条件：订单号+合同号
关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的雇员部门描述；若关联不上则保持dwd_订单明细模型数据不变 含业绩分配
销售组织编码 sales_org_code varchar(100) 维度 直接获取 a 销售组织编码
销售组织名称 sales_org_name varchar(255) 维度 直接获取 a 销售组织
销售组编码 sales_group_code varchar(100) 维度 直接获取 a
b 销售组编码 关联条件：订单号+合同号
关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的销售组编码；若关联不上则保持dwd_订单明细模型数据不变
销售组名称 sales_group_name varchar(255) 维度 直接获取 a
b 销售组 关联条件：订单号+合同号
关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的销售组描述；若关联不上则保持dwd_订单明细模型数据不变
分销渠道编码 sales_channel_code varchar(100) 维度 直接获取 a 分销渠道编码
分销渠道名称 sales_channel_name varchar(255) 维度 直接获取 a 分销渠道
销售战区名称 sales_unit_name varchar(255) 维度 直接获取 a
b 销售战区 关联条件：订单号+合同号
关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的省份/国家；若关联不上则保持dwd_订单明细模型数据不变
销售大区名称 sales_area_name varchar(255) 维度 直接获取 a
b 销售大区 关联条件：订单号+合同号
关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的销售大区；若关联不上则保持dwd_订单明细模型数据不变
销售区域名称 sales_region_name varchar(255) 维度 直接获取 a
b 销售区域 关联条件：订单号+合同号
关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的目标范围；若关联不上则保持dwd_订单明细模型数据不变
客户所属国家编码 customer_country_code varchar(100) 维度 直接获取 a 客户所属国家
客户所属国家名称 customer_country_name varchar(255) 维度 直接获取 a 客户所属国家名称 /
货代 freight_forwarder varchar(100) 维度 直接获取 a 货代
运输方式 transport_method varchar(100) 维度 直接获取 a 运输方式
付款方式 payment_method varchar(100) 维度 自定义 a 付款方式
是否需要验收 is_need_inspection varchar(100) 维度 直接获取 a 是否需要验收
是否已验收 is_inspected varchar(100) 维度 直接获取 a 是否已验收
验收单号 inspection_order_num varchar(100) 维度 直接获取 a 验收单号
验收开票日期 inspection_invoice_date date 维度 直接获取 a 验收开票日期
贸易条款 trade_terms varchar(500) 维度 直接获取 a 贸易条款
制造公司销售订单号 manu_sales_order_num varchar(100) 维度 直接获取 a 制造公司销售订单号
质保期 warranty_period varchar(100) 维度 直接获取 a 质保期
项目文本 project_text varchar(500) 维度 直接获取 a 项目文本
销售订单行币种 currency varchar(100) 维度 直接获取 a 销售订单行币种
汇率（to_CNY） cny_exchange_rate decimal(27,8) 维度 直接获取 a 汇率（to_CNY）
汇率（to_USD） usd_exchange_rate decimal(27,8) 维度 直接获取 a 汇率（to_USD）
new 业绩分配比例 sales_percent decimal(3,8) 维度 直接获取 b 配额 关联条件：订单号+合同号 含业绩分配
new 出货数量 shipment_qty decimal(27,8) 度量 计算 a 出货数量 / 【交货数量】*【业绩分配比例】/100 含业绩分配
new 出货面积 shipment_area decimal(27,8) 度量 计算 a 出货面积 / 【出货面积】*【业绩分配比例】/100 含业绩分配
new 出货金额（含运费）_含税-原币 shipment_tax_amt decimal(27,8) 度量 计算 a 出货金额（含运费）_含税-原币 / 【出货金额（含运费）_含税-原币】*【业绩分配比例】/100 含业绩分配
new 出货金额（含运费）_含税-CNY shipment_tax_cny_amt decimal(27,8) 度量 计算 a 出货金额（含运费）_含税-CNY / 【出货金额（含运费）_含税-CNY】*【业绩分配比例】/100 含业绩分配
new 出货金额（含运费）_含税-USD shipment_tax_usd_amt decimal(27,8) 度量 计算 a 出货金额（含运费）_含税-USD / 【出货金额（含运费）_含税-USD】*【业绩分配比例】/100 含业绩分配
new 出货金额（含运费）_不含税-原币 shipment_notax_amt decimal(27,8) 度量 计算 a 出货金额（含运费）_不含税-原币 / 【出货金额（含运费）_不含税-原币】*【业绩分配比例】/100 含业绩分配
new 出货金额（含运费）_不含税-CNY shipment_notax_cny_amt decimal(27,8) 度量 计算 a 出货金额（含运费）_不含税-CNY / 【出货金额（含运费）_不含税-CNY】*【业绩分配比例】/100 含业绩分配
new 出货金额（含运费）_不含税-USD shipment_notax_usd_amt decimal(27,8) 度量 计算 a 出货金额（含运费）_不含税-USD / 【出货金额（含运费）_不含税-USD】*【业绩分配比例】/100 含业绩分配
场景标签 scenario_tag varchar(100) 维度 直接获取 a 场景标签 删除 场景标签以“D”开头的数据
数仓数据更新时间 insert_dt datetime 审计 直接获取 / / now()
来源系统 source_system varchar(100) 审计 直接获取 a 来源系统 值=SAP
```

## 建议开发步骤

- 解析数据来源表/模型逻辑概述/模型逻辑详情三段内容，锁定目标表与字段。
- 校验字段类型、主键、分区列，生成 StarRocks DDL（可选执行）。
- 生成 INSERT SELECT 逻辑，按业务逻辑完成关联、去重、计算。
- 落地中间结果或目标表，补充数据质量校验与唯一性检查。
- 补充自动化脚本与回填验证（行数、键唯一性、NULL 分布）。
