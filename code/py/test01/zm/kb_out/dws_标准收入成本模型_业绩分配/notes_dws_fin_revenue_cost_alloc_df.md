# dws_标准收入成本模型_业绩分配 开发说明

## 目标表

- 英文表名: `dws_fin_revenue_cost_alloc_df`
- 中文含义: dws_标准收入成本模型_业绩分配
- 用途: table
- 建议业务主键: `Unnamed: 2`

## 数据来源表

```
来源系统 英文表名 中文含义 表别名 备注
数仓 dws_fin_revenue_cost_df dws_标准收入成本模型 主表
数仓 dwd_sd_performance_allocation_ratio_df DWD_业绩分配比例
```

## 模型逻辑概述（原文）

```
序号 英文表名 中文含义 应用场景 备注
1 dws_fin_revenue_cost_alloc_df dws_标准收入成本模型_业绩分配
关联条件
```

## 目标表字段结构（解析记忆）

| 字段编码 | 字段类型 | 主键 | 非空 | 字段说明 | 来源表 | 来源字段 | 计算逻辑 |
|----------|----------|------|------|----------|--------|----------|----------|
| `company_code` | VARCHAR(256) | Y |  | 公司代码 | Y | Y |  |
| `company_name` | VARCHAR(256) |  |  | 公司名称 |  |  |  |
| `sales_invoice_num` | VARCHAR(256) | Y |  | 销售订单号 |  |  |  |
| `sales_invoice_line` | VARCHAR(256) | Y |  | 销售订单行号 |  |  |  |
| `fiscal_year` | VARCHAR(256) | Y |  | 会计年度 |  |  |  |
| `fiscal_period` | VARCHAR(256) | Y |  | 会计期间 |  |  |  |
| `voucher_post_date` | VARCHAR(256) | Y |  | 过账日期 |  |  |  |
| `transfer_node` | VARCHAR(256) |  |  | 订单抛转节点 |  |  |  |
| `manu_order_num` | VARCHAR(256) |  |  | 制造公司销售订单号 |  |  |  |
| `terminal_order_num` | VARCHAR(256) |  |  | 终端销售公司销售订单号 |  |  |  |
| `contract_num` | VARCHAR(256) |  |  | 采购订单号（合同号） |  |  |  |
| `contract_m_num` | VARCHAR(256) |  |  | 母合同号 |  |  |  |
| `order_type_code` | VARCHAR(256) |  |  | 销售订单类型 |  |  |  |
| `order_type_name` | VARCHAR(256) |  |  | 销售订单类型描述 |  |  |  |
| `customer_code` | VARCHAR(256) |  |  | 客户编码 |  |  |  |
| `customer_name` | VARCHAR(256) |  |  | 客户名称 |  |  |  |
| `in_customer_flag` | VARCHAR(256) |  |  | 是否内部关联客户 |  |  |  |
| `product_code` | VARCHAR(256) |  |  | 产品编码 |  |  |  |
| `product_name` | VARCHAR(256) |  |  | 产品描述 |  |  |  |
| `product_series_code` | VARCHAR(256) |  |  | 产品所属产品族编码 |  |  |  |
| `product_series_name` | VARCHAR(256) |  |  | 产品所属产品族名称 |  |  |  |
| `product_group_code` | VARCHAR(256) |  |  | 产品所属产品线编码 |  |  |  |
| `product_group_name` | VARCHAR(256) |  |  | 产品所属产品线名称 |  |  |  |
| `industry_code` | VARCHAR(256) |  |  | 产品所属产业编码 |  |  |  |
| `industry_name` | VARCHAR(256) |  |  | 产品所属产业名称 |  |  |  |
| `sales_person_num` | VARCHAR(256) |  |  | 业务员编码 |  |  |  |
| `sales_person_name` | VARCHAR(256) |  |  | 业务员姓名 |  |  |  |
| `sales_org_code` | VARCHAR(256) |  |  | 销售组织编码 |  |  |  |
| `sales_org_name` | VARCHAR(256) |  |  | 销售组织名称 |  |  |  |
| `sales_dept_code` | VARCHAR(256) |  |  | 销售部门编码 |  |  |  |
| `sales_dept_name` | VARCHAR(256) |  |  | 销售部门名称 |  |  |  |
| `sales_group_code` | VARCHAR(256) |  |  | 销售组编码 |  |  |  |
| `sales_group_name` | VARCHAR(256) |  |  | 销售组名称 |  |  |  |
| `sales_unit_code` | VARCHAR(256) |  |  | 销售战区 |  |  |  |
| `sales_unit_name` | VARCHAR(256) |  |  | 销售战区名称 |  |  |  |
| `sales_area_code` | VARCHAR(256) |  |  | 销售大区 |  |  |  |
| `sales_area_name` | VARCHAR(256) |  |  | 销售大区名称 |  |  |  |
| `sales_region_code` | VARCHAR(256) |  |  | 销售区域 |  |  |  |
| `sales_region_name` | VARCHAR(256) |  |  | 销售区域名称 |  |  |  |
| `employee_dept_code` | VARCHAR(256) |  |  | 雇员部门编码 |  |  |  |
| `employee_dept_name` | VARCHAR(256) |  |  | 雇员部门名称 |  |  |  |
| `sales_channel_code` | VARCHAR(256) |  |  | 分销渠道编码 |  |  |  |
| `sales_channel_name` | VARCHAR(256) |  |  | 分销渠道名称 |  |  |  |
| `industry_type` | VARCHAR(256) |  |  | 行业类型 |  |  |  |
| `factory_code` | VARCHAR(256) |  |  | 工厂编码 |  |  |  |
| `factory_name` | VARCHAR(256) |  |  | 工厂名称 |  |  |  |
| `piz_line_name` | VARCHAR(100) |  |  | 业务产品线 |  |  |  |
| `sales_percent` | VARCHAR(256) |  |  | 业绩分配比例 |  |  |  |
| `order_qty` | VARCHAR(256) |  |  | 销售数量 |  |  |  |
| `sales_percent` | VARCHAR(256) |  |  |  |  |  |  |
| `product_area` | VARCHAR(256) |  |  | 产品单位面积 |  |  |  |
| `order_area` | VARCHAR(256) |  |  | 销售面积 |  |  |  |
| `sales_percent` | VARCHAR(256) |  |  |  |  |  |  |
| `post_qty` | VARCHAR(256) |  |  | 过账数量 |  |  |  |
| `sales_percent` | VARCHAR(256) |  |  |  |  |  |  |
| `dr_cr_tag` | VARCHAR(256) |  |  | 借贷项标识 |  |  |  |
| `account_code` | VARCHAR(256) | Y |  | 科目编码 |  |  |  |
| `account_name` | VARCHAR(256) |  |  | 科目名称 |  |  |  |
| `parent_acc_code` | VARCHAR(256) |  |  | 父级科目编码 |  |  |  |
| `currency` | VARCHAR(256) |  |  | 原币别 |  |  |  |
| `local_currency` | VARCHAR(256) |  |  | 本位币别 |  |  |  |
| `ccy_notax_amt` | VARCHAR(256) |  |  | 按原币计的金额_不含税 |  |  |  |
| `sales_percent` | VARCHAR(256) |  |  |  |  |  |  |
| `lcy_notax_amt` | VARCHAR(256) |  |  | 按本位币计的金额_不含税 |  |  |  |
| `sales_percent` | VARCHAR(256) |  |  |  |  |  |  |
| `cny_notax_amt` | VARCHAR(256) |  |  | 金额_人民币_不含税 |  |  |  |
| `sales_percent` | VARCHAR(256) |  |  |  |  |  |  |
| `usd_notax_amt` | VARCHAR(256) |  |  | 金额_美元_不含税 |  |  |  |
| `sales_percent` | VARCHAR(256) |  |  |  |  |  |  |

## 模型逻辑详情（原文备忘）

```
模型标准 系统A（独立逻辑，可来源于多系统）
序号 字段说明 字段编码 字段类型 维度/度量 主键 值域 非空 获取方式 来源表 来源字段 来源字段名称 来源字段类型 来源字段说明 计算逻辑 备注
1 公司代码 company_code 维度 Y Y 直接获取 dws_标准收入成本模型 company_code 公司代码
公司名称 company_name 维度 直接获取 dws_标准收入成本模型 company_name
2 销售订单号 sales_invoice_num 维度 Y 直接获取 dws_标准收入成本模型 order_num sales_invoice_num
3 销售订单行号 sales_invoice_line 维度 Y 直接获取 dws_标准收入成本模型 order_item_num sales_invoice_line
4 会计年度 fiscal_year 维度 Y 直接获取 dws_标准收入成本模型 iscal_year fiscal_year
5 会计期间 fiscal_period 维度 Y 直接获取 dws_标准收入成本模型 account_period fiscal_period
过账日期 voucher_post_date 维度 Y 直接获取 dws_标准收入成本模型 voucher_post_date
6 订单抛转节点 transfer_node 维度 直接获取 dws_标准收入成本模型 transfer_node 订单抛转节点
7 制造公司销售订单号 manu_order_num 维度 直接获取 dws_标准收入成本模型 manu_order_num 制造公司销售订单号
8 终端销售公司销售订单号 terminal_order_num 维度 直接获取 dws_标准收入成本模型 terminal_order_num 终端销售公司销售订单号
9 采购订单号（合同号） contract_num 维度 直接获取 dws_标准收入成本模型 contract_num 采购订单号（合同号）
10 母合同号 contract_m_num 维度 直接获取 dws_标准收入成本模型 contract_m_num 母合同号
11 销售订单类型 order_type_code 维度 直接获取 dws_标准收入成本模型 order_type_code 销售订单类型
12 销售订单类型描述 order_type_name 维度 直接获取 dws_标准收入成本模型 order_type_name 销售订单类型描述
13 客户编码 customer_code 维度 直接获取 dws_标准收入成本模型 customer_code 客户编码
14 客户名称 customer_name 维度 直接获取 dws_标准收入成本模型 customer_name 客户名称
15 是否内部关联客户 in_customer_flag 维度 直接获取 dws_标准收入成本模型 in_customer_flag 是否内部关联客户
16 产品编码 product_code 维度 直接获取 dws_标准收入成本模型 product_code 产品编码
17 产品描述 product_name 维度 直接获取 dws_标准收入成本模型 product_name 产品描述
18 产品所属产品族编码 product_series_code 维度 直接获取 dws_标准收入成本模型 product_series_code 产品所属产品族编码
19 产品所属产品族名称 product_series_name 维度 直接获取 dws_标准收入成本模型 product_series_name 产品所属产品族名称
20 产品所属产品线编码 product_group_code 维度 直接获取 dws_标准收入成本模型 product_group_code 产品所属产品线编码
21 产品所属产品线名称 product_group_name 维度 直接获取 dws_标准收入成本模型 product_group_name 产品所属产品线名称
22 产品所属产业编码 industry_code 维度 直接获取 dws_标准收入成本模型 industry_code 产品所属产业编码
23 产品所属产业名称 industry_name 维度 直接获取 dws_标准收入成本模型 industry_name 产品所属产业名称
24 业务员编码 sales_person_num 维度 直接获取 dws_标准收入成本模型 sales_person_num 业务员编码 dws_标准收入成本模型-order_num~dwd_业绩分配比例-order_num，dws_标准收入成本模型-contract_num~dwd_业绩分配比例-contract_num，关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的业务员编码；若关联不上则保持dws_接单业绩模型数据不变
25 业务员姓名 sales_person_name 维度 直接获取 dws_标准收入成本模型 sales_person_name 业务员姓名 dws_标准收入成本模型-order_num~dwd_业绩分配比例-order_num，dws_标准收入成本模型-contract_num~dwd_业绩分配比例-contract_num，关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的业务员名称；若关联不上则保持dws_接单业绩模型数据不变
26 销售组织编码 sales_org_code 维度 直接获取 dws_标准收入成本模型 sales_org_code 销售组织编码
27 销售组织名称 sales_org_name 维度 直接获取 dws_标准收入成本模型 sales_org_name 销售组织名称
28 销售部门编码 sales_dept_code 维度 直接获取 dws_标准收入成本模型 sales_dept_code 销售部门编码 dws_接单业绩模型-order_num~dwd_业绩分配比例-order_num，dws_接单业绩模型-contract_num~dwd_业绩分配比例-contract_num，关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的销售部门编码；若关联不上则保持dws_接单业绩模型数据不变
29 销售部门名称 sales_dept_name 维度 直接获取 dws_标准收入成本模型 sales_dept_name 销售部门名称 dws_接单业绩模型-order_num~dwd_业绩分配比例-order_num，dws_接单业绩模型-contract_num~dwd_业绩分配比例-contract_num，关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的销售部门名称；若关联不上则保持dws_接单业绩模型数据不变
30 销售组编码 sales_group_code 维度 直接获取 dws_标准收入成本模型 sales_group_code 销售组编码 dws_接单业绩模型-order_num~dwd_业绩分配比例-order_num，dws_接单业绩模型-contract_num~dwd_业绩分配比例-contract_num，关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的销售组编码；若关联不上则保持dws_接单业绩模型数据不变
31 销售组名称 sales_group_name 维度 直接获取 dws_标准收入成本模型 sales_group_name 销售组名称 dws_接单业绩模型-order_num~dwd_业绩分配比例-order_num，dws_接单业绩模型-contract_num~dwd_业绩分配比例-contract_num，关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的销售组名称；若关联不上则保持dws_接单业绩模型数据不变
32 销售战区 sales_unit_code 维度 直接获取 dws_标准收入成本模型 sales_unit_code 销售战区 dws_接单业绩模型-order_num~dwd_业绩分配比例-order_num，dws_接单业绩模型-contract_num~dwd_业绩分配比例-contract_num，关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的销售区域；若关联不上则保持dws_接单业绩模型数据不变
33 销售战区名称 sales_unit_name 维度 直接获取 dws_标准收入成本模型 sales_unit_name 销售战区名称 dws_接单业绩模型-order_num~dwd_业绩分配比例-order_num，dws_接单业绩模型-contract_num~dwd_业绩分配比例-contract_num，关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的销售战区名称；若关联不上则保持dws_接单业绩模型数据不变
34 销售大区 sales_area_code 维度 直接获取 dws_标准收入成本模型 sales_area_code 销售大区 dws_接单业绩模型-order_num~dwd_业绩分配比例-order_num，dws_接单业绩模型-contract_num~dwd_业绩分配比例-contract_num，关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的销售大区；若关联不上则保持dws_接单业绩模型数据不变
35 销售大区名称 sales_area_name 维度 直接获取 dws_标准收入成本模型 sales_area_name 销售大区名称 dws_接单业绩模型-order_num~dwd_业绩分配比例-order_num，dws_接单业绩模型-contract_num~dwd_业绩分配比例-contract_num，关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的销售大区；若关联不上则保持dws_接单业绩模型数据不变
36 销售区域 sales_region_code 维度 直接获取 dws_标准收入成本模型 sales_region_code 销售区域 dws_接单业绩模型-order_num~dwd_业绩分配比例-order_num，dws_接单业绩模型-contract_num~dwd_业绩分配比例-contract_num，关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的销售区域编码；若关联不上则保持dws_接单业绩模型数据不变
37 销售区域名称 sales_region_name 维度 直接获取 dws_标准收入成本模型 sales_region_name 销售区域名称 dws_接单业绩模型-order_num~dwd_业绩分配比例-order_num，dws_接单业绩模型-contract_num~dwd_业绩分配比例-contract_num，关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的销售区域名称；若关联不上则保持dws_接单业绩模型数据不变
38 雇员部门编码 employee_dept_code 维度 直接获取 dws_标准收入成本模型 employee_dept_code 雇员部门编码 dws_接单业绩模型-order_num~dwd_业绩分配比例-order_num，dws_接单业绩模型-contract_num~dwd_业绩分配比例-contract_num，关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的雇员部门编码；若关联不上则保持dws_接单业绩模型数据不变
39 雇员部门名称 employee_dept_name 维度 直接获取 dws_标准收入成本模型 employee_dept_name 雇员部门名称 dws_接单业绩模型-order_num~dwd_业绩分配比例-order_num，dws_接单业绩模型-contract_num~dwd_业绩分配比例-contract_num，关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的雇员部门描述；若关联不上则保持dws_接单业绩模型数据不变
40 分销渠道编码 sales_channel_code 维度 直接获取 dws_标准收入成本模型 sales_channel_code 分销渠道编码
41 分销渠道名称 sales_channel_name 维度 显示通用、照明工程类、照明EMC... 直接获取 dws_标准收入成本模型 sales_channel_name 分销渠道名称
42 行业类型 industry_type 维度 直接获取 dws_标准收入成本模型 industry_type 行业类型
43 工厂编码 factory_code 维度 直接获取 dws_标准收入成本模型 factory_code 工厂编码
44 工厂名称 factory_name 维度 直接获取 dws_标准收入成本模型 factory_name 工厂名称
业务产品线 piz_line_name varchar(100) 维度 直接获取 dws_标准收入成本模型 piz_line_name
业绩分配比例 sales_percent 度量 直接获取 dwd_业绩分配比例 sales_percent 业绩分配比例 以order_num~DWD业绩分配比例表order_num，contract_num~DWD业绩分配比例表contract_num，根据前置拆分出的业务员编码和销售组、雇员部门编码获取sales_percent,匹配不上默认为100%
45 销售数量 order_qty 度量 直接获取 dws_标准收入成本模型 order_qty 销售数量 dws_标准收入成本模型【order_qty】*dwd_业绩分配比例
【sales_percent】
46 产品单位面积 product_area 度量 直接获取 dws_标准收入成本模型 product_area 产品单位面积
47 销售面积 order_area 度量 直接获取 dws_标准收入成本模型 order_area 销售面积 dws_标准收入成本模型【order_area】*dwd_业绩分配比例
【sales_percent】
48 过账数量 post_qty 度量 直接获取 dws_标准收入成本模型 post_qty 过账数量 dws_标准收入成本模型【unit_number】*dwd_业绩分配比例
【sales_percent】
49 借贷项标识 dr_cr_tag 度量 直接获取 dws_标准收入成本模型 dr_cr_tag 借贷项标识
50 科目编码 account_code 维度 Y 直接获取 dws_标准收入成本模型 account_code 科目编码
51 科目名称 account_name 维度 直接获取 dws_标准收入成本模型 account_name 科目名称
父级科目编码 parent_acc_code 维度 直接获取 dim_科目维 parent_acc_code
52 原币别 currency 维度 直接获取 dws_标准收入成本模型 currency 原币别
53 本位币别 local_currency 维度 直接获取 dws_标准收入成本模型 local_currency 本位币别
54 按原币计的金额_不含税 ccy_notax_amt 度量 直接获取 dws_标准收入成本模型 ccy_notax_amt 按原币计的金额_不含税 dws_标准收入成本模型【currency_no_tax_amt】*dwd_业绩分配比例
【sales_percent】
55 按本位币计的金额_不含税 lcy_notax_amt 度量 直接获取 dws_标准收入成本模型 lcy_notax_amt 按本位币计的金额_不含税 dws_标准收入成本模型【local_currency_no_tax_amt】*dwd_业绩分配比例
【sales_percent】
56 金额_人民币_不含税 cny_notax_amt 度量 直接获取 dws_标准收入成本模型 cny_notax_amt 金额_人民币_不含税 dws_标准收入成本模型【currency_no_tax_rmb_amt】*dwd_业绩分配比例
【sales_percent】
57 金额_美元_不含税 usd_notax_amt 度量 直接获取 dws_标准收入成本模型 usd_notax_amt 金额_美元_不含税 dws_标准收入成本模型【currency_no_tax_usd_amt】*dwd_业绩分配比例
【sales_percent】
58 数据插入日期 审计 自定义
59 数据更新日期 审计 自定义
```

## 建议开发步骤

- 解析数据来源表/模型逻辑概述/模型逻辑详情三段内容，锁定目标表与字段。
- 校验字段类型、主键、分区列，生成 StarRocks DDL（可选执行）。
- 生成 INSERT SELECT 逻辑，按业务逻辑完成关联、去重、计算。
- 落地中间结果或目标表，补充数据质量校验与唯一性检查。
- 补充自动化脚本与回填验证（行数、键唯一性、NULL 分布）。
