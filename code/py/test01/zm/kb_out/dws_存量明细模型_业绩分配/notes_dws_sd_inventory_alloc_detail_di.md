# dws_存量明细模型_业绩分配 开发说明

## 目标表

- 英文表名: `dws_sd_inventory_alloc_detail_di`
- 中文含义: dws_存量明细模型_业绩分配
- 用途: table
- 建议业务主键: `Unnamed: 2`

## 模型逻辑概述（原文）

```
序号 英文表名 中文含义 应用场景 备注
1 dws_sd_inventory_alloc_detail_di dws_存量明细模型_业绩分配 含业绩分配的存量模型

```

## 目标表字段结构（解析记忆）

| 字段编码 | 字段类型 | 主键 | 非空 | 字段说明 | 来源表 | 来源字段 | 计算逻辑 |
|----------|----------|------|------|----------|--------|----------|----------|
| `dt` | VARCHAR(256) |  |  | 日期(分区字段YYYYMMDD） |  |  |  |
| `a` | VARCHAR(256) |  |  | ） |  |  |  |
| `order_num` | VARCHAR(256) |  |  | 销售公司销售订单号 |  |  |  |
| `contract_num` | VARCHAR(256) |  |  | 客户采购订单编号 |  |  |  |
| `order_item_num` | VARCHAR(256) |  |  | 订单行项目号 |  |  |  |
| `order_type_code` | VARCHAR(256) |  |  | 订单类型编码 |  |  |  |
| `manu_sales_order_num` | VARCHAR(256) |  |  | 制造公司销售订单号 |  |  |  |
| `order_num_transfer` | VARCHAR(256) |  |  | 销售订单号_含抛转 |  |  |  |
| `create_date` | VARCHAR(256) |  |  | 订单创建日期 |  |  |  |
| `approval_date` | VARCHAR(256) |  |  | 订单首次审批日期 |  |  |  |
| `newest_date` | VARCHAR(256) |  |  | 订单最新日期 |  |  |  |
| `contract_need_days` | VARCHAR(256) |  |  | 合同需求天数 |  |  |  |
| `contract_need_date` | VARCHAR(256) |  |  | 合同需求日期 |  |  |  |
| `review_del_days` | VARCHAR(256) |  |  | 评审交期（天数） |  |  |  |
| `pmc_review_del_date` | VARCHAR(256) |  |  | PMC评审交期 |  |  |  |
| `request_del_date` | VARCHAR(256) |  |  | 预计出货日期 |  |  |  |
| `pmc_change_date` | VARCHAR(256) |  |  | PMC变更日期 |  |  |  |
| `last_receipt_date` | VARCHAR(256) |  |  |  |  |  |  |
| `a` | VARCHAR(256) |  |  | latest_warehousing_date 维度 |  |  |  |
| `last_receipt_date` | VARCHAR(256) |  |  |  |  |  |  |
| `latest_warehousing_date` | VARCHAR(256) |  |  |  |  |  |  |
| `transport_method` | VARCHAR(256) |  |  | 运输方式 |  |  |  |
| `payment_method` | VARCHAR(256) |  |  | 贸易条款 |  |  |  |
| `sales_org_code` | VARCHAR(256) |  |  | 销售组织编码 |  |  |  |
| `sales_org_name` | VARCHAR(256) |  |  | 销售组织名称 |  |  |  |
| `factory_code` | VARCHAR(256) |  |  | 工厂编码 |  |  |  |
| `factory_name` | VARCHAR(256) |  |  | 工厂名称 |  |  |  |
| `employee_dept_code` | VARCHAR(256) |  |  | 雇员部门编码 |  |  |  |
| `employee_dept_code` | VARCHAR(256) |  |  | b 雇员部门编号 |  |  |  |
| `employee_dept_name` | VARCHAR(256) |  |  | 雇员部门名称 |  |  |  |
| `employee_dept_name` | VARCHAR(256) |  |  | b 雇员部门 |  |  |  |
| `sales_person_num` | VARCHAR(256) |  |  | 业务员编码 |  |  |  |
| `sales_person_num` | VARCHAR(256) |  |  | b 人员编码 |  |  |  |
| `sales_person_name` | VARCHAR(256) |  |  | 业务员姓名 |  |  |  |
| `sales_person_name` | VARCHAR(256) |  |  | 名 |  |  |  |
| `sales_dept_code` | VARCHAR(256) |  |  | 销售部门编码 |  |  |  |
| `sales_dept_code` | VARCHAR(256) |  |  | b 销售部门编码 |  |  |  |
| `sales_dept_name` | VARCHAR(256) |  |  | 销售部门名称 |  |  |  |
| `sales_dept_name` | VARCHAR(256) |  |  | b 销售部门描述 |  |  |  |
| `sales_group_code` | VARCHAR(256) |  |  | 销售组编码 |  |  |  |
| `sales_group_code` | VARCHAR(256) |  |  | b 销售组编码 |  |  |  |
| `sales_group_name` | VARCHAR(256) |  |  | 销售组名称 |  |  |  |
| `sales_group_name` | VARCHAR(256) |  |  | b 销售组 |  |  |  |
| `sales_unit_code` | VARCHAR(256) |  |  | 销售战区编码 |  |  |  |
| `sales_unit_code` | VARCHAR(256) |  |  | b 销售战区编码 |  |  |  |
| `sales_unit_name` | VARCHAR(255) |  |  | 销售战区名称 |  |  |  |
| `sales_unit_name` | VARCHAR(256) |  |  | b 销售战区名称 |  |  |  |
| `sales_area_code` | VARCHAR(256) |  |  | 销售大区编码 |  |  |  |
| `sales_area_code` | VARCHAR(256) |  |  | b 销售大区编码 |  |  |  |
| `sales_area_name` | VARCHAR(255) |  |  | 销售大区名称 |  |  |  |
| `sales_area_name` | VARCHAR(256) |  |  | b 销售大区名称 |  |  |  |
| `sales_region_code` | VARCHAR(256) |  |  | 销售区域编码 |  |  |  |
| `sales_region_code` | VARCHAR(256) |  |  | b 销售区域编码 |  |  |  |
| `sales_region_name` | VARCHAR(255) |  |  | 销售区域名称 |  |  |  |
| `sales_region_name` | VARCHAR(256) |  |  | b 销售区域名称 |  |  |  |
| `customer_code` | VARCHAR(256) |  |  | 客户编码 |  |  |  |
| `customer_name` | VARCHAR(256) |  |  | 客户名称 |  |  |  |
| `product_type` | VARCHAR(256) |  |  | 产品类型 |  |  |  |
| `industry_type` | VARCHAR(256) |  |  | 行业类型 |  |  |  |
| `industry_code` | VARCHAR(256) |  |  | 产业编码 |  |  |  |
| `industry_name` | VARCHAR(256) |  |  | 产业描述 |  |  |  |
| `order_line_inv_flag` | VARCHAR(256) |  |  | 订单行库存标识 |  |  |  |
| `industry_code` | VARCHAR(256) |  |  | 产业编码 |  |  |  |
| `industry_name` | VARCHAR(256) |  |  | 产业描述 |  |  |  |
| `product_line_code` | VARCHAR(256) |  |  | 产品线编码 |  |  |  |
| `product_line_name` | VARCHAR(256) |  |  | 产品线描述 |  |  |  |
| `product_group_code` | VARCHAR(256) |  |  | 产品族编码 |  |  |  |
| `product_group_name` | VARCHAR(256) |  |  | 产品族描述 |  |  |  |
| `piz_line_name` | VARCHAR(100) |  |  | 业务产品线 |  |  |  |
| `product_area` | VARCHAR(256) |  |  | 产品单位面积 |  |  |  |
| `inventory_status` | VARCHAR(256) |  |  | 库存状态 |  |  |  |
| `order_exec_status` | VARCHAR(256) |  |  | 订单执行状态 |  |  |  |
| `noship_reason` | VARCHAR(256) |  |  | 未出货原因 |  |  |  |
| `noship_reason_category` | VARCHAR(256) |  |  | 未出货原因类别 |  |  |  |
| `cny_exchange_rate` | VARCHAR(256) |  |  | 汇率（to_CNY） |  |  |  |
| `usd_exchange_rate` | VARCHAR(256) |  |  | 汇率（to_USD） |  |  |  |
| `sales_percent` | VARCHAR(256) |  |  | new 业绩分配比例 |  |  |  |
| `unshipment_qty` | VARCHAR(256) |  |  | 业绩分配-未出货数量 |  |  |  |
| `unshipment_area` | VARCHAR(256) |  |  | 业绩分配-未出货面积 |  |  |  |
| `unshipment_tax_amt` | VARCHAR(256) |  |  | 业绩分配-未出货金额（含运费）_含税-原币 |  |  |  |
| `unshipment_tax_cny_amt` | VARCHAR(256) |  |  | 业绩分配-未出货金额（含运费）_含税-CNY |  |  |  |
| `unshipment_tax_usd_amt` | VARCHAR(256) |  |  | 业绩分配-未出货金额（含运费）_含税-USD |  |  |  |
| `unshipment_notax_amt` | VARCHAR(256) |  |  | 业绩分配-未出货金额（含运费）_不含税-原币 |  |  |  |
| `unshipment_notax_cny_amt` | VARCHAR(256) |  |  | 业绩分配-未出货金额（含运费）_不含税-CNY |  |  |  |
| `unshipment_notax_usd_amt` | VARCHAR(256) |  |  | 业绩分配-未出货金额（含运费）_不含税-USD |  |  |  |
| `source_system` | VARCHAR(256) |  |  | 最新更新日期 |  |  |  |
| `insert_dt` | VARCHAR(256) |  |  | 来源系统 |  |  |  |

## 模型逻辑详情（原文备忘）

```
模型标准 系统（独立逻辑，可来源于多系统）
序号 字段备注 字段名称 字段编码 字段类型 维度/度量 主键 值域 非空 获取方式 来源表 来源字段 来源字段说明 数据类型 值列表 关联条件 计算逻辑 备注1 备注2
日期(分区字段YYYYMMDD） dt 维度 a 版本日期
工厂（这个单是哪个工厂生产的？） a 工厂（这个单是哪个工厂生产的？）
销售公司销售订单号 order_num 维度 a 销售公司销售订单号 order_num
客户采购订单编号 contract_num 维度 a 客户采购订单编号（合同号） contract_num
订单行项目号 order_item_num 维度 a 订单行项目号 order_item_num
订单类型编码 order_type_code 维度 a 订单类型编码 order_type_code
制造公司销售订单号 manu_sales_order_num 维度 a 制造公司销售订单号 manu_sales_order_num
销售订单号_含抛转 order_num_transfer 维度 a 销售订单号_含抛转 order_num_transfer
订单创建日期 create_date 维度 a 订单创建日期 create_date
订单首次审批日期 approval_date 维度 a 订单首次审批日期 approval_date
订单最新日期 newest_date a 订单最新日期 newest_date
合同需求天数 contract_need_days 维度 a 合同需求天数 contract_need_days
合同需求日期 contract_need_date 维度 a 合同需求日期 contract_need_date
评审交期（天数） review_del_days 维度 a 请求交货天数 review_del_days
PMC评审交期 pmc_review_del_date 维度 a PMC评审交期 pmc_review_del_date
预计出货日期 request_del_date 维度 a 预计出货日期 request_del_date
PMC变更日期 pmc_change_date 维度 a PMC变更日期 pmc_change_date
最后入库日期
最新入库日期 last_receipt_date
latest_warehousing_date 维度 a 最后入库日期
最新入库日期 last_receipt_date
latest_warehousing_date
运输方式 transport_method a 运输方式 transport_method
贸易条款 payment_method a 贸易条款 payment_method
销售组织编码 sales_org_code 维度 直接获取 a 销售组织 sales_org_code
销售组织名称 sales_org_name 维度 直接获取 a 销售组织名称 sales_org_name
工厂编码 factory_code a 工厂编码 factory_code
工厂名称 factory_name a 工厂名称 factory_name
雇员部门编码 employee_dept_code 维度 直接获取 a
b 雇员部门编号 employee_dept_code 关联条件：订单号+合同号
关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的雇员部门编码；若关联不上则保持dwd_订单明细模型数据不变
雇员部门名称 employee_dept_name 维度 直接获取 a
b 雇员部门 employee_dept_name 关联条件：订单号+合同号
关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的雇员部门描述；若关联不上则保持dwd_订单明细模型数据不变
业务员编码 sales_person_num 维度 直接获取 a
b 人员编码 sales_person_num 关联条件：订单号+合同号
关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的业务员编码；若关联不上则保持dwd_订单明细模型数据不变
业务员姓名 sales_person_name 维度 直接获取 a
b 姓+名 sales_person_name 关联条件：订单号+合同号
关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的业务员描述；若关联不上则保持dwd_订单明细模型数据不变
销售部门编码 sales_dept_code a
b 销售部门编码 sales_dept_code 关联条件：订单号+合同号
关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的销售部门编码；若关联不上则保持dwd_订单明细模型数据不变
销售部门名称 sales_dept_name a
b 销售部门描述 sales_dept_name 关联条件：订单号+合同号
关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的销售部门描述；若关联不上则保持dwd_订单明细模型数据不变
销售组编码 sales_group_code a
b 销售组编码 sales_group_code 关联条件：订单号+合同号
关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的销售组编码；若关联不上则保持dwd_订单明细模型数据不变
销售组名称 sales_group_name a
b 销售组 sales_group_name 关联条件：订单号+合同号
关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的销售组描述；若关联不上则保持dwd_订单明细模型数据不变
销售战区编码 sales_unit_code 维度 直接获取 a
b 销售战区编码 sales_unit_code 关联条件：订单号+合同号
关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的省份/国家销售战区；若关联不上则保持dwd_订单明细模型数据不变
销售战区名称 sales_unit_name varchar(255) 维度 直接获取 a
b 销售战区名称 sales_unit_name 关联条件：订单号+合同号
关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的省份/国家销售战区；若关联不上则保持dwd_订单明细模型数据不变
销售大区编码 sales_area_code 维度 直接获取 a
b 销售大区编码 sales_area_code 关联条件：订单号+合同号
关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的销售大区；若关联不上则保持dwd_订单明细模型数据不变
销售大区名称 sales_area_name varchar(255) 维度 直接获取 a
b 销售大区名称 sales_area_name 关联条件：订单号+合同号
关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的销售大区；若关联不上则保持dwd_订单明细模型数据不变
销售区域编码 sales_region_code 维度 直接获取 a
b 销售区域编码 sales_region_code 关联条件：订单号+合同号
关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的目标范围销售区域；若关联不上则保持dwd_订单明细模型数据不变
销售区域名称 sales_region_name varchar(255) 维度 直接获取 a
b 销售区域名称 sales_region_name 关联条件：订单号+合同号
关联到多条数据则需要将数据拆分为多条，取业绩分配比例表的目标范围销售区域；若关联不上则保持dwd_订单明细模型数据不变
客户编码 customer_code 维度 直接获取 a 客户编码 customer_code
客户名称 customer_name 维度 直接获取 a 客户名称 customer_name
产品类型 product_type 维度 直接获取 a 产品类型 product_type
行业类型 industry_type 维度 直接获取 a 行业类型 industry_type
产业编码 industry_code 维度 直接获取 a 物料号 industry_code
产业描述 industry_name 维度 直接获取 a 物料描述 industry_name
订单行库存标识 order_line_inv_flag 度量 a 订单行库存标识 order_line_inv_flag
产业编码 industry_code a 产业编码 industry_code
产业描述 industry_name a 产业描述 industry_name
产品线编码 product_line_code a 产品线编码 product_line_code
产品线描述 product_line_name a 产品线描述 product_line_name
产品族编码 product_group_code a 产品族编码 product_group_code
产品族描述 product_group_name a 产品族描述 product_group_name
业务产品线 piz_line_name varchar(100) 维度 直接获取 b 业务产品线 piz_line_name
产品单位面积 product_area a 产品单位面积 product_area
库存状态 inventory_status 维度 直接获取 a 库存状态 inventory_status
订单执行状态 order_exec_status 维度 a 订单执行状态 order_exec_status
未出货原因 noship_reason 直接获取 a 未出货原因 noship_reason
未出货原因类别 noship_reason_category 直接获取 a 未出货原因类别 noship_reason_category
汇率（to_CNY） cny_exchange_rate 度量 直接获取 a 汇率（to_CNY） cny_exchange_rate
汇率（to_USD） usd_exchange_rate 度量 直接获取 a 汇率（to_USD） usd_exchange_rate
new 业绩分配比例 sales_percent 维度 直接获取 b 配额 sales_percent 关联条件：订单号+合同号 含业绩分配
业绩分配-未出货数量 unshipment_qty 度量 a 未出货数量 【未出货数量】*【业绩分配比例】/100
业绩分配-未出货面积 unshipment_area 度量 a 未出货面积 【未出货面积】*【业绩分配比例】/100
业绩分配-未出货金额（含运费）_含税-原币 unshipment_tax_amt a 未出货金额（含运费）_含税-原币 【未出货金额（含运费）_含税-原币】*【业绩分配比例】/100
业绩分配-未出货金额（含运费）_含税-CNY unshipment_tax_cny_amt a 未出货金额（含运费）_含税-CNY 【未出货金额（含运费）_含税-CNY】*【业绩分配比例】/100
业绩分配-未出货金额（含运费）_含税-USD unshipment_tax_usd_amt 度量 a 未出货金额（含运费）_含税-USD 【未出货金额（含运费）_含税-USD】*【业绩分配比例】/100
业绩分配-未出货金额（含运费）_不含税-原币 unshipment_notax_amt a 未出货金额（含运费）_不含税-原币 【未出货金额（含运费）_不含税-原币】*【业绩分配比例】/100
业绩分配-未出货金额（含运费）_不含税-CNY unshipment_notax_cny_amt 度量 a 未出货金额（含运费）_不含税-CNY 【未出货金额（含运费）_不含税-CNY】*【业绩分配比例】/100
业绩分配-未出货金额（含运费）_不含税-USD unshipment_notax_usd_amt a 未出货金额（含运费）_不含税-USD 【未出货金额（含运费）_不含税-USD】*【业绩分配比例】/100
最新更新日期 source_system 审计 直接获取 a 最新更新日期
来源系统 insert_dt 审计 直接获取 a 来源系统
```

## 建议开发步骤

- 解析数据来源表/模型逻辑概述/模型逻辑详情三段内容，锁定目标表与字段。
- 校验字段类型、主键、分区列，生成 StarRocks DDL（可选执行）。
- 生成 INSERT SELECT 逻辑，按业务逻辑完成关联、去重、计算。
- 落地中间结果或目标表，补充数据质量校验与唯一性检查。
- 补充自动化脚本与回填验证（行数、键唯一性、NULL 分布）。
