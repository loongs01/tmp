# dws_标准收入成本模型 开发说明

## 目标表

- 英文表名: `dws_fin_revenue_cost_df`
- 中文含义: dws_标准收入成本模型
- 用途: table
- 建议业务主键: `Unnamed: 2`

## 数据来源表

```
来源系统 英文表名 中文含义 表别名 备注
数仓 dwd_fin_acc_voucher_detail_df dwd_会计凭证明细表 主表
数仓 dwd_sd_order_detail_df dwd_订单明细模型
dwd_sd_order_transfer_relation_df dwd_订单抛转关系表
```

## 模型逻辑概述（原文）

```
序号 英文表名 中文含义 应用场景 备注
1 dws_fin_revenue_cost_df dws_标准收入成本模型
关联条件
①将会计凭证明细表关联科目维，会计科目编号_subject_no~科目编码_account_code，限定科目类型编码_account_type_code=06，按照公司代码_company_code、销售凭证_【order_num】、销售凭证项目_【order_item_num】、会计年度_【iscal_year】、会计期间_【account_period】、凭证过账期间【doc_post_date】、会计科目编号_【subject_no】对按按原币计的金额_不含税currency_no_tax_amt、按本位币计的金额_不含税local_currency_no_tax_amt、数量_unit_number进行汇总
②会计凭证明细表_销售订单号【order_num】与订单抛转信息-【original_order_num】关联
③会计凭证明细表_销售订单号【order_num】与订单明细模型-【order_num】关联，会计凭证明细表【order_item_num】=订单明细模型【order_item_num】
④会计凭证明细表_科目编码【subject_num】~dim_科目维【account_code】，dim_科目维.【parent_acc_code】in （'600100','605100','630100','640100'）
```

## 目标表字段结构（解析记忆）

| 字段编码 | 字段类型 | 主键 | 非空 | 字段说明 | 来源表 | 来源字段 | 计算逻辑 |
|----------|----------|------|------|----------|--------|----------|----------|
| `company_code` | VARCHAR(256) | Y |  | 公司代码 | Y | Y |  |
| `BKPF` | VARCHAR(256) |  |  | join |  |  |  |
| `where` | VARCHAR(256) |  |  |  |  |  |  |
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
| `order_qty` | VARCHAR(256) |  |  | 销售数量 |  |  |  |
| `product_area` | VARCHAR(256) |  |  | 产品单位面积 |  |  |  |
| `order_area` | VARCHAR(256) |  |  | 销售面积 |  |  |  |
| `post_qty` | VARCHAR(256) |  |  | 过账数量 |  |  |  |
| `dr_cr_tag` | VARCHAR(256) |  |  | 借贷项标识 |  |  |  |
| `account_code` | VARCHAR(256) | Y |  | 科目编码 |  |  |  |
| `account_name` | VARCHAR(256) |  |  | 科目名称 |  |  |  |
| `parent_acc_code` | VARCHAR(256) |  |  | 父层科目编码 |  |  |  |
| `currency` | VARCHAR(256) |  |  | 原币别 |  |  |  |
| `local_currency` | VARCHAR(256) |  |  | 本位币 |  |  |  |
| `ccy_notax_amt` | VARCHAR(256) |  |  | 按原币计的金额_不含税 |  |  |  |
| `lcy_notax_amt` | VARCHAR(256) |  |  | 按本位币计的金额_不含税 |  |  |  |
| `cny_notax_amt` | VARCHAR(256) |  |  | 金额_人民币_不含税 |  |  |  |
| `usd_notax_amt` | VARCHAR(256) |  |  | 金额_美元_不含税 |  |  |  |
| `insert_dt` | VARCHAR(256) |  |  | 数仓数据更新时间 |  |  |  |

## 模型逻辑详情（原文备忘）

```
模型标准 系统A（独立逻辑，可来源于多系统）
序号 字段说明 字段编码 字段类型 维度/度量 主键 值域 非空 获取方式 来源表 来源字段 来源字段名称 来源字段类型 来源字段说明 计算逻辑 备注
1 公司代码 company_code 维度 Y Y 直接获取 dwd_会计凭证明细表 company_code 公司代码 from BSEG
join BKPF on BKPF-BUKRS=BSEG-BUKRS and BKPF-GJAHR=BSEG-GJAHR and BKPF-BELNR=BSEG-BELNR
where BSEG.mandt='800' and BKPF.MANDT='800'
2 公司名称 company_name 维度 dwd_会计凭证明细表 company_name
3 销售订单号 sales_invoice_num 维度 Y 直接获取 dwd_会计凭证明细表 sales_invoice_num 销售凭证
4 销售订单行号 sales_invoice_line 维度 Y 直接获取 dwd_会计凭证明细表 sales_invoice_line 销售凭证项目
5 会计年度 fiscal_year 维度 Y 直接获取 dwd_会计凭证明细表 iscal_year 会计年度
6 会计期间 fiscal_period 维度 Y 直接获取 dwd_会计凭证明细表 account_period 会计期间
7 过账日期 voucher_post_date 维度 Y 直接获取 dwd_会计凭证明细表 voucher_post_date
8 订单抛转节点 transfer_node 维度 直接获取 dwd_订单抛转关系表 transfer_node 订单抛转节点 订单抛转关系表获取制造公司销售订单号，销售订单号与订单抛转信息-【original_order_num】关联
9 制造公司销售订单号 manu_order_num 维度 直接获取 dwd_订单抛转关系表 manu_order_num 制造公司销售订单号 订单抛转关系表获取制造公司销售订单号，销售订单号与订单抛转信息-【original_order_num】关联
10 终端销售公司销售订单号 terminal_order_num 维度 直接获取 dwd_订单抛转关系表 terminal_order_num 终端销售公司销售订单号 订单抛转关系表获取制造公司销售订单号，销售订单号与订单抛转信息-【original_order_num】关联
11 采购订单号（合同号） contract_num 维度 直接获取 dwd_订单明细模型 contract_num
12 母合同号 contract_m_num 维度 直接获取 dwd_订单明细模型 contract_m_num
13 销售订单类型 order_type_code 维度 直接获取 dwd_订单明细模型 order_type_code
14 销售订单类型描述 order_type_name 维度 直接获取 dwd_订单明细模型 order_type_name
15 客户编码 customer_code 维度 直接获取 dwd_订单明细模型 customer_code D是客户、S是总账科目类型
16 客户名称 customer_name 维度 直接获取 dwd_订单明细模型 customer_name
17 是否内部关联客户 in_customer_flag 维度 直接获取 dwd_订单明细模型 in_customer_flag 是否内部关联客户
18 产品编码 product_code 维度 直接获取 dwd_订单明细模型 product_code
19 产品描述 product_name 维度 直接获取 dwd_订单明细模型 product_name
20 产品所属产品族编码 product_series_code 维度 直接获取 dwd_订单明细模型 product_series_code 获取到的开票凭证，总账科目=2221010002的科目或BSEG
KOART科目类型为D没有销售凭证
21 产品所属产品族名称 product_series_name 维度 直接获取 dwd_订单明细模型 product_series_name
22 产品所属产品线编码 product_group_code 维度 直接获取 dwd_订单明细模型 product_group_code 获取到的开票凭证，同会计凭证编码只有BSEG
KOART科目类型为D的才会有开票凭证
23 产品所属产品线名称 product_group_name 维度 直接获取 dwd_订单明细模型 product_group_name
24 产品所属产业编码 industry_code 维度 直接获取 dwd_订单明细模型 industry_code
25 产品所属产业名称 industry_name 维度 直接获取 dwd_订单明细模型 industry_name
26 业务员编码 sales_person_num 维度 直接获取 dwd_订单明细模型 sales_person_num
27 业务员姓名 sales_person_name 维度 直接获取 dwd_订单明细模型 sales_person_name
28 销售组织编码 sales_org_code 维度 直接获取 dwd_订单明细模型 sales_org_code 会计凭证日期
29 销售组织名称 sales_org_name 维度 直接获取 dwd_订单明细模型 sales_org_name 过账日期
30 销售部门编码 sales_dept_code 维度 直接获取 dwd_订单明细模型 sales_dept_code 明细时间
31 销售部门名称 sales_dept_name 维度 直接获取 dwd_订单明细模型 sales_dept_name 创建人
32 销售组编码 sales_group_code 维度 直接获取 dwd_订单明细模型 sales_group_code
33 销售组名称 sales_group_name 维度 直接获取 dwd_订单明细模型 sales_group_name
34 销售战区 sales_unit_code 维度 直接获取 dwd_订单明细模型 sales_unit_code
35 销售战区名称 sales_unit_name 维度 直接获取 dwd_订单明细模型 sales_unit_name
36 销售大区 sales_area_code 维度 直接获取 dwd_订单明细模型 sales_area_code
37 销售大区名称 sales_area_name 维度 直接获取 dwd_订单明细模型 sales_area_name
38 销售区域 sales_region_code 维度 直接获取 dwd_订单明细模型 sales_region_code
39 销售区域名称 sales_region_name 维度 直接获取 dwd_订单明细模型 sales_region_name
40 雇员部门编码 employee_dept_code 维度 直接获取 dwd_订单明细模型 employee_dept_code
41 雇员部门名称 employee_dept_name 维度 直接获取 dwd_订单明细模型 employee_dept_name
42 分销渠道编码 sales_channel_code 维度 直接获取 dwd_订单明细模型 sales_channel_code
43 分销渠道名称 sales_channel_name 维度 显示通用、照明工程类、照明EMC... 直接获取 dwd_订单明细模型 sales_channel_name
44 行业类型 industry_type 维度 直接获取 dwd_订单明细模型 industry_type
45 工厂编码 factory_code 维度 直接获取 dwd_订单明细模型 factory_code
46 工厂名称 factory_name 维度 直接获取 dwd_订单明细模型 factory_name
47 销售数量 order_qty 度量 直接获取 dwd_订单明细模型 order_qty
48 产品单位面积 product_area 度量 直接获取 dwd_订单明细模型 product_area
49 销售面积 order_area 度量 直接获取 dwd_订单明细模型 order_area
50 过账数量 post_qty 度量 直接获取 dwd_会计凭证明细表 qty 数量
51 借贷项标识 dr_cr_tag 度量 直接获取 dwd_会计凭证明细表 dr_cr_tag
52 科目编码 account_code 维度 Y 直接获取 dwd_会计凭证明细表 account_code 会计科目编号
53 科目名称 account_name 维度 直接获取 dim_科目维 account_name 科目名称
54 父层科目编码 parent_acc_code 维度 直接获取 dim_科目维 parent_acc_code
55 原币别 currency string 维度 直接获取 dwd_会计凭证明细表 currency
56 本位币 local_currency string 维度 直接获取 dwd_会计凭证明细表 local_currency
57 按原币计的金额_不含税 ccy_notax_amt string 度量 直接获取 dwd_会计凭证明细表 ccy_notax_amt
58 按本位币计的金额_不含税 lcy_notax_amt string 度量 直接获取 dwd_会计凭证明细表 lcy_notax_amt
59 金额_人民币_不含税 cny_notax_amt 度量 直接获取 dwd_会计凭证明细表 cny_notax_amt
60 金额_美元_不含税 usd_notax_amt 度量 直接获取 dwd_会计凭证明细表 usd_notax_amt
61 数仓数据更新时间 insert_dt 审计 自定义 数据插入时间
```

## 建议开发步骤

- 解析数据来源表/模型逻辑概述/模型逻辑详情三段内容，锁定目标表与字段。
- 校验字段类型、主键、分区列，生成 StarRocks DDL（可选执行）。
- 生成 INSERT SELECT 逻辑，按业务逻辑完成关联、去重、计算。
- 落地中间结果或目标表，补充数据质量校验与唯一性检查。
- 补充自动化脚本与回填验证（行数、键唯一性、NULL 分布）。
