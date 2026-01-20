# dws_存量明细模型 开发说明

## 目标表

- 英文表名: `dws_sd_inventory_detail_di`
- 中文含义: dws_存量明细模型
- 用途: table
- 建议业务主键: `Unnamed: 3`

## 模型逻辑概述（原文）

```
序号 英文表名 中文含义 应用场景 备注
1 dws_sd_inventory_detail_di dws_存量明细模型 存量
筛选条件
1. 排除已出货行数量>=销售订单行数量的数据（或者使用逻辑：排除未交货数量=0的数据）
2.排除行拒绝数据
表关联条件
1、表关联条件看以下单元格第一次出现的关联条件
1. b~a:销售订单号&销售订单行号
```

## 目标表字段结构（解析记忆）

| 字段编码 | 字段类型 | 主键 | 非空 | 字段说明 | 来源表 | 来源字段 | 计算逻辑 |
|----------|----------|------|------|----------|--------|----------|----------|
| `dt` | VARCHAR(256) |  |  | new 日期(分区字段YYYYMMDD） |  |  |  |
| `order_num` | VARCHAR(256) |  |  | 销售公司销售订单号 |  |  |  |
| `contract_num` | VARCHAR(256) |  |  | 客户采购订单编号 |  |  |  |
| `order_item_num` | VARCHAR(256) |  |  | 订单行项目号 |  |  |  |
| `order_type_code` | VARCHAR(256) |  |  | 订单类型编码 |  |  |  |
| `manu_sales_order_num` | VARCHAR(256) |  |  | 制造公司销售订单号 |  |  |  |
| `order_num_transfer` | VARCHAR(256) |  |  | new 销售订单号_含抛转 |  |  |  |
| `create_date` | VARCHAR(256) |  |  | 订单创建日期 |  |  |  |
| `approval_date` | VARCHAR(256) |  |  | 订单首次审批日期 |  |  |  |
| `newest_date` | VARCHAR(256) |  |  | 订单最新日期 |  |  |  |
| `contract_need_days` | VARCHAR(256) |  |  | 合同需求天数 |  |  |  |
| `contract_need_date` | VARCHAR(256) |  |  | 合同需求日期 |  |  |  |
| `review_del_days` | VARCHAR(256) |  |  | 评审交期（天数） | vbak | ZQQJHTS |  |
| `pmc_review_del_date` | VARCHAR(256) |  |  | PMC评审交期 |  |  |  |
| `vbak` | VARCHAR(256) |  |  |  |  |  |  |
| `request_del_date` | VARCHAR(256) |  |  | 预计出货日期 |  |  |  |
| `c` | VARCHAR(256) |  |  |  |  |  |  |
| `f` | VARCHAR(256) |  |  |  |  |  |  |
| `plan_vl_date` | VARCHAR(256) |  |  |  |  |  |  |
| `YJCHSJ` | VARCHAR(256) |  |  |  |  |  |  |
| `vbak` | VARCHAR(256) |  |  |  |  |  |  |
| `vbak` | VARCHAR(256) |  |  |  |  |  |  |
| `pmc_change_date` | VARCHAR(256) |  |  | PMC变更日期 |  |  |  |
| `c` | VARCHAR(256) |  |  |  |  |  |  |
| `f` | VARCHAR(256) |  |  |  |  |  |  |
| `PMC_CHANGE_DATE` | VARCHAR(256) |  |  |  |  |  |  |
| `BGHJQ` | VARCHAR(256) |  |  |  |  |  |  |
| `vbak` | VARCHAR(256) |  |  |  |  |  |  |
| `vbak` | VARCHAR(256) |  |  |  |  |  |  |
| `last_receipt_date` | VARCHAR(256) |  |  | 最后入库日期 |  |  |  |
| `e` | VARCHAR(256) |  |  |  |  |  |  |
| `ON` | VARCHAR(256) |  |  | MSEG | MSEG | MAT_KDAUF |  |
| `BWART` | VARCHAR(256) |  |  |  |  |  |  |
| `transport_method` | VARCHAR(256) |  |  | 运输方式 | f | YSFS |  |
| `trade_terms` | VARCHAR(256) |  |  | 贸易条款 |  |  |  |
| `payment_method` | VARCHAR(256) |  |  | 付款方式 |  |  |  |
| `need_accept_flag` | VARCHAR(256) |  |  | 是否需要验收 |  |  |  |
| `sales_org_code` | VARCHAR(256) |  |  | 销售组织编码 |  |  |  |
| `sales_org_name` | VARCHAR(256) |  |  | 销售组织名称 |  |  |  |
| `factory_code` | VARCHAR(256) |  |  | 工厂编码 |  |  |  |
| `factory_name` | VARCHAR(256) |  |  | 工厂名称 |  |  |  |
| `employee_dept_code` | VARCHAR(256) |  |  | 雇员部门编码 |  |  |  |
| `employee_dept_name` | VARCHAR(256) |  |  | 雇员部门名称 |  |  |  |
| `sales_person_num` | VARCHAR(256) |  |  | 业务员编码 |  |  |  |
| `sales_person_name` | VARCHAR(256) |  |  | 业务员姓名 |  |  |  |
| `sales_region_code` | VARCHAR(256) |  |  | 销售区域编码 |  |  |  |
| `sales_area_code` | VARCHAR(256) |  |  | 销售大区编码 |  |  |  |
| `sales_unit_code` | VARCHAR(256) |  |  | 销售战区编码 |  |  |  |
| `customer_code` | VARCHAR(256) |  |  | 客户编码 |  |  |  |
| `customer_name` | VARCHAR(256) |  |  | 客户名称 |  |  |  |
| `product_type` | VARCHAR(256) |  |  | 产品类型 |  |  |  |
| `industry_type` | VARCHAR(256) |  |  | 行业类型 |  |  |  |
| `product_code` | VARCHAR(256) |  |  | 产品编码 |  |  |  |
| `product_name` | VARCHAR(256) |  |  | 产品描述 |  |  |  |
| `industry_code` | VARCHAR(100) |  |  | 产业编码 |  |  |  |
| `industry_name` | VARCHAR(100) |  |  | 产业描述 |  |  |  |
| `product_line_code` | VARCHAR(100) |  |  | 产品线编码 |  |  |  |
| `product_line_name` | VARCHAR(100) |  |  | 产品线描述 |  |  |  |
| `product_group_code` | VARCHAR(100) |  |  | 产品族编码 |  |  |  |
| `product_group_name` | VARCHAR(100) |  |  | 产品族描述 |  |  |  |
| `product_series_code` | VARCHAR(100) |  |  | 产品系列编码 |  |  |  |
| `product_series_name` | VARCHAR(100) |  |  | 产品系列描述 |  |  |  |
| `product_area` | DECIMAL(27,8) |  |  | 产品单位面积 |  |  |  |
| `order_line_inv_flag` | VARCHAR(256) |  |  | 订单行库存标识 | d | KALAB |  |
| `sum` | VARCHAR(256) |  |  |  |  |  |  |
| `AND` | VARCHAR(256) |  |  |  |  |  |  |
| `inventory_status` | VARCHAR(256) |  |  | 库存状态 |  |  |  |
| `e` | VARCHAR(256) |  |  |  |  |  |  |
| `MSEG` | VARCHAR(256) |  |  |  |  |  |  |
| `X` | VARCHAR(256) |  |  |  |  |  |  |
| `order_exec_status` | VARCHAR(256) |  |  | 订单执行状态 | c | remark1 |  |
| `noship_reason` | VARCHAR(256) |  |  | 未出货原因 | c | UNDELIVER_REASON |  |
| `noship_reason_category` | VARCHAR(256) |  |  | 未出货原因类别 | c | ZREASON |  |
| `cny_exchange_rate` | VARCHAR(256) |  |  | 汇率（to_CNY） | h | UKURS |  |
| `usd_exchange_rate` | VARCHAR(256) |  |  | 汇率（to_USD） | h | UKURS |  |
| `M` | VARCHAR(256) |  |  |  |  |  |  |
| `order_qty` | VARCHAR(256) |  |  | 订单数量 |  |  |  |
| `order_area` | VARCHAR(256) |  |  | 订单面积 |  |  |  |
| `order_tax_amt` | VARCHAR(256) |  |  | 订单金额（含运费）_含税-原币 | VBAP | KZWI1 |  |
| `order_tax_cny_amt` | VARCHAR(256) |  |  | 订单金额（含运费）_含税-CNY |  |  |  |
| `order_tax_usd_amt` | VARCHAR(256) |  |  | 订单金额（含运费）_含税-USD |  |  |  |
| `order_notax_amt` | VARCHAR(256) |  |  | 订单金额（含运费）_不含税-原币 |  |  |  |
| `order_notax_cny_amt` | VARCHAR(256) |  |  | 订单金额（含运费）_不含税-CNY |  |  |  |
| `order_notax_usd_amt` | VARCHAR(256) |  |  | 订单金额（含运费）_不含税-USD |  |  |  |
| `shipment_qty` | VARCHAR(256) |  |  | 出货数量 |  |  |  |
| `shipment_area` | VARCHAR(256) |  |  | 出货面积 |  |  |  |
| `shipment_tax_amt` | VARCHAR(256) |  |  | 出货金额（含运费）_含税-原币 |  |  |  |
| `shipment_tax_cny_amt` | VARCHAR(256) |  |  | 出货金额（含运费）_含税-CNY |  |  |  |
| `shipment_tax_usd_amt` | VARCHAR(256) |  |  | 出货金额（含运费）_含税-USD |  |  |  |
| `shipment_notax_amt` | VARCHAR(256) |  |  | 出货金额（含运费）_不含税-原币 |  |  |  |
| `shipment_notax_cny_amt` | VARCHAR(256) |  |  | 出货金额（含运费）_不含税-CNY |  |  |  |
| `shipment_notax_usd_amt` | VARCHAR(256) |  |  | 出货金额（含运费）_不含税-USD |  |  |  |
| `unshipment_qty` | VARCHAR(256) |  |  | 未出货数量 |  |  |  |
| `unshipment_area` | VARCHAR(256) |  |  | 未出货面积 |  |  |  |
| `unshipment_tax_amt` | VARCHAR(256) |  |  | 未出货金额（含运费）_含税-原币 |  |  |  |
| `unshipment_tax_cny_amt` | VARCHAR(256) |  |  | 未出货金额（含运费）_含税-CNY |  |  |  |
| `unshipment_tax_usd_amt` | VARCHAR(256) |  |  | 未出货金额（含运费）_含税-USD |  |  |  |
| `unshipment_notax_amt` | VARCHAR(256) |  |  | 未出货金额（含运费）_不含税-原币 |  |  |  |
| `unshipment_notax_cny_amt` | VARCHAR(256) |  |  | 未出货金额（含运费）_不含税-CNY |  |  |  |
| `unshipment_notax_usd_amt` | VARCHAR(256) |  |  | 未出货金额（含运费）_不含税-USD |  |  |  |
| `source_system` | VARCHAR(256) |  |  | 来源系统 |  |  |  |
| `insert_dt` | VARCHAR(256) |  |  | 数仓数据更新时间 |  |  |  |

## 模型逻辑详情（原文备忘）

```
模型标准 系统（独立逻辑，可来源于多系统）
序号 字段备注 字段名称 字段编码 字段类型 维度/度量 主键 值域 非空 获取方式 来源表 来源字段 来源字段说明 国内、国际 数据类型 值列表 关联条件 计算逻辑 备注1 备注2
new 日期(分区字段YYYYMMDD） dt 维度 / / / 版本日期 数仓每日更新时间对应的日期
销售公司销售订单号 order_num 维度 b 订单号 销售公司销售订单
深圳洲明SAP
客户采购订单编号 contract_num 维度 b 客户采购订单编号（合同号） 合同号
订单号
订单行项目号 order_item_num 维度 b 订单行项目号 订单行
订单类型编码 order_type_code 维度 b 订单类型编码 订单类型
制造公司销售订单号 manu_sales_order_num 维度 b 制造公司销售订单号
new 销售订单号_含抛转 order_num_transfer 维度 / 销售公司销售订单号
制造公司销售订单号 / 若【制造公司销售订单号】有值 ，则值=【制造公司销售订单号】否则 值=【销售公司销售订单号】 合
订单创建日期 create_date 维度 b 订单创建日期 销售订单创建日期
创建日期
订单首次审批日期 approval_date 维度 b 订单首次审批日期 首次审批日期
审批日期
订单最新日期 newest_date b 订单最新日期 / 若为免审订单类型，则用创建日期，否则用首次审批日期 合
合同需求天数 contract_need_days 维度 b 合同需求天数 合同需求天数
合同需求日期 contract_need_date 维度 b 合同需求日期 合同需求日期 值=审批日期（订单类型免审的取创建日期，否则取审批日期）+合同需求天数
模型侧：值=订单最新日期+合同需求天数
评审交期（天数） review_del_days 维度 b 评审交期（天数）（vbak.ZQQJHTS） 请求交货天数
PMC评审交期 pmc_review_del_date 维度 b 订单最新日期
vbak.ZQQJHTS【请求交货天数】
PMC评审交期 PMC评审交期
PMC交期 值=审批日期（订单类型免审的取创建日期，否则取审批日期）+请求交货天数
模型侧：值=订单最新日期+请求交货天数
预计出货日期 request_del_date 维度 b
c
f 请求交货日期
plan_vl_date--国内填报
YJCHSJ--国际填报 预计出货日期
预计出货时间 国内：
vbak~c:关联条件：订单号，订单行号
优先取c表值，取不到的取vbak表
国际：
vbak~f:关联条件：订单号
优先取f表值，取不到的取vbak表
PMC变更日期 pmc_change_date 维度 b
c
f 请求交货日期
PMC_CHANGE_DATE-国内填报
BGHJQ--国际填报 PMC变更日期
PMC评审变更后交期 国内：
vbak~c:关联条件：订单号，订单行号
优先取c表值，取不到的取vbak表
国际：
vbak~f:关联条件：订单号
优先取f表值，取不到的取vbak表
最后入库日期 last_receipt_date 维度 当前表
e BUDAT_MKPF--过账日期 最后入库日期 MSEG-【BUDAT_MKPF】
注：有抛转则按制造公司销售订单取入库日期，无则按原单取入库日期
        当前表~MSEG ON MSEG.MAT_KDAUF =当前表.【销售订单号_含抛转】
筛【BWART】=in("101","413"),取销售订单最新的入库日期 筛【BWART】="101"、"413",取最新的入库日期 //101： 收货 ；413：销售库存的调拨（定制品按单库存的调拨，即库存挪用，eg:A订单的库存挪用到B订单上）
最后入库日期 整单入库日期 在【最后入库日期】的基础上：
【订单库存数量】<（【订单行数量】-【已交货数量】 ；则值=”00000000“
否则 值=【最后入库日期】 仅国际
运输方式 transport_method f YSFS 运输方式 运输方式 {海运整柜，海运散货，空运，铁路，汽运，快递，待定} ~ZSD_CLXX_GJ ON LIKP.VBELN = ZSD_CLXX_GJ.VBELN 仅国际
贸易条款 trade_terms b 贸易条款 贸易条款 {FCA,FCB,EXW,CIF,CNF,……} 仅国际
付款方式 payment_method b 付款方式
是否需要验收 need_accept_flag b 是否需要验收
销售组织编码 sales_org_code 维度 直接获取 b 销售组织 销售组织编码
销售组织名称 sales_org_name 维度 直接获取 b 销售组织名称 销售组织描述
工厂编码 factory_code b 工厂编码
工厂名称 factory_name b 工厂名称
雇员部门编码 employee_dept_code 维度 直接获取 b 雇员部门编码 雇员部门编码
雇员部门名称 employee_dept_name 维度 直接获取 b 雇员部门描述 雇员部门名称
业务员编码 sales_person_num 维度 直接获取 b 业务员编码 业务员编码
业务员姓名 sales_person_name 维度 直接获取 b 业务员姓名 业务员姓名
销售区域编码 sales_region_code 维度 直接获取 b 销售区域 销售区域 {国内/国际}
销售大区编码 sales_area_code 维度 直接获取 b 销售大区 销售大区 国内6，国际8
销售战区编码 sales_unit_code 维度 直接获取 b 销售战区 销售战区 国内32，国际26
客户编码 customer_code 维度 直接获取 b 客户编码 客户编码
客户名称 customer_name 维度 直接获取 b 客户名称 客户名称
产品类型 product_type 维度 直接获取 b 产品类型 产品类型
行业类型 industry_type 维度 直接获取 b 行业类型 行业类型
产品编码 product_code 维度 直接获取 b 物料号 物料号
产品描述 product_name 维度 直接获取 b 物料描述 物料描述
产业编码 industry_code varchar(100) b 产业编码
产业描述 industry_name varchar(100) b 产业描述
产品线编码 product_line_code varchar(100) b 产品线编码
产品线描述 product_line_name varchar(100) b 产品线描述
产品族编码 product_group_code varchar(100) b 产品族编码
产品族描述 product_group_name varchar(100) b 产品族描述
产品系列编码 product_series_code varchar(100) b 产品系列编码
产品系列描述 product_series_name varchar(100) b 产品系列描述
产品单位面积 product_area decimal(27,8) b 产品单位面积
订单行库存标识 order_line_inv_flag 度量 d KALAB 订单库存数量 关联条件：订单号+订单行+物料号
若【制造公司销售订单】为空，取【销售公司销售订单】的号+销售订单行号；否则取【制造公司销售订单】的号+销售订单行号；
sum(KALAB)by【订单号】+【订单行】
与【已交货数量】进行比对，若【订单库存数量】>=【已交货数量】AND 【已交货数量】>0,打标识X
库存状态 inventory_status 维度 直接获取 b
e 库存状态
当前生产状态 {生产完成未出货，在制} 按订单 ：若该订单的所有行都打上标识X,则该订单为X，否则，为空
销售订单~MSEG:【销售订单号_含抛转】= MSEG.MAT_KDAUF ，如果在MSEG可以找到订单，则认为有入库记录
若有入库记录AND订单号标识=X：
则 值="生产完成未出货" 否则 值=”在制“
若无入库记录：
则 值=”在制“ 逻辑差异：
国内ZSD158B：生产完成未出货：有入库记录且整单有X标识【标记全部入库】；否则 为 在制
国际ZSD167：在制：整单中物料类型存在（非原材料且入库日期为”00000000“），则为在制，否则 为生产完成未出货
订单执行状态 order_exec_status 维度 c remark1 备注 国内：{合同清尾，已直发未过账，订单存在取消风险，订单待取消，订单暂停已备料，订单暂停未备料}
国际：无值集，直接填写
未出货原因 noship_reason 直接获取 c UNDELIVER_REASON 未出货原因
未出货原因类别 noship_reason_category 直接获取 c ZREASON 未出货原因类别 {出货时间待定，客诉问题待解决，待船司放舱，现场不具备安装条件，资金困难}
汇率（to_CNY） cny_exchange_rate 度量 直接获取 h UKURS 汇率 关联 ：当前表【币种】=TCURR.FCURR AND TUCRR.TCURR="CNY" AND dwd_订单明细宽表.创建日期=汇率表【日期】的汇率
汇率（to_USD） usd_exchange_rate 度量 直接获取 h UKURS 汇率 关联 ：当前表【币种】=TCURR.FCURR AND TUCRR.TCURR="USD" AND dwd_订单明细宽表.创建日期=汇率表【日期】的汇率 对于荷兰{公司代码5300}：取汇率类型=“EURX”的汇率（SAP当前逻辑，之后考虑转M）
其余情况：取汇率类型=“M”的汇率
订单数量 order_qty 度量 b 销售数量 VBAP-【KWMENG】
订单面积 order_area 度量 b 销售面积
订单金额（含运费）_含税-原币 order_tax_amt b 订单行金额_含税 VBAP.KZWI1 小计
订单金额（含运费）_含税-CNY order_tax_cny_amt b 订单行人民币金额_含税 含税售价
订单金额（含运费）_含税-USD order_tax_usd_amt 度量 b 订单行美元金额_含税
订单金额（含运费）_不含税-原币 order_notax_amt b 订单行金额_不含税
订单金额（含运费）_不含税-CNY order_notax_cny_amt 度量 b 订单行人民币金额_不含税
订单金额（含运费）_不含税-USD order_notax_usd_amt b 订单行美元金额_不含税
出货数量 shipment_qty 度量 a 出货数量 LIPS-【LFIMG】
按交货单的销售订单号和行进行汇总 关联：销售订单号+销售订单行
出货面积 shipment_area 度量 a 出货面积
出货金额（含运费）_含税-原币 shipment_tax_amt a 出货金额（含运费）_含税-原币 【订单金额-含税-原币】/【订单数量】*【已交货数量】
出货金额（含运费）_含税-CNY shipment_tax_cny_amt a 出货金额（含运费）_含税-CNY
出货金额（含运费）_含税-USD shipment_tax_usd_amt 度量 a 出货金额（含运费）_含税-USD 已交货金额-RMB
出货金额（含运费）_不含税-原币 shipment_notax_amt a 出货金额（含运费）_不含税-原币
出货金额（含运费）_不含税-CNY shipment_notax_cny_amt 度量 a 出货金额（含运费）_不含税-CNY 已交货金额-USD
出货金额（含运费）_不含税-USD shipment_notax_usd_amt a 出货金额（含运费）_不含税-USD
未出货数量 unshipment_qty 度量 / / 未交货数量 【订单数量】-【出货数量】
未出货面积 unshipment_area 度量 / / 未交货面积 【订单面积】-【出货面积】
未出货金额（含运费）_含税-原币 unshipment_tax_amt 度量 / / 未交货金额-RMB 【订单金额（含运费）_含税-原币】-【出货金额（含运费）_含税-原币】
未出货金额（含运费）_含税-CNY unshipment_tax_cny_amt 度量 / / 未交货金额-USD 【订单金额（含运费）_含税-CNY】-【出货金额（含运费）_含税-CNY】
未出货金额（含运费）_含税-USD unshipment_tax_usd_amt / / 【订单金额（含运费）_含税-USD】-【出货金额（含运费）_含税-USD】
未出货金额（含运费）_不含税-原币 unshipment_notax_amt / / 【订单金额（含运费）_不含税-原币】-【出货金额（含运费）_不含税-原币】
未出货金额（含运费）_不含税-CNY unshipment_notax_cny_amt / / 【订单金额（含运费）_不含税-CNY】-【出货金额（含运费）_不含税-CNY】
未出货金额（含运费）_不含税-USD unshipment_notax_usd_amt / / 【订单金额（含运费）_不含税-USD】-【出货金额（含运费）_不含税-USD】
来源系统 source_system 审计 直接获取 / / / now()
数仓数据更新时间 insert_dt 审计 直接获取 a 来源系统 值=SAP
```

## 建议开发步骤

- 解析数据来源表/模型逻辑概述/模型逻辑详情三段内容，锁定目标表与字段。
- 校验字段类型、主键、分区列，生成 StarRocks DDL（可选执行）。
- 生成 INSERT SELECT 逻辑，按业务逻辑完成关联、去重、计算。
- 落地中间结果或目标表，补充数据质量校验与唯一性检查。
- 补充自动化脚本与回填验证（行数、键唯一性、NULL 分布）。
