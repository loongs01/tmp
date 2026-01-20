# dwd_物料价格计算 开发说明

## 目标表

- 英文表名: `dwd_co_mat_price_df`
- 中文含义: dwd_物料价格计算
- 用途: table
- 建议业务主键: `Unnamed: 2`

## 数据来源表

```
来源系统 英文表名 中文含义 表别名 备注 matnr 1.0 varchar(100) 物料编码
sap ods_sap_erp_zhone_mat_purchase_price_get_df ods_sap_物料价格 price werks 2.0 varchar(100) 工厂编码
dim dim_exchange_rate_di dim_汇率 dim
srm ods_srm_mat_est_price_df ods_srm_物料预估价格 srm
ods ods_hone_manu_factory_mapping_df ods_hone_制造工厂映射 factory
esokz 3.0 varchar(100) 采购信息记录分类
```

## 模型逻辑概述（原文）

```
序号 英文表名 中文含义 应用场景 备注 meins 5.0 varchar(100) 基本计量单位
1 dwd_co_mat_price_df dwd_物料价格计算 peinh 6.0 decimal(27, 8) 价格单位
mtart 7.0 varchar(100) 物料类型
筛选条件
1、dim表筛选【最终货币】to_ccy='CNY'，【汇率】取dim.rate_type="M"；dim.dt取current_time对应的YYYYMMDD
表关联条件
price为主表
左关联srm、factory
price.factory_code=factory.purchase_org_code
price.mat_code=srm.mat_code
kbetr_l 10.0 decimal(27, 8) 最低采购价（CNY）
```

## 目标表字段结构（解析记忆）

| 字段编码 | 字段类型 | 主键 | 非空 | 字段说明 | 来源表 | 来源字段 | 计算逻辑 |
|----------|----------|------|------|----------|--------|----------|----------|
| `mat_code` | VARCHAR(100) | Y |  | 物料编码 | 14 | 0 |  |
| `factory_code` | VARCHAR(100) | Y |  | 工厂编码 | 15 | 0 |  |
| `mat_name` | VARCHAR(100) |  |  | 物料名称 | 16 | 0 |  |
| `basic_unit` | VARCHAR(100) |  |  | 基本计量单位 | 17 | 0 |  |
| `mat_type_code` | VARCHAR(100) |  |  | 物料类型编码 | 18 | 0 |  |
| `mat_type_name` | VARCHAR(100) |  |  | 物料类型名称 | 19 | 0 |  |
| `mat_pur_type` | VARCHAR(100) |  |  | 物料采购类型 | 20 | 0 |  |
| `special_pur_type` | VARCHAR(100) |  |  | 特殊采购类型 |  |  |  |
| `price_unit` | DECIMAL(27,8) |  |  | 价格单位 |  |  |  |
| `moq_unit` | DECIMAL(27,8) |  |  | MOQ |  |  |  |
| `max_unit_price` | DECIMAL(27,8) |  |  | 最高价单个物料单价 | factory | manu_factory_code | price kbetr_h 按照factory.manu_factory_code分组，取《ods_sap_物料价格》中对应factory.purchase_org_code的最高值 |
| `CNY` | VARCHAR(256) |  |  |  |  |  |  |
| `min_unit_price` | DECIMAL(27,8) |  |  | 最低价单个物料单价 |  |  | price kbetr_l |
| `latest_unit_price` | DECIMAL(27,8) |  |  | 最近价单个物料单价 |  |  | price kbetr_j |
| `std_unit_price` | DECIMAL(27,8) |  |  | 标准价单个物料单价 |  |  | price stprs |
| `moving_avg_unit_price` | DECIMAL(27,8) |  |  | 移动加权平均价单个物料单价 |  |  | price zstprs_hs |
| `po_est_unit_price` | DECIMAL(27,8) |  |  | 采购预估单个物料单价 | factory | purchase_org_code | 1、若factory.purchase_org_code属于B010、2020，且price.mat_code=srm.mat_code成立时， |
| `MOQ` | VARCHAR(256) |  |  |  |  |  |  |
| `purchase_org_code` | VARCHAR(256) |  |  |  |  |  |  |
| `factory` | VARCHAR(256) |  |  |  |  |  |  |
| `dim` | VARCHAR(256) |  |  |  |  |  |  |
| `date` | VARCHAR(256) |  |  | 补录价格有效期 |  |  |  |
| `insert_dt` | DATETIME |  |  | 数仓数据更新时间 |  |  |  |

## 模型逻辑详情（原文备忘）

```
模型标准 sap、srm stprs 12.0 decimal(27, 8) 标准成本价（CNY）
序号 字段说明 字段编码 字段类型 维度/度量 主键 值域 非空 获取方式 来源表 来源字段 来源字段名称 来源字段类型 来源字段说明 计算逻辑 备注 zstprs_hs 13.0 decimal(27, 8) 含税标准成本价（CNY）
1 物料编码 mat_code varchar(100) Y 直接获取 price matnr 物料编码 verpr 14.0 decimal(27, 8) 移动加权平均价（CNY）
2 工厂编码 factory_code varchar(100) Y 直接获取 factory manu_factory_code 制造工厂编码 zverpr_hs 15.0 decimal(27, 8) 含税移动加权平均价（CNY）
3 物料名称 mat_name varchar(100) 直接获取 price maktx 物料描述 vprsv 16.0 varchar(100) 价格控制指示符
4 基本计量单位 basic_unit varchar(100) 直接获取 price meins 基本计量单位 werks_t 17.0 varchar(100) 工厂名称
5 物料类型编码 mat_type_code varchar(100) 直接获取 price mtart 物料类型 beskz 18.0 varchar(100) 物料采购类型
6 物料类型名称 mat_type_name varchar(100) 直接获取 price mtbez 物料类型描述 sobsl 19.0 varchar(100) 特殊采购类型
7 物料采购类型 mat_pur_type varchar(100) 直接获取 price beskz 物料采购类型 insert_dt 20.0 datetime 数仓数据更新时间
8 特殊采购类型 special_pur_type varchar(100) 直接获取 price sobsl 特殊采购类型
9 价格单位 price_unit decimal(27,8) 直接获取 price peinh 价格单位
10 MOQ moq_unit decimal(27,8) 直接获取 srm moq MOQ
11 最高价单个物料单价 max_unit_price decimal(27,8) 计算 price kbetr_h 按照factory.manu_factory_code分组，取《ods_sap_物料价格》中对应factory.purchase_org_code的最高值

取《ods_sap_物料价格》中【XX采购价（CNY）】/10000/【价格单位】
12 最低价单个物料单价 min_unit_price decimal(27,8) 计算 price kbetr_l
13 最近价单个物料单价 latest_unit_price decimal(27,8) 计算 price kbetr_j
14 标准价单个物料单价 std_unit_price decimal(27,8) 计算 price stprs
15 移动加权平均价单个物料单价 moving_avg_unit_price decimal(27,8) 计算 price zstprs_hs
16 采购预估单个物料单价 po_est_unit_price decimal(27,8) 计算 1、若factory.purchase_org_code属于B010、2020，且price.mat_code=srm.mat_code成立时，
则值为：【采购预估物料单价】*【汇率】/【MOQ】
2、若不成立，该字段值置空。

当factory.purchase_org_code=B010
【采购预估物料单价】取《ods_srm_物料预估价格》.【物料本次预估采购金额（南昌生产基地）】
factory.purchase_org_code对应2020
【采购预估物料单价】取《ods_srm_物料预估价格》.【物料本次预估采购金额（大亚湾生产基地）】
【汇率】当srm.物料本次预估采购币别=dim.从货币，取dim.final_rate，否则默认为1
17 补录价格有效期 date 直接获取 srm validity_period 价格有效期
18 数仓数据更新时间 insert_dt datetime 默认值current_time
```

## 建议开发步骤

- 解析数据来源表/模型逻辑概述/模型逻辑详情三段内容，锁定目标表与字段。
- 校验字段类型、主键、分区列，生成 StarRocks DDL（可选执行）。
- 生成 INSERT SELECT 逻辑，按业务逻辑完成关联、去重、计算。
- 落地中间结果或目标表，补充数据质量校验与唯一性检查。
- 补充自动化脚本与回填验证（行数、键唯一性、NULL 分布）。
