# dim_汇率 开发说明

## 目标表

- 英文表名: `dim_exchange_rate_di`
- 中文含义: dim_汇率
- 用途: dimension
- 建议业务主键: `Unnamed: 1`

## 数据来源表

```
来源系统 英文表名 中文含义 表别名 备注
SAP ods_sap_erp_tcurr_df 汇率表 a 主表
筛选条件：kurst = 'M', 'EURX','PEND'
SAP ods_sap_erp_tcurf_df 汇率转换因子 b 筛选条件：kurst = 'M', 'EURX','PEND'
```

## 模型逻辑概述（原文）

```
序号 英文表名 中文含义 应用场景 备注
1 dim_exchange_rate_di dim_汇率
```

## 目标表字段结构（解析记忆）

| 字段编码 | 字段类型 | 主键 | 非空 | 字段说明 | 来源表 | 来源字段 | 计算逻辑 |
|----------|----------|------|------|----------|--------|----------|----------|
| `dt` | VARCHAR(50) | Y |  | 日期（YYYYMMDD） |  |  |  |
| `rate_type` | VARCHAR(50) | Y |  | 汇率类型 | ods_sap_erp_tcurr_df | kurst |  |
| `from_ccy` | VARCHAR(50) | Y |  | 从货币 | ods_sap_erp_tcurr_df | fcurr |  |
| `to_ccy` | VARCHAR(50) | Y |  | 最终货币 | ods_sap_erp_tcurr_df | tcurr |  |
| `start_date` | VARCHAR(50) | Y |  | 汇率起始日期 | ods_sap_erp_tcurr_df | gdatu | a gdatu 汇率有效起始日期 99999999-a.gdatu 再将其转为日期格式%Y-%m-%d |
| `raw_rate` | DECIMAL(27,8) |  |  | 汇率（未转换因子） | ods_sap_erp_tcurr_df | ukurs | a ukurs 汇率（未转换因子） a表partition by 【汇率类型】，【从货币】，【最终货币】order by 【有效期从】desc ,取各组第一条数据（取离当天日期和昨天日期最近的一条） |
| `from_unit_rate` | DECIMAL(27,8) |  |  | 来自货币单位的比率 | ods_sap_erp_tcurf_df | ffact | b ffact 来自货币单位的比率 b表partition by 【汇率类型】，【从货币】，【最终货币】order by 【有效期从】desc ,取各组第一条数据（取离当天日期和昨天日期最近的一条） |
| `by` | VARCHAR(256) |  |  |  |  |  |  |
| `to_unit_rate` | DECIMAL(27,8) |  |  | 到 货币单位汇率 | ods_sap_erp_tcurf_df | tfact |  |
| `final_rate` | DECIMAL(27,8) |  |  | 汇率 |  |  | 当前表计算 【汇率（未转换因子）】*【到货币单位汇率】/【来自货币单位的比率】 |
| `insert_dt` | DATETIME |  |  | 数仓数据更新时间 | ods_sap_erp_tcurr_df | insert_dt |  |

## 模型逻辑详情（原文备忘）

```
模型标准 sap
序号 字段名称 字段编码 字段类型 维度/度量 主键 值域 非空 获取方式 来源表 来源字段 来源字段说明 数据类型 值列表 关联条件 计算逻辑 备注
1 日期（YYYYMMDD） dt varchar(50) 维度 Y 当天日期和昨天日期（每天凌晨更新昨天和今天的汇率） 分区字段
2 汇率类型 rate_type varchar(50) 维度 Y 直接获取 a kurst 汇率类型
3 从货币 from_ccy varchar(50) 维度 Y 直接获取 a fcurr 从货币
4 最终货币 to_ccy varchar(50) 维度 Y 直接获取 a tcurr 最终货币
5 汇率起始日期 start_date varchar(50) 维度 计算 a gdatu 汇率有效起始日期 99999999-a.gdatu 再将其转为日期格式%Y-%m-%d
6 汇率（未转换因子） raw_rate decimal(27,8) 度量 计算 a ukurs 汇率（未转换因子） a表partition by 【汇率类型】，【从货币】，【最终货币】order by 【有效期从】desc ,取各组第一条数据（取离当天日期和昨天日期最近的一条）
7 来自货币单位的比率 from_unit_rate decimal(27,8) 度量 计算 b ffact 来自货币单位的比率 b表partition by 【汇率类型】，【从货币】，【最终货币】order by 【有效期从】desc ,取各组第一条数据（取离当天日期和昨天日期最近的一条）
处理后与a表关联：by【汇率类型】，【从货币】，【最终货币】
8 到 货币单位汇率 to_unit_rate decimal(27,8) 度量 直接获取 b tfact 到 货币单位汇率 /
9 汇率 final_rate decimal(27,8) 度量 计算 当前表计算 【汇率（未转换因子）】*【到货币单位汇率】/【来自货币单位的比率】
10 数仓数据更新时间 insert_dt datetime 审计 自定义 a insert_dt 数仓数据更新时间
```

## 建议开发步骤

- 解析数据来源表/模型逻辑概述/模型逻辑详情三段内容，锁定目标表与字段。
- 校验字段类型、主键、分区列，生成 StarRocks DDL（可选执行）。
- 生成 INSERT SELECT 逻辑，按业务逻辑完成关联、去重、计算。
- 落地中间结果或目标表，补充数据质量校验与唯一性检查。
- 补充自动化脚本与回填验证（行数、键唯一性、NULL 分布）。
