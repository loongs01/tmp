# Generated ETL script for sheet: dws_存量明细模型
# This script reads the sheet from the original Excel file and loads into StarRocks.
# Review and test before running against production.

import sys
from pathlib import Path
import importlib.util

# Try to locate starrocks_utils.py by walking up parent dirs and load it directly
_here = Path(__file__).resolve()
_sr_mod = None
for _p in [_here] + list(_here.parents):
    _cand1 = _p / 'starrocks_utils.py'
    _cand2 = _p / 'lib' / 'starrocks_utils.py'
    _cand = _cand1 if _cand1.exists() else (_cand2 if _cand2.exists() else None)
    if _cand is not None and _cand.exists():
        spec = importlib.util.spec_from_file_location('starrocks_utils', str(_cand))
        _sr_mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(_sr_mod)
        break
if _sr_mod is None:
    try:
        import starrocks_utils as _sr_mod
    except Exception:
        _sr_mod = None
if _sr_mod is None:
    raise ImportError('Cannot locate starrocks_utils.py — please ensure it exists in the project tree')
STARROCKS_CONFIG = getattr(_sr_mod, "STARROCKS_CONFIG", {})
import pandas as pd
import pymysql

EXCEL_PATH = r"D:\note\code\py\document\模型设计清单-技术开发.xlsx"
SHEET_NAME = "dws_存量明细模型"
COL_NAMES = ["数据来源表", "返回", "Unnamed: 2", "Unnamed: 3", "Unnamed: 4", "Unnamed: 5", "Unnamed: 6", "Unnamed: 7", "Unnamed: 8", "Unnamed: 9", "Unnamed: 10", "Unnamed: 11", "Unnamed: 12", "Unnamed: 13", "Unnamed: 14", "Unnamed: 15", "Unnamed: 16", "Unnamed: 17", "Unnamed: 18", "Unnamed: 19", "Unnamed: 20", "Unnamed: 21", "Unnamed: 22"]
COL_LIST = "`数据来源表`, `返回`, `Unnamed: 2`, `Unnamed: 3`, `Unnamed: 4`, `Unnamed: 5`, `Unnamed: 6`, `Unnamed: 7`, `Unnamed: 8`, `Unnamed: 9`, `Unnamed: 10`, `Unnamed: 11`, `Unnamed: 12`, `Unnamed: 13`, `Unnamed: 14`, `Unnamed: 15`, `Unnamed: 16`, `Unnamed: 17`, `Unnamed: 18`, `Unnamed: 19`, `Unnamed: 20`, `Unnamed: 21`, `Unnamed: 22`"

CREATE_TABLE_SQL = r"""
CREATE TABLE IF NOT EXISTS `dws_sd_inventory_detail_di` (
  `dt` VARCHAR(256) COMMENT 'new 日期(分区字段YYYYMMDD）',
  `order_num` VARCHAR(256) COMMENT '销售公司销售订单号',
  `contract_num` VARCHAR(256) COMMENT '客户采购订单编号',
  `order_item_num` VARCHAR(256) COMMENT '订单行项目号',
  `order_type_code` VARCHAR(256) COMMENT '订单类型编码',
  `manu_sales_order_num` VARCHAR(256) COMMENT '制造公司销售订单号',
  `order_num_transfer` VARCHAR(256) COMMENT 'new 销售订单号_含抛转',
  `create_date` VARCHAR(256) COMMENT '订单创建日期',
  `approval_date` VARCHAR(256) COMMENT '订单首次审批日期',
  `newest_date` VARCHAR(256) COMMENT '订单最新日期',
  `contract_need_days` VARCHAR(256) COMMENT '合同需求天数',
  `contract_need_date` VARCHAR(256) COMMENT '合同需求日期',
  `review_del_days` VARCHAR(256) COMMENT '评审交期（天数）',
  `pmc_review_del_date` VARCHAR(256) COMMENT 'PMC评审交期',
  `vbak` VARCHAR(256),
  `request_del_date` VARCHAR(256) COMMENT '预计出货日期',
  `c` VARCHAR(256),
  `f` VARCHAR(256),
  `plan_vl_date` VARCHAR(256),
  `YJCHSJ` VARCHAR(256),
  `vbak` VARCHAR(256),
  `vbak` VARCHAR(256),
  `pmc_change_date` VARCHAR(256) COMMENT 'PMC变更日期',
  `c` VARCHAR(256),
  `f` VARCHAR(256),
  `PMC_CHANGE_DATE` VARCHAR(256),
  `BGHJQ` VARCHAR(256),
  `vbak` VARCHAR(256),
  `vbak` VARCHAR(256),
  `last_receipt_date` VARCHAR(256) COMMENT '最后入库日期',
  `e` VARCHAR(256),
  `ON` VARCHAR(256) COMMENT 'MSEG',
  `BWART` VARCHAR(256),
  `transport_method` VARCHAR(256) COMMENT '运输方式',
  `trade_terms` VARCHAR(256) COMMENT '贸易条款',
  `payment_method` VARCHAR(256) COMMENT '付款方式',
  `need_accept_flag` VARCHAR(256) COMMENT '是否需要验收',
  `sales_org_code` VARCHAR(256) COMMENT '销售组织编码',
  `sales_org_name` VARCHAR(256) COMMENT '销售组织名称',
  `factory_code` VARCHAR(256) COMMENT '工厂编码',
  `factory_name` VARCHAR(256) COMMENT '工厂名称',
  `employee_dept_code` VARCHAR(256) COMMENT '雇员部门编码',
  `employee_dept_name` VARCHAR(256) COMMENT '雇员部门名称',
  `sales_person_num` VARCHAR(256) COMMENT '业务员编码',
  `sales_person_name` VARCHAR(256) COMMENT '业务员姓名',
  `sales_region_code` VARCHAR(256) COMMENT '销售区域编码',
  `sales_area_code` VARCHAR(256) COMMENT '销售大区编码',
  `sales_unit_code` VARCHAR(256) COMMENT '销售战区编码',
  `customer_code` VARCHAR(256) COMMENT '客户编码',
  `customer_name` VARCHAR(256) COMMENT '客户名称',
  `product_type` VARCHAR(256) COMMENT '产品类型',
  `industry_type` VARCHAR(256) COMMENT '行业类型',
  `product_code` VARCHAR(256) COMMENT '产品编码',
  `product_name` VARCHAR(256) COMMENT '产品描述',
  `industry_code` VARCHAR(100) COMMENT '产业编码',
  `industry_name` VARCHAR(100) COMMENT '产业描述',
  `product_line_code` VARCHAR(100) COMMENT '产品线编码',
  `product_line_name` VARCHAR(100) COMMENT '产品线描述',
  `product_group_code` VARCHAR(100) COMMENT '产品族编码',
  `product_group_name` VARCHAR(100) COMMENT '产品族描述',
  `product_series_code` VARCHAR(100) COMMENT '产品系列编码',
  `product_series_name` VARCHAR(100) COMMENT '产品系列描述',
  `product_area` DECIMAL(27,8) COMMENT '产品单位面积',
  `order_line_inv_flag` VARCHAR(256) COMMENT '订单行库存标识',
  `sum` VARCHAR(256),
  `AND` VARCHAR(256),
  `inventory_status` VARCHAR(256) COMMENT '库存状态',
  `e` VARCHAR(256),
  `MSEG` VARCHAR(256),
  `X` VARCHAR(256),
  `order_exec_status` VARCHAR(256) COMMENT '订单执行状态',
  `noship_reason` VARCHAR(256) COMMENT '未出货原因',
  `noship_reason_category` VARCHAR(256) COMMENT '未出货原因类别',
  `cny_exchange_rate` VARCHAR(256) COMMENT '汇率（to_CNY）',
  `usd_exchange_rate` VARCHAR(256) COMMENT '汇率（to_USD）',
  `M` VARCHAR(256),
  `order_qty` VARCHAR(256) COMMENT '订单数量',
  `order_area` VARCHAR(256) COMMENT '订单面积',
  `order_tax_amt` VARCHAR(256) COMMENT '订单金额（含运费）_含税-原币',
  `order_tax_cny_amt` VARCHAR(256) COMMENT '订单金额（含运费）_含税-CNY',
  `order_tax_usd_amt` VARCHAR(256) COMMENT '订单金额（含运费）_含税-USD',
  `order_notax_amt` VARCHAR(256) COMMENT '订单金额（含运费）_不含税-原币',
  `order_notax_cny_amt` VARCHAR(256) COMMENT '订单金额（含运费）_不含税-CNY',
  `order_notax_usd_amt` VARCHAR(256) COMMENT '订单金额（含运费）_不含税-USD',
  `shipment_qty` VARCHAR(256) COMMENT '出货数量',
  `shipment_area` VARCHAR(256) COMMENT '出货面积',
  `shipment_tax_amt` VARCHAR(256) COMMENT '出货金额（含运费）_含税-原币',
  `shipment_tax_cny_amt` VARCHAR(256) COMMENT '出货金额（含运费）_含税-CNY',
  `shipment_tax_usd_amt` VARCHAR(256) COMMENT '出货金额（含运费）_含税-USD',
  `shipment_notax_amt` VARCHAR(256) COMMENT '出货金额（含运费）_不含税-原币',
  `shipment_notax_cny_amt` VARCHAR(256) COMMENT '出货金额（含运费）_不含税-CNY',
  `shipment_notax_usd_amt` VARCHAR(256) COMMENT '出货金额（含运费）_不含税-USD',
  `unshipment_qty` VARCHAR(256) COMMENT '未出货数量',
  `unshipment_area` VARCHAR(256) COMMENT '未出货面积',
  `unshipment_tax_amt` VARCHAR(256) COMMENT '未出货金额（含运费）_含税-原币',
  `unshipment_tax_cny_amt` VARCHAR(256) COMMENT '未出货金额（含运费）_含税-CNY',
  `unshipment_tax_usd_amt` VARCHAR(256) COMMENT '未出货金额（含运费）_含税-USD',
  `unshipment_notax_amt` VARCHAR(256) COMMENT '未出货金额（含运费）_不含税-原币',
  `unshipment_notax_cny_amt` VARCHAR(256) COMMENT '未出货金额（含运费）_不含税-CNY',
  `unshipment_notax_usd_amt` VARCHAR(256) COMMENT '未出货金额（含运费）_不含税-USD',
  `source_system` VARCHAR(256) COMMENT '来源系统',
  `insert_dt` VARCHAR(256) COMMENT '数仓数据更新时间'
) ENGINE=OLAP
DUPLICATE KEY(`dt`)
PARTITION BY RANGE(`dt`) ()
DISTRIBUTED BY HASH(`dt`) BUCKETS 10;
"""

def load_dataframe():
    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME, engine='openpyxl')
    return df

def insert_into_starrocks(df):
    # simple row-by-row insert using pymysql; for large volumes use batch load or broker load
    cfg = STARROCKS_CONFIG.copy()
    if cfg.get('cursorclass') is None:
        cfg.pop('cursorclass', None)
    conn = pymysql.connect(**cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
            insert_sql = "INSERT INTO `{}` ({}) VALUES ({})".format("dws_sd_inventory_detail_di", COL_LIST, "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s")
            values = []
            for _, row in df.iterrows():
                tup = []
                for col in COL_NAMES:
                    val = row.get(col, None)
                    if pd.isna(val):
                        tup.append(None)
                    else:
                        tup.append(val)
                values.append(tuple(tup))
            if values:
                cur.executemany(insert_sql, values)
                conn.commit()
    finally:
        conn.close()

if __name__ == '__main__':
    df = load_dataframe()
    print('Loaded', len(df), 'rows from', SHEET_NAME)
    # Uncomment to perform DB load
    # insert_into_starrocks(df)