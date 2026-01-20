# Generated ETL script for sheet: dws_标准收入成本模型
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
SHEET_NAME = "dws_标准收入成本模型"
COL_NAMES = ["返回", "模型设计清单", "Unnamed: 2", "Unnamed: 3", "Unnamed: 4", "Unnamed: 5", "Unnamed: 6", "Unnamed: 7", "Unnamed: 8", "Unnamed: 9", "Unnamed: 10", "Unnamed: 11", "Unnamed: 12", "Unnamed: 13", "Unnamed: 14", "Unnamed: 15"]
COL_LIST = "`返回`, `模型设计清单`, `Unnamed: 2`, `Unnamed: 3`, `Unnamed: 4`, `Unnamed: 5`, `Unnamed: 6`, `Unnamed: 7`, `Unnamed: 8`, `Unnamed: 9`, `Unnamed: 10`, `Unnamed: 11`, `Unnamed: 12`, `Unnamed: 13`, `Unnamed: 14`, `Unnamed: 15`"

CREATE_TABLE_SQL = r"""
CREATE TABLE IF NOT EXISTS `dws_fin_revenue_cost_df` (
  `company_code` VARCHAR(256) COMMENT '公司代码',
  `BKPF` VARCHAR(256) COMMENT 'join',
  `where` VARCHAR(256),
  `company_name` VARCHAR(256) COMMENT '公司名称',
  `sales_invoice_num` VARCHAR(256) COMMENT '销售订单号',
  `sales_invoice_line` VARCHAR(256) COMMENT '销售订单行号',
  `fiscal_year` VARCHAR(256) COMMENT '会计年度',
  `fiscal_period` VARCHAR(256) COMMENT '会计期间',
  `voucher_post_date` VARCHAR(256) COMMENT '过账日期',
  `transfer_node` VARCHAR(256) COMMENT '订单抛转节点',
  `manu_order_num` VARCHAR(256) COMMENT '制造公司销售订单号',
  `terminal_order_num` VARCHAR(256) COMMENT '终端销售公司销售订单号',
  `contract_num` VARCHAR(256) COMMENT '采购订单号（合同号）',
  `contract_m_num` VARCHAR(256) COMMENT '母合同号',
  `order_type_code` VARCHAR(256) COMMENT '销售订单类型',
  `order_type_name` VARCHAR(256) COMMENT '销售订单类型描述',
  `customer_code` VARCHAR(256) COMMENT '客户编码',
  `customer_name` VARCHAR(256) COMMENT '客户名称',
  `in_customer_flag` VARCHAR(256) COMMENT '是否内部关联客户',
  `product_code` VARCHAR(256) COMMENT '产品编码',
  `product_name` VARCHAR(256) COMMENT '产品描述',
  `product_series_code` VARCHAR(256) COMMENT '产品所属产品族编码',
  `product_series_name` VARCHAR(256) COMMENT '产品所属产品族名称',
  `product_group_code` VARCHAR(256) COMMENT '产品所属产品线编码',
  `product_group_name` VARCHAR(256) COMMENT '产品所属产品线名称',
  `industry_code` VARCHAR(256) COMMENT '产品所属产业编码',
  `industry_name` VARCHAR(256) COMMENT '产品所属产业名称',
  `sales_person_num` VARCHAR(256) COMMENT '业务员编码',
  `sales_person_name` VARCHAR(256) COMMENT '业务员姓名',
  `sales_org_code` VARCHAR(256) COMMENT '销售组织编码',
  `sales_org_name` VARCHAR(256) COMMENT '销售组织名称',
  `sales_dept_code` VARCHAR(256) COMMENT '销售部门编码',
  `sales_dept_name` VARCHAR(256) COMMENT '销售部门名称',
  `sales_group_code` VARCHAR(256) COMMENT '销售组编码',
  `sales_group_name` VARCHAR(256) COMMENT '销售组名称',
  `sales_unit_code` VARCHAR(256) COMMENT '销售战区',
  `sales_unit_name` VARCHAR(256) COMMENT '销售战区名称',
  `sales_area_code` VARCHAR(256) COMMENT '销售大区',
  `sales_area_name` VARCHAR(256) COMMENT '销售大区名称',
  `sales_region_code` VARCHAR(256) COMMENT '销售区域',
  `sales_region_name` VARCHAR(256) COMMENT '销售区域名称',
  `employee_dept_code` VARCHAR(256) COMMENT '雇员部门编码',
  `employee_dept_name` VARCHAR(256) COMMENT '雇员部门名称',
  `sales_channel_code` VARCHAR(256) COMMENT '分销渠道编码',
  `sales_channel_name` VARCHAR(256) COMMENT '分销渠道名称',
  `industry_type` VARCHAR(256) COMMENT '行业类型',
  `factory_code` VARCHAR(256) COMMENT '工厂编码',
  `factory_name` VARCHAR(256) COMMENT '工厂名称',
  `order_qty` VARCHAR(256) COMMENT '销售数量',
  `product_area` VARCHAR(256) COMMENT '产品单位面积',
  `order_area` VARCHAR(256) COMMENT '销售面积',
  `post_qty` VARCHAR(256) COMMENT '过账数量',
  `dr_cr_tag` VARCHAR(256) COMMENT '借贷项标识',
  `account_code` VARCHAR(256) COMMENT '科目编码',
  `account_name` VARCHAR(256) COMMENT '科目名称',
  `parent_acc_code` VARCHAR(256) COMMENT '父层科目编码',
  `currency` VARCHAR(256) COMMENT '原币别',
  `local_currency` VARCHAR(256) COMMENT '本位币',
  `ccy_notax_amt` VARCHAR(256) COMMENT '按原币计的金额_不含税',
  `lcy_notax_amt` VARCHAR(256) COMMENT '按本位币计的金额_不含税',
  `cny_notax_amt` VARCHAR(256) COMMENT '金额_人民币_不含税',
  `usd_notax_amt` VARCHAR(256) COMMENT '金额_美元_不含税',
  `insert_dt` VARCHAR(256) COMMENT '数仓数据更新时间'
) ENGINE=OLAP
PRIMARY KEY(`company_code`, `sales_invoice_num`, `sales_invoice_line`, `fiscal_year`, `fiscal_period`, `voucher_post_date`, `account_code`)
DISTRIBUTED BY HASH(`company_code`) BUCKETS 10;
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
            insert_sql = "INSERT INTO `{}` ({}) VALUES ({})".format("dws_fin_revenue_cost_df", COL_LIST, "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s")
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