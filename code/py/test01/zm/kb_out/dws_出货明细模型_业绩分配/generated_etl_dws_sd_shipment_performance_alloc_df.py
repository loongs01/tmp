# Generated ETL script for sheet: dws_出货明细模型_业绩分配
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
SHEET_NAME = "dws_出货明细模型_业绩分配"
COL_NAMES = ["数据来源表", "返回", "Unnamed: 2", "Unnamed: 3", "Unnamed: 4", "Unnamed: 5", "Unnamed: 6", "Unnamed: 7", "Unnamed: 8", "Unnamed: 9", "Unnamed: 10", "Unnamed: 11", "Unnamed: 12", "Unnamed: 13", "Unnamed: 14", "Unnamed: 15", "Unnamed: 16", "Unnamed: 17", "Unnamed: 18"]
COL_LIST = "`数据来源表`, `返回`, `Unnamed: 2`, `Unnamed: 3`, `Unnamed: 4`, `Unnamed: 5`, `Unnamed: 6`, `Unnamed: 7`, `Unnamed: 8`, `Unnamed: 9`, `Unnamed: 10`, `Unnamed: 11`, `Unnamed: 12`, `Unnamed: 13`, `Unnamed: 14`, `Unnamed: 15`, `Unnamed: 16`, `Unnamed: 17`, `Unnamed: 18`"

CREATE_TABLE_SQL = r"""
CREATE TABLE IF NOT EXISTS `dws_sd_shipment_performance_alloc_df` (
  `delivery_order_num` VARCHAR(100) COMMENT '交货单号',
  `delivery_order_item_num` VARCHAR(100) COMMENT '交货单行号',
  `order_num` VARCHAR(100) COMMENT '销售订单号',
  `order_item_num` VARCHAR(100) COMMENT '销售订单行号',
  `contract_num` VARCHAR(100) COMMENT '合同号（客户采购订单编号）',
  `contract_m_num` VARCHAR(100) COMMENT '母合同号',
  `order_type_code` VARCHAR(100) COMMENT '订单类型编码',
  `order_type_name` VARCHAR(255) COMMENT '订单类型名称',
  `shipment_date` DATETIME COMMENT '出货日期',
  `request_del_date` DATETIME COMMENT '请求交货日期',
  `approval_date` DATETIME COMMENT '订单首次审批日期',
  `create_date` DATETIME COMMENT '订单创建日期',
  `latest_warehousing_date` DATETIME COMMENT '最新入库日期',
  `mat_code` VARCHAR(100) COMMENT '物料编码',
  `mat_desc` VARCHAR(255) COMMENT '物料描述',
  `product_type` VARCHAR(100) COMMENT '产品类型',
  `industry_type` VARCHAR(100) COMMENT '行业类型',
  `industry_code` VARCHAR(100) COMMENT '产业编码',
  `industry_name` VARCHAR(255) COMMENT '产业描述',
  `product_line_code` VARCHAR(100) COMMENT '产品线编码',
  `product_line_name` VARCHAR(255) COMMENT '产品线描述',
  `product_group_code` VARCHAR(100) COMMENT '产品族编码',
  `product_group_name` VARCHAR(255) COMMENT '产品族描述',
  `product_series_code` VARCHAR(100) COMMENT '产品系列编码',
  `product_series_name` VARCHAR(255) COMMENT '产品系列描述',
  `product_area` DECIMAL(27,8) COMMENT '产品单位面积',
  `special_stock_flag` VARCHAR(100) COMMENT '特殊库存标识',
  `special_stock_name` VARCHAR(100) COMMENT '特殊库存标识名称',
  `delivery_address` VARCHAR(255) COMMENT '交货地址',
  `factory_code` VARCHAR(100) COMMENT '工厂编码',
  `factory_name` VARCHAR(255) COMMENT '工厂名称',
  `company_code` VARCHAR(100) COMMENT '公司代码',
  `company_name` VARCHAR(255) COMMENT '公司名称',
  `customer_code` VARCHAR(100) COMMENT '客户编码',
  `customer_name` VARCHAR(255) COMMENT '客户名称',
  `sales_person_num` VARCHAR(100) COMMENT '业务员编码',
  `b` VARCHAR(256),
  `sales_person_name` VARCHAR(255) COMMENT '业务员姓名',
  `b` VARCHAR(256),
  `sales_dept_code` VARCHAR(100) COMMENT '销售部门编码',
  `b` VARCHAR(256),
  `sales_dept_name` VARCHAR(255) COMMENT '销售部门名称',
  `b` VARCHAR(256),
  `employee_dept_code` VARCHAR(100) COMMENT '雇员部门编码',
  `b` VARCHAR(256),
  `employee_dept_name` VARCHAR(255) COMMENT '雇员部门描述',
  `b` VARCHAR(256),
  `sales_org_code` VARCHAR(100) COMMENT '销售组织编码',
  `sales_org_name` VARCHAR(255) COMMENT '销售组织名称',
  `sales_group_code` VARCHAR(100) COMMENT '销售组编码',
  `b` VARCHAR(256),
  `sales_group_name` VARCHAR(255) COMMENT '销售组名称',
  `b` VARCHAR(256),
  `sales_channel_code` VARCHAR(100) COMMENT '分销渠道编码',
  `sales_channel_name` VARCHAR(255) COMMENT '分销渠道名称',
  `sales_unit_name` VARCHAR(255) COMMENT '销售战区名称',
  `b` VARCHAR(256),
  `sales_area_name` VARCHAR(255) COMMENT '销售大区名称',
  `b` VARCHAR(256),
  `sales_region_name` VARCHAR(255) COMMENT '销售区域名称',
  `b` VARCHAR(256),
  `customer_country_code` VARCHAR(100) COMMENT '客户所属国家编码',
  `customer_country_name` VARCHAR(255) COMMENT '客户所属国家名称',
  `freight_forwarder` VARCHAR(100) COMMENT '货代',
  `transport_method` VARCHAR(100) COMMENT '运输方式',
  `payment_method` VARCHAR(100) COMMENT '付款方式',
  `is_need_inspection` VARCHAR(100) COMMENT '是否需要验收',
  `is_inspected` VARCHAR(100) COMMENT '是否已验收',
  `inspection_order_num` VARCHAR(100) COMMENT '验收单号',
  `inspection_invoice_date` DATETIME COMMENT '验收开票日期',
  `trade_terms` VARCHAR(500) COMMENT '贸易条款',
  `manu_sales_order_num` VARCHAR(100) COMMENT '制造公司销售订单号',
  `warranty_period` VARCHAR(100) COMMENT '质保期',
  `project_text` VARCHAR(500) COMMENT '项目文本',
  `currency` VARCHAR(100) COMMENT '销售订单行币种',
  `cny_exchange_rate` DECIMAL(27,8) COMMENT '汇率（to_CNY）',
  `usd_exchange_rate` DECIMAL(27,8) COMMENT '汇率（to_USD）',
  `sales_percent` DECIMAL(3,8) COMMENT 'new 业绩分配比例',
  `shipment_qty` DECIMAL(27,8) COMMENT 'new 出货数量',
  `shipment_area` DECIMAL(27,8) COMMENT 'new 出货面积',
  `shipment_tax_amt` DECIMAL(27,8) COMMENT 'new 出货金额（含运费）_含税-原币',
  `shipment_tax_cny_amt` DECIMAL(27,8) COMMENT 'new 出货金额（含运费）_含税-CNY',
  `shipment_tax_usd_amt` DECIMAL(27,8) COMMENT 'new 出货金额（含运费）_含税-USD',
  `shipment_notax_amt` DECIMAL(27,8) COMMENT 'new 出货金额（含运费）_不含税-原币',
  `shipment_notax_cny_amt` DECIMAL(27,8) COMMENT 'new 出货金额（含运费）_不含税-CNY',
  `shipment_notax_usd_amt` DECIMAL(27,8) COMMENT 'new 出货金额（含运费）_不含税-USD',
  `scenario_tag` VARCHAR(100) COMMENT '场景标签',
  `insert_dt` DATETIME COMMENT '数仓数据更新时间',
  `source_system` VARCHAR(100) COMMENT '来源系统'
) ENGINE=OLAP
DUPLICATE KEY(`delivery_order_num`)
DISTRIBUTED BY HASH(`delivery_order_num`) BUCKETS 10;
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
            insert_sql = "INSERT INTO `{}` ({}) VALUES ({})".format("dws_sd_shipment_performance_alloc_df", COL_LIST, "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s")
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