# Generated ETL script for sheet: dwd_物料价格计算
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
SHEET_NAME = "dwd_物料价格计算"
COL_NAMES = ["返回", "模型设计清单", "Unnamed: 2", "Unnamed: 3", "Unnamed: 4", "Unnamed: 5", "Unnamed: 6", "Unnamed: 7", "Unnamed: 8", "Unnamed: 9", "Unnamed: 10", "Unnamed: 11", "Unnamed: 12", "Unnamed: 13", "Unnamed: 14", "Unnamed: 15", "Unnamed: 16", "Unnamed: 17", "Unnamed: 18", "Unnamed: 19", "Unnamed: 20", "Unnamed: 21"]
COL_LIST = "`返回`, `模型设计清单`, `Unnamed: 2`, `Unnamed: 3`, `Unnamed: 4`, `Unnamed: 5`, `Unnamed: 6`, `Unnamed: 7`, `Unnamed: 8`, `Unnamed: 9`, `Unnamed: 10`, `Unnamed: 11`, `Unnamed: 12`, `Unnamed: 13`, `Unnamed: 14`, `Unnamed: 15`, `Unnamed: 16`, `Unnamed: 17`, `Unnamed: 18`, `Unnamed: 19`, `Unnamed: 20`, `Unnamed: 21`"

CREATE_TABLE_SQL = r"""
CREATE TABLE IF NOT EXISTS `dwd_co_mat_price_df` (
  `mat_code` VARCHAR(100) COMMENT '物料编码',
  `factory_code` VARCHAR(100) COMMENT '工厂编码',
  `mat_name` VARCHAR(100) COMMENT '物料名称',
  `basic_unit` VARCHAR(100) COMMENT '基本计量单位',
  `mat_type_code` VARCHAR(100) COMMENT '物料类型编码',
  `mat_type_name` VARCHAR(100) COMMENT '物料类型名称',
  `mat_pur_type` VARCHAR(100) COMMENT '物料采购类型',
  `special_pur_type` VARCHAR(100) COMMENT '特殊采购类型',
  `price_unit` DECIMAL(27,8) COMMENT '价格单位',
  `moq_unit` DECIMAL(27,8) COMMENT 'MOQ',
  `max_unit_price` DECIMAL(27,8) COMMENT '最高价单个物料单价',
  `CNY` VARCHAR(256),
  `min_unit_price` DECIMAL(27,8) COMMENT '最低价单个物料单价',
  `latest_unit_price` DECIMAL(27,8) COMMENT '最近价单个物料单价',
  `std_unit_price` DECIMAL(27,8) COMMENT '标准价单个物料单价',
  `moving_avg_unit_price` DECIMAL(27,8) COMMENT '移动加权平均价单个物料单价',
  `po_est_unit_price` DECIMAL(27,8) COMMENT '采购预估单个物料单价',
  `MOQ` VARCHAR(256),
  `purchase_org_code` VARCHAR(256),
  `factory` VARCHAR(256),
  `dim` VARCHAR(256),
  `date` VARCHAR(256) COMMENT '补录价格有效期',
  `insert_dt` DATETIME COMMENT '数仓数据更新时间'
) ENGINE=OLAP
PRIMARY KEY(`mat_code`, `factory_code`)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(`mat_code`) BUCKETS 10;
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
            insert_sql = "INSERT INTO `{}` ({}) VALUES ({})".format("dwd_co_mat_price_df", COL_LIST, "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s")
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