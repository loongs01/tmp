# Generated ETL script for sheet: dim_汇率
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
SHEET_NAME = "dim_汇率"
COL_NAMES = ["返回", "Unnamed: 1", "Unnamed: 2", "Unnamed: 3", "Unnamed: 4", "Unnamed: 5", "Unnamed: 6", "Unnamed: 7", "Unnamed: 8", "Unnamed: 9", "Unnamed: 10", "Unnamed: 11", "Unnamed: 12", "Unnamed: 13", "Unnamed: 14", "Unnamed: 15", "Unnamed: 16"]
COL_LIST = "`返回`, `Unnamed: 1`, `Unnamed: 2`, `Unnamed: 3`, `Unnamed: 4`, `Unnamed: 5`, `Unnamed: 6`, `Unnamed: 7`, `Unnamed: 8`, `Unnamed: 9`, `Unnamed: 10`, `Unnamed: 11`, `Unnamed: 12`, `Unnamed: 13`, `Unnamed: 14`, `Unnamed: 15`, `Unnamed: 16`"

CREATE_TABLE_SQL = r"""
CREATE TABLE IF NOT EXISTS `dim_exchange_rate_di` (
  `dt` VARCHAR(50) COMMENT '日期（YYYYMMDD）',
  `rate_type` VARCHAR(50) COMMENT '汇率类型',
  `from_ccy` VARCHAR(50) COMMENT '从货币',
  `to_ccy` VARCHAR(50) COMMENT '最终货币',
  `start_date` VARCHAR(50) COMMENT '汇率起始日期',
  `raw_rate` DECIMAL(27,8) COMMENT '汇率（未转换因子）',
  `from_unit_rate` DECIMAL(27,8) COMMENT '来自货币单位的比率',
  `by` VARCHAR(256),
  `to_unit_rate` DECIMAL(27,8) COMMENT '到 货币单位汇率',
  `final_rate` DECIMAL(27,8) COMMENT '汇率',
  `insert_dt` DATETIME COMMENT '数仓数据更新时间'
) ENGINE=OLAP
PRIMARY KEY(`dt`, `rate_type`, `from_ccy`, `to_ccy`, `start_date`)
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
            insert_sql = "INSERT INTO `{}` ({}) VALUES ({})".format("dim_exchange_rate_di", COL_LIST, "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s")
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