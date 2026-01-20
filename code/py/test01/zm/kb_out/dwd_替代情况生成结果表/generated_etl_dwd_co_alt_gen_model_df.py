# Generated ETL script for sheet: dwd_替代情况生成结果表
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
SHEET_NAME = "dwd_替代情况生成结果表"
COL_NAMES = ["返回", "模型设计清单", "Unnamed: 2", "Unnamed: 3", "Unnamed: 4", "Unnamed: 5", "Unnamed: 6", "Unnamed: 7", "Unnamed: 8", "Unnamed: 9", "Unnamed: 10", "Unnamed: 11", "Unnamed: 12", "Unnamed: 13", "Unnamed: 14", "Unnamed: 15"]
COL_LIST = "`返回`, `模型设计清单`, `Unnamed: 2`, `Unnamed: 3`, `Unnamed: 4`, `Unnamed: 5`, `Unnamed: 6`, `Unnamed: 7`, `Unnamed: 8`, `Unnamed: 9`, `Unnamed: 10`, `Unnamed: 11`, `Unnamed: 12`, `Unnamed: 13`, `Unnamed: 14`, `Unnamed: 15`"

CREATE_TABLE_SQL = r"""
CREATE TABLE IF NOT EXISTS `dwd_co_alt_gen_model_df` (
  `dt` VARCHAR(100) COMMENT '日期（分区字段）',
  `pci_bom_code` VARCHAR(100) COMMENT 'PCI-BOM编码',
  `replace_rule_code` VARCHAR(500) COMMENT '替代规则编码',
  `replace_mat_group_code` VARCHAR(1000) COMMENT '替代物料组合编码',
  `idnrk_s` VARCHAR(256),
  `PCI` VARCHAR(256),
  `original_rule_code` VARCHAR(500) COMMENT '原始物料编码',
  `original_mat_flag` VARCHAR(100) COMMENT '原始物料标识',
  `light_mat_code` VARCHAR(100) COMMENT '灯珠物料编码',
  `ic_mat_code` VARCHAR(100) COMMENT '恒流IC物料编码',
  `power_mat_code` VARCHAR(100) COMMENT '电源物料编码',
  `card_mat_code` VARCHAR(100) COMMENT '接收卡物料编码',
  `insert_dt` DATETIME COMMENT '数仓数据更新时间'
) ENGINE=OLAP
PRIMARY KEY(`pci_bom_code`, `replace_rule_code`)
PARTITION BY RANGE(`dt`) ()
DISTRIBUTED BY HASH(`pci_bom_code`) BUCKETS 10;
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
            insert_sql = "INSERT INTO `{}` ({}) VALUES ({})".format("dwd_co_alt_gen_model_df", COL_LIST, "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s")
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