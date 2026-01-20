# Generated ETL script for sheet: dim_PBI产品信息维表
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
SHEET_NAME = "dim_PBI产品信息维表"
COL_NAMES = ["返回", "模型设计清单", "Unnamed: 2", "Unnamed: 3", "Unnamed: 4", "Unnamed: 5", "Unnamed: 6", "Unnamed: 7", "Unnamed: 8", "Unnamed: 9", "Unnamed: 10", "Unnamed: 11", "Unnamed: 12", "Unnamed: 13", "Unnamed: 14"]
COL_LIST = "`返回`, `模型设计清单`, `Unnamed: 2`, `Unnamed: 3`, `Unnamed: 4`, `Unnamed: 5`, `Unnamed: 6`, `Unnamed: 7`, `Unnamed: 8`, `Unnamed: 9`, `Unnamed: 10`, `Unnamed: 11`, `Unnamed: 12`, `Unnamed: 13`, `Unnamed: 14`"

CREATE_TABLE_SQL = r"""
CREATE TABLE IF NOT EXISTS `dim_product_info_df` (
  `pci_code` VARCHAR(100) COMMENT 'PCI编码',
  `pci_bom_code` VARCHAR(100) COMMENT 'PCI-BOM编码',
  `pci_label_start_date` DATETIME COMMENT 'PCI标签开始时间',
  `pci_tag` VARCHAR(100) COMMENT 'PCI标签',
  `product_catalog_code` VARCHAR(100) COMMENT '关联产业目录树编码',
  `industry_code` VARCHAR(100) COMMENT '产业编码',
  `industry_name` VARCHAR(100) COMMENT '产业名称',
  `product_line_code` VARCHAR(100) COMMENT '产品线编码',
  `product_line_name` VARCHAR(100) COMMENT '产品线名称',
  `product_group_code` VARCHAR(100) COMMENT '产品族编码',
  `product_group_name` VARCHAR(100) COMMENT '产品族名称',
  `product_series_code` VARCHAR(100) COMMENT '产品系列编码',
  `product_series_name` VARCHAR(100) COMMENT '产品系列名称',
  `product_mcm_type` VARCHAR(100) COMMENT '产品类型（模组整机）',
  `scene_type` VARCHAR(100) COMMENT '场景类型',
  `offering_code` VARCHAR(100) COMMENT 'Offering编码',
  `offering` VARCHAR(100) COMMENT 'Offering描述',
  `pci_short_desc` VARCHAR(100) COMMENT 'PCI短描述',
  `pitch_mm` DECIMAL(27,8) COMMENT '间距',
  `led_grade` VARCHAR(100) COMMENT '灯档次',
  `length_mm` DECIMAL(27,8) COMMENT '长',
  `width_mm` DECIMAL(27,8) COMMENT '宽',
  `product_area` DECIMAL(27,8) COMMENT '产品面积',
  `warranty_max_y` VARCHAR(100) COMMENT '质保上限'
) ENGINE=OLAP
PRIMARY KEY(`pci_code`, `pci_bom_code`, `pci_label_start_date`)
DISTRIBUTED BY HASH(`pci_code`) BUCKETS 10;
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
            insert_sql = "INSERT INTO `{}` ({}) VALUES ({})".format("dim_product_info_df", COL_LIST, "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s")
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