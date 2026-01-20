# Script: generate_dwd_co_mat_price.py
# Purpose: read StarRocks schema JSON and produce CREATE & INSERT SQL for dwd.dwd_co_mat_price_df
# Note: This file is written to disk; content not printed to the chat per user request.
from pathlib import Path
import json
from datetime import datetime, timedelta, timezone
import re

OUT_DIR_BASE = Path(__file__).resolve().parent / 'kb_out'
OUT_DIR_BASE.mkdir(parents=True, exist_ok=True)
SCHEMA_FILE = Path(__file__).resolve().parent / 'out' / 'starrocks_schemas.json'


def slugify(name: str) -> str:
    # simple slug: replace non-alnum with underscore, collapse underscores, lowercase
    s = re.sub(r'[\W]+', '_', name, flags=re.UNICODE)
    s = re.sub(r'_+', '_', s)
    return s.strip('_') or 'sheet'


def load_schemas():
    if not SCHEMA_FILE.exists():
        raise FileNotFoundError(f'schema file not found: {SCHEMA_FILE}')
    return json.loads(SCHEMA_FILE.read_text(encoding='utf-8'))


def get_columns(schemas, fullname):
    # fullname like 'ods.ods_sap_erp_zhone_mat_purchase_price_get_df'
    if '.' in fullname:
        db, tbl = fullname.split('.', 1)
    else:
        db, tbl = None, fullname
    for s in schemas:
        if s.get('table') == tbl and (db is None or s.get('database') == db):
            return s.get('columns', [])
    return []


def gen_create_sql():
    cols = [
        ('mat_code', 'VARCHAR(100)', '物料编码'),
        ('factory_code', 'VARCHAR(100)', '工厂编码'),
        ('mat_name', 'VARCHAR(200)', '物料名称'),
        ('basic_unit', 'VARCHAR(100)', '基本计量单位'),
        ('mat_type_code', 'VARCHAR(100)', '物料类型编码'),
        ('mat_type_name', 'VARCHAR(200)', '物料类型名称'),
        ('mat_pur_type', 'VARCHAR(100)', '物料采购类型'),
        ('special_pur_type', 'VARCHAR(100)', '特殊采购类型'),
        ('price_unit', 'DECIMAL(27,8)', '价格单位'),
        ('moq_unit', 'DECIMAL(27,8)', 'MOQ'),
        ('max_unit_price', 'DECIMAL(27,8)', '最高价单个物料单价'),
        ('min_unit_price', 'DECIMAL(27,8)', '最低价单个物料单价'),
        ('latest_unit_price', 'DECIMAL(27,8)', '最近价单个物料单价'),
        ('std_unit_price', 'DECIMAL(27,8)', '标准价单个物料单价'),
        ('moving_avg_unit_price', 'DECIMAL(27,8)', '移动加权平均价单个物料单价'),
        ('po_est_unit_price', 'DECIMAL(27,8)', '采购预估单个物料单价'),
        ('validity_period', 'VARCHAR(64)', '补录价格有效期'),
        ('insert_dt', 'DATETIME', '数仓数据更新时间'),
        ('dt', 'VARCHAR(8)', '分区日期 YYYYMMDD')
    ]
    cols_sql = ',\n'.join([f"  `{c[0]}` {c[1]} COMMENT '{c[2]}'" for c in cols])
    create = f"""CREATE TABLE IF NOT EXISTS `dwd`.`dwd_co_mat_price_df` (
{cols_sql}
) ENGINE=OLAP
DUPLICATE KEY(`mat_code`, `factory_code`)
DISTRIBUTED BY HASH(`mat_code`) BUCKETS 12
PROPERTIES ("replication_num" = "3");
"""
    return create


def gen_insert_sql(run_dt):
    # generate insert SQL using business logic from sheet
    # assumptions: source tables exist with fields as in schema or standard names
    today = run_dt
    yesterday = (datetime.strptime(run_dt, '%Y%m%d') - timedelta(days=1)).strftime('%Y%m%d')

    insert_sql = f"""
-- Insert/overwrite partitions {yesterday}, {today} for dwd.dwd_co_mat_price_df
WITH dim_filtered AS (
  SELECT dt, rate_type, from_ccy, to_ccy, final_rate
  FROM dim.dim_exchange_rate_di
  WHERE to_ccy = 'CNY' AND rate_type = 'M' AND dt IN ('{yesterday}', '{today}')
),
price AS (
  SELECT
    matnr AS mat_code,
    werks AS factory_code,
    maktx AS mat_name,
    meins AS basic_unit,
    mtart AS mat_type_code,
    mtbez AS mat_type_name,
    beskz AS mat_pur_type,
    sobsl AS special_pur_type,
    peinh AS price_unit,
    kbetr_h, kbetr_l, kbetr_j, stprs, zstprs_hs,
    insert_dt
  FROM ods.ods_sap_erp_zhone_mat_purchase_price_get_df
  -- price is main table
),
srm AS (
  SELECT
    mat_code,
    moq,
    -- NOTE: replace the following two fields with actual names in ods_srm_mat_est_price_df
    material_est_amount_nanchang AS est_amount_nanchang,
    material_est_amount_dayawan AS est_amount_dayawan,
    est_currency,
    validity_period
  FROM ods.ods_srm_mat_est_price_df
),
factory AS (
  SELECT
    purchase_org_code,
    manu_factory_code
  FROM ods.ods_hone_manu_factory_mapping_df
),
price_agg AS (
  SELECT
    p.mat_code,
    f.purchase_org_code AS factory_code,
    MAX(p.kbetr_h) AS kbetr_h_max,
    MIN(p.kbetr_l) AS kbetr_l_min,
    MAX(p.kbetr_j) AS kbetr_j_latest,
    MAX(p.stprs) AS stprs_max,
    MAX(p.zstprs_hs) AS zstprs_hs_max,
    MAX(p.price_unit) AS price_unit_sample,
    MAX(p.insert_dt) AS insert_dt
  FROM price p
  LEFT JOIN factory f ON p.factory_code = f.purchase_org_code
  GROUP BY p.mat_code, f.purchase_org_code
)

INSERT INTO dwd.dwd_co_mat_price_df
SELECT
  pa.mat_code AS mat_code,
  pa.factory_code AS factory_code,
  pr.mat_name AS mat_name,
  pr.basic_unit AS basic_unit,
  pr.mat_type_code AS mat_type_code,
  pr.mat_type_name AS mat_type_name,
  pr.mat_pur_type AS mat_pur_type,
  pr.special_pur_type AS special_pur_type,
  CAST(pa.price_unit_sample AS DECIMAL(27,8)) AS price_unit,
  CAST(s.moq AS DECIMAL(27,8)) AS moq_unit,
  CAST(COALESCE(pa.kbetr_h_max,0)/10000.0/NULLIF(pa.price_unit_sample,0) AS DECIMAL(27,8)) AS max_unit_price,
  CAST(COALESCE(pa.kbetr_l_min,0)/10000.0/NULLIF(pa.price_unit_sample,0) AS DECIMAL(27,8)) AS min_unit_price,
  CAST(COALESCE(pa.kbetr_j_latest,0)/10000.0/NULLIF(pa.price_unit_sample,0) AS DECIMAL(27,8)) AS latest_unit_price,
  CAST(COALESCE(pa.stprs_max,0)/10000.0/NULLIF(pa.price_unit_sample,0) AS DECIMAL(27,8)) AS std_unit_price,
  CAST(COALESCE(pa.zstprs_hs_max,0)/10000.0/NULLIF(pa.price_unit_sample,0) AS DECIMAL(27,8)) AS moving_avg_unit_price,
  -- po_est_unit_price logic
  CASE
    WHEN pa.factory_code IN ('B010','2020') AND pr.mat_code = s.mat_code THEN
      CASE WHEN s.est_currency = df.from_ccy THEN
        CAST( (CASE WHEN pa.factory_code = 'B010' THEN s.est_amount_nanchang ELSE s.est_amount_dayawan END) * df.final_rate / NULLIF(s.moq,0) AS DECIMAL(27,8))
      ELSE
        CAST( (CASE WHEN pa.factory_code = 'B010' THEN s.est_amount_nanchang ELSE s.est_amount_dayawan END) / NULLIF(s.moq,0) AS DECIMAL(27,8))
      END
    ELSE NULL
  END AS po_est_unit_price,
  s.validity_period AS validity_period,
  CURRENT_TIMESTAMP() AS insert_dt,
  COALESCE(df.dt, '{today}') AS dt
FROM price_agg pa
LEFT JOIN price pr ON pa.mat_code = pr.mat_code AND pa.factory_code = pr.werks
LEFT JOIN srm s ON pr.mat_code = s.mat_code
LEFT JOIN factory f ON pa.factory_code = f.purchase_org_code
LEFT JOIN dim_filtered df ON df.dt IN ('{yesterday}','{today}') AND df.to_ccy='CNY'
;"""
    # replace placeholders {today} and {yesterday}
    insert_sql = insert_sql.replace('{today}', today).replace('{yesterday}', yesterday)
    return insert_sql


def write_outputs(create_sql, insert_sql, sheet_name: str = None):
    # create per-sheet directory under kb_out
    if sheet_name:
        folder = slugify(sheet_name)
    else:
        folder = 'dwd_co_mat_price_df'
    out_dir = OUT_DIR_BASE / folder
    out_dir.mkdir(parents=True, exist_ok=True)

    (out_dir / 'create_table_dwd_co_mat_price_df.sql').write_text(create_sql, encoding='utf-8')
    (out_dir / 'insert_dwd_co_mat_price_df.sql').write_text(insert_sql, encoding='utf-8')
    plan = {
        'target_table': 'dwd.dwd_co_mat_price_df',
        'create_sql_file': str((out_dir / 'create_table_dwd_co_mat_price_df.sql').resolve()),
        'insert_sql_file': str((out_dir / 'insert_dwd_co_mat_price_df.sql').resolve()),
        'generated_at': datetime.now(timezone.utc).isoformat(),
    }
    (out_dir / 'plan_dwd_co_mat_price_df.json').write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding='utf-8')


if __name__ == '__main__':
    # optional args: [run_dt] [sheet_name]
    import sys
    run_dt = datetime.today().strftime('%Y%m%d')
    sheet_name = None
    if len(sys.argv) >= 2:
        run_dt = sys.argv[1]
    if len(sys.argv) >= 3:
        sheet_name = sys.argv[2]
    schemas = load_schemas()
    create_sql = gen_create_sql()
    insert_sql = gen_insert_sql(run_dt)
    write_outputs(create_sql, insert_sql, sheet_name)
    print('Generated files in', OUT_DIR_BASE / (slugify(sheet_name) if sheet_name else 'dwd_co_mat_price_df'))
