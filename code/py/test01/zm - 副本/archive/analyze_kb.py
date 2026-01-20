"""
analyze_kb.py

Usage:
  python analyze_kb.py [--file <path>] [--out <path>]

Description:
  Read an Excel file (all sheets), produce per-sheet metadata and simple statistics,
  and save the result as JSON.

Defaults:
  file: d:\note\code\py\document\模型设计清单-技术开发.xlsx
  out : d:\note\code\py\test01\zm\kb_summary.json

The script checks for the optional dependency `openpyxl` and prints an exact
pip install command using the current Python executable if it's missing.
"""

import sys
import json
from pathlib import Path
from typing import Any, Dict
import re

# Dependency checks
try:
    import pandas as pd
except Exception as e:
    print("pandas is required but failed to import:", e)
    print("Install it with:")
    print(f"    & '{sys.executable}' -m pip install pandas")
    sys.exit(1)

# openpyxl is required by pandas to read .xlsx files
try:
    import openpyxl  # noqa: F401
except ImportError:
    print("Missing optional dependency 'openpyxl'.")
    print("Install it with pip using the same Python executable that runs this script:")
    print(f"    & '{sys.executable}' -m pip install openpyxl")
    sys.exit(1)

"""
Note on "memory" and StarRocks integration:
- We integrate with `starrocks_utils` so that analysis results (including parsed target-table
  schemas and field mappings) can be reused by other components (kb_agent, generated ETL).
- When available, we also expose STARROCKS_CONFIG and a connection helper so that, if desired,
  callers can further fetch live table metadata from StarRocks based on the parsed source tables.
"""

# integrate starrocks utils
try:
    # Preferred: installed as package `zm`
    from zm.lib.starrocks_utils import (  # type: ignore
        pandas_dtype_to_starrocks,
        generate_create_table_sql,
        STARROCKS_CONFIG,
        get_starrocks_connection,
    )
except Exception:
    try:
        # Fallback: running from repo root where `lib` is importable
        from lib.starrocks_utils import (  # type: ignore
            pandas_dtype_to_starrocks,
            generate_create_table_sql,
            STARROCKS_CONFIG,
            get_starrocks_connection,
        )
    except Exception:
        # Will still run without starrocks_utils but skip SQL generation & live metadata
        pandas_dtype_to_starrocks = None
        generate_create_table_sql = None
        STARROCKS_CONFIG = {}  # type: ignore[var-annotated]
        get_starrocks_connection = None  # type: ignore[assignment]


def summarize_dataframe(df: pd.DataFrame) -> Dict[str, Any]:
    """Return metadata and simple statistics for a DataFrame."""
    summary: Dict[str, Any] = {}
    summary["rows"] = int(df.shape[0])
    summary["columns"] = int(df.shape[1])

    cols = []
    for col in df.columns:
        ser = df[col]
        col_meta: Dict[str, Any] = {}
        col_meta["dtype"] = str(ser.dtype)
        col_meta["missing"] = int(ser.isna().sum())
        try:
            col_meta["unique"] = int(ser.nunique(dropna=True))
        except Exception:
            col_meta["unique"] = None

        # sample max length for strings
        try:
            max_len = ser.dropna().astype(str).map(len).max()
            if pd.isna(max_len):
                max_len = 0
            col_meta["sample_max_len"] = int(max_len)
        except Exception:
            col_meta["sample_max_len"] = 0

        if pd.api.types.is_numeric_dtype(ser):
            desc = ser.describe().to_dict()
            # Convert numpy types to native Python types
            col_meta["stats"] = {k: (None if pd.isna(v) else float(v)) for k, v in desc.items()}
        else:
            # for non-numeric, give top 5 value counts
            try:
                vc = ser.fillna("<NA>").value_counts(dropna=False)
                top = []
                for val, cnt in vc.head(5).items():
                    # cast val to str for JSON
                    top.append({"value": str(val), "count": int(cnt)})
                col_meta["top_values"] = top
            except Exception:
                col_meta["top_values"] = []

        cols.append({"name": str(col), "meta": col_meta})

    summary["columns_detail"] = cols
    return summary


def infer_business_keys(df: pd.DataFrame) -> Dict[str, Any]:
    """Try to infer business keys for dimension tables.

    Heuristics:
    - If a column name contains 'id' or 'code' and has many unique values, prefer it.
    - If a column name is 'date'/'日期' or 'ts', treat as timestamp column.
    - If a single column is unique and non-null, pick it as surrogate key.
    """
    candidates = []
    for col in df.columns:
        name = str(col).lower()
        ser = df[col]
        unique = None
        try:
            unique = int(ser.nunique(dropna=True))
        except Exception:
            unique = None
        missing = int(ser.isna().sum()) if hasattr(ser, 'isna') else None

        score = 0
        if 'id' in name or 'code' in name or '编号' in name:
            score += 2
        if 'name' in name or '名称' in name:
            score += 1
        if unique and unique > max(1, int(len(df) * 0.5)):
            score += 2
        if missing == 0:
            score += 1

        candidates.append((score, name, col, unique, missing))

    # sort by score desc, unique desc
    candidates.sort(key=lambda x: (x[0], x[3] if x[3] is not None else 0), reverse=True)

    result = {}
    if candidates:
        top = candidates[0]
        if top[0] > 0:
            result['suggested_key'] = top[2]
            result['candidates'] = [{'name': c[2], 'score': c[0], 'unique': c[3], 'missing': c[4]} for c in candidates[:5]]
    return result


def parse_sections_from_df(df: pd.DataFrame) -> Dict[str, str]:
    """Attempt to extract three sections from a sheet DataFrame:
    - 数据来源表 (data sources)
    - 模型逻辑概述 (overview)
    - 模型逻辑详情 (details)

    Heuristic: look for rows where any cell contains the section header keyword
    and collect the following rows until next header or end.
    Returns dict with keys 'data_sources','overview','details' (may be empty strings).
    """
    headers = {'数据来源表': None, '模型逻辑概述': None, '模型逻辑详情': None}
    # normalize df to string grid
    grid = df.fillna('').astype(str).astype(object)
    rows = []
    for i in range(len(grid.index)):
        # join row cells to a single string for header detection
        row_text = ' '.join([str(x).strip() for x in grid.iloc[i].tolist() if str(x).strip()])
        rows.append(row_text)
        for h in headers:
            if headers[h] is None and h in row_text:
                headers[h] = i
    # Build sections by slicing from header index to next header
    sec_text = {k: '' for k in headers}
    # sort headers by index
    found = [(k, v) for k, v in headers.items() if v is not None]
    if not found:
        # fallback: try to find keyword in entire sheet text
        all_text = '\n'.join(rows)
        return {'data_sources': '', 'overview': all_text, 'details': ''}

    found.sort(key=lambda x: x[1])
    for idx, (key, start) in enumerate(found):
        end = len(rows)
        if idx + 1 < len(found):
            end = found[idx + 1][1]
        # collect rows between start+1 and end as section body (exclude header row)
        body_lines = []
        for r in range(start + 1, end):
            if rows[r].strip():
                body_lines.append(rows[r])
        sec_text[key] = '\n'.join(body_lines).strip()
    return {'data_sources': sec_text.get('数据来源表', ''), 'overview': sec_text.get('模型逻辑概述', ''), 'details': sec_text.get('模型逻辑详情', '')}


def extract_table_name_and_comment_from_overview(overview: str, sheet_name: str) -> Dict[str, str]:
    """From the overview text try to extract English table name and Chinese meaning.

    Returns dict with 'english' and 'chinese' (fall back to sheet_name for chinese and
    sheet_based english name if not found).
    """
    res = {'english': None, 'chinese': None}
    if not overview:
        res['english'] = sheet_name
        res['chinese'] = sheet_name
        return res

    # Direct pattern: 表名 xxx 表注释 yyy
    m_direct = re.search(r"表名[:：]?\s*([A-Za-z0-9_\.]+)\s*[，,]?\s*表注释[:：]?\s*([^\s，,]+)", overview)
    if m_direct:
        res['english'] = m_direct.group(1).strip().lower()
        res['chinese'] = m_direct.group(2).strip()
        return res

    # Try to parse as a small table: find a line that looks like "序号 英文表名 中文含义 ..."
    lines = [ln.strip() for ln in overview.splitlines() if ln.strip()]
    # 优先识别形如: 1 dwd_co_mat_price_df dwd_物料价格计算
    for ln in lines:
        toks = re.split(r"\s+", ln)
        if len(toks) >= 3 and re.match(r"^\d+$", toks[0]) and re.match(r"^[A-Za-z0-9_\.]+$", toks[1]) and re.search(r"[\u4e00-\u9fff]", toks[2]):
            res['english'] = toks[1].lower()
            res['chinese'] = toks[2]
            return res

    # 次优先：维度表名中包含 dim 的情况
    for ln in lines:
        toks = re.split(r"\s+", ln)
        for i, tok in enumerate(toks):
            if re.match(r"^[A-Za-z0-9_]*dim[A-Za-z0-9_]*$", tok, flags=re.I):
                eng = tok.lower()
                chi = None
                if i + 1 < len(toks) and re.search(r"[\u4e00-\u9fff]", toks[i + 1]):
                    chi = toks[i + 1]
                else:
                    for j in range(i + 1, len(toks)):
                        if re.search(r"[\u4e00-\u9fff]", toks[j]):
                            chi = toks[j]
                            break
                if not chi and i - 1 >= 0 and re.search(r"[\u4e00-\u9fff]", toks[i - 1]):
                    chi = toks[i - 1]
                res['english'] = eng
                res['chinese'] = chi or sheet_name
                return res

    # fallback to regex search as before
    m = re.search(r"\b([a-z0-9_]*dim[a-z0-9_]*[a-z0-9_]*)\b", overview, flags=re.I)
    if m:
        res['english'] = m.group(1).lower()
    m2 = re.search(r"([\u4e00-\u9fff]{2,})", overview)
    if m2:
        res['chinese'] = m2.group(1)
    if not res['english']:
        res['english'] = re.sub(r'[^a-z0-9_]', '_', sheet_name.lower())
    if not res['chinese']:
        res['chinese'] = sheet_name
    return res


def parse_data_sources(data_sources_text: str) -> Dict[str, str]:
    """Parse data_sources_text to map alias (like a/b) to table name (like ods_sap_erp_tcurr_df).
    Expected lines like: 'SAP ods_sap_erp_tcurr_df 汇率表 a 主表'
    Returns dict alias->table_name.
    """
    alias_map = {}
    if not data_sources_text:
        return alias_map
    for line in data_sources_text.splitlines():
        line = line.strip()
        if not line:
            continue
        # try to find table name token (ascii with underscores) and trailing alias (single letter)
        m = re.search(r"\b([A-Za-z0-9_]+)\b.*\b([a-zA-Z])\b$", line)
        if m:
            table = m.group(1)
            alias = m.group(2)
            alias_map[alias] = table
            continue
        # fallback: if line contains a token that looks like a table and a token 'a' or 'b' anywhere
        toks = re.split(r"\s+", line)
        potential_tables = [t for t in toks if re.match(r"^[A-Za-z0-9_]+$", t) and '_' in t]
        potential_aliases = [t for t in toks if re.match(r"^[a-zA-Z]$", t)]
        if potential_tables and potential_aliases:
            alias_map[potential_aliases[-1]] = potential_tables[0]
    return alias_map


def extract_source_tables(data_sources_text: str) -> Dict[str, str]:
    """从“数据来源表”文本中提取源表名列表（含库名），例如：
    ods.ods_sap_erp_zhone_mat_purchase_price_get_df
    dim.dim_exchange_rate_di
    返回 dict: 表名全称 -> 简单中文含义（如果能从同一行提取到中文）。
    """
    tables: Dict[str, str] = {}
    if not data_sources_text:
        return tables
    for line in data_sources_text.splitlines():
        line = line.strip()
        if not line:
            continue
        # 捕获类似 schema.table 形式
        m = re.search(r"([A-Za-z0-9_]+\.[A-Za-z0-9_]+)", line)
        if not m:
            continue
        tbl = m.group(1)
        # 尝试从该行提取中文含义（连续中文字符）
        m_cn = re.search(r"([\u4e00-\u9fff]{2,})", line)
        cn = m_cn.group(1) if m_cn else ""
        tables[tbl] = cn
    return tables


def fetch_starrocks_table_schema(table_fqn: str) -> Dict[str, Any]:
    """根据 STARROCKS_CONFIG，从 StarRocks 拉取指定表结构（列名、类型、注释、主键信息）。

    table_fqn: 可以是 'db.table' 或 'table'；如果没有库名，则使用 STARROCKS_CONFIG['database']。
    返回:
      {
        "table": "db.table",
        "columns": [
          {"name": ..., "data_type": ..., "is_nullable": True/False, "is_primary_key": bool, "comment": ...},
          ...
        ]
      }
    如果连接或查询失败，返回 {"table": ..., "error": "..."}。
    """
    if not STARROCKS_CONFIG or get_starrocks_connection is None:
        return {"table": table_fqn, "error": "STARROCKS_CONFIG or get_starrocks_connection not available"}

    # 解析库名和表名
    if "." in table_fqn:
        db, tbl = table_fqn.split(".", 1)
    else:
        db = STARROCKS_CONFIG.get("database") or STARROCKS_CONFIG.get("db") or ""
        tbl = table_fqn
    if not db:
        return {"table": table_fqn, "error": "No database specified in STARROCKS_CONFIG and table has no schema prefix"}

    try:
        conn = get_starrocks_connection(STARROCKS_CONFIG)  # type: ignore[call-arg]
    except Exception as e:  # pragma: no cover - best-effort integration
        return {"table": f"{db}.{tbl}", "error": f"connect failed: {e}"}

    try:
        with conn.cursor() as cur:  # type: ignore[union-attr]
            cur.execute(
                """
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_COMMENT
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
                """,
                (db, tbl),
            )
            rows = cur.fetchall()
    except Exception as e:  # pragma: no cover
        try:
            conn.close()
        except Exception:
            pass
        return {"table": f"{db}.{tbl}", "error": f"query failed: {e}"}

    try:
        conn.close()
    except Exception:
        pass

    cols = []
    for col_name, data_type, is_nullable, column_key, comment in rows:
        cols.append(
            {
                "name": col_name,
                "data_type": data_type,
                "is_nullable": (str(is_nullable).upper() == "YES"),
                "is_primary_key": (str(column_key).upper() == "PRI"),
                "comment": comment or "",
            }
        )
    return {"table": f"{db}.{tbl}", "columns": cols}


def detect_template_id_from_sections(sections: Dict[str, str]) -> str | None:
    """从 sections（overview/details 等文本）中解析模板 ID。

    约定写法示例：
      - TEMPLATE_ID: dim_exchange_rate_v1
      - 模板ID：dim_exchange_rate_v1
    """
    text = (sections.get("overview", "") or "") + "\n" + (sections.get("details", "") or "")
    if not text.strip():
        return None
    # 英文 TEMPLATE_ID
    m = re.search(r"TEMPLATE_ID[:：]\s*([A-Za-z0-9_]+)", text, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()
    # 中文 “模板ID”
    m2 = re.search(r"模板ID[:：]\s*([A-Za-z0-9_]+)", text, flags=re.IGNORECASE)
    if m2:
        return m2.group(1).strip()
    return None


def generate_exchange_rate_dim_sql_from_template() -> Dict[str, str]:
    """基于固定模板生成汇率维表 dim.dim_exchange_rate_di 的 CREATE/INSERT SQL。

    该模板等价于 test01/zm/demo 下的人工脚本，但不再依赖 demo 目录存在。
    """
    create_sql = """CREATE TABLE IF NOT EXISTS dim.dim_exchange_rate_di (
    dt INT COMMENT '日期（YYYYMMDD）',
    rate_type VARCHAR(50) COMMENT '汇率类型',
    from_ccy VARCHAR(50) COMMENT '从货币',
    to_ccy VARCHAR(50) COMMENT '最终货币',
    start_date VARCHAR(50) COMMENT '汇率起始日期',
    raw_rate DECIMAL(27,8) COMMENT '汇率（未转换因子）',
    from_unit_rate DECIMAL(27,8) COMMENT '来自货币单位的比率',
    to_unit_rate DECIMAL(27,8) COMMENT '到货币单位汇率',
    final_rate DECIMAL(27,8) COMMENT '汇率',
    insert_dt DATETIME COMMENT '数仓数据更新时间'
) ENGINE=OLAP
PRIMARY KEY(dt, rate_type, from_ccy, to_ccy)
COMMENT 'dim_汇率'
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(rate_type, from_ccy, to_ccy)
PROPERTIES (
    "compression" = "LZ4",
    "enable_persistent_index" = "true",
    "fast_schema_evolution" = "true",
    "replicated_storage" = "true",
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "32"
);"""

    insert_sql = """INSERT OVERWRITE TABLE dim.dim_exchange_rate_di
WITH 
-- 1. 处理汇率表(a表) - 取各分组最新数据
latest_rate AS (
    SELECT 
        a.kurst,
        a.fcurr,
        a.tcurr,
        a.gdatu,
        a.ukurs,
        a.insert_dt,
        ROW_NUMBER() OVER (
            PARTITION BY a.kurst, a.fcurr, a.tcurr 
            ORDER BY a.gdatu DESC
        ) AS rn
    FROM ods.ods_sap_erp_tcurr_df a 
    WHERE a.kurst IN ('M', 'EURX', 'PEND')
      -- 取昨天和今天有效期内数据
      AND (a.gdatu <= DATE_FORMAT(DATE_SUB(CURRENT_DATE(), 1), '%Y%m%d')
           OR a.gdatu <= DATE_FORMAT(CURRENT_DATE(), '%Y%m%d'))
),

-- 2. 处理汇率转换因子表(b表) - 取各分组最新数据
latest_factor AS (
    SELECT 
        b.kurst,
        b.fcurr,
        b.tcurr,
        b.ffact,
        b.tfact,
        ROW_NUMBER() OVER (
            PARTITION BY b.kurst, b.fcurr, b.tcurr 
            ORDER BY b.gdatu DESC
        ) AS rn
    FROM ods.ods_sap_erp_tcurf_df b 
    WHERE b.kurst IN ('M', 'EURX', 'PEND')
      -- 取昨天和今天有效期内数据
      AND (b.gdatu <= DATE_FORMAT(DATE_SUB(CURRENT_DATE(), 1), '%Y%m%d')
           OR b.gdatu <= DATE_FORMAT(CURRENT_DATE(), '%Y%m%d'))
),

-- 3. 获取最终数据（为昨天和今天分别生成数据）
final_data AS (
    -- 为昨天生成数据
    SELECT 
        DATE_FORMAT(DATE_SUB(CURRENT_DATE(), 1), '%Y%m%d') AS dt,
        lr.kurst AS rate_type,
        lr.fcurr AS from_ccy,
        lr.tcurr AS to_ccy,
        lr.gdatu AS start_date,
        CAST(lr.ukurs AS DECIMAL(27,8)) AS raw_rate,
        CAST(lf.ffact AS DECIMAL(27,8)) AS from_unit_rate,
        CAST(lf.tfact AS DECIMAL(27,8)) AS to_unit_rate,
        CASE 
            WHEN lf.ffact IS NOT NULL AND lf.ffact != 0 
            THEN CAST(lr.ukurs * lf.tfact / lf.ffact AS DECIMAL(27,8))
            ELSE CAST(lr.ukurs AS DECIMAL(27,8))
        END AS final_rate,
        lr.insert_dt
    FROM latest_rate lr
    LEFT JOIN latest_factor lf 
        ON lr.kurst = lf.kurst 
        AND lr.fcurr = lf.fcurr 
        AND lr.tcurr = lf.tcurr
        AND lf.rn = 1
    WHERE lr.rn = 1
      AND lr.gdatu <= DATE_FORMAT(DATE_SUB(CURRENT_DATE(), 1), '%Y%m%d')
    
    UNION ALL
    
    -- 为今天生成数据
    SELECT 
        DATE_FORMAT(CURRENT_DATE(), '%Y%m%d') AS dt,
        lr.kurst AS rate_type,
        lr.fcurr AS from_ccy,
        lr.tcurr AS to_ccy,
        lr.gdatu AS start_date,
        CAST(lr.ukurs AS DECIMAL(27,8)) AS raw_rate,
        CAST(lf.ffact AS DECIMAL(27,8)) AS from_unit_rate,
        CAST(lf.tfact AS DECIMAL(27,8)) AS to_unit_rate,
        CASE 
            WHEN lf.ffact IS NOT NULL AND lf.ffact != 0 
            THEN CAST(lr.ukurs * lf.tfact / lf.ffact AS DECIMAL(27,8))
            ELSE CAST(lr.ukurs AS DECIMAL(27,8))
        END AS final_rate,
        lr.insert_dt
    FROM latest_rate lr
    LEFT JOIN latest_factor lf 
        ON lr.kurst = lf.kurst 
        AND lr.fcurr = lf.fcurr 
        AND lr.tcurr = lf.tcurr
        AND lf.rn = 1
    WHERE lr.rn = 1
      AND lr.gdatu <= DATE_FORMAT(CURRENT_DATE(), '%Y%m%d')
)

-- 4. 插入数据
SELECT 
    dt,
    rate_type,
    from_ccy,
    to_ccy,
    start_date,
    raw_rate,
    from_unit_rate,
    to_unit_rate,
    final_rate,
    insert_dt
FROM final_data
WHERE dt IS NOT NULL 
  AND rate_type IS NOT NULL 
  AND from_ccy IS NOT NULL 
  AND to_ccy IS NOT NULL
;"""

    return {"create_sql": create_sql, "insert_sql": insert_sql}


def parse_mappings_from_details(details: str, data_sources_text: str = '') -> Dict[str, str]:
    """Parse simple target=source mappings from details text using alias map.

    Return dict target_col -> source_expression. Enhanced heuristics:
    - detect field code token in line (e.g., rate_type, dt, raw_rate)
    - detect source occurrence like 'a kurst' or table.column patterns
    - if alias found, map to alias_table.column
    """
    mappings = {}
    if not details:
        return mappings

    alias_map = parse_data_sources(data_sources_text)

    for line in details.splitlines():
        line = line.strip()
        if not line:
            continue
        # First, try explicit separators like tgt = src
        m = re.search(r"^([\w\u4e00-\u9fff\-]+)\s*(?:=|：|:|->|→)\s*([\w\u4e00-\u9fff\.`\.]*)$", line)
        if m:
            tgt = m.group(1).strip()
            src = m.group(2).strip()
            mappings[tgt] = src
            continue
        # Split tokens and try to find a field code (ascii, not purely numeric)
        toks = re.split(r"\s+", line)
        field_code = None
        for t in toks:
            if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", t):
                # exclude common words like '维度','度量','计算'
                if t.lower() in ('维度','度量','计算'):
                    continue
                # if t is short and likely alias, skip
                if len(t) == 1:
                    continue
                field_code = t
                break
        # find source pattern like 'a kurst' or 'a.gdatu' or 'ods_sap_erp_tcurr_df.gdatu'
        src = None
        m2 = re.search(r"([A-Za-z])\s+([A-Za-z0-9_]+)", line)
        if m2:
            alias = m2.group(1)
            col = m2.group(2)
            tbl = alias_map.get(alias, alias)
            src = f"{tbl}.{col}"
        else:
            m3 = re.search(r"([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)", line)
            if m3:
                src = f"{m3.group(1)}.{m3.group(2)}"
        if field_code and src:
            mappings[field_code] = src
            continue
        # fallback: try to detect pattern where source column appears later in line
        if field_code:
            # search for any token that looks like a column name (fcurr, tcurr, ukurs, gdatu, ffact, tfact, insert_dt)
            for candidate in ['fcurr','tcurr','ukurs','gdatu','ffact','tfact','kurst','insert_dt']:
                if re.search(r"\b" + re.escape(candidate) + r"\b", line):
                    # find table from alias map if present
                    m4 = re.search(r"([A-Za-z])\s+" + re.escape(candidate), line)
                    if m4:
                        alias = m4.group(1)
                        tbl = alias_map.get(alias, alias)
                        mappings[field_code] = f"{tbl}.{candidate}"
                        break
    return mappings


def generate_insert_select_from_mappings(target_table: str, mappings: Dict[str, str], data_sources_text: str) -> str:
    """Create an INSERT ... SELECT SQL based on mappings and data_sources.

    If mappings present, use them; otherwise produce a template with TODOs.
    Try to determine FROM clause using a table mentioned in mappings or data_sources_text.
    """
    if mappings:
        tgt_cols = []
        src_exprs = []
        source_tables = set()
        for tgt, src in mappings.items():
            tgt_cols.append(f"`{tgt}`")
            src_exprs.append(src)
            # detect table if src contains dot
            if '.' in src:
                tbl = src.split('.')[0]
                source_tables.add(tbl)
        tgt_list = ', '.join(tgt_cols)
        src_list = ', '.join(src_exprs)
        # pick a source table for FROM
        from_clause = None
        if source_tables:
            # pick first
            from_clause = list(source_tables)[0]
        else:
            # try to parse data_sources_text for a table-like token
            m = re.search(r"([A-Za-z0-9_]+\.[A-Za-z0-9_]+)", data_sources_text)
            if m:
                from_clause = m.group(1)
            else:
                # fallback to a staging placeholder
                from_clause = f"staging.{target_table}_staging"
        sql = f"INSERT INTO {target_table} ({tgt_list}) SELECT {src_list} FROM {from_clause};"
        return sql
    else:
        # no mappings -> generic template
        sql = (
            f"-- TODO: fill column mappings for target {target_table}\n"
            f"INSERT INTO {target_table} (/* col1, col2, ... */)\n"
            f"SELECT /* src.col1, src.col2, ... */\n"
            f"FROM /* source_table */;"
        )
        return sql


def map_declared_type_to_starrocks(declared: str) -> str:
    """Map a declared type string from the detail (e.g., varchar(50), decimal(27,8)) to StarRocks SQL type."""
    if not declared:
        return 'VARCHAR(256)'
    s = declared.lower()
    m = re.search(r"varchar\((\d+)\)", s)
    if m:
        return f"VARCHAR({m.group(1)})"
    m = re.search(r"decimal\((\d+),(\d+)\)", s)
    if m:
        return f"DECIMAL({m.group(1)},{m.group(2)})"
    if 'datetime' in s or 'timestamp' in s or 'date' in s:
        return 'DATETIME'
    if 'int' in s or 'bigint' in s:
        return 'BIGINT'
    if 'double' in s or 'float' in s:
        return 'DOUBLE'
    return 'VARCHAR(256)'


def parse_detail_fields(details_text: str) -> list:
    """Parse the 模型逻辑详情 block into a list of field dicts with keys:
    code, name, declared_type, sr_type, is_pk, is_nullable, source_alias, source_field, compute_logic
    This is heuristic and best-effort.
    """
    fields = []
    if not details_text:
        return fields
    lines = [ln.strip() for ln in details_text.splitlines() if ln.strip()]
    # skip header lines until we see a header that contains '字段编码' or similar
    header_idx = None
    for i, ln in enumerate(lines):
        if '字段编码' in ln or '字段名称' in ln:
            header_idx = i
            break
    start = header_idx + 1 if header_idx is not None else 0
    for ln in lines[start:]:
        # try to capture: optional leading number, then Chinese name, then code, then declared type
        # Regex: optional number and Chinese name, then code (ascii), then type (varchar(...)/decimal(...)/datetime)
        m = re.search(r"(?:^\d+\s+)?([\u4e00-\u9fff（）()\w\s％%-]+?)\s+([A-Za-z_][A-Za-z0-9_]*)\s+([^\s]+)", ln)
        if m:
            name = m.group(1).strip()
            code = m.group(2).strip()
            declared = m.group(3).strip()
            sr_type = map_declared_type_to_starrocks(declared)
            # detect PK（既支持“主键”字样，也兼容独立主键标记列里的 Y）
            is_pk = bool(re.search(r"\b主键\b|\b主键标记\b|\bPK\b|\bY\b", ln, flags=re.IGNORECASE))
            # detect non-null（“非空”列或标记）
            is_nullable = not bool(re.search(r"\b非空\b|\bNOT\s+NULL\b", ln, flags=re.IGNORECASE))
            # detect source alias and field (e.g., 'a kurst' or 'a.ukurs')
            src_alias = None
            src_field = None
            ma = re.search(r"\b([a-zA-Z])\s+([A-Za-z0-9_]+)\b", ln)
            if ma:
                src_alias = ma.group(1)
                src_field = ma.group(2)
            else:
                m2 = re.search(r"([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)", ln)
                if m2:
                    src_alias = m2.group(1)
                    src_field = m2.group(2)
            # compute logic（自然语言描述）
            comp = None
            cm = re.search(r"计算[：:]?\s*(.+)$", ln)
            if cm:
                comp = cm.group(1).strip()
            # 可选：半结构化模板参数，便于后续精确生成 SQL
            expr_template = None
            m_expr = re.search(r"(?:EXPR_TEMPLATE|表达式模板)[:：]\s*(.+)", ln, flags=re.IGNORECASE)
            if m_expr:
                expr_template = m_expr.group(1).strip()
            dedup_rule = None
            m_dedup = re.search(r"(?:DEDUP_RULE|去重规则)[:：]\s*(.+)", ln, flags=re.IGNORECASE)
            if m_dedup:
                dedup_rule = m_dedup.group(1).strip()
            join_keys = None
            m_join = re.search(r"(?:JOIN_KEYS|关联键)[:：]\s*(.+)", ln, flags=re.IGNORECASE)
            if m_join:
                join_keys = m_join.group(1).strip()

            fields.append({
                'code': code,
                'name': name,
                'declared_type': declared,
                'sr_type': sr_type,
                'is_pk': is_pk,
                'is_nullable': is_nullable,
                'source_alias': src_alias,
                'source_field': src_field,
                'compute': comp,
                'expr_template': expr_template,
                'dedup_rule': dedup_rule,
                'join_keys': join_keys,
                'raw_line': ln
            })
        else:
            # fallback: try to find ascii code and type anywhere
            mc = re.search(r"\b([A-Za-z_][A-Za-z0-9_]*)\b", ln)
            mt = re.search(r"(varchar\(\d+\)|decimal\(\d+,\d+\)|datetime)", ln, flags=re.I)
            if mc:
                code = mc.group(1)
                declared = mt.group(1) if mt else 'varchar(256)'
                sr_type = map_declared_type_to_starrocks(declared)
                fields.append({
                    'code': code,
                    'name': '',
                    'declared_type': declared,
                    'sr_type': sr_type,
                    'is_pk': False,
                    'is_nullable': True,
                    'source_alias': None,
                    'source_field': None,
                    'compute': None,
                    'expr_template': None,
                    'dedup_rule': None,
                    'join_keys': None,
                    'raw_line': ln
                })
    return fields


def generate_create_and_insert_for_details(target_table: str, fields: list, data_sources_text: str) -> Dict[str, str]:
    r"""Given parsed fields and data sources text, generate create_table_sql and insert_select_sql.

    相比通用模板，这里会更多地利用“模型逻辑详情”里的结构化信息：
    - 字段中文名称作为列注释 COMMENT
    - 主键信息决定 PRIMARY KEY 或 DUPLICATE KEY
    - dt/date 等日期列自动作为分区列（如果存在）
    """
    # create table SQL：列定义（包含注释）
    col_defs = []
    for f in fields:
        null_str = '' if f.get('is_nullable', True) else ' NOT NULL'
        comment = (f.get('name') or '').strip()
        if comment:
            comment_escaped = comment.replace("'", "''")
            comment_sql = f" COMMENT '{comment_escaped}'"
        else:
            comment_sql = ''
        col_defs.append(f"  `{f['code']}` {f['sr_type']}{null_str}{comment_sql}")
    cols_sql = ',\n'.join(col_defs)

    # 主键列集合（如果有的话）
    pk_cols = [f['code'] for f in fields if f.get('is_pk')]
    # 分布键优先用主键第一列，否则用第一个字段
    dist_key = pk_cols[0] if pk_cols else (fields[0]['code'] if fields else 'id')

    create_lines = []
    create_lines.append(f"CREATE TABLE IF NOT EXISTS `{target_table}` (")
    create_lines.append(cols_sql)
    create_lines.append(") ENGINE=OLAP")

    if pk_cols:
        pk_list = ', '.join(f"`{c}`" for c in pk_cols)
        create_lines.append(f"PRIMARY KEY({pk_list})")
    else:
        create_lines.append(f"DUPLICATE KEY(`{dist_key}`)")

    # 自动识别分区列（dt / date / biz_date / stat_date）
    part_col = None
    for cand in ['dt', 'date', 'biz_date', 'stat_date']:
        for f in fields:
            if str(f['code']).lower() == cand:
                part_col = f['code']
                break
        if part_col:
            break
    if part_col:
        create_lines.append(f"PARTITION BY RANGE(`{part_col}`) ()")

    create_lines.append(f"DISTRIBUTED BY HASH(`{dist_key}`) BUCKETS 10")

    create_sql = '\n'.join(create_lines) + ';'

    # parse data sources for alias mapping
    alias_map = parse_data_sources(data_sources_text)

    # build CTE for aliases that have dedup descriptions in raw_lines (heuristic for a and b)
    ctes = []
    from_tables = set()
    for alias, tbl in alias_map.items():
        from_tables.add(tbl)
        # attempt to detect partition/ordering hints in details raw content
        # create a dedup CTE template
        cte = f"{alias}_dedup AS (\n  SELECT * FROM (\n    SELECT *, ROW_NUMBER() OVER (PARTITION BY rate_type, from_ccy, to_ccy ORDER BY gdatu DESC) AS rn\n    FROM {tbl} /* filter/where conditions may be needed */\n  ) t WHERE rn = 1\n)"
        ctes.append(cte)

    # build select expressions: use source_alias.source_field if available, otherwise use code as placeholder
    select_exprs = []
    for f in fields:
        code = f['code']
        sa = f.get('source_alias')
        sf = f.get('source_field')
        if sa and sf:
            # map alias to dedup alias if present
            if sa in alias_map:
                select_exprs.append(f"{sa}_dedup.{sf} AS {code}")
            else:
                select_exprs.append(f"{sa}.{sf} AS {code}")
        else:
            # try to infer common columns
            # map certain codes to computed expressions
            if code in ('final_rate',):
                # use expression based on known fields
                select_exprs.append(f"(a.ukurs * b.tfact / b.ffact) AS {code}")
            elif code == 'dt':
                select_exprs.append(f"CAST(current_date AS VARCHAR) AS {code} /* YYYYMMDD formatting may be required */")
            else:
                select_exprs.append(f"NULL AS {code} /* TODO: map source */")

    tgt_cols = ', '.join([f"`{f['code']}`" for f in fields])
    select_list = ', '.join(select_exprs)

    # build FROM clause: prefer joins between first two alias dedups
    if len(alias_map) >= 2:
        aliases = list(alias_map.keys())
        a = aliases[0]
        b = aliases[1]
        from_clause = f"{a}_dedup {a} LEFT JOIN {b}_dedup {b} ON {a}.rate_type = {b}.rate_type AND {a}.fcurr = {b}.fcurr AND {a}.tcurr = {b}.tcurr"
    elif len(alias_map) == 1:
        a = list(alias_map.keys())[0]
        from_clause = f"{a}_dedup {a}"
    else:
        from_clause = f"staging.{target_table}_staging s"

    # assemble final SQL with CTEs
    cte_block = ''
    if ctes:
        cte_block = 'WITH ' + ',\n'.join(ctes) + '\n'

    insert_sql = cte_block + f"INSERT INTO {target_table} ({tgt_cols})\nSELECT {select_list}\nFROM {from_clause};"

    return {'create_sql': create_sql, 'insert_sql': insert_sql}


def generate_sql_for_sheet(sheet_name: str, df: pd.DataFrame) -> Dict[str, Any]:
    """Generate StarRocks SQL and ETL hints for a sheet (when possible).

    This implementation parses the sheet into sections, extracts target table name
    and comment, parses mappings, and generates insert SQL. If starrocks utils are
    available, also generate CREATE TABLE SQL from inferred column metadata.
    """
    out: Dict[str, Any] = {}

    # summarize columns
    cols = []
    for c in df.columns:
        ser = df[c]
        meta = {
            'name': str(c),
            'dtype': str(ser.dtype),
            'sample_max_len': 0,
            'nullable': True
        }
        try:
            max_len = ser.dropna().astype(str).map(len).max()
            if pd.isna(max_len):
                max_len = 0
            meta['sample_max_len'] = int(max_len)
        except Exception:
            meta['sample_max_len'] = 0
        try:
            meta['nullable'] = bool(ser.isna().any())
        except Exception:
            meta['nullable'] = True
        cols.append(meta)

    # parse sections: data_sources, overview, details
    sections = parse_sections_from_df(df)
    out['sections'] = sections
    overview = sections.get('overview','')
    details = sections.get('details','')
    data_sources_text = sections.get('data_sources','')
    # 检测模板 ID（例如 TEMPLATE_ID: dim_exchange_rate_v1）
    template_id = detect_template_id_from_sections(sections)
    if template_id:
        out["template_id"] = template_id

    # extract target table name and chinese comment
    names = extract_table_name_and_comment_from_overview(overview, sheet_name)
    target_table = names.get('english') or sheet_name
    out['target_table_english'] = target_table
    out['target_table_chinese'] = names.get('chinese')

    # use starrocks utils if available to create DDL（初始版本，后面可能被详情解析或 demo 覆盖）
    if generate_create_table_sql is not None:
        out['create_table_sql'] = generate_create_table_sql(target_table, cols)

    # infer business keys
    out['business_key_inference'] = infer_business_keys(df)

    # sample etl sql default
    cols_list = ', '.join([f"`{c['name']}`" for c in cols])
    out['sample_etl_sql'] = f"INSERT INTO {target_table} ({cols_list}) SELECT {cols_list} FROM staging.{target_table}_staging;"

    # parse mappings and generate insert-select（粗粒度字段映射）
    mappings = parse_mappings_from_details(details, data_sources_text)
    out['field_mappings'] = mappings
    out['generated_insert_sql'] = generate_insert_select_from_mappings(target_table, mappings, data_sources_text)

    # parse details into structured fields（细粒度目标表字段结构 + 来源 & 计算逻辑）
    try:
        detail_fields = parse_detail_fields(details)
        out['detail_fields'] = detail_fields

        # 目标表表结构（字段名、类型、注释、主键、是否可空、来源表/字段、计算逻辑）——相当于“记忆”
        alias_map = parse_data_sources(data_sources_text)
        target_schema = []
        for f in detail_fields:
            sa = f.get('source_alias')
            sf = f.get('source_field')
            src_table = alias_map.get(sa) if sa else None
            target_schema.append(
                {
                    "column_name": f.get("code"),
                    "data_type": f.get("sr_type"),
                    "comment": f.get("name", ""),
                    "is_primary_key": bool(f.get("is_pk")),
                    "is_nullable": bool(f.get("is_nullable", True)),
                    "source_table_alias": sa,
                    "source_table": src_table,
                    "source_column": sf,
                    "compute_logic": f.get("compute"),
                    "expr_template": f.get("expr_template"),
                    "dedup_rule": f.get("dedup_rule"),
                    "join_keys": f.get("join_keys"),
                    "raw_line": f.get("raw_line", ""),
                }
            )
        out["target_table_schema"] = {
            "table": target_table,
            "columns": target_schema,
        }

        # 基于“模型逻辑详情”生成更精细的 CREATE / INSERT SQL
        create_insert_sql = generate_create_and_insert_for_details(target_table, detail_fields, data_sources_text)
        out['create_table_sql'] = create_insert_sql['create_sql']
        out['generated_insert_sql'] = create_insert_sql['insert_sql']
    except Exception as e:
        out['detail_parsing_error'] = str(e)

    # 根据“数据来源表”获取源表名，并尝试从 StarRocks 读出源表结构（可用于对照/辅助开发）
    try:
        source_tables = extract_source_tables(data_sources_text)
        out["source_tables"] = source_tables

        # 可选：如果配置和连接函数可用，则附加 live 元数据
        live_meta = {}
        if source_tables and STARROCKS_CONFIG and get_starrocks_connection is not None:
            for tbl in source_tables.keys():
                live_meta[tbl] = fetch_starrocks_table_schema(tbl)
        if live_meta:
            out["source_tables_starrocks_meta"] = live_meta
    except Exception as e:
        out["source_tables_error"] = str(e)

    # 如果声明了汇率维表模板 ID，则使用固定模板覆盖 CREATE/INSERT，使结果更接近人工脚本
    try:
        normalized_template = (template_id or "").strip().lower()
        if normalized_template in ("dim_exchange_rate", "dim_exchange_rate_v1"):
            tmpl_sql = generate_exchange_rate_dim_sql_from_template()
            out["create_table_sql"] = tmpl_sql["create_sql"]
            out["generated_insert_sql"] = tmpl_sql["insert_sql"]
    except Exception as e:
        out.setdefault("template_sql_error", str(e))

    return out


def analyze_excel(path: Path) -> Dict[str, Any]:
    """Read all sheets and return a dict of summaries."""
    if not path.exists():
        raise FileNotFoundError(f"Excel file not found: {path}")

    # read all sheets
    xls = pd.read_excel(path, sheet_name=None, engine="openpyxl")

    result: Dict[str, Any] = {}
    result["file"] = str(path)
    result["sheets"] = []

    for sheet_name, df in xls.items():
        sheet_info: Dict[str, Any] = {}
        sheet_info["sheet_name"] = sheet_name
        sheet_info.update(summarize_dataframe(df))
        # add SQL generation for dim_* sheets
        try:
            sql_info = generate_sql_for_sheet(sheet_name, df)
            sheet_info.update(sql_info)
        except Exception as e:
            sheet_info["sql_generation_error"] = str(e)
        result["sheets"].append(sheet_info)

    return result


def main(argv):
    import argparse

    p = argparse.ArgumentParser(description="Analyze an Excel knowledge-base file and produce a JSON summary.")
    p.add_argument("--file", "-f", default=r"d:\\note\\code\\py\\document\\模型设计清单-技术开发.xlsx", help="Path to Excel file")
    p.add_argument("--out", "-o", default=r"d:\\note\\code\\py\\test01\\zm\\kb_summary.json", help="Output JSON path")

    args = p.parse_args(argv)

    excel_path = Path(args.file)
    out_path = Path(args.out)

    try:
        summary = analyze_excel(excel_path)
    except Exception as e:
        print(f"读取文件时出错：{e}")
        sys.exit(1)

    # ensure parent exists
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"分析完成，结果已保存到: {out_path}")
    # Print brief console summary
    for s in summary.get("sheets", []):
        print(f"- {s['sheet_name']}: rows={s['rows']} cols={s['columns']}")


if __name__ == "__main__":
    main(sys.argv[1:])











