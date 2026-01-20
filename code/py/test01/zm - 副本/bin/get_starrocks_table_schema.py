# -*- coding: utf-8 -*-
"""
Utility: fetch StarRocks table schemas (information_schema + SHOW CREATE TABLE)
Saved to: zm/out/starrocks_schemas.json
This file prefers to import STARROCKS_CONFIG from project (zm.starrocks_utils or starrocks_utils),
else falls back to a built-in default.
"""
from pathlib import Path
import json
from typing import List, Dict, Any, Tuple
import pymysql

# Import centralized config (prefer package import, fallback to module import)
try:
    from zm.lib.config import STARROCKS_CONFIG, get_starrocks_connection  # type: ignore
except ImportError:
    try:
        from lib.config import STARROCKS_CONFIG, get_starrocks_connection  # type: ignore
    except ImportError:
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
        try:
            from zm.lib.config import STARROCKS_CONFIG, get_starrocks_connection  # type: ignore
        except ImportError:
            raise ImportError(
                "Cannot import zm.lib.config. Please ensure config.py is available. "
                "Set STARROCKS_PASSWORD environment variable for database connection."
            )


# default tables to fetch (can be overridden by editing TABLES or calling main with custom list)
TABLES: List[str] = [
    "ods.ods_sap_erp_zhone_mat_purchase_price_get_df",
    "dim.dim_exchange_rate_di",
    "ods.ods_srm_mat_est_price_df",
    "ods.ods_hone_manu_factory_mapping_df",
]

OUT_DIR = Path(__file__).resolve().parent / "out"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE = OUT_DIR / "starrocks_schemas.json"


def _parse_table(fullname: str, default_db: str) -> Tuple[str, str]:
    if "." in fullname:
        db, tbl = fullname.split(".", 1)
        return db, tbl
    return default_db, fullname


def fetch_table_schema(conn: pymysql.Connection, database: str, table: str) -> Dict[str, Any]:
    """Fetch table schema from StarRocks.
    
    Args:
        conn: Database connection.
        database: Database name.
        table: Table name.
        
    Returns:
        Dict with database, table, columns, and create_table keys.
    """
    result: Dict[str, Any] = {
        "database": database,
        "table": table,
        "columns": [],
        "create_table": None
    }
    
    with conn.cursor() as cur:
        # Fetch column information
        q_cols = (
            "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, "
            "COLUMN_DEFAULT, COLUMN_COMMENT, ORDINAL_POSITION "
            "FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s "
            "ORDER BY ORDINAL_POSITION"
        )
        cur.execute(q_cols, (database, table))
        cols = cur.fetchall()
        
        columns = []
        for row in cols:
            if isinstance(row, dict):
                columns.append(row)
            else:
                columns.append({
                    "COLUMN_NAME": row[0],
                    "DATA_TYPE": row[1],
                    "COLUMN_TYPE": row[2],
                    "IS_NULLABLE": row[3],
                    "COLUMN_DEFAULT": row[4],
                    "COLUMN_COMMENT": row[5],
                    "ORDINAL_POSITION": row[6],
                })
        result["columns"] = columns
        
        # Fetch CREATE TABLE statement
        try:
            cur.execute(f"SHOW CREATE TABLE `{database}`.`{table}`")
            create_rows = cur.fetchall()
            if create_rows:
                if isinstance(create_rows[0], dict):
                    # Take first value from dict result
                    val = list(create_rows[0].values())[0]
                else:
                    val = create_rows[0][0]
                result["create_table"] = val
        except Exception as e:
            result["create_table_error"] = str(e)
    
    return result


def main(tables: Optional[List[str]] = None) -> None:
    """Fetch schemas for specified tables and save to JSON file.
    
    Args:
        tables: List of table names (can include database prefix).
                If None, uses default TABLES list.
    """
    tables = tables or TABLES
    conn: Optional[pymysql.Connection] = None
    schemas = []
    
    try:
        # Use centralized helper
        conn = get_starrocks_connection(STARROCKS_CONFIG)
        default_db = STARROCKS_CONFIG.get("database", "test")
        
        for fullname in tables:
            db, tbl = _parse_table(fullname, default_db)
            print(f"Fetching schema for {db}.{tbl} ...")
            try:
                schema = fetch_table_schema(conn, db, tbl)
                schemas.append(schema)
                status = 'ok' if schema.get('create_table') else 'missing'
                print(f"  -> columns: {len(schema['columns'])}, create_table {status}")
            except Exception as e:
                print(f"  ERROR fetching {db}.{tbl}: {e}")
                schemas.append({
                    "database": db,
                    "table": tbl,
                    "error": str(e)
                })
    except Exception as e:
        print(f"Fatal error: {e}")
        raise
    finally:
        if conn:
            conn.close()
    
    # Write results to file
    OUT_FILE.write_text(
        json.dumps(schemas, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    print(f"Schemas written to `{OUT_FILE}`")


if __name__ == "__main__":
    main()
