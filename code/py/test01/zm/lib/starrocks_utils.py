"""
starrocks_utils.py

Utilities to map pandas dtypes to StarRocks column types and generate SQL snippets.
Uses centralized configuration from zm.lib.config.
"""
from typing import Dict, List, Optional, Any

# Import centralized config (prefer package import, fallback to module import)
try:
    from zm.lib.config import STARROCKS_CONFIG, get_starrocks_connection  # type: ignore
except ImportError:
    try:
        from lib.config import STARROCKS_CONFIG, get_starrocks_connection  # type: ignore
    except ImportError:
        # Fallback: create minimal config (should not happen in normal usage)
        STARROCKS_CONFIG: Dict[str, Any] = {}
        def get_starrocks_connection(cfg: Optional[Dict[str, Any]] = None) -> None:
            raise ImportError("Cannot import zm.lib.config. Please ensure config.py is available.")


def pandas_dtype_to_starrocks(dtype_str: str, sample_max_len: int = 0) -> str:
    """Map a pandas dtype string to a StarRocks SQL type string.

    Args:
        dtype_str: Pandas dtype string (e.g., 'int64', 'object', 'datetime64[ns]').
        sample_max_len: Used to suggest VARCHAR length for string types.
        
    Returns:
        StarRocks SQL type string (e.g., 'BIGINT', 'VARCHAR(256)').
    """
    dtype_lower = dtype_str.lower()
    
    if "int" in dtype_lower:
        return "BIGINT"
    if "float" in dtype_lower or "double" in dtype_lower:
        return "DOUBLE"
    if "bool" in dtype_lower:
        return "BOOLEAN"
    if "datetime" in dtype_lower or "timestamp" in dtype_lower:
        return "DATETIME"
    if "date" in dtype_lower:
        return "DATE"
    
    # Fallback to VARCHAR with a recommended length
    if sample_max_len <= 0:
        length = 256
    else:
        # Cap length to reasonable sizes (min 32, max 2000)
        length = min(max(32, sample_max_len), 2000)
    return f"VARCHAR({length})"


def generate_create_table_sql(
    table_name: str, 
    columns: List[Dict], 
    engine: str = "olap",
    buckets: int = 10,
    replication_num: int = 3
) -> str:
    """Generate a StarRocks CREATE TABLE statement from columns metadata.

    Args:
        table_name: Target table name (can include database prefix).
        columns: List of dicts with keys: name, dtype (pandas dtype string), 
                 sample_max_len (optional), nullable (optional).
        engine: Table engine type (default: 'olap').
        buckets: Number of buckets for distribution (default: 10).
        replication_num: Replication number (default: 3).
        
    Returns:
        CREATE TABLE SQL statement string.
        
    Raises:
        ValueError: If columns list is empty or missing required keys.
    """
    if not columns:
        raise ValueError("columns list cannot be empty")
    
    col_defs = []
    for c in columns:
        name = c.get("name")
        if not name:
            raise ValueError(f"Column missing 'name' key: {c}")
        
        dtype = c.get("dtype", "object")
        sample_max = c.get("sample_max_len", 0)
        sr_type = pandas_dtype_to_starrocks(dtype, sample_max)
        nullable = c.get("nullable", True)
        null_str = "" if nullable else " NOT NULL"
        
        comment = c.get("comment", "")
        comment_str = f" COMMENT '{comment}'" if comment else ""
        
        col_defs.append(f"  `{name}` {sr_type}{null_str}{comment_str}")

    cols_sql = ",\n".join(col_defs)
    
    # Use first column as distribution key
    dist_key = columns[0]['name']
    
    sql = (
        f"CREATE TABLE IF NOT EXISTS `{table_name}` (\n"
        f"{cols_sql}\n"
        f") ENGINE={engine}\n"
        f"DUPLICATE KEY(`{dist_key}`)\n"
        f"DISTRIBUTED BY HASH(`{dist_key}`) BUCKETS {buckets}\n"
        f"PROPERTIES(\"replication_num\" = \"{replication_num}\");"
    )
    return sql


def generate_insert_select_sql(
    table_name: str, 
    columns: List[Dict], 
    staging_schema: str = 'staging',
    staging_table: Optional[str] = None
) -> str:
    """Generate a simple INSERT ... SELECT ... FROM staging table SQL.
    
    Args:
        table_name: Target table name.
        columns: List of column dicts with 'name' key.
        staging_schema: Staging schema name (default: 'staging').
        staging_table: Staging table name. If None, uses '{table_name}_staging'.
        
    Returns:
        INSERT ... SELECT SQL statement string.
    """
    if not columns:
        raise ValueError("columns list cannot be empty")
    
    col_list = ', '.join([f"`{c['name']}`" for c in columns])
    staging_tbl = staging_table or f"{table_name}_staging"
    sql = (
        f"INSERT INTO `{table_name}` ({col_list})\n"
        f"SELECT {col_list}\n"
        f"FROM {staging_schema}.{staging_tbl};"
    )
    return sql


def generate_upsert_sql(
    table_name: str, 
    columns: List[Dict], 
    key_columns: List[str], 
    staging_schema: str = 'staging',
    staging_table: Optional[str] = None,
    order_by: Optional[str] = None
) -> str:
    """Generate a suggested upsert pattern for StarRocks using DELETE+INSERT with deduplication.

    This returns a multi-statement SQL that:
      1) Deletes rows from target that exist in staging (based on keys),
      2) Inserts deduplicated rows from staging.

    Args:
        table_name: Target table name.
        columns: List of column dicts with 'name' key.
        key_columns: List of key column names for deduplication.
        staging_schema: Staging schema name (default: 'staging').
        staging_table: Staging table name. If None, uses '{table_name}_staging'.
        order_by: ORDER BY clause for ROW_NUMBER (default: 'ORDER BY 1').
        
    Returns:
        Multi-statement SQL string for upsert operation.
        
    Note:
        Review before executing in production. StarRocks DELETE may have performance
        implications for large tables.
    """
    if not columns:
        raise ValueError("columns list cannot be empty")
    if not key_columns:
        raise ValueError("key_columns list cannot be empty")
    
    col_list = ', '.join([f"`{c['name']}`" for c in columns])
    key_list = ', '.join([f"`{k}`" for k in key_columns])
    staging_tbl = staging_table or f"{table_name}_staging"
    order_clause = order_by or "ORDER BY 1"

    sql_lines = [
        '-- 1) Delete existing target rows matching staging keys',
        f"DELETE FROM `{table_name}` WHERE ({key_list}) IN (SELECT {key_list} FROM {staging_schema}.{staging_tbl});",
        '',
        '-- 2) Insert deduplicated rows from staging (keep first row per key)',
        f"INSERT INTO `{table_name}` ({col_list})",
        f"SELECT {col_list} FROM (",
        f"  SELECT {col_list}, ROW_NUMBER() OVER (PARTITION BY {key_list} {order_clause}) AS rn",
        f"  FROM {staging_schema}.{staging_tbl}",
        ") t WHERE rn = 1;"
    ]

    return '\n'.join(sql_lines)
