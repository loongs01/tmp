# -*- coding: utf-8 -*-
"""
Lightweight SmartETLAgent skeleton for parsing business logic and generating SQL templates,
exporting documentation and simple QA checks.

This file is intentionally concise; it provides functions used by other scripts.
"""
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple, Any
import re
import json


class SmartETLAgent:
    """Agent for parsing business logic and generating ETL SQL templates."""
    
    def __init__(self, out_dir: Path) -> None:
        """Initialize the agent with an output directory.
        
        Args:
            out_dir: Directory path for output files.
        """
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)

    def parse_business_logic(self, text: str) -> Dict[str, Any]:
        """Parse business logic text to extract table metadata.
        
        Args:
            text: Business logic description text.
            
        Returns:
            Dict containing parsed information: target_table, target_comment,
            sources, filters, joins, fields.
        """
        res: Dict[str, Any] = {
            "target_table": None,
            "target_comment": None,
            "sources": [],
            "filters": [],
            "joins": [],
            "fields": []
        }
        
        # Extract table name and comment
        m = re.search(
            r'表名[:：]?\s*([a-z0-9_.]+)\s*[，,]?\s*表注释[:：]?\s*([\u4e00-\u9fa5_a-zA-Z0-9]+)',
            text,
            re.IGNORECASE
        )
        if m:
            res["target_table"] = m.group(1).strip()
            res["target_comment"] = m.group(2).strip()
        
        # Extract source tables
        lines = text.splitlines()
        seen_sources: set[str] = set()
        for line in lines:
            if re.search(r'\bods\.|\bdim\.|\bdwd\.', line, re.IGNORECASE):
                parts = re.findall(
                    r'([a-z0-9_.]+)\s*[,，]?\s*([\u4e00-\u9fa5_a-zA-Z0-9]*)',
                    line,
                    re.IGNORECASE
                )
                for table_name, comment in parts:
                    if table_name and table_name not in seen_sources:
                        seen_sources.add(table_name)
                        res['sources'].append({
                            "name": table_name,
                            "comment": comment
                        })
        
        # Extract field definitions
        field_lines = [
            ln for ln in lines 
            if '\t' in ln or '字段编码' in ln
        ]
        for ln in field_lines[:200]:  # Limit to prevent excessive processing
            cols = [c.strip() for c in re.split(r'\t| {2,}', ln) if c.strip()]
            if len(cols) >= 2 and re.match(r'^[a-zA-Z0-9_]+$', cols[1]):
                res["fields"].append({
                    "name": cols[1],
                    "comment": cols[0],
                    "dtype": cols[2] if len(cols) > 2 else None
                })
        
        # Extract joins and filters
        for ln in lines:
            line_lower = ln.lower()
            if '关联' in ln or 'join' in line_lower:
                res["joins"].append(ln.strip())
            if '筛选' in ln or 'where' in line_lower or ('取' in ln and 'in' in line_lower):
                res["filters"].append(ln.strip())
        
        return res

    def generate_create_table_sql(
        self,
        target_table: str,
        fields: List[Dict[str, Any]],
        partition: str = "dt",
        buckets: int = 8,
        replication_num: int = 3
    ) -> str:
        """Generate CREATE TABLE SQL statement.
        
        Args:
            target_table: Target table name.
            fields: List of field dicts with 'name', 'dtype', 'comment' keys.
            partition: Partition column name (default: 'dt').
            buckets: Number of buckets (default: 8).
            replication_num: Replication number (default: 3).
            
        Returns:
            CREATE TABLE SQL statement string.
            
        Raises:
            ValueError: If fields list is empty.
        """
        if not fields:
            raise ValueError("fields list cannot be empty")
        
        cols_sql = []
        for f in fields:
            name = f.get('name')
            if not name:
                continue
            
            dtype = (f.get('dtype') or 'varchar(256)').lower()
            if dtype.startswith('varchar') or dtype in ('object', 'str'):
                col_type = 'VARCHAR(256)'
            elif 'decimal' in dtype:
                col_type = 'DECIMAL(27,8)'
            elif 'datetime' in dtype or 'timestamp' in dtype:
                col_type = 'DATETIME'
            elif 'date' in dtype:
                col_type = 'DATE'
            elif 'int' in dtype:
                col_type = 'BIGINT'
            else:
                col_type = 'VARCHAR(256)'
            
            comment = f.get('comment', '').replace("'", "''")  # Escape single quotes
            cols_sql.append(f"  `{name}` {col_type} COMMENT '{comment}'")
        
        if not cols_sql:
            raise ValueError("No valid fields found")
        
        cols_part = ",\n".join(cols_sql)
        pk = fields[0]['name']
        
        create = (
            f"CREATE TABLE IF NOT EXISTS `{target_table}` (\n"
            f"{cols_part}\n"
            f") ENGINE=OLAP\n"
            f"DUPLICATE KEY(`{pk}`)\n"
            f"DISTRIBUTED BY HASH(`{pk}`) BUCKETS {buckets}\n"
            f"PROPERTIES(\"replication_num\" = \"{replication_num}\");"
        )
        return create

    def generate_insert_template(self, parsed: Dict[str, Any]) -> Tuple[str, str]:
        """Generate INSERT SQL template with date partitioning logic.
        
        Args:
            parsed: Parsed business logic dict from parse_business_logic().
            
        Returns:
            Tuple of (insert_sql, notes).
        """
        tgt = parsed.get('target_table') or 'target_table'
        fields = parsed.get('fields') or [
            {"name": "dt", "comment": "日期", "dtype": "varchar(50)"},
            {"name": "rate_type", "comment": "汇率类型", "dtype": "varchar(50)"},
        ]
        
        if not fields:
            raise ValueError("No fields found in parsed data")
        
        col_names = [f['name'] for f in fields if f.get('name')]
        if not col_names:
            raise ValueError("No valid column names found")
        
        col_list = ", ".join([f"`{c}`" for c in col_names])
        srcs = parsed.get('sources') or []
        src0 = srcs[0]['name'] if srcs else 'ods_main'
        
        today = datetime.today().strftime("%Y%m%d")
        yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
        
        insert_sql = (
            f"-- 覆盖分区 {yesterday}, {today}\n"
            f"WITH dt_list AS (\n"
            f"  SELECT '{yesterday}' AS dt UNION ALL SELECT '{today}' AS dt\n"
            f"),\n"
            f"src_a AS (\n"
            f"  SELECT * FROM {src0} WHERE kurst IN ('M','EURX','PEND')\n"
            f"),\n"
            f"a_with_dt AS (\n"
            f"  SELECT d.dt, a.* FROM dt_list d CROSS JOIN src_a a WHERE a.gdatu <= d.dt\n"
            f"),\n"
            f"a_ranked AS (\n"
            f"  SELECT *, ROW_NUMBER() OVER (PARTITION BY dt, kurst, fcurr, tcurr ORDER BY gdatu DESC) rn\n"
            f"  FROM a_with_dt\n"
            f"),\n"
            f"a_latest AS (\n"
            f"  SELECT * FROM a_ranked WHERE rn = 1\n"
            f")\n"
            f"INSERT INTO `{tgt}` ({col_list})\n"
            f"SELECT {col_list} FROM a_latest;"
        )
        
        notes = (
            "请按真实字段替换占位字段，必要时在大表上先做分区/时间范围预筛选以避免全表扫描。"
        )
        return insert_sql, notes

    def optimize_sql(self, sql: str) -> Dict[str, Any]:
        """Analyze SQL and provide optimization suggestions.
        
        Args:
            sql: SQL statement to analyze.
            
        Returns:
            Dict with 'sql' and 'suggestions' keys.
        """
        suggestions: List[str] = []
        sql_upper = sql.upper()
        
        if "CROSS JOIN" in sql_upper:
            suggestions.append(
                "检测到 CROSS JOIN，建议限制 dt_list 或提前按分区筛选源表以避免笛卡尔积。"
            )
        if "SELECT *" in sql_upper:
            suggestions.append("存在 SELECT *，建议显式列出所需列以减少 IO。")
        if "WHERE 1=1" in sql_upper:
            suggestions.append("检测到 WHERE 1=1，建议移除不必要的条件。")
        
        return {"sql": sql, "suggestions": suggestions}

    def run_quality_checks(
        self,
        conn: Any,
        table: str,
        checks: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """Run data quality checks on a table.
        
        Args:
            conn: Database connection object (pymysql connection).
            table: Table name to check.
            checks: Optional list of check definitions.
            
        Returns:
            Dict with check results.
            
        Note:
            This is a stub implementation. Override or extend for actual checks.
        """
        return {
            "note": "run_quality_checks 是 stub，需传入有效连接并实现检查。",
            "table": table,
            "checks": checks or []
        }

    def generate_docs(self, parsed: Dict[str, Any], create_sql: str) -> Path:
        """Generate Markdown documentation for a table.
        
        Args:
            parsed: Parsed business logic dict.
            create_sql: CREATE TABLE SQL statement.
            
        Returns:
            Path to generated documentation file.
        """
        md: List[str] = []
        tgt = parsed.get('target_table') or 'target_table'
        
        md.append(f"# 表 `{tgt}`")
        if parsed.get('target_comment'):
            md.append(f"**说明**：{parsed['target_comment']}\n")
        
        md.append("## 来源表")
        sources = parsed.get('sources', [])
        if sources:
            for s in sources:
                md.append(f"- `{s['name']}` {s.get('comment', '')}")
        else:
            md.append("- 无")
        
        md.append("\n## 字段\n")
        md.append("| 字段编码 | 字段说明 | 类型 |")
        md.append("|---:|---|---|")
        fields = parsed.get('fields', [])
        if fields:
            for f in fields:
                name = f.get('name', '')
                comment = f.get('comment', '')
                dtype = f.get('dtype', 'varchar(256)')
                md.append(f"| `{name}` | {comment} | {dtype} |")
        else:
            md.append("| - | - | - |")
        
        md.append("\n## CREATE TABLE\n")
        md.append("```sql")
        md.append(create_sql)
        md.append("```")
        
        out = self.out_dir / f"{tgt.replace('.', '_')}_doc.md"
        out.write_text("\n".join(md), encoding="utf-8")
        return out

    def export_plan(
        self,
        parsed: Dict[str, Any],
        insert_sql: str,
        notes: str
    ) -> Path:
        """Export development plan as JSON.
        
        Args:
            parsed: Parsed business logic dict.
            insert_sql: Generated INSERT SQL statement.
            notes: Additional notes.
            
        Returns:
            Path to generated plan file.
        """
        tgt = parsed.get('target_table') or 'target_table'
        plan = {
            "parsed": parsed,
            "insert_sql": insert_sql,
            "notes": notes,
            "generated_at": datetime.now(timezone.utc).isoformat()
        }
        out = self.out_dir / f"plan_{tgt.replace('.', '_')}.json"
        out.write_text(
            json.dumps(plan, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )
        return out


if __name__ == "__main__":
    sample_text = ("表名: dim.dim_exchange_rate_di，表注释: dim_汇率\n"
                   "来源表: ods.ods_sap_erp_tcurr_df\n来源表: ods.ods_sap_erp_tcurf_df\n"
                   "字段说明\t字段编码\tdtype\n日期（YYYYMMDD）\tdt\tvarchar(50)\n汇率类型\trate_type\tvarchar(50)")
    agent = SmartETLAgent(out_dir=Path(__file__).resolve().parent / "out")
    parsed = agent.parse_business_logic(sample_text)
    create_sql = agent.generate_create_table_sql(parsed.get('target_table') or 'dim.dim_exchange_rate_di', parsed.get('fields') or [
        {"name":"dt","comment":"日期","dtype":"varchar(50)"},
        {"name":"rate_type","comment":"汇率类型","dtype":"varchar(50)"},
    ])
    insert_sql, notes = agent.generate_insert_template(parsed)
    doc_path = agent.generate_docs(parsed, create_sql)
    plan_path = agent.export_plan(parsed, insert_sql, notes)
    print("plan written to", plan_path)
