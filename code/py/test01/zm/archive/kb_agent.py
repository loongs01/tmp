"""
kb_agent.py

A lightweight "agent" that selects an Excel file and sheet from the `document` folder,
analyses the sheet (via analyze_kb), produces a development plan and skeleton ETL code,
and optionally (if --app
ly-sr and --yes) executes StarRocks CREATE TABLE using
STARROCKS_CONFIG from starrocks_utils.

Usage examples:
  python kb_agent.py --project-dir d:\\note\\code\\py --file "document\\模型设计清单-技术开发.xlsx" --sheet dim_汇率 --out-dir d:\\tmp\\kb_out
  python kb_agent.py --project-dir d:\\note\\code\\py --file auto --sheet dim_汇率 --apply-sr --yes

Notes:
 - By default `--file auto` scans the ../document folder and picks the first .xlsx file found.
 - `--apply-sr` will attempt to connect to StarRocks and create the suggested tables.
   For safety you must also pass `--yes` to perform DB mutations.
 - The script writes outputs (JSON plan, generated ETL Python) into the out-dir.

"""
# 在这个项目里，kb_agent 里的 kb 是 “knowledge base”（知识库） 的缩写。
# 也就是：kb_agent.py = “知识库分析代理脚本”，负责解析 Excel 知识库（各 sheet 的模型逻辑），并生成建表、插入、ETL 和文档等产物。和文档等产物


import sys
import json
from pathlib import Path
import argparse
from typing import Optional
import shutil

# import analysis helpers (support both "python -m zm.archive.kb_agent" and direct script execution)
try:
    # When run as a package module (recommended)
    from .analyze_kb import analyze_excel
except ImportError:
    try:
        # When run from the same directory as a plain script
        from analyze_kb import analyze_excel  # type: ignore
    except ImportError:
        # Last resort: add this file's parent to sys.path then retry
        _here = Path(__file__).resolve()
        _parent = _here.parent
        if str(_parent) not in sys.path:
            sys.path.insert(0, str(_parent))
        from analyze_kb import analyze_excel  # type: ignore
try:
    # Preferred: installed/used as `zm` package
    from zm.lib.starrocks_utils import generate_create_table_sql, STARROCKS_CONFIG  # type: ignore
except Exception:
    try:
        # Fallback: running from repo root where `lib` is importable
        from lib.starrocks_utils import generate_create_table_sql, STARROCKS_CONFIG  # type: ignore
    except Exception:
        # Last resort: try to locate starrocks_utils.py by walking up parent dirs
        import importlib.util

        _here2 = Path(__file__).resolve()
        _sr_mod2 = None
        for _p in [_here2] + list(_here2.parents):
            _cand1 = _p / 'starrocks_utils.py'
            _cand2 = _p / 'lib' / 'starrocks_utils.py'
            _cand = _cand1 if _cand1.exists() else (_cand2 if _cand2.exists() else None)
            if _cand is not None and _cand.exists():
                _spec = importlib.util.spec_from_file_location('starrocks_utils', str(_cand))
                _sr_mod2 = importlib.util.module_from_spec(_spec)
                assert _spec.loader is not None
                _spec.loader.exec_module(_sr_mod2)
                break
        if _sr_mod2 is None:
            # one more attempt: plain module import if already on sys.path
            try:
                import starrocks_utils as _sr_mod2  # type: ignore
            except Exception:
                _sr_mod2 = None
        if _sr_mod2 is None:
            raise ImportError('Cannot locate starrocks_utils.py — please ensure it exists in the project tree or is importable as zm.lib.starrocks_utils/lib.starrocks_utils')
        generate_create_table_sql = getattr(_sr_mod2, 'generate_create_table_sql')
        STARROCKS_CONFIG = getattr(_sr_mod2, 'STARROCKS_CONFIG', {})


def find_excel_in_document(base_dir: Path) -> Optional[Path]:
    """Return first xlsx under base_dir/document."""
    doc = base_dir / 'document'
    if not doc.exists() or not doc.is_dir():
        return None
    for p in doc.glob('**/*.xlsx'):
        return p
    return None


def choose_sheet(summary: dict, preferred: Optional[str] = None) -> str:
    """Pick a sheet by preference, then dim_* first, else largest."""
    sheets = summary.get('sheets', [])
    if preferred and any(s['sheet_name'] == preferred for s in sheets):
        return preferred
    # prefer dim_ sheets
    for s in sheets:
        if s['sheet_name'].lower().startswith('dim_'):
            return s['sheet_name']
    # fallback: pick sheet with most rows
    best = max(sheets, key=lambda x: x.get('rows', 0)) if sheets else None
    return best['sheet_name'] if best else ''


def generate_development_plan(sheet_summary: dict) -> dict:
    """Create a concise development plan with DDL/DML artifacts."""
    plan = {}
    name = sheet_summary.get('sheet_name')
    plan['sheet'] = name
    plan['purpose'] = 'dimension' if name and name.lower().startswith('dim_') else 'table'

    # business key suggestion
    bk = sheet_summary.get('business_key_inference') or {}
    plan['business_key'] = bk.get('suggested_key') if bk else None
    plan['bk_candidates'] = bk.get('candidates') if bk else []

    # recommended starrocks create table & insert
    plan['create_table_sql'] = sheet_summary.get('create_table_sql')
    plan['insert_sql'] = sheet_summary.get('generated_insert_sql')

    # recommended ETL skeleton file
    plan['etl_script'] = f"generated_etl_{name}.py"

    plan['steps'] = [
        '解析数据来源表/模型逻辑概述/模型逻辑详情三段内容，锁定目标表与字段。',
        '校验字段类型、主键、分区列，生成 StarRocks DDL（可选执行）。',
        '生成 INSERT SELECT 逻辑，按业务逻辑完成关联、去重、计算。',
        '落地中间结果或目标表，补充数据质量校验与唯一性检查。',
        '补充自动化脚本与回填验证（行数、键唯一性、NULL 分布）。'
    ]
    return plan


def render_markdown_notes(sheet_summary: dict, plan: dict) -> str:
    """Render a human-friendly Markdown dev note for the analyzed sheet."""
    sheet_name = sheet_summary.get('sheet_name', '')
    target_table = sheet_summary.get('target_table_english') or sheet_name
    table_comment = sheet_summary.get('target_table_chinese') or sheet_name
    sections = sheet_summary.get('sections', {}) or {}
    data_sources = sections.get('data_sources', '')
    overview = sections.get('overview', '')
    details = sections.get('details', '')

    # target schema memory
    schema = sheet_summary.get('target_table_schema', {}) or {}
    cols = schema.get('columns', []) or []

    lines = []
    lines.append(f"# {sheet_name} 开发说明")
    lines.append('')
    lines.append("## 目标表")
    lines.append('')
    lines.append(f"- 英文表名: `{target_table}`")
    lines.append(f"- 中文含义: {table_comment}")
    lines.append(f"- 用途: {plan.get('purpose', '')}")
    if plan.get('business_key'):
        lines.append(f"- 建议业务主键: `{plan['business_key']}`")
    lines.append('')

    if data_sources:
        lines.append("## 数据来源表")
        lines.append('')
        lines.append("```")
        lines.append(data_sources)
        lines.append("```")
        lines.append('')

    if overview:
        lines.append("## 模型逻辑概述（原文）")
        lines.append('')
        lines.append("```")
        lines.append(overview)
        lines.append("```")
        lines.append('')

    # structured target schema
    if cols:
        lines.append("## 目标表字段结构（解析记忆）")
        lines.append('')
        lines.append("| 字段编码 | 字段类型 | 主键 | 非空 | 字段说明 | 来源表 | 来源字段 | 计算逻辑 |")
        lines.append("|----------|----------|------|------|----------|--------|----------|----------|")
        for c in cols:
            col = c.get('column_name') or c.get('code')
            dtype = c.get('data_type') or ''
            pk = 'Y' if c.get('is_primary_key') else ''
            nn = '' if c.get('is_nullable', True) else 'Y'
            comment = (c.get('comment') or '').replace('|', '/')
            src_tbl = c.get('source_table') or c.get('source_table_alias') or ''
            src_col = c.get('source_column') or ''
            logic = (c.get('compute_logic') or '').replace('\n', ' ').replace('|', '/')
            lines.append(f"| `{col}` | {dtype} | {pk} | {nn} | {comment} | {src_tbl} | {src_col} | {logic} |")
        lines.append('')

    if details:
        lines.append("## 模型逻辑详情（原文备忘）")
        lines.append('')
        lines.append("```")
        lines.append(details)
        lines.append("```")
        lines.append('')

    lines.append("## 建议开发步骤")
    lines.append('')
    for step in plan.get('steps', []):
        lines.append(f"- {step}")
    lines.append('')

    return '\n'.join(lines)


def write_etl_script(out_dir: Path, sheet_name: str, sheet_summary: dict, target_table: Optional[str] = None):
    """Generate a simple ETL script for this sheet.

    - sheet_name: Excel 工作表名（用于读取源数据）
    - target_table: 目标 StarRocks 表名（用于 INSERT INTO），如果不传则默认为 sheet_name
    """
    table_name = target_table or sheet_name
    script_path = out_dir / f"generated_etl_{table_name}.py"
    cols = sheet_summary.get('columns_detail', [])
    # Generate simple insert template
    col_names = [c['name'] for c in cols]
    col_list = ', '.join([f"`{n}`" for n in col_names])

    create_sql = sheet_summary.get('create_table_sql', '') or generate_create_table_sql(sheet_name, [
        {
            'name': c['name'],
            'dtype': c['meta'].get('dtype', 'object'),
            'sample_max_len': c['meta'].get('sample_max_len', 0),
            'nullable': (c['meta'].get('missing', 0) > 0)
        } for c in cols
    ])

    # safe JSON for column list
    import json as _json
    col_names_json = _json.dumps(col_names, ensure_ascii=False)

    placeholders = ', '.join(['%s'] * len(col_names))

    parts = []
    parts.append('# Generated ETL script for sheet: {}'.format(sheet_name))
    parts.append('# This script reads the sheet from the original Excel file and loads into StarRocks.')
    parts.append('# Review and test before running against production.')
    parts.append('')
    # include robust logic so generated scripts can locate starrocks_utils by walking up directories
    parts.append('import sys')
    parts.append('from pathlib import Path')
    parts.append('import importlib.util')
    parts.append('')
    parts.append("# Try to locate starrocks_utils.py by walking up parent dirs and load it directly")
    parts.append('_here = Path(__file__).resolve()')
    parts.append('_sr_mod = None')
    parts.append('for _p in [_here] + list(_here.parents):')
    parts.append("    _cand1 = _p / 'starrocks_utils.py'")
    parts.append("    _cand2 = _p / 'lib' / 'starrocks_utils.py'")
    parts.append("    _cand = _cand1 if _cand1.exists() else (_cand2 if _cand2.exists() else None)")
    parts.append('    if _cand is not None and _cand.exists():')
    parts.append("        spec = importlib.util.spec_from_file_location('starrocks_utils', str(_cand))")
    parts.append("        _sr_mod = importlib.util.module_from_spec(spec)")
    parts.append('        spec.loader.exec_module(_sr_mod)')
    parts.append('        break')
    parts.append('if _sr_mod is None:')
    parts.append('    try:')
    parts.append('        import starrocks_utils as _sr_mod')
    parts.append('    except Exception:')
    parts.append('        _sr_mod = None')
    parts.append('if _sr_mod is None:')
    parts.append("    raise ImportError('Cannot locate starrocks_utils.py — please ensure it exists in the project tree')")
    parts.append('STARROCKS_CONFIG = getattr(_sr_mod, "STARROCKS_CONFIG", {})')
    parts.append('import pandas as pd')
    parts.append('import pymysql')
    parts.append('')
    parts.append('EXCEL_PATH = r"{}"'.format(sheet_summary.get('file', '')))
    parts.append('SHEET_NAME = "{}"'.format(sheet_name))
    parts.append('COL_NAMES = {}'.format(col_names_json))
    parts.append('COL_LIST = "{}"'.format(col_list))
    parts.append('')
    parts.append('CREATE_TABLE_SQL = r"""')
    parts.append(create_sql)
    parts.append('"""')
    parts.append('')
    parts.append('def load_dataframe():')
    parts.append('    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME, engine=\'openpyxl\')')
    parts.append('    return df')
    parts.append('')
    parts.append('def insert_into_starrocks(df):')
    parts.append('    # simple row-by-row insert using pymysql; for large volumes use batch load or broker load')
    parts.append('    cfg = STARROCKS_CONFIG.copy()')
    parts.append("    if cfg.get('cursorclass') is None:")
    parts.append("        cfg.pop('cursorclass', None)")
    parts.append('    conn = pymysql.connect(**cfg)')
    parts.append('    try:')
    parts.append('        with conn.cursor() as cur:')
    parts.append('            cur.execute(CREATE_TABLE_SQL)')
    parts.append('            insert_sql = "INSERT INTO `{}` ({}) VALUES ({})".format("' + table_name + '", COL_LIST, "' + placeholders + '")')
    parts.append('            values = []')
    parts.append('            for _, row in df.iterrows():')
    parts.append('                tup = []')
    parts.append('                for col in COL_NAMES:')
    parts.append('                    val = row.get(col, None)')
    parts.append('                    if pd.isna(val):')
    parts.append('                        tup.append(None)')
    parts.append('                    else:')
    parts.append('                        tup.append(val)')
    parts.append('                values.append(tuple(tup))')
    parts.append('            if values:')
    parts.append('                cur.executemany(insert_sql, values)')
    parts.append('                conn.commit()')
    parts.append('    finally:')
    parts.append('        conn.close()')
    parts.append('')
    parts.append("if __name__ == '__main__':")
    parts.append("    df = load_dataframe()")
    parts.append("    print('Loaded', len(df), 'rows from', SHEET_NAME)")
    parts.append("    # Uncomment to perform DB load")
    parts.append("    # insert_into_starrocks(df)")

    script_content = '\n'.join(parts)
    script_path.write_text(script_content, encoding='utf-8')
    return script_path


def maybe_execute_starrocks(create_sql: str, apply_sr: bool, yes: bool):
    if not apply_sr:
        print('Skipping StarRocks DDL execution (use --apply-sr --yes to execute).')
        return {'executed': False}
    if not yes:
        print('Not executing because --yes was not provided.')
        return {'executed': False}

    # execute against STARROCKS_CONFIG
    import pymysql
    cfg = STARROCKS_CONFIG.copy()
    # remove None cursorclass if present
    if cfg.get('cursorclass') is None:
        cfg.pop('cursorclass', None)
    try:
        conn = pymysql.connect(**cfg)
        with conn.cursor() as cur:
            cur.execute(create_sql)
            conn.commit()
        conn.close()
        return {'executed': True}
    except Exception as e:
        return {'executed': False, 'error': str(e)}


def main(argv):
    p = argparse.ArgumentParser(description='KB Agent: analyze Excel and generate ETL/StarRocks artifacts')
    p.add_argument('--project-dir', default=None, help='Project root (contains document/ and zm/). Defaults to repo root.')
    p.add_argument('--file', default='auto', help='Excel file path or "auto" to scan document folder (relative to project-dir if not absolute)')
    p.add_argument('--sheet', default='auto', help='Sheet name or "auto" to pick heuristically')
    p.add_argument('--out-dir', default=None, help='Output directory for plan and scripts (default: <project>/zm/kb_out)')
    p.add_argument('--apply-sr', action='store_true', help='If set, attempt to execute StarRocks DDL')
    p.add_argument('--yes', action='store_true', help='Confirm destructive actions when combined with --apply-sr')

    args = p.parse_args(argv)

    # Resolve project root
    project_root = Path(args.project_dir).resolve() if args.project_dir else Path(__file__).resolve().parents[2]

    out_dir = Path(args.out_dir) if args.out_dir else project_root / 'zm' / 'kb_out'
    out_dir.mkdir(parents=True, exist_ok=True)

    # resolve excel path
    if args.file == 'auto':
        excel = find_excel_in_document(project_root)
        if excel is None:
            print(f'No Excel file found under {project_root / "document"}. Specify --file explicitly.')
            sys.exit(1)
    else:
        excel = Path(args.file)
        if not excel.is_absolute():
            excel = (project_root / args.file).resolve()
        if not excel.exists():
            print(f'Excel file not found: {excel}')
            sys.exit(1)

    print('Analyzing', excel)
    summary = analyze_excel(excel)

    # choose sheet
    sheet_name = args.sheet if args.sheet != 'auto' else choose_sheet(summary, None)
    if not sheet_name:
        print('No sheet selected/found')
        sys.exit(1)

    sheet_summary = next((s for s in summary.get('sheets', []) if s['sheet_name'] == sheet_name), None)
    if sheet_summary is None:
        print('Selected sheet not found in analysis')
        sys.exit(1)

    # derive target table info
    target_table = sheet_summary.get('target_table_english') or sheet_name
    table_comment = sheet_summary.get('target_table_chinese') or sheet_name
    create_sql = sheet_summary.get('create_table_sql') or ''
    insert_sql = sheet_summary.get('generated_insert_sql') or ''

    plan = generate_development_plan(sheet_summary)

    # 以 sheet 名作为二级目录，比如 kb_out/dwd_物料价格计算/*
    # 目录名直观对应 Excel 中的 sheet，文件名仍然使用目标表英文名
    out_dir = out_dir / sheet_name
    # 如果目录已存在，表示重跑本次分析：为避免残留旧文件，先整个删除再重建
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # write artifacts
    plan_payload = {
        'plan': plan,
        'sheet_summary': sheet_summary,
        'target_table': target_table,
        'table_comment': table_comment,
        'sections': sheet_summary.get('sections', {})
    }
    plan_path = out_dir / f'plan_{target_table}.json'
    plan_path.write_text(json.dumps(plan_payload, ensure_ascii=False, indent=2), encoding='utf-8')

    # write human-friendly markdown dev notes
    notes_md = render_markdown_notes(sheet_summary, plan)
    notes_path = out_dir / f'notes_{target_table}.md'
    notes_path.write_text(notes_md, encoding='utf-8')

    # write DDL/DML files
    ddl_path = out_dir / f'create_{target_table}.sql'
    ddl_path.write_text(create_sql, encoding='utf-8')
    dml_path = out_dir / f'insert_{target_table}.sql'
    dml_path.write_text(insert_sql, encoding='utf-8')

    etl_script_path = write_etl_script(out_dir, sheet_name, {'file': str(excel), **sheet_summary}, target_table)

    # maybe execute StarRocks DDL
    sr_result = maybe_execute_starrocks(create_sql, args.apply_sr, args.yes)

    print('Plan written to', plan_path)
    print('DDL written to', ddl_path)
    print('DML written to', dml_path)
    print('ETL script written to', etl_script_path)
    print('StarRocks DDL execution result:', sr_result)


if __name__ == '__main__':
    main(sys.argv[1:])







