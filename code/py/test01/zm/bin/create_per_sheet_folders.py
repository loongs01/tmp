# Script: create_per_sheet_folders.py
# Purpose: read an Excel file and create per-sheet output folders under zm/kb_out
from pathlib import Path
import json
import sys

try:
    import openpyxl
except Exception:
    openpyxl = None

# prefer centralized config
try:
    from zm.config import KB_OUT_DIR, DOCUMENT_DIR  # type: ignore
except Exception:
    try:
        from config import KB_OUT_DIR, DOCUMENT_DIR  # type: ignore
    except Exception:
        BASE_DIR = Path(__file__).resolve().parent
        KB_OUT_DIR = BASE_DIR / 'kb_out'
        DOCUMENT_DIR = BASE_DIR.parent / 'document'

KB_OUT_DIR.mkdir(parents=True, exist_ok=True)

BASE_DIR = Path(__file__).resolve().parent
# OUT_DIR_BASE used previously; now use KB_OUT_DIR
OUT_DIR_BASE = KB_OUT_DIR
OUT_DIR_BASE.mkdir(parents=True, exist_ok=True)


def slugify(name: str) -> str:
    import re
    s = re.sub(r'[\W]+', '_', name, flags=re.UNICODE)
    s = re.sub(r'_+', '_', s)
    return s.strip('_') or 'sheet'


def create_folders_for_workbook(xlsx_path: Path):
    if openpyxl is None:
        raise RuntimeError('openpyxl is required but not installed. Please pip install openpyxl')
    wb = openpyxl.load_workbook(str(xlsx_path), read_only=True)
    sheets = wb.sheetnames
    created = []
    for sh in sheets:
        folder = OUT_DIR_BASE / slugify(sh)
        folder.mkdir(parents=True, exist_ok=True)
        # write a small metadata file
        meta = {'sheet_name': sh, 'created_at': __import__('datetime').datetime.now().isoformat(), 'source_file': str(xlsx_path)}
        (folder / 'sheet_meta.json').write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding='utf-8')
        created.append(str(folder))
    return created


if __name__ == '__main__':
    # arg1: path to excel (optional)
    xlsx = BASE_DIR.parent / 'document' / '模型设计清单-技术开发.xlsx'
    if len(sys.argv) >= 2:
        xlsx = Path(sys.argv[1])
    if not xlsx.exists():
        print('Excel not found:', xlsx)
        sys.exit(2)
    created = create_folders_for_workbook(xlsx)
    print('Created folders:')
    for c in created:
        print(' -', c)
