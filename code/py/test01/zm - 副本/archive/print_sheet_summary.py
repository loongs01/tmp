import sys, json
from pathlib import Path

def main():
    if len(sys.argv) < 2:
        print('Usage: print_sheet_summary.py SHEET_NAME')
        sys.exit(1)
    sheet = sys.argv[1]
    p = Path(__file__).parent / 'kb_summary.json'
    if not p.exists():
        print('kb_summary.json not found at', p)
        sys.exit(1)
    data = json.loads(p.read_text(encoding='utf-8'))
    for s in data.get('sheets', []):
        if s.get('sheet_name') == sheet:
            print(json.dumps(s, ensure_ascii=False, indent=2))
            return
    print('Sheet not found:', sheet)

if __name__ == '__main__':
    main()

