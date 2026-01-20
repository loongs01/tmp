import os
import sys
import pandas as pd

def test_read_excel(file_path):
    print(f"Attempting to read: {file_path}")
    if not os.path.exists(file_path):
        print("Error: File does not exist!")
        return

    try:
        dfs = pd.read_excel(file_path, sheet_name=None)
        text = ""
        for sheet_name, df in dfs.items():
            text += f"Sheet: {sheet_name}\n{df.to_string()}\n"
        print("Successfully read file!")
        print(f"Content length: {len(text)}")
        print("First 100 chars:")
        print(text[:100])
    except Exception as e:
        print(f"Failed to read excel: {e}")

if __name__ == "__main__":
    file_path = r"d:\note\code\py\document\系统清单及管理.xlsx"
    print("\n--- Testing openpyxl direct read ---")
    try:
        import openpyxl
        wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
        text = ""
        for sheet in wb.worksheets:
            text += f"Sheet: {sheet.title}\n"
            for row in sheet.iter_rows(values_only=True):
                # Filter None values and convert to string
                row_text = " ".join([str(cell) for cell in row if cell is not None])
                text += row_text + "\n"
        print("Successfully read file with openpyxl!")
        print(f"Content length: {len(text)}")
        print("First 100 chars:")
        print(text[:100])
    except Exception as e:
        print(f"Failed to read with openpyxl: {e}")

    print("\n--- Testing zipfile extraction (Shared Strings) ---")
    try:
        import zipfile
        import xml.etree.ElementTree as ET
        
        with zipfile.ZipFile(file_path, 'r') as z:
            if 'xl/sharedStrings.xml' in z.namelist():
                with z.open('xl/sharedStrings.xml') as f:
                    tree = ET.parse(f)
                    root = tree.getroot()
                    # Namespace usually: {http://schemas.openxmlformats.org/spreadsheetml/2006/main}
                    # We just want all text inside <t> tags
                    texts = []
                    for elem in root.iter():
                        if elem.tag.endswith('}t'):
                            if elem.text:
                                texts.append(elem.text)
                    
                    full_text = "\n".join(texts)
                    print("Successfully extracted text from sharedStrings!")
                    print(f"Content length: {len(full_text)}")
                    print("First 100 chars:")
                    print(full_text[:100])
            else:
                print("No sharedStrings.xml found (maybe all inline strings?)")
    except Exception as e:
        print(f"Failed to extract with zipfile: {e}")
