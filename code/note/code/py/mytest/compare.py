import pandas as pd


def read_csv_columns(file_path):
    """读取 CSV 文件并返回列名列表。"""
    try:
        df = pd.read_csv(file_path, nrows=0)  # 只读取表头
        return df.columns.tolist()
    except Exception as e:
        print(f"Error reading CSV file {file_path}: {e}")
        return []


def read_excel_columns(file_path, sheet_name=0):
    """读取 Excel 文件并返回指定工作表的列名列表。"""
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, nrows=0, header=0)  # 只读取表头
        return df.columns.tolist()
    except Exception as e:
        print(f"Error reading Excel file {file_path}: {e}")
        return []


def compare_columns(columns1, columns2):
    """比较两个列名列表是否一致。"""
    return set(columns1) == set(columns2)


def main():
    csv_file = r'C:\Users\admin\Desktop\bi优化.xlsx'  # 替换为实际的 CSV 文件路径
    excel_file = r'C:\Users\admin\Desktop\bi优化01.xlsx'  # 替换为实际的 Excel 文件路径

    columns1 = read_excel_columns(csv_file)
    columns2 = read_excel_columns(excel_file)

    if compare_columns(columns1, columns2):
        print("The columns in both tables are consistent.")
    else:
        print("The columns in both tables are NOT consistent.")
        print(f"CSV file columns: {columns1}")
        print(f"Excel file columns: {columns2}")


if __name__ == "__main__":
    main()
