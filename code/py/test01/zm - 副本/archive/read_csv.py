import pandas as pd

# 文件路径
file_path = r'd:\note\code\py\document\模型设计清单-技术开发.xlsx'

# 读取Excel文件
try:
    df = pd.read_excel(file_path)
    print("Excel文件读取成功！")
    print("数据预览：")
    print(df.head())  # 显示前5行
    print("\n数据信息：")
    print(df.info())
except Exception as e:
    print(f"读取文件时出错：{e}")