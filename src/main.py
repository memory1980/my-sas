import os
import pandas as pd


月线文件 = glob.glob("../data/stock_data_M_*.csv")[0]  # 取第一个月线文件
周线文件 = glob.glob("../data/stock_data_W_*.csv")[0]  # 取第一个周线文件

print(f"月线文件: {月线文件}")
print(f"周线文件: {周线文件}")

月线 = pd.read_csv(月线文件, encoding='utf-8-sig')
周线 = pd.read_csv(周线文件, encoding='utf-8-sig')

print(f"月线数据: {月线.shape}")
print(f"周线数据: {周线.shape}")