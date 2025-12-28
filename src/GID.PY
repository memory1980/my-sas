import os
import time
import pandas as pd
import baostock as bs
from typing import List
from datetime import datetime
from GTD import get_trade_date


from SKL import get_skline_data

from FKL import get_fkline_data

from index_codes import  index_codes

codes=index_codes[:]




print(f"总共 {len(codes[:])} 只股票...")

# 登录baostock
lg = bs.login()
if lg.error_code != '0':
    print(f"登录失败: {lg.error_msg}")
    exit()




# 获取日线数据
print("\n" + "="*70)
print("📈 获取月线数据")
print("="*70)
daily_data = get_fkline_data(
    days=600,  # 约7年数据
    codes=codes,
    frequency='m',
    delay=0.00000,
    save_to_csv=True,
    save_folder=r"d:\my-sas\data"
)


    # 获取日线数据
print("\n" + "="*70)
print("📈 获取周线数据")
print("="*70)
daily_data = get_fkline_data(
    days=400,  # 约7年数据
    codes=codes,
    frequency='w',
    delay=0.00000,
    save_to_csv=True,
    save_folder=r"d:\my-sas\data"
)



# 获取日线数据
print("\n" + "="*70)
print("📈 获取日线数据")
print("-"*50)
daily_data = get_fkline_data(
    days=600,  # 约7年数据
    codes=codes,
    frequency='d',
    delay=0.00000,
    save_to_csv=True,
    save_folder=r"d:\my-sas\data"
)

print(daily_data)

# 退出登录
bs.logout()
print("\n✅ 所有数据获取完成！")