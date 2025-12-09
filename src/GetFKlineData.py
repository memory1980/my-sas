import baostock as bs
import pandas as pd
import os
import time
from datetime import datetime, timedelta
from typing import Optional, List, Literal, Union
from tqdm import tqdm

from high_growth_stock_list import high_growth_stocks

from GetTradeDate import get_trade_date

from GetSKLineData import  get_skline_data

def get_fkline_data(
    days: int = 180,
    codes: Union[str, List[str], None] = None,
    frequency: Literal["d", "w", "m"] = "d",
    delay: float = 0.1,
    save_to_csv: bool = True,
    save_folder: str = "my-sas/data"
) -> pd.DataFrame:
    """简洁版本 - 只做循环调用"""
    
    if isinstance(codes, str):
        stock_codes = [codes]
        print(f"📋 单只股票: {codes}")
    elif isinstance(codes, list):
        stock_codes = codes
        print(f"📋 股票数量: {len(stock_codes)} 只")
        if len(stock_codes) <= 10:
            print(f"📋 股票列表: {stock_codes}")
    else:
        raise ValueError("❌ 参数codes必须是字符串或列表")
    
    print(f"获取 {len(stock_codes)} 只股票数据...")
    
    # 循环调用get_skline_data
    all_data = []
    success_count = 0
    for code in stock_codes:
        df = get_skline_data(days=days, code=code, frequency=frequency)
        if df is not None:
            all_data.append(df)
            success_count += 1
        if delay > 0:
            time.sleep(delay)
    
    print(f"✅ 成功获取 {success_count}/{len(stock_codes)} 只股票")
    
    # 合并数据
    if all_data:
        result = pd.concat(all_data, ignore_index=True)
        
        # 保存文件
        if save_to_csv:
            os.makedirs(save_folder, exist_ok=True)
            
            # 生成文件名 - 只加频率标识
            current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
            # 频率标识：d->D, w->W, m->M
            freq_upper = frequency.upper()
            filename = f"stock_data_{freq_upper}.csv"
            filepath = os.path.join(save_folder, filename)
            result.to_csv(filepath, index=False, encoding='utf-8-sig')
            print(f"💾 数据已保存: {filepath}")
            
            # 显示文件信息
            file_size = os.path.getsize(filepath) / 1024 / 1024
            print(f"📏 文件大小: {file_size:.2f} MB")
            print(f"📊 数据行数: {len(result):,} 行")
            print(f"📈 包含股票数: {result['code'].nunique() if 'code' in result.columns else '未知'}")
        
        return result
    
    return pd.DataFrame()

if __name__ == "__main__":
    
    from high_growth_stock_list import high_growth_stocks 
    
    # 使用所有股票
    codes = high_growth_stocks[:]
    
  
    print(f"总共 {len(codes)} 只股票...")
    
    # 登录baostock
    lg = bs.login()
    if lg.error_code != '0':
        print(f"登录失败: {lg.error_msg}")
        exit()
    
    # # 获取月线数据
    # print("\n" + "="*70)
    # print("📈 获取月线数据")
    # print("="*70)
    # monthly_data = get_fkline_data(
    #     days=1000,  # 10年数据
    #     codes=codes,
    #     frequency='m',
    #     delay=0.1,
    #     save_to_csv=True,
    #     save_folder="data"
    # )
    
    # # 获取周线数据
    # print("\n" + "="*70)
    # print("📈 获取周线数据")
    # print("="*70)
    # weekly_data = get_fkline_data(
    #     days=1000,  # 约6年数据
    #     codes=codes,
    #     frequency='w',
    #     delay=0.1,
    #     save_to_csv=True,
    #     save_folder="data"
    # )
    
    # 获取日线数据
    print("\n" + "="*70)
    print("📈 获取日线数据")
    print("="*70)
    daily_data = get_fkline_data(
        days=1000,  # 约7年数据
        codes=codes,
        frequency='d',
        delay=0.1,
        save_to_csv=True,
        save_folder="data"
    )
    
    # 退出登录
    bs.logout()
    print("\n✅ 所有数据获取完成！")

















if __name__ == "__main__":
    
    
    from high_growth_stock_list import high_growth_stocks 
    
    # 测试10只股票
    
    codes = high_growth_stocks[:]
    
    print(codes[0:5])
    print(f"测试 {len(codes)} 只股票...")
    
    # 登录baostock
    lg = bs.login()
    if lg.error_code != '0':
        print(f"登录失败: {lg.error_msg}")
        exit()
    

    
    # 获取数据
    data = get_fkline_data(
        days=3650,
        codes=codes,
        frequency='m',
        delay=0.1,
        save_to_csv=True,
        save_folder="data"
    )
    
    data1 = get_fkline_data(
        days=3650,
        codes=codes,
        frequency='m',
        delay=0.1,
        save_to_csv=True,
        save_folder="data"
    )
    
    data2 = get_fkline_data(
        days=1500,
        codes=codes,
        frequency='m',
        delay=0.1,
        save_to_csv=True,
        save_folder="data"
    )
    
    
    
    #     # 获取数据
    # data = get_fkline_data(
    #     days=300,
    #     codes=codes,
    #     frequency='w',
    #     delay=0.1,
    #     save_to_csv=True,
    #     save_folder="my-sas/data"
    # )
    
    #     # 获取数据
    # data = get_fkline_data(
    #     days=18000,
    #     codes=codes,
    #     frequency='d',
    #     delay=0.1,
    #     save_to_csv=True,
    #     save_folder="my-sas/data"
    # )
    
    
    
    # 退出登录
    bs.logout()