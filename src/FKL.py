import baostock as bs
import pandas as pd
import os
import time
from datetime import datetime, timedelta
from typing import Optional, List, Literal, Union
from tqdm import tqdm



from GTD import get_trade_date

from SKL import  get_skline_data

def get_fkline_data(
    days: int = 180,
    codes: Union[str, List[str], None] = None,
    frequency: Literal["d", "w", "m"] = "d",
    delay: float = 0.1,
    save_to_csv: bool = True,
    save_folder: str = "my-sas/data"
) -> pd.DataFrame:
    """简洁版本 - 只做循环调用"""
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # 构建正确的绝对路径
    save_folder = os.path.join(current_dir, save_folder)
    print(f"📁 修正后的绝对保存目录：{save_folder}")
    # ====== 新增结束 ======
    
    if isinstance(codes, str):
        stock_codes = [codes]
        print(f"📋 单只股票: {codes}")
    
    
    
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
    
    for code in tqdm(stock_codes, desc="查询进度",mininterval=0.01,ncols=100,colour=None):
    
    
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
        
        # 数据处理（如需保留）
        result = result.applymap(lambda x: pd.to_numeric(x, errors='ignore'))
        result = result.round(2)
        
        print(result.head())
        
        # 保存文件
        if save_to_csv:
            os.makedirs(save_folder, exist_ok=True)

            freq_upper = frequency.upper()
            filename = f"stock_data_{freq_upper}.csv"
            filepath = os.path.join(save_folder, filename)
            
            print(f"🔒 目标文件：{filepath}")
            print(f"🔍 绝对路径：{os.path.abspath(filepath)}")


            # 执行保存
            result.to_csv(filepath, index=False, encoding='utf-8-sig')
            print(f"💾 已保存")

            # 验证
            if os.path.exists(filepath):
                print(f"✅ 文件存在")
                actual_size = os.path.getsize(filepath)
                print(f"✅ 文件大小：{actual_size} 字节")
                
                try:
                    with open(filepath, 'r', encoding='utf-8-sig') as f:
                        line_count = sum(1 for _ in f)
                    print(f"✅ 文件行数：{line_count}")
                except Exception as e:
                    print(f"⚠️  读取行数出错：{e}")
            else:
                print(f"❌ 文件不存在")
                return result

            # 最终信息
            file_size_mb = actual_size / 1024 / 1024
            print(f"📏 文件大小: {file_size_mb:.2f} MB")
            print(f"📊 数据行数: {len(result):,} 行")
            print(f"📈 股票数: {result['code'].nunique()}")


    return result
    
   

if __name__ == "__main__":
    
    
    
    from hcp_stocklist import  hcp_stocklist
    
    codes= hcp_stocklist[:]
    


  
    # print(f"总共 {len(codes[:])} 只股票...")
    
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
        days=300,  # 约7年数据
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
        days=30,  # 约7年数据
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
        days=10,  # 约7年数据
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


