import os
import time
import pandas as pd
import baostock as bs
from tqdm import tqdm
from typing import List
from datetime import datetime
from GTD import get_trade_date

def get_stock_codes(date: str) -> List[str]:   
    target_date=date
    
    # print(f"📅 目标交易日: {target_date}")
    
    print("=" * 50)   
    start_time = time.time()    # 1. 网络请求阶段（耗时最长，必须显示）
    print(f"🕐 [{datetime.now().strftime('%H:%M:%S')}] 开始获取数据...")  
    
    rs = bs.query_all_stock(day=target_date)  
    if rs.error_code != '0':
        print(f"❌ 获取数据失败: {rs.error_msg}")
        return []
    temp_data = []           # 获取数据
    while rs.next():
        temp_data.append(rs.get_row_data())    
    download_time = time.time() - start_time     # 只显示超过0.5秒的步骤
    if download_time >= 0.5:
        print(f"✅ [{datetime.now().strftime('%H:%M:%S')}] 下载完成 ({download_time:.1f}秒)，共 {len(temp_data)} 条数据")
    
    data_list = []

    
    for row in temp_data:
        data_list.append(row)
        
    result = pd.DataFrame(data_list, columns=rs.fields)
    #只筛选股票代码
    
    prefix_mask = result['code'].str.startswith(('sh.600', 'sh.601', 'sh.603','sh.605','sh.688', 
                                                 
                                                 'sz.000','sz.300','sz.301','sz.302'))
    
    st_mask = ~result['code'].str.contains('ST') #剔除ST
    
    
    
    mask = prefix_mask & st_mask
    fullstock_codes = result.loc[mask, 'code'].tolist() #把代码列转成列表形式
    total_time = time.time() - start_time
    print("*" * 80)
    print(f"📊 结果统计:")
    print(f"   全部股票代码: {len(result):,} 条")
    print(f"   筛选完成: {len(fullstock_codes):,} 只A股非ST股票")
    print(f"   总耗时: {total_time:.1f}秒")
    if len(fullstock_codes) > 5:
        print(f"   示例: {fullstock_codes[:5]}")
    else:
        print(f"   全部: {fullstock_codes}")
    print("*" * 80)
    
    current_dir = os.path.dirname(os.path.abspath(__file__)) # 获取当前目录
    py_path = os.path.join(current_dir, "full_stock_list.py")
    with open(py_path, 'w', encoding='utf-8') as f:     # 保存操作
        f.write(f"# {len(fullstock_codes)}只A股非ST股票 | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"# 数据源交易日: {target_date}\n\n")        
        codes_str = '[' + ', '.join(f"'{code}'" for code in fullstock_codes) + ']'
        f.write(f"full_stockcode = {codes_str}\n")
    print(f"💾 文件已保存到: {os.path.basename(py_path)}")
    
    return fullstock_codes

if __name__ == "__main__":
    try:
        print("🔐 登录baostock...")
        bs.login()     
           
        end_date= get_trade_date()  
        
          
        codes = get_stock_codes(end_date)  # get_trade_date()返回的是一个交易日列表，应用时取最近一个。      
        # print(f"🎉 完成! 共获取 {len(codes):,} 只股票")        
    except Exception as e:
        print(f"❌ 程序运行出错: {e}")        
    finally:
        bs.logout()
        print("👋 已登出baostock")
    


        
