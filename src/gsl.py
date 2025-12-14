import os
import time
import pandas as pd
import baostock as bs
from typing import List
from datetime import datetime
from gtd import get_trade_date

def get_stock_codes(date: str) -> List[str]:   
    target_date=date
    print(f"📅 目标交易日: {target_date}")
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
    prefix_mask = result['code'].str.startswith(('sh.60', 'sh.68', 'sz.00', 'sz.30'))
    st_mask = ~result['code'].str.contains('ST') #剔除ST
    mask = prefix_mask & st_mask
    fullstock_codes = result.loc[mask, 'code'].tolist() #把代码列转成列表形式
    total_time = time.time() - start_time
    print("=" * 50)
    print(f"📊 结果统计:")
    print(f"   原始数据: {len(result):,} 条")
    print(f"   筛选后: {len(fullstock_codes):,} 只A股非ST股票")
    print(f"   总耗时: {total_time:.1f}秒")
    if len(fullstock_codes) > 5:
        print(f"   示例: {fullstock_codes[:5]}")
    else:
        print(f"   全部: {fullstock_codes}")
    print("=" * 50)
    
    current_dir = os.path.dirname(os.path.abspath(__file__)) # 获取当前目录
    py_path = os.path.join(current_dir, "fulsl.py")
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
        print(f"🎉 完成! 共获取 {len(codes):,} 只股票")        
    except Exception as e:
        print(f"❌ 程序运行出错: {e}")        
    finally:
        bs.logout()
        print("👋 已登出baostock")
    

# 运行结果        
# (myenv) PS D:\py> & D:/py/myenv/Scripts/python.exe d:/py/myenv/stockfun/1.py
# 🔐 登录baostock...
# login success!
# 开始日期为： 2025-11-17
# 当前日期为： 2025-12-08
# 当前时间为 2025-12-08-21-40
# 获取交易日错误 0
# 获取交易日状态 success
# 获取22个日期，其中16个为交易日
# 获取的交易日列表最后5个日期： ['2025-12-01', '2025-12-02', '2025-12-03', '2025-12-04', '2025-12-05']
# 函数的返回值为，获取的交易日列表最后1个： ['2025-12-05']
# 📅 目标交易日: 2025-12-08
# ==================================================
# 🕐 [21:40:53] 开始获取数据...
# ✅ [21:41:54] 下载完成 (61.8秒)，共 5666 条数据
# ==================================================
# 📊 结果统计:
#    原始数据: 5,666 条
#    筛选后: 5,170 只A股非ST股票
#    总耗时: 61.8秒
#    示例: ['sh.600000', 'sh.600004', 'sh.600006', 'sh.600007', 'sh.600008']
# ==================================================
# 💾 文件已保存到: full_stockcode.py
# 🎉 完成! 共获取 5,170 只股票
# logout success!
# 👋 已登出baostock

# (myenv) PS D:\py> & D:/py/myenv/Scripts/python.exe d:/py/myenv/stockfun/f2.py
# login success!
# login respond error_code:0
# login respond  error_msg:success
# query_all_stock respond error_code:0
# query_all_stock respond  error_msg:success
#            code tradeStatus   code_name
# 0     sh.000001           1      上证综合指数
# 1     sh.000002           1      上证A股指数
# 2     sh.000003           1      上证B股指数
# 3     sh.000004           1     上证工业类指数
# 4     sh.000005           1     上证商业类指数
# ...         ...         ...         ...
# 5673  sz.399994           1  中证信息安全主题指数
# 5674  sz.399995           1    中证基建工程指数
# 5675  sz.399996           1    中证智能家居指数
# 5676  sz.399997           1      中证白酒指数
# 5677  sz.399998           1      中证煤炭指数

# [5678 rows x 3 columns]
# logout success!

        
