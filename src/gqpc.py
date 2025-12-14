import pandas as pd
from datetime import datetime
import baostock as bs
import os
from typing import List, Tuple, Optional 

from tqdm import tqdm

from gtd import get_trade_date

from gqp import generate_quarter_params

from cgp import calculate_growth_pure



def get_query_profit_codes(
    stock_codes: List[str] = None,
    quarter_params: List[Tuple[int, int]] = None,
    show_progress: bool = True,
    yoy_threshold: float = 0.10,
    qoq_threshold: float = 0.05
) -> Optional[List[str]]:
    """
    查询股票盈利数据并筛选符合增长阈值的股票
    """
    # 如果没有提供股票代码，使用默认清单
    if stock_codes is None:
   
       stock_codes  = ['sh.600000', 'sh.600004']
 
    print(f"📋 使用默认股票清单（共{len(stock_codes)}只股票）")
    
    # 季度参数处理：要么用传入的，要么生成7个
    if quarter_params is None:
        trading_date = get_trade_date(-2)  # 获取交易日
        quarter_params = generate_quarter_params(trading_date, 7)
    
    # 测试用，只处理前50只
    stock_codes = stock_codes[:]

    if lg.error_code != '0':
        print(f'登录失败: {lg.error_msg}')
        return []
    
    df_list = []
    total_tasks = len(stock_codes) * len(quarter_params)
    
    print(f"总查询任务: {total_tasks} (股票:{len(stock_codes)} × 季度:{len(quarter_params)})")
    
    # 创建进度条
    if show_progress:
        pbar = tqdm(total=total_tasks, desc="查询盈利数据", ncols=100, unit="次")
    
    for code in stock_codes:
        for year_q, quarter_q in quarter_params:
            rs_profit = bs.query_profit_data(
                code=code,
                year=year_q,
                quarter=quarter_q
            )
            
            profit_list = []
            while (rs_profit.error_code == '0') & rs_profit.next():
                profit_list.append(rs_profit.get_row_data())
            
            if profit_list:
                result_profit = pd.DataFrame(profit_list, columns=rs_profit.fields)
                result_profit['stock_code'] = code
                result_profit['query_year'] = year_q
                result_profit['query_quarter'] = quarter_q
                df_list.append(result_profit)
            elif rs_profit.error_code != '0':
                print(f"❌ {code} {year_q}Q{quarter_q}: {rs_profit.error_msg}")
            
            if show_progress:
                pbar.update(1)
                pbar.set_postfix({
                    "当前": f"{code}",
                    "季度": f"{year_q}Q{quarter_q}",
                    "已获取": len(df_list)
                })
    
    if show_progress:
        pbar.close()

    print(f"\n✅ 查询完成！")
    print(f"   成功查询数: {len(df_list)}")
    print(f"   总任务数: {total_tasks}")

    if not df_list:
        print("❌ 未获取到任何数据")
        return []
    
    # 合并所有数据
    df_all = pd.concat(df_list, ignore_index=True)
    
    # 选择需要的列
    df_all = df_all[['code', 'statDate', 'netProfit', 'query_year', 'query_quarter']]
    
    print(f"\n📊 数据汇总:")
    print(f"   总记录数: {len(df_all)} 条")
    print(f"   股票数量: {df_all['code'].nunique()} 只")
    print(f"   查询季度范围: {df_all['query_year'].min()}Q{df_all['query_quarter'].min()} 到 {df_all['query_year'].max()}Q{df_all['query_quarter'].max()}")
    print(f"   每个股票平均季度数: {len(df_all) / df_all['code'].nunique():.1f}")
    
    # 转换数值列
    df_all['netProfit'] = pd.to_numeric(df_all['netProfit'], errors='coerce')
    
    # 检查数据连续性
    print("\n🔍 检查数据连续性...")
    stock_stats = []
    for code, group in df_all.groupby('code'):
        if len(group) >= 6:
            # 创建季度编号检查连续性
            group = group.sort_values(['query_year', 'query_quarter'])
            group['qtr_num'] = group['query_year'] * 10 + group['query_quarter']
            diff = group['qtr_num'].diff().iloc[1:]
            is_continuous = ((diff == 1) | (diff == 9)).all()
            stock_stats.append((code, len(group), is_continuous))
    
    continuous_stocks = [s for s in stock_stats if s[2]]
    print(f"  数据完整且连续的股票: {len(continuous_stocks)} 只")
    
    # ================= 阈值筛选 =================
    print(f"\n🔍 开始筛选：同比≥{yoy_threshold:.1%}，环比≥{qoq_threshold:.1%}")
    
    # 使用向量化计算方法
    growth_df = calculate_growth_pure(df_all, yoy_threshold, qoq_threshold)
    
    if len(growth_df) == 0:
        print("🎯 没有股票符合增长阈值")
        return []
    
    # 输出筛选结果
    print(f"\n🎯 符合增长阈值的股票（共{len(growth_df)}只）:")
    for _, row in growth_df.iterrows():
        print(f"  {row['stock_code']}: {row['year']}Q{row['quarter']} - 同比: {row['yoy_growth']:.2%}, 环比: {row['qoq_growth']:.2%}")
    
        # ================= 保存结果 =================
    currenttime = datetime.now().strftime("%Y%m%d_%H") 
    filenamecsv = f"high_growth_stocks_{currenttime}.csv"
    filenamepy = f"high_growth_stocks_{currenttime}.py"

    # 1. 保存完整数据为CSV - 按同比增长从高到低排序
    folder_name2 = "data"
    os.makedirs(folder_name2, exist_ok=True)
    csv_path2 = os.path.join(folder_name2, filenamecsv)
    
    # 对结果按同比增长率降序排序
    growth_df_sorted = growth_df.sort_values('yoy_growth', ascending=False).reset_index(drop=True)
    growth_df_sorted.to_csv(csv_path2, index=False, encoding='utf-8-sig')
    print(f"✅ 完整数据保存到: {csv_path2}")
    
    # 2. 保存股票清单为Python文件，包含更多数据
    folder_name3 = "src"
    os.makedirs(folder_name3, exist_ok=True)
    
    # 使用排序后的数据
    stock_list = growth_df_sorted['stock_code'].tolist()
    
    # 创建新的详细数据文件
    filename = "high_growth_stock_list.py"
    filepath = f"src/{filename}"

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(f"# 自动生成的高增长股票清单（{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}）\n")
        f.write(f"# 筛选条件：同比≥{yoy_threshold:.1%}，环比≥{qoq_threshold:.1%}\n\n")
        
        # 写入详细的股票数据
        f.write("high_growth_stocks_details = [\n")
        for _, row in growth_df_sorted.iterrows():
            f.write(f"    {{\n")
            f.write(f"        'stock_code': '{row['stock_code']}',\n")
            f.write(f"        'year': {row['year']},\n")
            f.write(f"        'quarter': {row['quarter']},\n")
            f.write(f"        'yoy_growth': {row['yoy_growth']:.4f},  # 同比增长 {row['yoy_growth']:.2%}\n")
            f.write(f"        'qoq_growth': {row['qoq_growth']:.4f},  # 环比增长 {row['qoq_growth']:.2%}\n")
            f.write(f"        'net_profit': {row['net_profit'] if 'net_profit' in row else 'N/A'},\n")
            f.write(f"        'stat_date': '{row['stat_date'] if 'stat_date' in row else ''}',\n")
            f.write(f"    }},\n")
        f.write("]\n\n")
        
        # 仍然保留简单的股票代码列表
        stocks_str = ', '.join([f"'{stock}'" for stock in stock_list])
        f.write(f"high_growth_stocks = [{stocks_str}]  # {len(stock_list)}只股票，按同比增长率降序排列\n")

    print(f"已保存详细数据到 {filepath}")
    
    # 返回股票列表
    return stock_list
    
    
    
    
    
    
# ========== 主程序 ==========
if __name__ == "__main__":
    
    from hcpsl import  hcp_stocklist
    
    codes=hcp_stocklist[:]
    
    print("=" * 60)
    print("             高增长股票筛选程序（向量化版本）")
    print("=" * 60)
    
    lg=bs.login()
    # 记录开始时间
    start_time = datetime.now()
    
    # 方案：外部生成精确的6个季度
    trading_date = get_trade_date()
    quarter_params = generate_quarter_params(trading_date, 7)
    
    print(f"📅 当前交易日: {trading_date}")
    print(f"📊 查询季度: {quarter_params}")
    
    print("\n" + "-" * 60)
    
    # 传入这6个季度进行查询
    high_growth_stocklist = get_query_profit_codes(
        quarter_params=quarter_params,
    
        stock_codes=codes,

        show_progress=True,
        yoy_threshold = 0.20,
        qoq_threshold= 0.1
        
        
    )
    
    # 记录结束时间
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print("\n" + "=" * 60)
    if high_growth_stocklist:
        print(f"✅ 程序完成！")
        print(f"   耗时: {duration:.1f} 秒")
        print(f"   筛选结果: {len(high_growth_stocklist)} 只高增长股票")
        print(f"   股票列表: {high_growth_stocklist[:10]}{'...' if len(high_growth_stocklist) > 10 else ''}")
    else:
        print("❌ 程序完成，但未找到符合条件的股票")
        print(f"   耗时: {duration:.1f} 秒")
    print("=" * 60)
    
    lg=bs.logout()