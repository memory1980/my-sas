import pandas as pd
import os
from datetime import datetime

def analyze_stocks_single_period(
    codes,                    # 股票代码列表（必须）
    data_dir="../data",       # 数据目录（相对路径）
    output_dir="../output",   # 输出目录（相对路径）
    period='M',               # 分析周期：'M'月线，'W'周线，'D'日线（默认'M'）
    threshold=5,              # turn倍数阈值，默认5倍
    window_n=None,            # 前后分析周期数
    base_n=None,              # 基准点数量
    close_n=None,             # close分析窗口
    save_csv=True             # 是否保存CSV
):
    """
    分析单周期的股票turn模式
    
    项目结构：
        my-sas/
        ├── src/      # 代码目录
        ├── data/     # 数据目录
        └── output/   # 输出目录
    """
    
    # 周期默认参数配置
    period_config = {
        'M': {'name': '月线', 'window_n': 10, 'base_n': 10, 'close_n': 20, 'filename': 'stock_data_M.csv'},
        'W': {'name': '周线', 'window_n': 15, 'base_n': 20, 'close_n': 30, 'filename': 'stock_data_W.csv'},
        'D': {'name': '日线', 'window_n': 30, 'base_n': 50, 'close_n': 60, 'filename': 'stock_data_D.csv'}
    }
    
    if period not in period_config:
        raise ValueError(f"period必须是 'M','W','D' 之一")
    
    config = period_config[period]
    window_n = window_n or config['window_n']
    base_n = base_n or config['base_n']
    close_n = close_n or config['close_n']
    filename = config['filename']
    
    # 构建文件路径
    file_path = os.path.join(data_dir, filename)
    abs_file_path = os.path.abspath(file_path)
    
    print("="*70)
    print(f"📊 {config['name']}数据分析")
    print("="*70)
    print(f"📋 股票数量: {len(codes)}")
    print(f"📁 数据文件: {abs_file_path}")
    print(f"📂 输出目录: {os.path.abspath(output_dir)}")
    print(f"🎯 turn阈值: {threshold}倍")
    print(f"📈 分析参数: 前后{window_n}周期, 取{base_n}个基准点, close窗口{close_n}")
    print("="*70)
    
    # 检查数据文件
    if not os.path.exists(abs_file_path):
        print(f"❌ 数据文件不存在: {abs_file_path}")
        return pd.DataFrame()
    
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 加载数据
    try:
        data = pd.read_csv(abs_file_path, encoding='utf-8-sig')
        print(f"✅ 成功加载: {len(data)} 行数据")
    except Exception as e:
        print(f"❌ 加载文件失败: {e}")
        return pd.DataFrame()
    
    # 分析数据
    results = []
    
    for code in codes:
        # 筛选股票数据
        df = data[data['code'] == code].sort_values('date')
        
        if len(df) < base_n:
            print(f"  ⚠️  {code}: 数据不足（需要{base_n}，实际{len(df)}）")
            continue
        
        print(f"  📈 分析: {code} ({len(df)}个数据点)")
        
        # 取最小的base_n个turn作为基准点
        base_points = df.nsmallest(base_n, 'turn')
        
        for _, row in base_points.iterrows():
            pos = df.index.get_loc(row.name)
            base_turn = row['turn']
            
            # 高turn分析
            window = df.iloc[max(0, pos-window_n):min(len(df), pos+window_n+1)]
            high_count = (window['turn'] >= base_turn * threshold).sum()
            
            # 最低close分析
            close_window = df.iloc[max(0, pos-close_n):min(len(df), pos+close_n+1)]
            if not close_window.empty:
                min_close = close_window.loc[close_window['close'].idxmin()]
                
                results.append({
                    '股票代码': code,
                    '周期': config['name'],
                    '基准日期': row['date'],
                    '基准turn': round(base_turn, 6),
                    '高turn数量': high_count,
                    '最低close日期': min_close['date'],
                    '最低close价格': round(min_close['close'], 4)
                })
    
    if not results:
        print("❌ 未分析到任何有效数据")
        return pd.DataFrame()
    
    # 创建结果DataFrame
    results_df = pd.DataFrame(results)
    
    # 保存结果到output目录
    if save_csv:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"{config['name']}_分析_{timestamp}.csv"
        output_file = os.path.join(output_dir, output_filename)
        results_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n💾 分析结果已保存: {output_file}")
    
    # 显示统计
    print(f"\n✅ 分析完成:")
    print(f"  总记录数: {len(results_df)}")
    print(f"  分析股票: {results_df['股票代码'].nunique()}只")
    print(f"  基准点总数: {len(results_df)}")
    
    return results_df


def analyze_all_periods(codes, data_dir="../data", output_dir="../output", **kwargs):
    """
    一键分析所有周期
    
    输出文件结构：
        output/
        ├── 月线_分析_YYYYMMDD_HHMMSS.csv
        ├── 周线_分析_YYYYMMDD_HHMMSS.csv
        ├── 日线_分析_YYYYMMDD_HHMMSS.csv
        └── 所有周期_汇总_YYYYMMDD_HHMMSS.csv
    """
    
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    all_results = []
    
    for period in ['M', 'W', 'D']:
        print(f"\n{'='*60}")
        print(f"分析 {period} 线数据")
        print(f"{'='*60}")
        
        results = analyze_stocks_single_period(
            codes=codes,
            data_dir=data_dir,
            output_dir=output_dir,
            period=period,
            save_csv=False,  # 先不单独保存
            **kwargs
        )
        
        if not results.empty:
            all_results.append(results)
    
    # 保存汇总文件
    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        summary_file = os.path.join(output_dir, f"所有周期_汇总_{timestamp}.csv")
        final_df.to_csv(summary_file, index=False, encoding='utf-8-sig')
        print(f"\n💾 所有周期汇总已保存: {summary_file}")
        
        return final_df
    
    return pd.DataFrame()


# ==================== 使用示例 ====================

if __name__ == "__main__":
    
    # 创建目录结构（如果不存在）
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(project_root, "data")
    output_dir = os.path.join(project_root, "output")
    
    print(f"📁 项目根目录: {project_root}")
    print(f"📂 数据目录: {data_dir}")
    print(f"📂 输出目录: {output_dir}")
    
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 股票列表
    stock_list = ['sh.600000', 'sh.600004']
    
    print("\n" + "="*70)
    print("📈 股票turn模式分析")
    print("="*70)
    
    # 示例1：分析月线
    print("\n示例1：分析月线")
    monthly_results = analyze_stocks_single_period(
        codes=stock_list,
        data_dir="../data",
        output_dir="../output",
        period='M'
    )
    
    # 示例2：一键分析所有周期
    print("\n\n示例2：一键分析所有周期")
    all_results = analyze_all_periods(
        codes=stock_list,
        data_dir="../data",
        output_dir="../output"
    )
    
    if not all_results.empty:
        print(f"\n📊 所有周期分析完成:")
        print(f"  总记录数: {len(all_results)}")
        print("  按周期统计:")
        print(all_results.groupby('周期').size())
        
        # 显示输出目录内容
        print(f"\n📂 输出目录内容:")
        for file in os.listdir(output_dir):
            if file.endswith('.csv'):
                file_path = os.path.join(output_dir, file)
                size_kb = os.path.getsize(file_path) / 1024
                print(f"  📄 {file} ({size_kb:.1f} KB)")