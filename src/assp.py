import pandas as pd
import os
from datetime import datetime

def analyze_stocks_single_period(
    codes,                    # 股票代码列表（必须）
    input_dir=None,           # 数据目录（可选，默认使用项目根目录/data）
    period='M',               # 分析周期：'M'月线，'W'周线，'D'日线（默认'M'）
    threshold=5,              # turn倍数阈值，默认5倍
    window_n=None,            # 前后分析周期数
    base_n=None,              # 基准点数量
    close_n=None,             # close分析窗口
    save_csv=True,            # 是否保存CSV
    output_dir=None           # 输出目录（可选）
):
    """
    分析单周期的股票turn模式
    每个基准点独立展示，后面跟着找到的高turn点和最低close点
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
    
    # 获取项目根目录
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # 设置数据目录（如果未提供，使用项目根目录/data）
    if input_dir is None:
        data_dir = os.path.join(project_root, "data")
    elif os.path.isabs(input_dir):
        data_dir = input_dir
    else:
        # 如果是相对路径，基于脚本位置
        script_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.abspath(os.path.join(script_dir, input_dir))
    
    # 构建文件路径
    file_path = os.path.join(data_dir, filename)
    abs_file_path = os.path.abspath(file_path)
    
    # 设置输出目录（如果未提供，使用项目根目录/anars）
    if output_dir is None:
        output_dir = os.path.join(project_root, "anars")
    elif not os.path.isabs(output_dir):
        # 如果是相对路径，基于脚本位置
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.abspath(os.path.join(script_dir, output_dir))
    
    print("="*70)
    print(f"📊 {config['name']}数据分析")
    print("="*70)
    print(f"📋 股票数量: {len(codes)}")
    print(f"📁 数据文件: {abs_file_path}")
    print(f"📂 输出目录: {output_dir}")
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
    summary_results = []
    all_extracted_data = []
    
    for code in codes:
        # 筛选股票数据
        df = data[data['code'] == code].sort_values('date')
        
        if len(df) < base_n:
            print(f"  ⚠️  {code}: 数据不足（需要{base_n}，实际{len(df)}）")
            continue
        
        print(f"  📈 分析: {code} ({len(df)}个数据点)")
        
        # 取最小的base_n个倒数日期作为基准点
        last_points = df.nlargest(base_n, 'date')
               
        
        base_points = last_points 
        
        print(base_points.head())
        
        base_count = 0
        found_count = 0
        
        for _, row in base_points.iterrows():
            base_count += 1
            pos = df.index.get_loc(row.name)
            base_turn = row['date']
            base_idx = row.name
            
            # 1. 获取高turn点indices
            window = df.iloc[pos:min(len(df), pos+window_n+1)]
            high_turn_mask = window['turn'] >= base_turn * threshold
            high_turn_mask = high_turn_mask & (window.index != base_idx)  # 排除基准点本身
            high_turn_indices = window[high_turn_mask].index.tolist()
            
            # 2. 获取最低close点index
            close_window = df.iloc[pos:min(len(df), pos+close_n+1)]
            min_close_idx = close_window['close'].idxmin() if not close_window.empty else None
            
            # 3. 构建这个基准点的所有相关索引
            # 基准点必须排在第一位，然后是它的高turn点（按时间排序），最后是最低close点
            indices_for_this_base = [base_idx]  # 基准点总是第一个
            
            # 按时间顺序排列高turn点
            if high_turn_indices:
                found_count += 1
                high_turn_dates = data.loc[high_turn_indices, 'date']
                high_turn_indices_sorted = [idx for idx, _ in sorted(zip(high_turn_indices, high_turn_dates), 
                                                                    key=lambda x: x[1])]
                indices_for_this_base.extend(high_turn_indices_sorted)
            
            # 添加最低close点（如果存在且不在列表中）
            if min_close_idx and min_close_idx not in indices_for_this_base:
                indices_for_this_base.append(min_close_idx)
            
            # 4. 提取这个基准点的所有相关数据
            extracted_df = data.loc[indices_for_this_base].copy()
            
            # 5. 添加标记信息
            extracted_df['标记'] = '其他'
            extracted_df.loc[base_idx, '标记'] = '基准点'
            
            for idx in high_turn_indices:
                if idx in extracted_df.index:
                    extracted_df.loc[idx, '标记'] = '高turn点'
            
            if min_close_idx and min_close_idx in extracted_df.index:
                if extracted_df.loc[min_close_idx, '标记'] == '其他':
                    extracted_df.loc[min_close_idx, '标记'] = '最低close点'
            
            # 6. 添加基准信息（所有行都加上相同的基准信息）
            extracted_df['基准日期'] = row['date']
            extracted_df['基准turn值'] = base_turn
            extracted_df['基准股票'] = code
            extracted_df['基准周期'] = config['name']
            extracted_df['阈值倍数'] = threshold
            extracted_df['高turn点数量'] = len(high_turn_indices)
            extracted_df['分析范围(前后)'] = f"±{window_n}周期"
            
            # 7. 添加相对turn倍数（高turn点相对于基准点的倍数）
            for idx in extracted_df.index:
                if idx != base_idx:  # 不是基准点
                    current_turn = extracted_df.loc[idx, 'turn']
                    relative_multiple = current_turn / base_turn if base_turn > 0 else 0
                    extracted_df.loc[idx, '相对于基准turn倍数'] = round(relative_multiple, 2)
                else:
                    extracted_df.loc[idx, '相对于基准turn倍数'] = 1.0
            
            # 8. 保存到列表
            columns_to_keep = [   'code', 'date','high','low','close', 'turn','volume',  '标记', '基准日期', '基准turn值', '基准周期' ]

            all_extracted_data.append(extracted_df[columns_to_keep])
            
            all_extracted_data.append(extracted_df)
       
            # 记录统计信息
            summary_results.append({
                '股票代码': code,
                '周期': config['name'],
                '基准日期': row['date'],
                '基准turn': round(base_turn, 6),
                '阈值倍数': threshold,
                '高turn点数': len(high_turn_indices),
                '高turn点情况': '有' if high_turn_indices else '无',
                '最低close日期': close_window.loc[min_close_idx, 'date'] if min_close_idx else None,
                '最低close价格': round(close_window.loc[min_close_idx, 'close'], 4) if min_close_idx else None,
                '分析范围': f"±{window_n}周期"
            })
        
        print(f"    分析完成: {base_count}个基准点中，{found_count}个找到了高turn点")
    
    # 汇总结果
    summary_df = pd.DataFrame(summary_results)
 
    # 保存提取的数据文件
    if all_extracted_data and save_csv:
        # 直接合并所有数据
        final_extracted_df = pd.concat(all_extracted_data, ignore_index=False)
        
        timestamp = datetime.now().strftime("%Y%m%d%H%M")
        extracted_file = os.path.join(output_dir, f"{config['name']}详细结果_{timestamp}.csv")
        final_extracted_df.to_csv(extracted_file, index=True, encoding='utf-8-sig')
        print(f"\n💾 详细结果已保存: {extracted_file}")
        print(f"📊 总行数: {len(final_extracted_df)} 行")
    elif save_csv and not all_extracted_data:
        print(f"\n⚠️  没有找到任何数据，不生成提取数据文件")
 
    return summary_df, all_extracted_data if all_extracted_data else []

# ==================== 使用示例 ====================

if __name__ == "__main__":
   
    # 1. 先获取项目路径
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    input_dir = os.path.join(project_root, "data")
    output_dir = os.path.join(project_root, "anars")
    
    print(f"📁 项目根目录: {project_root}")
    print(f"📂 默认数据目录: {input_dir}")
    print(f"📂 默认输出目录: {output_dir}")
    
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 2. 导入股票列表
    from high_growth_stock_list import high_growth_stocks
    
    # 3. 分析示例 - 直接修改这里的参数
    print("\n" + "="*70)
    print("📈 开始分析 - 请在下方修改参数")
    print("="*70)
    
    stock_list = high_growth_stocks[:]  # 测试3只股票，可以修改为更多
    
    # 在这里直接修改参数
    results_month, details_month= analyze_stocks_single_period(
        codes=stock_list,
        period='M',           # 'M'月线, 'W'周线, 'D'日线
        threshold=5,          # 调整阈值倍数
        window_n=5,          # 调整前后查找范围
        base_n=1,            # 调整基准点数量
        close_n=5,           # 调整close窗口
        save_csv=True,
        output_dir=output_dir
    )
    
    print("\n" + "="*70)
    print("✅ 分析完成")
    print("="*70)
    print("📝 参数说明:")
    print("   threshold: 阈值倍数，如5表示找5倍以上的turn")
    print("   window_n: 前后查找范围，如12表示前后12个月")
    print("   base_n: 基准点数量，取最小的n个turn值作为基准")
    print("   close_n: 找最低close点的窗口")
    print("="*70)
    
    # results_month, details_month = analyze_stocks_single_period(
    #     codes=stock_list,
    #     period='W',           # 'M'月线, 'W'周线, 'D'日线
    #     threshold=5,          # 调整阈值倍数
    #     window_n=10,          # 调整后查找范围
    #     base_n=10,            # 调整基准点数量
    #     close_n=10,           # 调整close窗口
    #     save_csv=True,
    #     output_dir=output_dir
    # )
    
    
    # print("\n" + "="*70)
    # print("✅ 分析完成")
    # print("="*70)
    # print("📝 参数说明:")
    # print("   threshold: 阈值倍数，如5表示找5倍以上的turn")
    # print("   window_n: 前后查找范围，如12表示前后12个月")
    # print("   base_n: 基准点数量，取最小的n个turn值作为基准")
    # print("   close_n: 找最低close点的窗口")
    # print("="*70)
  
    # results_month, details_month = analyze_stocks_single_period(
    #     codes=stock_list,
    #     period='D',           # 'M'月线, 'W'周线, 'D'日线
    #     threshold=5,          # 调整阈值倍数
    #     window_n=60,          # 调整后查找范围
    #     base_n=10,            # 调整基准点数量
    #     close_n=60,           # 调整close窗口
    #     save_csv=True,
    #     output_dir=output_dir
    # )
    
    
    # print("\n" + "="*70)
    # print("✅ 分析完成")
    # print("="*70)
    # print("📝 参数说明:")
    # print("   threshold: 阈值倍数，如5表示找5倍以上的turn")
    # print("   window_n: 前后查找范围，如12表示前后12个月")
    # print("   base_n: 基准点数量，取最小的n个turn值作为基准")
    # print("   close_n: 找最低close点的窗口")
    # print("="*70)
    