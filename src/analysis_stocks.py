import os
import pandas as pd
import time 
from datetime import datetime
from tqdm.auto import tqdm
import pprint


def analysis_stocks(
    stockcodes,                    # 股票代码列表（必须）
    input_dir=None,           # 数据目录（可选，默认使用项目根目录/data）
    period='M',               # 分析周期：'M'月线，'W'周线，'D'日线（默认'M'）
    threshold=5,              # turn倍数阈值，默认5倍
    window_n=5,               # 前后分析周期数
    base_n=5,                 # 基准点数量
    save_csv=True,            # 是否保存CSV
    output_dir=None           # 输出目录（可选）
):
    """
    分析单周期的股票turn模式
    每个基准点独立展示，后面跟着找到的高turn点和最低close点
    """

    try:
        terminal_width = os.get_terminal_size().columns
    except OSError:
        terminal_width = 80

    # 周期默认参数配置
    period_config = {
        'M': {'name': '月线', 'window_n': 10, 'base_n': 10, 'filename': 'stock_data_M.csv'},
        'W': {'name': '周线', 'window_n': 15, 'base_n': 20, 'filename': 'stock_data_W.csv'},
        'D': {'name': '日线', 'window_n': 30, 'base_n': 50, 'filename': 'stock_data_D.csv'}
    }
    
    if period not in period_config:
        raise ValueError(f"period必须是 'M','W','D' 之一")
    
    config = period_config[period]
    window_n = window_n or config['window_n']
    base_n = base_n or config['base_n']

    filename = config['filename']
    
    # 获取项目根目录
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # 设置数据目录
    if input_dir is None:
        data_dir = os.path.join(project_root, "data")
    elif os.path.isabs(input_dir):
        data_dir = input_dir
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.abspath(os.path.join(script_dir, input_dir))
    
    # 构建文件路径
    file_path = os.path.join(data_dir, filename)
    abs_file_path = os.path.abspath(file_path)
    
    # 设置输出目录
    if output_dir is None:
        output_dir = os.path.join(project_root, "anars")
    elif not os.path.isabs(output_dir):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.abspath(os.path.join(script_dir, output_dir))
    
    print("="*50)
    print(f"📊 {config['name']}数据分析")
    print("="*50)
    print(f"📋 股票数量: {len(stockcodes)}")
    print(f"📁 数据文件: {abs_file_path}")
    print(f"📂 输出目录: {output_dir}")
    print(f"🎯 turn阈值: {threshold}倍")
    print(f"📅 window_n: {window_n}")
    print(f"📊 base_n: {base_n}")
    print("="*50)

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
    
    pd.set_option('display.max_colwidth',None) # None 表示不限制宽度
    
    data = data.apply(pd.to_numeric, errors='ignore')  

   

    # 保留两位小数
    data = data.round(2)        
    

    
    # print(data.tail(1))
    
    # 确保日期列为datetime类型
    data['date'] = pd.to_datetime(data['date'])
    
    print(f"共计{len(stockcodes)}只股票")
    
    # 创建并初始化结果变量
    all_results = []
    
    
    tqdm._instances.clear()
    
    # 使用tqdm添加进度条
    for stock_code in tqdm(stockcodes, desc="分析进度", unit="只"):
        try:
            # 筛选当前股票的数据
            df = data[data['code'] == stock_code]
            
            
            if df.empty:
                print(f"\n⚠ 股票 {stock_code} 无数据")
                continue
            
            # 获取最新的数据点
            # latest_df = df.nlargest(1, 'date')
            
            # print("11111",latest_df)
            
                        # 获取最新的数据点
            latest_df = df.nlargest(2, 'date')
            
            # print("22222",latest_df)
            
            # print('33333',latest_df[-2:-1])
            
            # print('4444',latest_df[-1:])
            
            # # 按turn排序，获取最大的window_n个turn值，降序排列
            # df_sorted_by_turn = df.sort_values('turn', ascending=False)
            
            # max_turns = df_sorted_by_turn.head(window_n)
            
                      
            # print("22222",max_turns.head())
            
            temp_result=latest_df
            
            
            # # 合并结果
            # temp_result = pd.concat([latest_df, max_turns], ignore_index=True)
            
            # print("33333",temp_result)
            
            # # 获取具体的数值
            # latest_turn_value = latest_df['turn'].iloc[0]  # 或 .values[0]
            
            # max_turn_value = max_turns['turn'].max()
            
            # print("44444",max_turn_value)
            
            # if latest_df.turn[-1:]>= latest_df.turn[-2:-1]  *threshold:
            
            print(latest_df.iloc[0]['turn'])
            
            print(latest_df.iloc[1]['turn'])
            
            
                
            if latest_df.iloc[0]['turn']<=latest_df.iloc[1]['turn'] *threshold:
                
                
                all_results.append(temp_result)               
            
                print("55555",all_results)

            else: 
        
                temp_result = pd.DataFrame()
                
                 
            
            print("666",all_results)

            # print("66666",temp_result)
            
            
            
            
        except Exception as e:
            print(f"\n❌ 股票 {stock_code} 分析出错: {e}")
            continue
    
    # 合并所有结果
    if all_results:
        rs_data = pd.concat(all_results, ignore_index=True)
    else:
        rs_data = pd.DataFrame()
    
    # 安全地转换数值列
    def safe_numeric_conversion(df):
        """安全地转换数值列"""
        df_converted = df.copy()
        for col in df_converted.columns:
            # 跳过日期列和代码列
            if col in ['date', 'code', 'analysis_stock']:
                continue
            
            try:
                # 尝试转换为数值
                converted = pd.to_numeric(df_converted[col], errors='coerce')
                # 如果大部分能转换，则使用转换后的值
                if converted.notna().sum() / len(df_converted) > 0.5:
                    df_converted[col] = converted
            except:
                pass
        return df_converted
    
    rs_data = safe_numeric_conversion(rs_data)
    
    # 对数值列保留两位小数
    numeric_cols = rs_data.select_dtypes(include=['float64', 'float32', 'int64', 'int32']).columns
    rs_data[numeric_cols] = rs_data[numeric_cols].round(2)
    
    print("\n" + "="*50)
    print(f"📊 分析结果汇总")
    print("="*50)
    print(f"总数据行数: {len(rs_data)}")
    print(f"分析股票数: {rs_data['analysis_stock'].nunique() if 'analysis_stock' in rs_data.columns else 0}")
    print("\n前10行数据:")
    print(rs_data.head())
    print("="*50)
    
    # 保存结果
    if save_csv and not rs_data.empty:
        timestamp = datetime.now().strftime("%Y%m%d")
        rs_data_file = os.path.join(output_dir, f"{config['name']}详细结果_{timestamp}.csv")
        
        # 确保输出目录存在
        os.makedirs(os.path.dirname(rs_data_file), exist_ok=True)
        
        rs_data.to_csv(rs_data_file, index=False, encoding='utf-8-sig')
        print(f"💾 详细结果已保存: {rs_data_file}")
    
    return rs_data


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
    try:
        from high_growth_stock_list import high_growth_stocks
        stock_list = high_growth_stocks[:]  # 使用所有股票
        
   
        print(f"\n📋 股票列表样本（前5只）:")
        print(stock_list[:])
        print(f"总股票数量: {len(stock_list)}")
        
    except ImportError as e:
        print(f"❌ 导入股票列表失败: {e}")
        # 使用测试股票列表
        stock_list = ['sh.600000', 'sh.600004', 'sz.000001', 'sz.000002']
        print(f"📋 使用测试股票列表: {stock_list}")
    
    print("\n" + "="*50)
    print("📈 开始分析 - 月线数据")
    print("="*50)
    
    # 分析月线数据
    # try:
    #     results1 = analysis_stocks(
    #         stockcodes=stock_list,
    #         period='M',           # 'M'月线
    #         threshold=5,          # 调整阈值倍数
    #         window_n=5,          # 调整查找范围
    #         base_n=1,             # 调整基准点数量
    #         save_csv=True,
    #         output_dir=output_dir
    #     )
    # except Exception as e:
    #     print(f"❌ 月线分析失败: {e}")
    #     import traceback
    #     traceback.print_exc()
    
    # print("\n" + "="*50)
    # print("📈 开始分析 - 周线数据")
    # print("="*50)
    
    # #分析周线数据
    # try:
    #     results2 = analysis_stocks(
    #         stockcodes=stock_list[:],
    #         period='W',           # 'W'周线
    #         threshold=5,          # 调整阈值倍数
    #         window_n=12,          # 调整查找范围
    #         base_n=1,             # 调整基准点数量
    #         save_csv=True,
    #         output_dir=output_dir
    #     )
    # except Exception as e:
    #     print(f"❌ 周线分析失败: {e}")
    #     import traceback
    #     traceback.print_exc()
    
    # print("\n" + "="*50)
    # print("✅ 分析完成")
    # print("="*50)
    
    
    #    分析周线数据
    try:
        results2 = analysis_stocks(
            stockcodes=stock_list,
            period='D',           # 'W'周线
            threshold=2,          # 调整阈值倍数
            window_n=3,          # 调整查找范围
            base_n=1,             # 调整基准点数量
            save_csv=True,
            output_dir=output_dir
        )
    except Exception as e:
        print(f"❌ 周线分析失败: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "="*50)
    print("✅ 分析完成")
    print("="*50)