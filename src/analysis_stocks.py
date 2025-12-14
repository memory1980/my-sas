import os

import pandas as pd

import time 

from datetime import datetime

import pprint


def analysis_stocks(
    codes,                    # 股票代码列表（必须）
    input_dir=None,           # 数据目录（可选，默认使用项目根目录/data）
    period='M',               # 分析周期：'M'月线，'W'周线，'D'日线（默认'M'）
    threshold=5,              # turn倍数阈值，默认5倍
    window_n=5,            # 前后分析周期数
    base_n=5,              # 基准点数量

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
        terminal_width = 80  # 如果获取失败，使用一个默认值，例如80

    my_data = {...}  # 你的数据
    pprint.pprint(my_data, width=terminal_width)



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
  
    


    data['date'] = pd.to_datetime(data['date'])

    print(f"共计{len(codes)}只股票")
    
    #创建并初始化结果变量
    
    rs_data=pd.DataFrame()


    for code in codes:
        # 筛选股票数据
        df = data[data['code'] == code]
        
        temp_data = pd.DataFrame()  # 每次循环都重置为空！
        

        latest_df = df.nlargest(1, 'date')
    
        temp_data = pd.concat([temp_data, latest_df], ignore_index=True) 
        
         
        print('\n')
        
        tf = df.sort_values('turn')
        
        # print(tf)
        
        smallest_tf = tf.nsmallest(window_n, 'turn')

        temp_data = pd.concat([temp_data, smallest_tf], ignore_index=True) 


        cf = df.sort_values('close')
   
        smallest_cf = tf.nsmallest(window_n, 'close')
   
        temp_data = pd.concat([temp_data, smallest_cf], ignore_index=True) 
      
        hf = df.sort_values('high')
        
        smallest_hf = hf.nlargest(window_n, 'high')
        
        print('777',hf.info)
        
        
        print('\n')
        
        temp_data = pd.concat([temp_data, smallest_hf], ignore_index=True) 
        
        print('888',temp_data)
        
        print('\n')
    
        
        print('\n')
        
        rs_data =pd.concat([rs_data, temp_data], ignore_index=True) 
     
  
        
    print(rs_data.info)
        
    
        
    print('\n')
    
    
    # 保存提取的数据文件
   
    timestamp = datetime.now().strftime("%Y%m%d%H%M")
    
    rs_data_file = os.path.join(output_dir, f"{config['name']}详细结果_{timestamp}.csv")
    rs_data.to_csv(rs_data_file, index=True, encoding='utf-8-sig')
    
    print(f"\n💾 详细结果已保存: {rs_data_file}")
    print(f"📊 总行数: {len(rs_data)} 行")

        
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
    from ghgs import high_growth_stocks
    
    stock_list=high_growth_stocks[:]
    
    # 3. 分析示例 - 直接修改这里的参数
    
    print("\n" + "="*70)
    print("📈 开始分析 - 请在下方修改参数")
    print("="*70)
    
    stock_list = high_growth_stocks[:]  # 测试3只股票，可以修改为更多
    
    # 在这里直接修改参数
    results= analysis_stocks(
        codes=stock_list,
        period='D',           # 'M'月线, 'W'周线, 'D'日线
        threshold=3,          # 调整阈值倍数
        window_n=60,          # 调整他们后查找范围
        base_n=1,            # 调整基准点数量
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
