def calculate_growth_pure(df_all, yoy_threshold=0.10, qoq_threshold=0.05):
    """
    纯向量化版本：完全利用DataFrame操作，无显式循环
    这才是DataFrame的精髓！
    """
    import pandas as pd
    import numpy as np
    
    print("🔍 开始向量化增长计算...")
    
    # 1. 基本清洗和排序
    df = df_all[df_all['netProfit'] != 0].copy()
    print(f"  剔除净利润为0后剩余: {len(df)} 条记录")
    
    if len(df) == 0:
        print("❌ 无有效数据")
        return pd.DataFrame()
    
    df = df.sort_values(['code', 'query_year', 'query_quarter'])
    
    # 2. 创建季度编号，便于计算
    df['qtr_num'] = df['query_year'] * 10 + df['query_quarter']
    
    # 3. 使用shift获取前值（关键！这才是DataFrame精髓）
    print("  开始向量化计算...")
    
    # 为每个股票组内计算
    df['profit_prev'] = df.groupby('code')['netProfit'].shift(1)  # 上季度累计
    df['profit_prev_year'] = df.groupby('code')['netProfit'].shift(4)  # 去年同期累计
    
    # 4. 计算单季利润
    df['single_profit'] = df['netProfit'] - df['profit_prev']
    # 去年同期单季 = 去年同期累计 - 去年上季度累计
    df['single_profit_prev_year'] = df['profit_prev_year'] - df.groupby('code')['netProfit'].shift(5)
    
    # 5. 标记最新季度
    latest_idx = df.groupby('code').tail(1).index
    latest_df = df.loc[latest_idx].copy()
    print(f"  找到 {len(latest_df)} 只股票的最新季度数据")
    
    # 6. 计算增长率（向量化）
    latest_df['yoy'] = np.where(
        latest_df['profit_prev_year'] != 0,
        (latest_df['netProfit'] - latest_df['profit_prev_year']) / abs(latest_df['profit_prev_year']),
        np.nan
    )
    
    latest_df['qoq'] = np.where(
        latest_df['single_profit_prev_year'] != 0,
        (latest_df['single_profit'] - latest_df['single_profit_prev_year']) / abs(latest_df['single_profit_prev_year']),
        np.nan
    )
    
    # 7. 筛选有效数据
    valid_mask = latest_df['yoy'].notna() & latest_df['qoq'].notna()
    valid_df = latest_df[valid_mask].copy()
    print(f"  有效数据: {len(valid_df)} 只（有完整的同比环比数据）")
    
    # 8. 应用增长阈值筛选
    growth_mask = (valid_df['yoy'] >= yoy_threshold) & (valid_df['qoq'] >= qoq_threshold)
    result_df = valid_df[growth_mask].copy()
    
    # 9. 整理输出格式
    if len(result_df) > 0:
        result_df = result_df.rename(columns={
            'code': 'stock_code',
            'query_year': 'year',
            'query_quarter': 'quarter',
            'netProfit': 'net_profit'
        })
        # 确保数据类型正确
        result_df['year'] = result_df['year'].astype(int)
        result_df['quarter'] = result_df['quarter'].astype(int)
        result_df['yoy_growth'] = result_df['yoy']
        result_df['qoq_growth'] = result_df['qoq']
        
        result_df = result_df[['stock_code', 'year', 'quarter', 'net_profit', 'yoy_growth', 'qoq_growth']]
        
        # 格式化百分比显示
        result_df['yoy_growth_pct'] = result_df['yoy_growth'].apply(lambda x: f"{x:.2%}")
        result_df['qoq_growth_pct'] = result_df['qoq_growth'].apply(lambda x: f"{x:.2%}")
    
    print(f"✅ 向量化计算完成：{len(result_df)} 只股票符合增长条件")
    
    # 打印详细统计
    if len(valid_df) > 0:
        invalid_count = len(latest_df) - len(valid_df)
        print(f"  统计: {len(latest_df)}只股票 → {invalid_count}只数据不完整 → {len(valid_df)}只有效 → {len(result_df)}只符合条件")
    
    return result_df


if __name__ == "__main__":
    
    print("calculate_growth_pure_vectorized模块加载成功")
    print("使用方法：from calculate_growth_pure_vectorized import calculate_growth_pure_vectorized")