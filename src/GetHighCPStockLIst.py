import os
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Union
import baostock as bs
from tqdm import tqdm




try:
    from progressbar import ProgressBar, Percentage, Bar, Timer
    HAS_PROGRESSBAR = True
except ImportError:
    HAS_PROGRESSBAR = False


def get_high_market_value_stock(
        date: Union[str, int] = 180,
        stock_codes: List[str] = None,
        threshold: float = 100.0,  # 这里已经有默认值100.0了
        delay: float = 0.0001
) -> List[str]:

    hcp_stocklist = [] 



    print(f"🚀 开始市值筛选")
    print(f"   股票数量: {len(stock_codes) if stock_codes else '使用默认列表'} 只")  # ✅ 处理None
    print(f"   市值阈值: {threshold} 亿元")

    # 根据date类型显示不同信息
    if isinstance(date, int):
        query_days = date  # 如果是整数，直接使用
        print(f"   查询周期: 最近{query_days}天")  # 使用新变量
    else:
        query_days = 180  # 默认180天
        print(f"   默认查询日期: {date}")

    print("=" * 70)
    
    # 处理股票列表参数，如果没有则使用默认列表

 


    if stock_codes is None:
        # 使用默认列表
  
        stock_codes = ['sh.600000', 'sh.600004', 'sh.600006']
    
    #### 获取交易日信息 ####
    tradingday = bs.query_trade_dates()


    
    tradingdata_list = []
    while (tradingday.error_code == '0') & tradingday.next():
        # 获取一条记录，将记录合并在一起
        tradingdata_list.append(tradingday.get_row_data())

    # 这里变量名写错了，应该是 tradingdayrs
    tradingdayrs = pd.DataFrame(tradingdata_list, columns=tradingday.fields)


    trading_days = tradingdayrs[tradingdayrs['is_trading_day'] == '1']['calendar_date'].tolist()

    # 取倒数第二个

    tradingdata = trading_days[-2]
    print(f'倒数第二个交易日: {tradingdata}')
    
    end_date = datetime.strptime(tradingdata, '%Y-%m-%d')

    # 2. 减去date天
    start_date = end_date - timedelta(days=query_days)  # ✅ 使用新变量

    # 3. 再转成字串
    start_date_str = start_date.strftime('%Y-%m-%d')

    print(f"开始时间: {start_date_str}")
    print(f"结束时间: {tradingdata}")
    print(f"时间跨度: {query_days}天")  # ✅ 使用新变量
          
        
        
        
    data_list = []
    fields = None

    print(f"查询 {len(stock_codes)} 个股票:")
    
     
    
    
    
    
    for code in tqdm(stock_codes, desc="进度", ncols=100, unit="只"):
        # 查询数据
        rs = bs.query_history_k_data_plus(
            code,
            "date,code,close,volume,turn",
            start_date=start_date_str,
            end_date=tradingdata,
            frequency="d",
            adjustflag="3"
        )
        
        # 获取字段名
        if fields is None and rs.error_code == '0':
            fields = rs.fields
        
        # 收集数据
        while rs.next():
            data_list.append(rs.get_row_data())

    print("查询完成")



    # 处理数据
    if data_list and fields:  # ✅ 同时检查两个条件
        df = pd.DataFrame(data_list, columns=fields)
        # 转换类型
        df['close'] = pd.to_numeric(df['close'])
        df['volume'] = pd.to_numeric(df['volume'])
        df['turn'] = pd.to_numeric(df['turn'])
        
        
        
        # 删除零值
        df = df[(df['turn'] != 0) & (df['volume'] != 0)]
        
        # 取最新一天
        latest_df = df.groupby('code').tail(1).copy()
        
        # 计算市值
        latest_df['market_cap_billion'] = (latest_df['close'] * latest_df['volume'] * 100 / latest_df['turn'] / 1e8).round(2)
        
        # 筛选市值
        
        latest_df = latest_df[latest_df['market_cap_billion'] >= threshold]
        
        print(f"\n结果: {len(latest_df)} 个股票市值≥{threshold}亿")
        print(latest_df[['code', 'close', 'market_cap_billion']])
        
        
        
        hcp_stocklist = latest_df['code'].tolist()  # 正确，调用方法
        #保存文件
        
        current_dir = os.path.dirname(os.path.abspath(__file__)) # 获取当前目录
        py_path = os.path.join(current_dir, "hcp_stocklist.py")

        with open(py_path, 'w', encoding='utf-8') as f:
            f.write(f"# {len(hcp_stocklist)}只股票 | {datetime.now().strftime('%Y-%m-%d')}\n")
            f.write(f"hcp_stocklist = {hcp_stocklist}\n")
        print(f"✅ Python格式保存到: {py_path}")
    return hcp_stocklist
    
  
    

if __name__ == "__main__":
    
    from fulsl import full_stockcode as fsc
    
    codes=fsc[:]
    
    print(f'总共有{len(codes)}只股票，其中前5只是: {", ".join(codes[0:5])}')
    
    bs.login()
        
    high_cp_stocklist=get_high_market_value_stock(
        
        date = 1,
        stock_codes= codes,
        threshold = 200.0,  # 这里已经有默认值100.0了
        delay = 0.0001
        
    )
    
    
    print(high_cp_stocklist[0:5])
    
        # 登出
    bs.logout()









