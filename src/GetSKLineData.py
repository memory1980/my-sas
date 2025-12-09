import pandas as pd
import baostock as bs
from datetime import datetime, timedelta
from typing import Optional, Literal
from GetTradeDate import get_trade_date

def get_skline_data(
    days: int = 180,
    code: Optional[str] = 'sh.600000',  # 默认值
    frequency: Literal["d", "w", "m"] = "w",
    delay: float = 0.0001
) -> pd.DataFrame:   
    """获取单只股票的K线数据"""
    
    print(f"🚀 开始获取K线数据")
    print(f"   股票代码: {code}")
    print(f"   K线周期: {frequency}")
    print(f"   查询天数: {days}天")
    
    # 确保有股票代码
    if code is None:
        code = 'sh.600000'
        print(f"⚠️  使用默认股票代码: {code}")
    
    # 获取交易日
    end_date_str = get_trade_date()
    if not end_date_str:
        end_date_str = datetime.now().strftime('%Y-%m-%d')
        print(f"⚠️  无法获取交易日，使用当前日期: {end_date_str}")
    
    end_date_dt = datetime.strptime(end_date_str, '%Y-%m-%d')
    start_date_dt = end_date_dt - timedelta(days=days)
    start_date_str = start_date_dt.strftime('%Y-%m-%d')
    
    print(f"   开始日期: {start_date_str}")
    print(f"   结束日期: {end_date_str}")
    print("=" * 70)

    # 查询数据 - 关键：使用传入的code参数
    rs = bs.query_history_k_data_plus(
        code=code,  # 使用传入的股票代码
        fields="date,code,open,high,low,close,volume,amount,adjustflag,turn",
        start_date=start_date_str,
        end_date=end_date_str,
        frequency=frequency,
        adjustflag="3")
    
    # 检查查询是否成功
    if rs.error_code != '0':
        print(f"❌ 查询失败: {rs.error_msg}")
        return pd.DataFrame()
    
    # 获取数据
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    
    if not data_list:
        print(f"❌ 未获取到数据")
        return pd.DataFrame()
    
    result = pd.DataFrame(data_list, columns=rs.fields)
    
    # 数据验证
    if 'code' in result.columns and not result.empty:
        actual_code = result['code'].iloc[0]
        if actual_code != code:
            print(f"⚠️  数据验证：请求 {code}，返回 {actual_code}")
        else:
            print(f"✅ 数据验证：正确获取到 {actual_code} 的数据")
    
    print(f"✅ 获取完成: {len(result)} 条记录")
    return result

if __name__ == "__main__":
    # 测试
    lg = bs.login()
    
    # 测试不同的股票
    test_codes = ['sz.002594', 'sh.600000', 'sz.000001']
    
    for test_code in test_codes:
        print(f"\n{'='*70}")
        print(f"测试股票: {test_code}")
        print(f"{'='*70}")
        
        data = get_skline_data(
            days=30,
            code=test_code,
            frequency="w",
            delay=0.00001
        )
        
        if not data.empty:
            print(f"获取到的股票代码: {data['code'].iloc[0] if 'code' in data.columns else '未知'}")
            print(f"数据行数: {len(data)}")
            print(data.head())
        print("-" * 70)
    
    bs.logout()