import os
import pandas as pd
import baostock as bs
from datetime import datetime, timedelta


def get_trade_date(OffSetDay=0):   # 第一步，获取交易日
    
    days=OffSetDay      
    
    e_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')         
    s_date = (datetime.now() - timedelta(days=21)).strftime('%Y-%m-%d')
    # print("开始日期为：", s_date)        
    # print("当前日期为：", e_date)

    rs = bs.query_trade_dates(start_date=s_date, end_date=e_date) 
    # print("获取交易日错误", rs.error_code)
    # print("获取交易日状态", rs.error_msg)
    
    data_list = []
    while (rs.error_code == '0') & rs.next():    
        
        data_list.append(rs.get_row_data())
    
    result = pd.DataFrame(data_list, columns=rs.fields)
    
    trading_days = result[result['is_trading_day'] == '1']['calendar_date']     
    
    trading_days_list = trading_days.tolist() 
    
    #判断当前时间是否是下午6点半之前
    
    current_time = datetime.now().time()
    
    formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
    
       
    target_time = datetime.strptime("18:30", "%H:%M").time()
    
        
    if current_time > target_time:
        
        recent_trade_day = trading_days_list[-1]
        
        print(f'最近交易日：{recent_trade_day}')
   
    else:
   
        print(f'当前时间{current_time}，当前交易日没有结束，没有当天数据')
   
   
        recent_trade_day = trading_days_list[-2]
        
        print(f'最近的前一个交易日：{recent_trade_day}')
    
    return recent_trade_day
      
    
    

    
if __name__ == "__main__":
    lg=bs.login()  
    td_list=get_trade_date()
    # print(td_list)
    lg=bs.logout()
#实际运行结果：
# (myenv) PS D:\py> & D:/py/myenv/Scripts/python.exe d:/py/myenv/stockfun/get_trade_date.py
# login success!
# 开始日期为： 2025-11-17
# 当前日期为： 2025-12-08
# 当前时间为 2025-12-08-21-15
# 获取交易日错误 0
# 获取交易日状态 success
# 获取22个日期，其中16个为交易日
# 获取的交易日列表最后5个日期： ['2025-12-01', '2025-12-02', '2025-12-03', '2025-12-04', '2025-12-05']
# 函数的返回值为，获取的交易日列表最后1个： ['2025-12-05']