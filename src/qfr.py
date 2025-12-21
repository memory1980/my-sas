import baostock as bs
import pandas as pd
import tqdm
import time
import os
from datetime import datetime, timedelta


def qfr(
        stockcodes: List[str] = None,
        offsetday: Union[str, int] = 180,
        delay=0.00001,
        
    )-> pd.DataFrame:   
    
    s_date = (datetime.now() - timedelta(days=offsetday)).strftime('%Y-%m-%d')
    e_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')   
          
    if stockcodes is None:
        stockcodes = ['sh.600000', 'sh.600004', 'sh.600006']
    
    pd.set_option('display.max_colwidth',None) # None 表示不限制宽度
    
  
    
    print(f"开始时间: {s_date}")
    print(f"结束时间: {e_date}")
    print(f"时间跨度: {offsetday}天") 
    
    
        
    data_list = []
    fields = None

    print(f"查询 {len(stockcodes)} 个股票:")
    print(f"示例前5只股票代码: {stockcodes[0:5]}")
    
    # 添加进度条
    from tqdm import tqdm
    
    for code in tqdm(stockcodes, desc="查询进度"):
        try:
            # 正确的参数名是 code 而不是 stockcodes
            rs = bs.query_forecast_report(
                code=code,  # 关键修改
                start_date=s_date, 
                end_date=e_date
            )
            
            if rs.error_code != '0':
                print(f"\n股票 {code} 查询失败: {rs.error_msg}")
                continue
                
            # 获取字段名
            if fields is None:
                fields = rs.fields
            
            # 收集数据
            while rs.next():
                data_list.append(rs.get_row_data())
                
            # 添加延迟避免请求过快
            time.sleep(delay)
            
        except Exception as e:
            print(f"\n股票 {code} 查询异常: {str(e)}")
            continue

    print("查询完成")
    
    if not data_list:
        print("警告：没有获取到任何数据")
        return pd.DataFrame()
    
    # 更安全的 DataFrame 创建
    try:
        result = pd.DataFrame(data_list, columns=fields if fields else rs.fields)
       
    
   
        
    except:
        # 如果字段名获取失败，使用默认数字列名
        result = pd.DataFrame(data_list)    
    
    
    result = result.apply(pd.to_numeric, errors='ignore')  # 使用ignore而不是coerce，保留非数值列

    # 保留两位小数
    result = result.round(2)        
    

        
    result = result[result['profitForcastChgPctDwn'] > 0]
    
    
    result=result.sort_values('profitForcastChgPctDwn', ascending=False)  #按照增速下限进行排序
        
    print(result.head())
    
    #保存数据到文件
    
    
    
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) #获取项目目录
    
    data_dir = os.path.join(project_root, "data")

    py_path = os.path.join(data_dir, "qfr_data.csv")

    result.to_csv(py_path, index=False, encoding='utf-8-sig')


    print(f"✅ Python格式保存到: {py_path}")
    

    
    return result



if __name__ == "__main__":
    

    from fulsl import full_stockcode as fsc
    
    
    
    code=fsc[:]
    

    lg=bs.login()
       
   
    
    pd=qfr(stockcodes=code,offsetday=180)


    lg=bs.logout()