import baostock as bs
import pandas as pd
import tqdm
import time
import os
from datetime import datetime, timedelta

from GTD import get_trade_date


def qpe(
        stockcodes: List[str] = None,
        offsetday: Union[str, int] = 180,
        delay=0,
        
    )-> pd.DataFrame:   
    
        
    
    tradingdata = get_trade_date()
     
    e_date = datetime.strptime(tradingdata, '%Y-%m-%d')
    
    e_date = e_date.strftime('%Y-%m-%d')  #转成字串
    
    
    s_date = (datetime.now() - timedelta(days=offsetday)).strftime('%Y-%m-%d')
    
    
          
    if stockcodes is None:
        stockcodes = ['sh.600000', 'sh.600004', 'sh.600006']
    
    pd.set_option('display.max_colwidth',180) # None 表示不限制宽度
    
  
    
    print(f"开始时间 : {s_date}")
    print(f"结束时间 : {e_date}")
    print(f"查讯周期 : {offsetday}天") 
    
    
        
    data_list = []
    fields = None

    print(f"查询 {len(stockcodes)} 个股票:")
 
    
    # 添加进度条
    from tqdm import tqdm
    
    for code in tqdm(stockcodes, desc="查询进度"):
        try:
            # 正确的参数名是 code 而不是 stockcodes
            rs = bs.query_performance_express_report(
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
    
    
    result = result[result['performanceExpStatDate'] == "2025-12-31"]  #年报快报，一季报3-30，中报6-60，三季报9-31
    
        
    # result = result[result['performanceExpressOPYOY'] > 0]
    
    
    result=result.sort_values('performanceExpressOPYOY', ascending=False)  #按照增速下限进行排序
        
    print(result.head())
    
    #保存数据到文件
    
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # 设置输出目录（如果未提供，使用项目根目录/anars）

    output_dir = os.path.join(project_root, "data")

    # print(project_root)    

    # print(output_dir)

    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)

    # print(result.empty)

    # 保存结果

        
    timestamp = datetime.now().strftime("%Y%m%d")

    rs_data_file = os.path.join(output_dir, f"业绩快报_{timestamp}.csv")

    # print(timestamp)


    # print(rs_data_file)


    # 确保输出目录存在
    os.makedirs(os.path.dirname(rs_data_file), exist_ok=True)


    result.to_csv(rs_data_file, index=False, encoding='utf-8-sig')

    print(f"💾 详细结果已保存: {rs_data_file}")
      
        
    return result



if __name__ == "__main__":
    

    from full_stock_list   import full_stockcode as fsc
    
    
    
    code=fsc[:]
    
        
    print(f"示例前5只股票代码: {code[0:5]}")
    
    
    
    lg=bs.login()
       
       
    pd=qpe(stockcodes=code,offsetday=50)


    lg=bs.logout()