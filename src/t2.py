import os

import pandas as pd

import time 

from datetime import datetime

import pprint

try:
    terminal_width = os.get_terminal_size().columns
except OSError:
    terminal_width = 80  # 如果获取失败，使用一个默认值，例如80

my_data = {...}  # 你的数据
pprint.pprint(my_data, width=terminal_width)




# print(f"当前工作目录: {os.getcwd()}")
# print(f"脚本所在目录: {os.path.dirname(os.path.abspath(__file__))}")

# $ D:/my-sas/.venv/Scripts/python.exe d:/my-sas/src/t2.py
# 当前工作目录: D:\my-sas
# 脚本所在目录: d:\my-sas\src

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# input_data_dir = os.path.join(project_root, "data")
# output_dir = os.path.join(project_root, "anars")



print(f"当前工作目录: {project_root }")

# print(f"当前工作目录: {input_data_dir }")

# print(f"当前工作目录: {output_dir }")

print(os.path.abspath(__file__))
print(os.path.dirname(os.path.abspath(__file__)))

file_path = os.path.join(project_root, "data", "stock_data_M.csv")

print(f"打开文件: {file_path }")

abs_file_path = os.path.abspath(file_path)

try:
    data = pd.read_csv(abs_file_path, encoding='utf-8-sig')
    print(f"✅ 成功加载: {len(data)} 行数据")
except Exception as e:
    print(f"❌ 加载文件失败: {e}")


high_growth_stocks = ['sh.688498', 'sz.000029', 'sz.002261', 'sz.302132', 'sh.603256', 'sh.600115', 'sz.002414', 'sz.300456', 'sh.688295', 'sh.600100', 'sh.688313', 'sh.600392', 'sz.002195', 'sh.600884', 'sh.688728', 'sz.002074', 'sh.688183', 'sh.688333', 'sz.300251', 'sz.300972', 'sh.601456', 'sz.300476', 'sh.688256', 'sz.002926', 'sh.601020', 'sz.002739', 'sh.600410', 'sz.002497', 'sz.300502', 'sh.688322', 'sz.002624', 'sh.600460', 'sz.000657', 'sh.600745', 'sz.300548', 'sz.300438', 'sz.000959', 'sh.600150', 'sz.002487', 'sz.300442', 'sh.600095', 'sz.000750', 'sz.000825', 'sh.600673', 'sz.300803', 'sh.600685', 'sh.603379', 'sz.000893', 'sh.600329', 'sz.301217', 'sz.300748', 'sh.600160', 'sz.000564', 'sz.000050', 'sh.600166', 'sh.600111', 'sh.688213', 'sz.301035', 'sz.000981', 'sz.002407', 'sh.688297', 'sz.301200', 'sh.600918', 'sh.601162', 'sh.688048', 'sz.000783', 'sh.600010', 'sz.002602', 'sh.601211', 'sh.688235', 'sh.603486', 'sz.002244', 'sz.002080', 'sz.300604', 'sz.000725', 'sz.000100', 'sh.601069', 'sh.600126', 'sh.603893', 'sh.600499', 'sh.600120', 'sh.600578', 'sh.603185', 'sh.600808', 'sh.688702', 'sz.000686', 'sh.603236', 'sz.000301', 'sh.688072', 'sz.000066', 'sz.002217', 'sh.600118', 'sz.000987', 'sh.600143', 'sz.300450', 'sh.600536', 'sz.002402', 'sh.600183', 'sh.688772', 'sh.600989', 'sz.002056', 'sh.601106', 'sz.300308', 'sh.600482', 'sh.601901', 'sh.600109', 'sz.002736', 'sz.000166', 'sz.300454', 'sh.600518', 'sh.601519', 'sz.300033', 'sz.000623', 'sh.603259', 'sh.600246', 'sz.300458', 'sh.600988', 'sh.603444', 'sh.688200', 'sz.002603', 'sh.600732', 'sh.689009', 'sz.002230', 'sh.601099', 'sz.300102', 'sz.002436', 'sh.603737', 'sz.300570', 'sh.688981', 'sh.601606', 'sh.601360', 'sh.603766', 'sh.601212', 'sz.300620', 'sz.002939', 'sh.688336', 'sh.688114', 'sh.688608', 'sh.601377', 'sh.603993', 'sz.000877', 'sh.601878', 'sz.002673', 'sh.600176', 'sh.688271', 'sh.688082', 'sh.600909', 'sh.688052', 'sh.601127', 'sh.601066', 'sh.600839', 'sh.600547', 'sz.300058', 'sh.688008', 'sh.603083', 'sh.600004', 'sh.601555', 'sh.688002', 'sh.601628', 'sz.002460', 'sz.002156', 'sz.300001', 'sh.601336', 'sh.600801', 'sh.688017', 'sz.000776', 'sh.603979', 'sh.601881', 'sz.000709', 'sz.002916', 'sz.002155', 'sh.688019', 'sh.600958', 'sz.300274', 'sh.601899', 'sh.688027', 'sz.002281', 'sh.601233', 'sh.603296', 'sh.600378', 'sz.002185', 'sh.600517', 'sh.688778', 'sh.688578', 'sh.688123', 'sz.002895', 'sh.600873', 'sz.002202', 'sh.603160', 'sh.600601', 'sh.688018', 'sz.300059', 'sz.300394', 'sz.002028', 'sh.601138', 'sh.600415', 'sz.002463', 'sh.688561', 'sz.000408', 'sh.601012', 'sh.600369', 'sh.601991', 'sz.002493', 'sh.600011', 'sh.600031', 'sh.600489', 'sz.300373', 'sz.000009', 'sh.600848', 'sz.002335', 'sz.000792', 'sh.600104', 'sz.300870', 'sz.002050', 'sz.002236', 'sz.000975', 'sz.300017', 'sz.002600', 'sz.000988', 'sh.688208', 'sh.605499', 'sh.601108', 'sh.600027', 'sz.003022', 'sh.603799', 'sh.600030', 'sz.300207', 'sz.000960', 'sh.600066', 'sz.002222', 'sz.000729', 'sz.301308', 'sh.600995', 'sh.603501', 'sz.300750', 'sh.603699', 'sh.688041', 'sh.601788', 'sz.000062', 'sz.300627', 'sh.688249', 'sh.600595', 'sz.002558', 'sh.603986', 'sz.300888', 'sh.600098', 'sh.603129', 'sh.688180', 'sh.600009', 'sz.300762', 'sh.603920', 'sh.601990', 'sh.600498', 'sz.301358', 'sh.601319', 'sz.002475', 'sh.600323', 'sh.600061', 'sh.603659', 'sh.688012', 'sh.600019', 'sh.601696', 'sh.600660', 'sz.300866', 'sh.601808', 'sh.603308', 'sh.688318', 'sh.601118', 'sh.600089', 'sz.300395', 'sz.300003', 'sz.002518', 'sz.000737', 'sz.002389', 'sz.002595', 'sh.688065', 'sz.002049', 'sz.002472', 'sz.002138', 'sh.600999', 'sz.300054', 'sz.002709', 'sz.300857', 'sz.002517', 'sz.002340', 'sz.002555', 'sh.603529', 'sz.002085', 'sz.000887', 'sh.601216', 'sz.000932', 'sh.600704', 'sh.601208', 'sz.300408', 'sh.600060', 'sz.000021', 'sz.300347', 'sh.688220', 'sh.688676', 'sh.688195']  # 300只股票，按同比增长率降序排列



data['date'] = pd.to_datetime(data['date'])




codes = high_growth_stocks

print(f"共计{len(codes)}只股票")


rs_data=pd.DataFrame()



serial_n=5

for code in codes:
    # 筛选股票数据
    df = data[data['code'] == code]
    
    temp_data = pd.DataFrame()  # 每次循环都重置为空！
    

    
    print(temp_data.info)
    
    print('\n')
    
    latest_df = df.nlargest(serial_n, 'date')
      
    print(latest_df.info) 
    
    print('\n')
     
    temp_data = pd.concat([temp_data, latest_df], ignore_index=True) 
    
    print('1111',temp_data)
    
    print('\n')
    

    
    
    tf = df.sort_values('turn')
    
    # print(tf)
    
    smallest_tf = tf.nsmallest(serial_n, 'turn')
    
 
    
    print('3333',smallest_tf.info)
    
    
    print('\n')
    
    
    temp_data = pd.concat([temp_data, smallest_tf], ignore_index=True) 
    
    print('444',temp_data)
    
    print('\n')
    
    
    
    
    cf = df.sort_values('close')
    

    
    smallest_cf = tf.nsmallest(serial_n, 'close')
    
    print('555',smallest_cf.info)
    
    print('\n')
    
    
    
    temp_data = pd.concat([temp_data, smallest_cf], ignore_index=True) 
    
    print('666',temp_data)
    
    print('\n')
    
    

    
    
    hf = df.sort_values('high')
    
    smallest_hf = hf.nlargest(serial_n, 'high')
    
    print('777',hf.info)
    
    
    print('\n')
    
    temp_data = pd.concat([temp_data, smallest_hf], ignore_index=True) 
    
    print('888',temp_data)
    
    print('\n')
 
    
    print('\n')
    
    rs_data =pd.concat([rs_data, temp_data], ignore_index=True) 
    
    

    
    
print(rs_data.info)
    
    
    
    
    
print('\n')
    
     
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

input_dir = os.path.join(project_root, "data")
output_dir = os.path.join(project_root, "anars")
    
timestamp = datetime.now().strftime("%Y%m%d%H%M")

extracted_file = os.path.join(output_dir, f"详细结果_{timestamp}.csv")

rs_data.to_csv(extracted_file, index=True, encoding='utf-8-sig')    

    
    
    
    
    
    
    

  



