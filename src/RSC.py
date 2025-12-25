import os
import pandas as pd

from datetime import datetime


#Recommended stock codes

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


file_folder = os.path.join(project_root, r"anars")

print(f"📁 修正后的绝对保存目录：{file_folder}")

print(project_root)

data_M = pd.DataFrame()
data_W = pd.DataFrame()
data_D = pd.DataFrame()


try:
    data_M = pd.read_csv(f'{file_folder}/月线详细结果_20251223.csv', encoding='utf-8-sig')
    print(f"✅ 成功加载月线: {len(data_M)} 行数据")
    
    data_W = pd.read_csv(f'{file_folder}/周线详细结果_20251223.csv', encoding='utf-8-sig')
    print(f"✅ 成功加载周线: {len(data_W)} 行数据")
    
    data_D = pd.read_csv(f'{file_folder}/日线详细结果_20251223.csv', encoding='utf-8-sig')
    print(f"✅ 成功加载日线: {len(data_D)} 行数据")
    
    
    
    # 获取共同code列表
    common_codes = list(set(data_M['code']) & set(data_W['code']) & set(data_D['code']))
    
    print(f"✅ 共同代码数量: {len(common_codes)}")
    
except Exception as e:
    print(f"❌ 加载文件失败: {e}")
    common_codes = []  # 如果出错，返回空列表
    
    

print(common_codes)


timestamp = datetime.now().strftime("%Y%m%d")

filename = "rsc"

filepath = os.path.join(file_folder, f"{filename}_{timestamp}.py")



print(filepath)

# 将列表保存到Python文件
with open(filepath, 'w', encoding='utf-8') as f:
    # 将列表转换为字符串格式写入文件
    f.write(f"common_codes = {common_codes}")
    print(f"✅ Python格式保存到: {filepath}")
