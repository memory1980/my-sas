import pandas as pd
import os

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))



data_dir = os.path.join(project_root, "anars")


output_dir = os.path.join(project_root, "anars")

# 读取CSV文件
weekly_df = pd.read_csv(os.path.join(data_dir, "周线详细结果_20251226.csv"))
daily_df = pd.read_csv(os.path.join(data_dir, "日线详细结果_20251226.csv"))

# 找出共同代码
common_codes = list(set(weekly_df['code']).intersection(set(daily_df['code'])))

# 保存为py文件
output_file = os.path.join(output_dir, "common_codes.py")

with open(output_file, 'w', encoding='utf-8') as f:
    f.write("推荐代码：\n")
    f.write(f"common_codes = {common_codes}")

print(f"文件已保存到: {output_file}")