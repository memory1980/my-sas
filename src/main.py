import os
import pandas as pd
import glob
from datetime import datetime, timedelta
import numpy as np
# 从模块导入高成长股票列表
from high_growth_stock_list import high_growth_stocks

def 分析低量低价(周期: str = 'W', N: int = 10, 数据目录: str = "data/", 输出目录: str = "anars"):
    """
    分析股票低换手率和之后N周期的最低价
    使用相同周期数据：月线数据找月线低换手和月线最低价
    N=10 表示从当前周期到第9周期（包含当前）
    
    参数:
        周期: 'M'月线, 'W'周线, 'D'日线
        N: 向后查找的周期数（包含当前周期）
        数据目录: 数据文件目录
        输出目录: 结果保存目录
    """
    
    # 文件映射
    文件表 = {'M': 'stock_data_M.csv', 'W': 'stock_data_W.csv', 'D': 'stock_data_D.csv'}
    
    if 周期 not in 文件表:
        print(f"错误: 周期'{周期}'不支持")
        return
    
    文件路径 = os.path.join(数据目录, 文件表[周期])
    
    if not os.path.exists(文件路径):
        print(f"错误: 找不到文件 {文件路径}")
        return
    
    print(f"📊 读取{周期}线数据...")
    数据 = pd.read_csv(文件路径, encoding='utf-8-sig')
    
    # 转换日期
    if 'date' in 数据.columns:
        数据['date'] = pd.to_datetime(数据['date'])
    
    结果列表 = []
    周期名 = {'M': '月', 'W': '周', 'D': '日'}[周期]
    周期单位 = {'M': '个月', 'W': '周', 'D': '天'}[周期]
    
    # 分析每个股票
    for 代码 in high_growth_stocks:
        print(f"🔍 分析股票: {代码}")
        
        # 获取股票数据
        股票数据 = 数据[数据['code'] == 代码].copy()
        
        if 股票数据.empty:
            print(f"   ⚠️ 该股票在{周期}线数据中无记录")
            continue
        
        # 按日期排序
        股票数据 = 股票数据.sort_values('date')
        股票数据 = 股票数据.reset_index(drop=True)
        
        # 检查必需字段
        if 'turn' not in 股票数据.columns:
            print(f"   ⚠️ 缺少turn字段")
            continue
        if 'low' not in 股票数据.columns:
            print(f"   ⚠️ 缺少low字段")
            continue
        
        # 找turn最小的10个点
        low_turn_idx = 股票数据['turn'].nsmallest(10).index.tolist()
        
        # 分析每个低换手点
        for i, idx in enumerate(low_turn_idx[:10], 1):
            if idx >= len(股票数据):
                continue
            
            # 当前点（第0周期）
            当前 = 股票数据.iloc[idx]
            当前日期 = 当前['date']
            当前换手 = 当前['turn']
            当前价 = 当前['close'] if 'close' in 当前 else 当前['price']
            
            print(f"\n   {i}. {周期名}线低换手点:")
            print(f"      日期: {当前日期.date()}")
            print(f"      换手率: {当前换手:.4f}")
            print(f"      价格: {当前价:.2f}")
            
            # 向后查找N个周期（包含当前）
            # 从当前(idx)到 idx+N-1，总共N个周期
            start = idx  # 包含当前周期
            end = min(idx + N, len(股票数据))  # 到 idx+N-1
            
            if start >= end:
                print(f"      ⚠️ 数据不足，无法向后分析")
                continue
            
            分析区间 = 股票数据.iloc[start:end]
            
            if len(分析区间) == 0:
                print(f"      ⚠️ 分析区间为空")
                continue
            
            # 在分析区间内找最低价（包含当前周期）
            最低价 = 分析区间['low'].min()
            最低价_idx = 分析区间['low'].idxmin()
            最低价日期 = 股票数据.iloc[最低价_idx]['date']
            
            # 计算周期差
            周期差 = 最低价_idx - idx  # 0=同周期，1=下一个周期...
            
            # 计算跌幅
            跌幅 = ((最低价 - 当前价) / 当前价 * 100) if 当前价 != 0 else 0
            
            # 输出结果
            if 周期差 == 0:
                print(f"      ✅ {周期名}线同周期出现最低价!")
                print(f"      最低价: {最低价:.2f}")
                print(f"      最大跌幅: {跌幅:.1f}%")
                print(f"      分析区间: 当前{周期名}及之后{len(分析区间)-1}个{周期名}")
            else:
                print(f"      🔄 在之后{len(分析区间)-1}个{周期名}内出现最低价:")
                print(f"      最低价日期: {最低价日期.date()}")
                print(f"      最低价: {最低价:.2f}")
                print(f"      间隔: {周期差}个{周期名}")
                print(f"      最大跌幅: {跌幅:.1f}%")
                print(f"      分析区间: 当前{周期名}及之后{len(分析区间)-1}个{周期名}")
            
            # 保存结果
            结果列表.append({
                '股票代码': 代码,
                '数据周期': 周期,
                '周期名称': 周期名,
                '低换手日期': 当前日期,
                '低换手率': 当前换手,
                '低换手价格': 当前价,
                '最低价日期': 最低价日期,
                '最低价': 最低价,
                '周期差': 周期差,
                '是否同周期': '是' if 周期差 == 0 else '否',
                '分析周期数': len(分析区间),
                '有效周期数': len(分析区间) - 1,  # 除去当前周期
                '最大跌幅%': 跌幅,
                '备注': f"同{周期名}" if 周期差 == 0 else f"后{周期差}{周期名}"
            })
    
    # 保存结果到CSV
    if 结果列表:
        # 确保输出目录存在
        os.makedirs(输出目录, exist_ok=True)
        
        结果df = pd.DataFrame(结果列表)
        输出文件 = f"低量低价_{周期}_{N}周期.csv"
        完整路径 = os.path.join(输出目录, 输出文件)
        
        # 保存文件
        结果df.to_csv(完整路径, index=False, encoding='utf-8-sig')
        
        # 显示保存信息
        print(f"\n" + "="*60)
        print(f"✅ 分析完成！")
        print(f"📁 输出目录: {输出目录}")
        print(f"📄 文件名: {输出文件}")
        print(f"📍 完整路径: {完整路径}")
        print(f"📏 文件大小: {os.path.getsize(完整路径)/1024:.1f} KB")
        print(f"📊 共分析 {len(结果列表)} 个低换手点")
        
        # 统计信息
        同周期数量 = len(结果df[结果df['是否同周期'] == '是'])
        不同周期数量 = len(结果df) - 同周期数量
        
        print(f"\n📈 统计摘要:")
        print(f"  数据周期: {周期名}线")
        print(f"  分析周期: {N}个{周期名} (含当前)")
        print(f"  同{周期名}最低价: {同周期数量}次 ({同周期数量/len(结果列表)*100:.1f}%)")
        print(f"  后{周期名}最低价: {不同周期数量}次")
        
        if len(结果列表) > 0:
            跌幅列表 = 结果df['最大跌幅%'].tolist()
            print(f"  平均跌幅: {np.mean(跌幅列表):.1f}%")
            print(f"  最大跌幅: {min(跌幅列表):.1f}%")
            print(f"  最小跌幅: {max(跌幅列表):.1f}%")
            
            # 显示周期差分布
            if 不同周期数量 > 0:
                print(f"\n📊 {周期名}线周期差分布:")
                周期差分布 = 结果df['周期差'].value_counts().sort_index()
                for 差, 数量 in 周期差分布.items():
                    if 差 > 0:  # 只显示不同周期的
                        百分比 = 数量/len(结果列表)*100
                        print(f"  后{差}个{周期名}: {数量}次 ({百分比:.1f}%)")
        
        print(f"\n💡 分析说明:")
        print(f"  - 使用{周期名}线数据寻找低换手率点")
        print(f"  - 在相同{周期名}线数据中寻找最低价")
        print(f"  - 分析区间: 当前{周期名}及之后{N-1}个{周期名}")
        print(f"  - '周期差=0'表示同{周期名}出现最低价")
        print(f"  - '周期差=1'表示下个{周期名}出现最低价")
        
    else:
        print("\n⚠️ 没有找到任何结果")

def 交互输入():
    """获取用户输入"""
    print("\n" + "="*60)
    print("📊 股票低量低价分析 (同周期数据分析)")
    print("="*60)
    print("说明:")
    print("  • 月线数据: 找月线低换手，在月线数据中找最低价")
    print("  • 周线数据: 找周线低换手，在周线数据中找最低价")
    print("  • 日线数据: 找日线低换手，在日线数据中找最低价")
    print(f"  • N=10: 分析当前周期及之后9个周期")
    
    # 选择周期
    while True:
        print("\n请选择分析的数据周期:")
        print("  M - 月线 (分析月份数据)")
        print("  W - 周线 (分析周数据)")
        print("  D - 日线 (分析日数据)")
        周期 = input("请输入选择 (M/W/D, 默认W): ").strip().upper()
        
        if 周期 == "":
            周期 = "W"
            break
        elif 周期 in ["M", "W", "D"]:
            break
        else:
            print("❌ 输入错误，请重新输入")
    
    # 周期名称
    周期名 = {'M': '月', 'W': '周', 'D': '日'}[周期]
    
    # 输入分析周期
    while True:
        try:
            输入 = input(f"\n请输入分析周期数 N (分析当前{周期名}及之后N-1个{周期名}, 默认10): ").strip()
            if 输入 == "":
                N = 10
            else:
                N = int(输入)
            
            if N <= 0:
                print(f"❌ 必须大于0")
                continue
            elif N < 3:
                print(f"⚠️ 分析周期较小，建议至少3个{周期名}")
                confirm = input("是否继续？(y/n, 默认y): ").strip().lower()
                if confirm not in ["", "y", "yes", "是"]:
                    continue
                break
            elif N > 50:
                print(f"⚠️ 分析周期较大，可能包含过多数据")
                confirm = input("是否继续？(y/n, 默认y): ").strip().lower()
                if confirm not in ["", "y", "yes", "是"]:
                    continue
                break
            else:
                break
        except:
            print("❌ 请输入有效的数字")
    
    print(f"\n✅ 参数设置完成:")
    print(f"  数据周期: {周期名}线")
    print(f"  分析周期: {N}个{周期名} (当前{周期名}及之后{N-1}个{周期名})")
    
    return 周期, N

def 显示示例():
    """显示分析示例"""
    print("\n" + "="*60)
    print("📋 分析示例:")
    print("="*60)
    print("月线示例 (N=5):")
    print("  • 2025年5月出现最低换手率")
    print("  • 在月线数据中分析: 2025年5月及之后4个月")
    print("  • 可能结果: 同月(5月)出现最低价，或6月、7月等出现最低价")
    print()
    print("周线示例 (N=8):")
    print("  • 2025年第20周出现最低换手率")
    print("  • 在周线数据中分析: 第20周及之后7周")
    print("  • 可能结果: 同周出现最低价，或第21、22周等出现最低价")
    print()
    print("日线示例 (N=20):")
    print("  • 2025-05-10出现最低换手率")
    print("  • 在日线数据中分析: 5月10日及之后19天")
    print("  • 可能结果: 同日出现最低价，或5月11、12日等出现最低价")

# 主程序
if __name__ == "__main__":
    try:
        # 显示分析示例
        显示示例()
        
        # 获取用户输入
        周期, N = 交互输入()
        
        # 执行分析
        print(f"\n🚀 开始分析...")
        print(f"📅 数据周期: {周期}线")
        print(f"🔍 分析设置: 当前周期及之后{N-1}个周期")
        
        # 分析并保存到anars目录
        分析低量低价(
            周期=周期,
            N=N,
            数据目录="data/",
            输出目录="anars"
        )
        
    except Exception as e:
        print(f"\n❌ 错误: {e}")
        import traceback
        traceback.print_exc()
    
    input("\n按 Enter 退出...")