import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

import yfinance as yf

# 获取茅台（600519.SS）的股票数据，日期范围从 2020-01-01 到 2021-01-01
stock_data = yf.download('600000.SS', start='2020-01-01', end='2025-12-01')

# 查看数据的前几行
print(stock_data)