import pandas as pd
import os
import baostock as bs
from datetime import datetime

import sys 

from gtd import get_trade_date

from gsl import get_stock_codes

from ghcp import get_high_market_value_stock

from gqp import generate_quarter_params

from gqpc import get_query_profit_codes

from cgp import calculate_growth_pure

from analysis_stock import analysis_stocks

from datetime import datetime, timedelta







#获取交易日
lg=bs.login()

trade_day=get_trade_date()


print (trade_day)

rs=get_stock_codes(trade_day)

rs2=get_high_market_value_stock(
    

    date = 1,
    stock_codes=    rs[:]  ,
    threshold = 100.0,  
    delay = 0
    
    )

print(rs[0:5])

print(rs2[0:5])

rs3=get_query_profit_codes(rs2[0:10])

rs4=

lg=bs.logout()