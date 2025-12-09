import baostock


def generate_quarter_params(trading_date: str, num_quarters: int = 6) -> List[Tuple[int, int]]:
    """生成季度参数列表"""
    cal_year = int(trading_date[:4])
    cal_month = int(trading_date[5:7])
    
    print(cal_year)
    
    print(cal_month)
    
    cal_quarter = (cal_month - 1) // 3 + 1
    
    quarter_params = []
    current_year = cal_year
    current_quarter = cal_quarter
    
    for i in range(num_quarters):
        quarter_params.insert(0, (current_year, current_quarter))
        current_quarter -= 1
        if current_quarter == 0:
            current_quarter = 4
            current_year -= 1
    
    return quarter_params

if __name__ == "__main__":
    
    
    
    
    quarterrs=generate_quarter_params('2025-12-05')
    
    print(quarterrs[-7:-1])
    

