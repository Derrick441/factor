from operation_time import print_execute_time, date_addsub
from operation_database import data_from_mysql


# 日频量价因子的基础数据
@print_execute_time
def vpd_data_read(t1, t2, max_need):
    # t1调整
    t1_bf = date_addsub(t1, (-max_need), 'str')  # 数据前推max_need个月, 最大数据需求
    # sql
    sql = '''
    select 
    symbol as stock,date,close * adjustfactor as close_adj,volume,changepct
    from 
    data_day
    where 
    (date>=str_to_date({},'%Y%m%d')) and (date<=str_to_date({},'%Y%m%d'))
    '''.format(t1_bf, t2)
    # 取数
    data = data_from_mysql(sql)
    return data


# 读取一个table的因子
@print_execute_time
def factor_read(fac_table, t1, t2, max_need):
    t1_bf = date_addsub(t1, (-max_need), 'str')  # 数据前推max_need个月, 最大数据需求
    sql = '''
    select 
    stock,date,factor_name,factor_value 
    from 
    {} 
    where 
    (date>=str_to_date({},'%Y%m%d')) and (date<=str_to_date({},'%Y%m%d'))
    '''.format(fac_table, t1_bf, t2)
    data = data_from_mysql(sql)
    return data


# 读取一个因子
@print_execute_time
def factor_read_one(fac_table, fac_name, t1, t2, max_need):
    t1_bf = date_addsub(t1, (-max_need), 'str')  # 数据前推max_need个月, 最大数据需求
    sql = '''
    select 
    stock,date,factor_name,factor_value 
    from 
    {} 
    where 
    (factor_name='{}') and (date>=str_to_date({},'%Y%m%d')) and (date<=str_to_date({},'%Y%m%d'));
    '''.format(fac_table, fac_name, t1_bf, t2)
    data = data_from_mysql(sql)
    return data


@print_execute_time
def vpl1_data_read(t1, t2, max_need):
    pass


@print_execute_time
def vpl2_data_read(t1, t2, max_need):
    pass


@print_execute_time
def fin_data_read(t1, t2, max_need):
    pass


@print_execute_time
def exp_data_read(t1, t2, max_need):
    pass


@print_execute_time
def esg_data_read(t1, t2, max_need):
    pass
