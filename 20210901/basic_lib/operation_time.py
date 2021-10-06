from datetime import datetime
from dateutil.relativedelta import relativedelta


# 打印函数执行时间
def print_execute_time(fun):

    def wrapper(*args, **kwargs):

        t1 = datetime.now()
        fun_return = fun(*args, **kwargs)
        t2 = datetime.now()

        print(fun.__name__, 'execute time: ', t2 - t1)

        return fun_return

    return wrapper


# str时间加减操作
def date_addsub(t, num, t_format):

    # 时间加减操作
    t_dt = datetime.strptime(t, '%Y%m%d')  # str转datetime格式
    t_bf_dt = t_dt + relativedelta(months=num)  # 因子起始日t + num个月(num为负数，则是往前）

    # 返回
    if t_format == 'datetime':
        return t_bf_dt  # 返回datetime格式
    elif t_format == 'date':
        return t_bf_dt.date()  # 返回date格式
    else:
        return datetime.strftime(t_bf_dt, '%Y%m%d')  # 返回str格式
