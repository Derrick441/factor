import numpy as np
import numba as nb
import pandas as pd
from operation_time import print_execute_time
import pymysql
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR, Date, FLOAT


# MySQL中读取数据
# @print_execute_time
def data_from_mysql(sql):
    conn = pymysql.connect(host='localhost', user='root', password='123456', database='mysql')
    data = pd.read_sql(sql, conn)
    conn.close()
    print('data memory usage: ', np.sum(data.memory_usage()) / (1024**3))
    return data


# 因子数据读入MySQL
@print_execute_time
def data_to_mysql(save, factors, factor_table, factor_name):
    if save:
        engine = create_engine('mysql+pymysql://root:123456@localhost:3306/mysql?charset=utf8')
        dtypes = {
            'stock': NVARCHAR(10),
            'date': Date,
            'factor_name': NVARCHAR(20),
            'factor_type': NVARCHAR(20),
            'factor_value': FLOAT,
            'compute_time': NVARCHAR(40),
        }
        factors.to_sql(factor_table, engine, if_exists='append', dtype=dtypes, index=False)
        print('data memory usage: ', np.sum(factors.memory_usage()) / (1024 ** 3))
        print(factor_name + ' save to mysql finish')
    else:
        print(factor_name + ' not save to mysql')


if __name__ == '__main__':

    # sql语句
    t1, t2 = '20190501', '20190601'
    sql = '''
    select
    symbol as stock,date,close,adjustfactor,pe
    from
    data_day
    where
    (date>=str_to_date({},'%Y%m%d')) and (date<=str_to_date({},'%Y%m%d'))
    '''.format(t1, t2)

    # 数据读取
    data_raw = data_from_mysql(sql)

'''
# # 数据读取1，pymysql的connect+pd.read_sql
# conn = pymysql.connect(host='localhost', user='root', password='123456', database='mysql')
# data = pd.read_sql(sql, conn)
# conn.close()
# # 数据读取2，pymysql的connect的cursor方法，速度没有提升
# conn = pymysql.connect(host='localhost', user='root', password='123456', database='mysql')
# cursor = conn.cursor()
# cursor.execute(sql)
# data_cols = [i[0] for i in cursor.description]
# data_data = cursor.fetchall()
# data = pd.DataFrame(data_data, columns=data_cols)
# cursor.close()
# conn.close()
# # 数据读取3，sqlalchemy的create_engine方法创建engine替代conn+pd.read_sql，但速度没有提升，且有一些语法影响，不采用
# engine = create_engine('mysql+pymysql://root:123456@localhost:3306/mysql?charset=utf8')
# data = pd.read_sql(sql, engine) # engine的效果与engine.connect()的效果接近
# # 数据读取4：sqlalchemy的create_engine的raw_connection替代conn，可以采用游标法
# engine = create_engine('mysql+pymysql://root:123456@localhost:3306/mysql?charset=utf8')
# conn = engine.raw_connection()

# 数据从MySQL数据库中读取，pymysql较为方便，采用pymysql的方法
# 数据读入MySQL数据库中，sqlalchemy比pymysql更方便，采用sqlalchemy的方法

# # Oracle的连接方式
# import cx_Oracle
# conn = cx_Oracle.connect('database', 'password', 'ip/service_name')
# data = pd.read_sql(sql, conn)
# conn.close()

# # sqlserver的连接方式
# sql = """
# select
# innercode,secucode,secuabbr
# from
# secumain
# where
# secucategory=1 and (secumarket=83 or secumarket=90) and listedsector=1
# """
# import pyodbc
# info = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=10.2.47.124;DATABASE=JYDB;UID=syzx2020;PWD=syzx2020;PORT=1433"
# conn = pyodbc.connect(info)
# data = pd.read_sql(sql, conn)
# conn.close()
'''
