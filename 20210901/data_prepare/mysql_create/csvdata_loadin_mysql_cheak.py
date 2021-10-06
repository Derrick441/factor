import numpy as np
import numba as nb
import pandas as pd
from operation_time import print_execute_time
import pymysql


# 查看部分数据
@print_execute_time
def cheak_part():
    conn = pymysql.connect(host='localhost', user='root', password='123456', database='mysql')
    sql_read = 'select * from data_day where symbol="000001.SZ"'
    try:
        data = pd.read_sql(sql_read, conn)
    except Exception as e:
        print(e)


# 删除整个表
@print_execute_time
def delete_table(table_name):
    conn = pymysql.connect(host='localhost', user='root', password='123456', database='mysql')
    sql_drop = 'drop table if exists '+table_name
    cur = conn.cursor()
    try:
        cur.execute(sql_drop)
    except Exception as e:
        print(e)


if __name__ == '__main__':

    cheak_part()

    # delete_table('vpd_rev')
