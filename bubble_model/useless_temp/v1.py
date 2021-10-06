import pandas as pd
import numpy as np
from datetime import datetime
from LPPLS_WYQ import bl_compute, bl_compute_new, fix_compute, lppls_picture
import os
import warnings
warnings.filterwarnings("ignore")


if __name__ == '__main__':
    stock, t1, t2, mode = input('请输入：股票 开始时间 结束时间 模式（空格隔开）\n').split()

    data_get = 'excel'
    # stock = '600702'
    # t1 = '20200401'
    # t2 = '21000101'
    process_num = 3
    compute_num = 50
    # mode = 't1'
    # mode = 't2'
    keep_num = 30
    max_time = 3
    bl = 1
    file_dir = 'C:\\Users\\Administrator\\Desktop\\'

    # 遍历计算
    t = datetime.now()
    print('计算开始')
    bl_compute_new(data_get, stock, t1, t2, keep_num, max_time, process_num, compute_num, mode, bl, file_dir)
    lppls_picture(stock, t1, t2, file_dir, mode, compute_num)
    print('计算结束', mode, datetime.now() - t)
