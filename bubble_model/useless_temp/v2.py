import pandas as pd
import numpy as np
from datetime import datetime
from LPPLS_WYQ import bl_compute, bl_compute_new, fix_compute, lppls_picture
import os
import warnings
warnings.filterwarnings("ignore")


if __name__ == '__main__':
    data_get = 'excel'
    stock = '600702'
    t1 = '20200401'
    t2 = '21000101'
    process_num = 3
    compute_num = 100
    mode = 't3'
    file_dir = 'C:\\Users\\Administrator\\Desktop\\'

    # 遍历计算
    t = datetime.now()
    fix_compute(data_get, stock, t1, t2, process_num, compute_num, file_dir)
    lppls_picture(stock, t1, t2, file_dir, mode, compute_num)
    print(datetime.now() - t)
