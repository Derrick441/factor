import numpy as np
import numba as nb
import pandas as pd
from operation_time import print_execute_time
import os
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, FLOAT, NVARCHAR, Date


@print_execute_time
def csvdata_loadin_mysql(dir_file, dtypes):
    # 数据引擎
    engine = create_engine('mysql+pymysql://root:123456@localhost:3306/mysql?charset=utf8')

    # 全部csv数据读入MySQL
    files = os.listdir(dir_file)
    for i in range(len(files)):
        # 读取csv数据
        temp_raw = pd.read_csv(os.path.join(dir_file, files[i]))

        # 过大的数值除100000000
        item_adj_list = ['volume', 'volumeyuan', 'outstandingshares', 'mktfreeshares', 'totalmarketcap']
        temp_raw[item_adj_list] = temp_raw[item_adj_list] / 100000000.0
        # 去掉第一列
        temp = temp_raw.iloc[:, 1:]

        # 读入MySQL中
        try:
            temp.to_sql('data_day', engine, if_exists='append', dtype=dtypes)
        except Exception as e:
            print('problem!!!\n', e)

        # 当前csv导入完毕
        print(files[i]+' load in mysql finished')

    print('全部csv数据导入mysql完毕！')


if __name__ == '__main__':
    # 数据地址
    dir_file = 'D:\\data_day\\stock_data\\'
    # 数据格式
    dtypes = {
        'symbol': NVARCHAR(10),
        'date': Date,
        'tradingstatus': Integer,
        'open': FLOAT,
        'openrecovered': FLOAT,
        'high': FLOAT,
        'highrecovered': FLOAT,
        'low': FLOAT,
        'lowrecovered': FLOAT,
        'close': FLOAT,
        'closerecovered': FLOAT,
        'adjustfactor': FLOAT,
        'change_price': FLOAT,
        'changepct': FLOAT,
        'turnoverrate': FLOAT,
        'volume': FLOAT,
        'volumeyuan': FLOAT,
        'avgprice': FLOAT,
        'outstandingshares': FLOAT,
        'mktfreeshares': FLOAT,
        'totalmarketcap': FLOAT,
        '52weekhigh': FLOAT,
        '52weeklow': FLOAT,
        'pe': FLOAT,
        'pe_ttm': FLOAT,
        'peToSH': FLOAT,
        'pb': FLOAT,
        'ps': FLOAT,
        'ps_ttm': FLOAT,
        'est_eps': FLOAT,
        'vwaprecovered': FLOAT,
        'Maxupordown': FLOAT,
    }

    csvdata_loadin_mysql(dir_file, dtypes)
