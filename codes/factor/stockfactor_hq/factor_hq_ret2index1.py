import pandas as pd
import numpy as np
import time


class ReturnTwoIndex1(object):

    def __init__(self, file_indir, save_indir, file_name):
        self.file_indir = file_indir
        self.save_indir = save_indir
        self.file_name = file_name

    def filein(self):
        t = time.time()
        # 输入数据
        self.all_data = pd.read_pickle(self.file_indir + self.file_name)
        print('filein using time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        # 去除nan
        self.data_dropna = self.all_data.dropna().copy()
        print('datamanage running time:%10.4fs' % (time.time() - t))

    # ******************************************************************************************************************
    def overnight_return(self, data):
        temp = data.copy()
        temp['onr'] = (temp['s_dq_open'] - temp['s_dq_preclose']) / temp['s_dq_preclose'] * 100
        return pd.DataFrame({'trade_dt': temp.trade_dt.values, 'onr': temp['onr'].values})

    def compute(self):
        t = time.time()
        # 隔夜收益计算
        self.temp_result = self.all_data.groupby('s_info_windcode') \
                                        .apply(self.overnight_return) \
                                        .reset_index()
        print('compute running time:%10.4fs' % (time.time() - t))
    # ******************************************************************************************************************

    def fileout(self):
        t = time.time()
        # 数据对齐
        self.result = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.temp_result, how='left')
        # 输出到factor文件夹的stockfactor中
        item = ['trade_dt', 's_info_windcode', 'onr']
        self.result[item].to_pickle(self.save_indir + 'factor_price_onr.pkl')
        print('fileout running time:%10.4fs' % (time.time()-t))

    def runflow(self):
        t = time.time()
        print('start')
        self.filein()
        self.datamanage()
        self.compute()
        self.fileout()
        print('finish using time:%10.4fs' % (time.time() - t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    file_name = 'all_band_price.pkl'

    # ******************************************************************************************************************
    rti1 = ReturnTwoIndex1(file_indir, save_indir, file_name)
    rti1.runflow()
    # ******************************************************************************************************************

    # 加总两个收益率为收益率动量因子
    # 读入
    factor_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    factor_name = 'factor_hq_mild.pkl'
    factor = pd.read_pickle(factor_indir + factor_name)
    # 计算rmom
    data = pd.merge(factor, rti1.result[['trade_dt', 's_info_windcode', 'onr']], how='left')
    data['rmom'] = data['mild'] + data['onr']
    # 输出
    item = ['trade_dt', 's_info_windcode', 'rmom']
    data[item].to_pickle(save_indir + 'factor_hq_rmom.pkl')
