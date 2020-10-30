import pandas as pd
import numpy as np
import time


# 抢跑因子FR：基于过去20日交易数据计算的T-1日换手率与T日涨跌幅的相关系数
class FactorFR(object):

    def __init__(self, file_indir, save_indir, file_name):
        self.file_indir = file_indir
        self.save_indir = save_indir
        self.file_name = file_name

    def filein(self):
        t = time.time()
        # 股票日数据
        self.all_data = pd.read_pickle(self.file_indir + self.file_name)
        print('filein running time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        # 昨日换手率
        self.all_data['s_dq_freeturnover_y'] = self.all_data.groupby('s_info_windcode')['s_dq_freeturnover'].shift(1)
        # 去除空值
        item = ['trade_dt', 's_info_windcode', 's_dq_pctchange', 's_dq_freeturnover_y']
        self.data_dropna = self.all_data[item].dropna().copy()
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def method(self, data, perid):
        temp = data.copy()
        # 因子算法
        factor = temp[['s_dq_pctchange', 's_dq_freeturnover_y']].rolling(perid).corr().reset_index().iloc[::2, -1]
        return pd.DataFrame({'trade_dt': temp.trade_dt, 'fr': factor.values})

    def compute(self):
        t = time.time()
        # 因子计算
        self.temp_result = self.data_dropna.groupby('s_info_windcode') \
                                           .apply(self.method, 20) \
                                           .reset_index()
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 数据对齐
        self.result = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.temp_result, how='left')
        # 输出到factor文件夹的stockfactor中
        item = ['trade_dt', 's_info_windcode', 'fr']
        self.result[item].to_pickle(self.save_indir + 'factor_price_fr.pkl')
        print('fileout running time:%10.4fs' % (time.time()-t))

    def runflow(self):
        t = time.time()
        print('start')
        self.filein()
        self.datamanage()
        self.compute()
        self.fileout()
        print('finish running time:%10.4fs' % (time.time() - t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    file_name = 'all_dayindex.pkl'

    fr = FactorFR(file_indir, save_indir, file_name)
    fr.runflow()
