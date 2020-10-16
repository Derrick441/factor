import pandas as pd
import numpy as np
import time


# 收益的动量成分和反转成分：动量成分=对数（隔夜收益+日内温和收益之和），反转成分=对数（日内极端收益之和）
class ReturnThreeIndexD(object):

    def __init__(self, file_indir, save_indir, file_name):
        self.file_indir = file_indir
        self.save_indir = save_indir
        self.file_name = file_name

    def filein(self):
        t = time.time()
        # 输入股价和成交量数据
        self.data = pd.read_pickle(self.file_indir + self.file_name)
        print('filein using time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        # 去除nan
        self.data_dropna = self.data.dropna().copy()
        print('datamanage running time:%10.4fs' % (time.time() - t))

    # 日内收益分解
    def decompose(self, data):
        temp = data.copy()

        median = np.median(temp['change'])
        temp['temp'] = temp['change'].apply(lambda x: abs(x-median))
        MAD = np.median(temp['temp'])
        MADe = 1.483*MAD
        MADE = 1.96*MADe

        temp['flag'] = temp['change'].apply(lambda x: 1 if abs(x-median) > MADE else 0)
        mild = np.sum(temp['change'][temp['flag'] == 0])
        extreme = np.sum(temp['change'][temp['flag'] == 1])

        return mild, extreme

    def compute(self):
        t = time.time()
        # 计算arpp
        self.result = self.data_dropna.groupby(['s_info_windcode', 'trade_dt'])\
                                      .apply(self.decompose)\
                                      .apply(pd.Series)\
                                      .reset_index()\
                                      .rename(columns={0: 'mild', 1: 'rrev'})
        print('factor_compute using time:%10.4fs' % (time.time()-t))

    def fileout(self):
        t = time.time()
        # 数据输出
        item1 = ['trade_dt', 's_info_windcode', 'mild']
        self.result[item1].to_pickle(self.save_indir + 'factor_hq_mild_' + self.file_name[17:21] + '.pkl')
        item2 = ['trade_dt', 's_info_windcode', 'rrev']
        self.result[item2].to_pickle(self.save_indir + 'factor_hq_rrev_' + self.file_name[17:21] + '.pkl')
        print('fileout using time:%10.4fs' % (time.time() - t))

    def runflow(self):
        t = time.time()
        print('start')
        self.filein()
        self.datamanage()
        self.compute()
        self.fileout()
        print('finish using time:%10.4fs' % (time.time()-t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\annual_factor\\'
    file_names = ['all_store_hqdata_2012_5_derive.pkl', 'all_store_hqdata_2013_5_derive.pkl',
                  'all_store_hqdata_2014_5_derive.pkl', 'all_store_hqdata_2015_5_derive.pkl',
                  'all_store_hqdata_2016_5_derive.pkl', 'all_store_hqdata_2017_5_derive.pkl',
                  'all_store_hqdata_2018_5_derive.pkl', 'all_store_hqdata_2019_5_derive.pkl']

    for file_name in file_names:
        print(file_name)

        rtid = ReturnThreeIndexD(file_indir, save_indir, file_name)
        rtid.runflow()

    def merge_data(factor_name, names):
        # 分开数据读取、合并
        indir1 = 'D:\\wuyq02\\develop\\python\\data\\factor\\annual_factor\\'
        data_sum = []
        for name in names:
            data_sum.append(pd.read_pickle(indir1 + name))
        temp_result = pd.concat(data_sum)
        # 合并数据对齐、输出
        all_data = pd.read_pickle(file_indir + 'all_dayindex.pkl')
        result = pd.merge(all_data[['trade_dt', 's_info_windcode']], temp_result, how='left')
        item = ['trade_dt', 's_info_windcode', factor_name]
        indir2 = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
        result[item].to_pickle(indir2 + 'factor_hq_' + factor_name + '.pkl')

    factor_name = 'mild'
    names = ['factor_hq_' + factor_name + '_' + str(i) + '.pkl' for i in range(2012, 2020)]
    merge_data(factor_name, names)

    factor_name = 'rrev'
    names = ['factor_hq_' + factor_name + '_' + str(i) + '.pkl' for i in range(2012, 2020)]
    merge_data(factor_name, names)

    # 收益率动量因子：温和收益率+隔夜收益
    # 读入
    factor1 = pd.read_pickle('D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\' + 'factor_hq_mild.pkl')
    factor2 = pd.read_pickle('D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\' + 'factor_price_onr.pkl')
    # 计算rmom
    data = pd.merge(factor1, factor2, how='left')
    data['rmom'] = data['mild'] + data['onr']
    # 输出
    item = ['trade_dt', 's_info_windcode', 'rmom']
    data[item].to_pickle(save_indir + 'factor_hq_rmom.pkl')
