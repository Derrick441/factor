import pandas as pd
import numpy as np
import time


# 因子：高频量价相关性因子（均值、标准差）
class FactorPV(object):

    def __init__(self, file_indir, save_indir, file_name):
        self.file_indir = file_indir
        self.save_indir = save_indir
        self.file_name = file_name
        print(self.file_name)

    def filein(self):
        t = time.time()
        self.data = pd.read_pickle(self.file_indir + self.file_name)
        print('filein using time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        self.data_dropna = self.data.dropna().copy()
        print('datamanage using time:%10.4fs' % (time.time()-t))

    def compute(self):
        t = time.time()
        self.result = self.data_dropna.groupby(['s_info_windcode', 'trade_dt'])\
                                      .apply(lambda x: x[['closeprice', 'volume']].corr().iloc[0, -1])\
                                      .reset_index()\
                                      .rename(columns={0: 'pvcorr'})
        self.result['pvcorravg20'] = self.result.groupby(['s_info_windcode'])\
                                                .apply(lambda x: x['pvcorr'].rolling(20).mean())\
                                                .values
        self.result['pvcorrstd20'] = self.result.groupby(['s_info_windcode'])\
                                                .apply(lambda x: x['pvcorr'].rolling(20).std())\
                                                .values
        self.result['pvcorravg10'] = self.result.groupby(['s_info_windcode'])\
                                                .apply(lambda x: x['pvcorr'].rolling(10).mean())\
                                                .values
        self.result['pvcorrstd10'] = self.result.groupby(['s_info_windcode'])\
                                                .apply(lambda x: x['pvcorr'].rolling(10).std())\
                                                .values
        self.result['pvcorravg5'] = self.result.groupby(['s_info_windcode'])\
                                               .apply(lambda x: x['pvcorr'].rolling(5).mean())\
                                               .values
        self.result['pvcorrstd5'] = self.result.groupby(['s_info_windcode'])\
                                               .apply(lambda x: x['pvcorr'].rolling(5).std())\
                                               .values
        print('compute using time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        item1 = ['trade_dt', 's_info_windcode', 'pvcorravg20']
        self.result[item1].to_pickle(self.save_indir + 'factor_hq_pvcorravg20_' + self.file_name[17:21] + '.pkl')

        item2 = ['trade_dt', 's_info_windcode', 'pvcorrstd20']
        self.result[item2].to_pickle(self.save_indir + 'factor_hq_pvcorrstd20_' + self.file_name[17:21] + '.pkl')

        item1 = ['trade_dt', 's_info_windcode', 'pvcorravg10']
        self.result[item1].to_pickle(self.save_indir + 'factor_hq_pvcorravg10_' + self.file_name[17:21] + '.pkl')

        item2 = ['trade_dt', 's_info_windcode', 'pvcorrstd10']
        self.result[item2].to_pickle(self.save_indir + 'factor_hq_pvcorrstd10_' + self.file_name[17:21] + '.pkl')

        item1 = ['trade_dt', 's_info_windcode', 'pvcorravg5']
        self.result[item1].to_pickle(self.save_indir + 'factor_hq_pvcorravg5_' + self.file_name[17:21] + '.pkl')

        item2 = ['trade_dt', 's_info_windcode', 'pvcorrstd5']
        self.result[item2].to_pickle(self.save_indir + 'factor_hq_pvcorrstd5_' + self.file_name[17:21] + '.pkl')
        print('fileout using time:%10.4fs' % (time.time() - t))

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
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\annual_factor\\'
    file_names = ['all_store_hqdata_2012_5.pkl', 'all_store_hqdata_2013_5.pkl',
                  'all_store_hqdata_2014_5.pkl', 'all_store_hqdata_2015_5.pkl',
                  'all_store_hqdata_2016_5.pkl', 'all_store_hqdata_2017_5.pkl',
                  'all_store_hqdata_2018_5.pkl', 'all_store_hqdata_2019_5.pkl']

    for file_name in file_names:
        fpv = FactorPV(file_indir, save_indir, file_name)
        fpv.runflow()

    def merge_data(factor_name, names):
        readin_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\annual_factor\\'
        saveout_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
        # 数据读取合并
        data_sum = []
        for name in names:
            data_sum.append(pd.read_pickle(readin_indir + name))
        temp_result = pd.concat(data_sum)
        # 数据对齐
        all_data = pd.read_pickle(file_indir + 'all_dayindex.pkl')
        result = pd.merge(all_data[['trade_dt', 's_info_windcode']], temp_result, how='left')
        # 数据输出
        item = ['trade_dt', 's_info_windcode', factor_name]
        result[item].to_pickle(saveout_indir + 'factor_hq_' + factor_name + '.pkl')

    factor_name = 'pvcorravg20'
    names = ['factor_hq_' + factor_name + '_' + str(i) + '.pkl' for i in range(2012, 2020)]
    merge_data(factor_name, names)

    factor_name = 'pvcorrstd20'
    names = ['factor_hq_' + factor_name + '_' + str(i) + '.pkl' for i in range(2012, 2020)]
    merge_data(factor_name, names)

    factor_name = 'pvcorravg10'
    names = ['factor_hq_' + factor_name + '_' + str(i) + '.pkl' for i in range(2012, 2020)]
    merge_data(factor_name, names)

    factor_name = 'pvcorrstd10'
    names = ['factor_hq_' + factor_name + '_' + str(i) + '.pkl' for i in range(2012, 2020)]
    merge_data(factor_name, names)

    factor_name = 'pvcorravg5'
    names = ['factor_hq_' + factor_name + '_' + str(i) + '.pkl' for i in range(2012, 2020)]
    merge_data(factor_name, names)

    factor_name = 'pvcorrstd5'
    names = ['factor_hq_' + factor_name + '_' + str(i) + '.pkl' for i in range(2012, 2020)]
    merge_data(factor_name, names)
