import pandas as pd
import numpy as np
import time


# 因子：聪明钱因子基于5分钟数据,10天,对数
class FactorSmartMoneyln510(object):

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
        self.data['ret'] = (self.data['closeprice'] / self.data['openprice'] - 1) * 100
        self.data_drop = self.data[self.data.volume >= 100].copy()
        self.data_drop['lnv'] = np.log(self.data_drop['volume'])
        self.data_drop['Sln'] = self.data_drop['ret'] / self.data_drop['lnv']
        print('datamanage using time:%10.4fs' % (time.time()-t))

    def method(self, data, perid):
        t = time.time()
        temp = data.copy()
        date = temp.trade_dt.drop_duplicates()
        num = len(date)
        if num >= perid:
            x = perid-1
            Q1 = [np.nan] * x
            for i in range(x, num):
                temp_part = temp[(temp.trade_dt >= date.iloc[i-x]) & (temp.trade_dt <= date.iloc[i])]
                temp_data = temp_part.sort_values(by='Sln', ascending=False).copy()
                temp_data['cumsum'] = temp_data['volume'].cumsum() / np.sum(temp_data['volume'])
                smartmoney = temp_data[temp_data['cumsum'] <= 0.2].copy()
                VWAPsmart = np.sum(smartmoney.volume / np.sum(smartmoney.volume) * smartmoney.closeprice)
                VWAPall = np.sum(temp_data.volume / np.sum(temp_data.volume) * temp_data.closeprice)
                Q1.append(VWAPsmart / VWAPall)
            print(time.time() - t)
            return pd.DataFrame({'trade_dt': date,
                                 'smartmln510': Q1})
        else:
            return pd.DataFrame({'trade_dt': date,
                                 'smartmln510': [None for i in range(num)]})

    def compute(self):
        t = time.time()
        self.result = self.data_drop.groupby(['s_info_windcode'])\
                                    .apply(self.method, 10)\
                                    .reset_index()
        print('compute using time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        item1 = ['trade_dt', 's_info_windcode', 'smartmln510']
        self.result[item1].to_pickle(self.save_indir + 'factor_hq_smartmln510_' + self.file_name[17:21] + '.pkl')
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
        fsm5 = FactorSmartMoneyln510(file_indir, save_indir, file_name)
        fsm5.runflow()

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

    factor_name = 'smartmln510'
    names = ['factor_hq_' + factor_name + '_' + str(i) + '.pkl' for i in range(2012, 2020)]
    merge_data(factor_name, names)
