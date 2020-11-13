import pandas as pd
import numpy as np
import time


class Factormomo20(object):

    def __init__(self, file_indir, save_indir, file_name):
        self.file_indir = file_indir
        self.save_indir = save_indir
        self.file_name = file_name

    def filein(self):
        t = time.time()
        self.all_data = pd.read_pickle(self.file_indir + self.file_name)
        print('filein running time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        self.all_data['mom0'] = self.all_data['s_dq_open'] / self.all_data['s_dq_preclose'] - 1
        self.all_data['s_dq_freeturnover_y'] = self.all_data.groupby('s_info_windcode')['s_dq_freeturnover'].shift(1)
        self.data_dropna = self.all_data.dropna().copy()
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def method(self, data, perid, index1, index2):
        t = time.time()
        temp = data.copy()
        num = len(temp)
        if num >= perid:
            temp.sort_values(by='trade_dt', inplace=True)
            part1 = [np.nan] * (perid-1)
            part5 = [np.nan] * (perid-1)
            for i in range(perid, num+1):
                temp_data = temp.iloc[i-perid:i, :].sort_values(by=index1)
                n = int(perid / 5)
                part1.append(np.mean(temp_data[index2][:n]))
                part5.append(np.mean(temp_data[index2][-n:]))
            print(time.time() - t)
            return pd.DataFrame({'trade_dt': temp.trade_dt.values,
                                 'part1': part1,
                                 'part5': part5})
        else:
            return pd.DataFrame({'trade_dt': temp.trade_dt.values,
                                 'part1': [None for i in range(num)],
                                 'part5': [None for i in range(num)]})

    def mom_meanstd(self, data, index1, index2):
        temp = data.copy()
        return np.mean(temp[index1]), np.std(temp[index1]), np.mean(temp[index2]), np.std(temp[index2])

    def compute(self):
        t = time.time()
        item = ['s_dq_freeturnover_y', 'mom0']
        self.overnight_data = self.data_dropna.groupby('s_info_windcode')\
                                              .apply(self.method, 20, item[0], item[1])\
                                              .reset_index()
        # 横截面均值、标准差
        item = ['part1', 'part5']
        self.overnight_meanstd = self.overnight_data.groupby('trade_dt')\
                                                    .apply(self.mom_meanstd, item[0], item[1])\
                                                    .apply(pd.Series)\
                                                    .reset_index()\
                                                    .rename(columns={0: 'part1_mean', 1: 'part1_std',
                                                                     2: 'part5_mean', 3: 'part5_std'})
        # merge
        self.temp_result = pd.merge(self.overnight_data, self.overnight_meanstd, how='left')
        # t值
        self.temp_result['t1'] = ((self.temp_result.part1 - self.temp_result.part1_mean) / self.temp_result.part1_std)
        self.temp_result['t5'] = ((self.temp_result.part5 - self.temp_result.part5_mean) / self.temp_result.part5_std)
        self.temp_result['momo20'] = self.temp_result['t1'] - self.temp_result['t5']
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        self.result = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.temp_result, how='left')
        item = ['trade_dt', 's_info_windcode', 'momo20']
        self.result[item].to_pickle(self.save_indir + 'factor_price_momo20.pkl')
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

    momo20 = Factormomo20(file_indir, save_indir, file_name)
    momo20.runflow()
