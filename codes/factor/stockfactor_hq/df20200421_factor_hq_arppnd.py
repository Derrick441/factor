import pandas as pd
import numpy as np
import time


# arpp时间加权平均的相对价格位置: （p-l)/(h-l)在时间上的积分
# 若交易时间合计为1，则（p-l)/(h-l)*delta t等于（p-l)/(h-l)*1/n，n为微小单位的数量
# （p-l)/(h-l)在时间上的积分等价于n个（p-l)/(h-l)的平均值
class ArppNd(object):

    def __init__(self, file_indir1, file_indir2, save_indir, file_name1, file_name2):
        self.file_indir1 = file_indir1
        self.file_indir2 = file_indir2
        self.save_indir = save_indir
        self.file_name1 = file_name1
        self.file_name2 = file_name2
        print(self.file_name2)

    def filein(self):
        t = time.time()
        self.all_data = pd.read_pickle(self.file_indir1 + self.file_name1)
        self.data = pd.read_pickle(self.file_indir2 + self.file_name2)
        print('filein using time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        self.data_dropna = self.data.dropna().copy()
        print('datamanage using time:%10.4fs' % (time.time() - t))

    def method(self, data, perid):
        temp = data.copy()
        name = 'arpp' + str(perid) + 'd'
        temp['roll_twap'] = temp['twap'].rolling(perid).apply(lambda x: np.mean(x))
        temp['roll_L'] = temp['L'].rolling(perid).apply(lambda x: np.min(x))
        temp['roll_H'] = temp['H'].rolling(perid).apply(lambda x: np.max(x))

        temp['temp1'] = temp['roll_twap'] - temp['roll_L']
        temp['temp2'] = temp['roll_H'] - temp['roll_L']
        temp['temp2'] = temp['temp2'].replace(0, np.nan)
        temp[name] = temp['temp1'] / temp['temp2']

        result = temp[['trade_dt', name]].copy()
        return result

    def compute(self):
        t = time.time()
        tempdata = self.data_dropna.copy()
        tempdata['arpp1d'] = (tempdata['twap'] - tempdata['L'])/(tempdata['H'] - tempdata['L'])
        self.temp_result1 = tempdata.copy()
        self.temp_result5 = self.data_dropna.groupby('s_info_windcode')\
                                            .apply(self.method, 5)\
                                            .reset_index()
        self.temp_result20 = self.data_dropna.groupby('s_info_windcode')\
                                             .apply(self.method, 20)\
                                             .reset_index()
        print('factor_compute using time:%10.4fs' % (time.time()-t))

    def fileout(self):
        t = time.time()
        self.result1 = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.temp_result1, how='left')
        item = ['trade_dt', 's_info_windcode', 'arpp1d']
        self.result1[item].to_pickle(self.save_indir + 'factor_hq_arpp1d.pkl')

        self.result5 = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.temp_result5, how='left')
        item = ['trade_dt', 's_info_windcode', 'arpp5d']
        self.result5[item].to_pickle(self.save_indir + 'factor_hq_arpp5d.pkl')

        self.result20 = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.temp_result20, how='left')
        item = ['trade_dt', 's_info_windcode', 'arpp20d']
        self.result20[item].to_pickle(self.save_indir + 'factor_hq_arpp20d.pkl')
        print('fileout using time:%10.4fs' % (time.time()-t))

    def runflow(self):
        t = time.time()
        print('start')
        self.filein()
        self.datamanage()
        self.compute()
        self.fileout()
        print('finish using time:%10.4fs' % (time.time() - t))


if __name__ == '__main__':
    file_indir1 = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    file_indir2 = 'D:\\wuyq02\\develop\\python\\data\\factor\\annual_factor\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    file_name1 = 'all_dayindex.pkl'
    file_name2 = 'factor_hq_arpp.pkl'

    arppnd = ArppNd(file_indir1, file_indir2, save_indir, file_name1, file_name2)
    arppnd.runflow()
