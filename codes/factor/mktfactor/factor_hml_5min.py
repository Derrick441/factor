import pandas as pd
import time


# 5分钟频率
# 估值因子：根据股票估值，取估值前1/3股票作低估值组合，取估值后1/3股票作高估值组合，两组股票收益率均值之差作估值因子
class Hml(object):

    def __init__(self, indir, index):
        self.indir = indir
        self.index = index

    def filein(self):
        t = time.time()
        self.all_data = pd.read_pickle(self.indir + self.index)
        print('filein running time:%10.4fs' % (time.time() - t))

    def datamanage(self):
        pass

    def minhml(self, data):
        len_3 = int(len(data) / 3)
        # 取流通市值最小1/3股票的收益率均值
        low = data.sort_values(by='pe').iloc[:len_3, :].change.mean()
        # 取流通市值最大1/3股票的收益率均值
        high = data.sort_values(by='pe').iloc[-len_3:, :].change.mean()
        # 市值因子
        return low - high

    def compute_hml(self):
        t = time.time()
        # 每5分钟市值因子
        self.result = self.all_data.groupby(['trade_dt', 'bargaintime']) \
                                   .apply(self.minhml) \
                                   .reset_index() \
                                   .rename(columns={0: 'hml'})
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 存在factor文件夹的basicfactor中
        indir_factor = 'D:\\wuyq02\\develop\\python\\data\\factor\\mktfactor\\'
        self.result.to_pickle(indir_factor + 'factor_hml_5min_' + self.index[17:21] + '.pkl')
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        print('compute start')
        t = time.time()
        self.filein()
        self.datamanage()
        self.compute_hml()
        self.fileout()
        print('compute end, running time:%10.4f' % (time.time() - t))
        return self


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    file_index = ['all_store_hqdata_2012_5_derive.pkl']
    # file_index = ['all_store_hqdata_2012_5_derive.pkl', 'all_store_hqdata_2013_5_derive.pkl',
    #               'all_store_hqdata_2014_5_derive.pkl', 'all_store_hqdata_2015_5_derive.pkl',
    #               'all_store_hqdata_2016_5_derive.pkl', 'all_store_hqdata_2017_5_derive.pkl',
    #               'all_store_hqdata_2018_5_derive.pkl', 'all_store_hqdata_2019_5_derive.pkl',
    #               'all_store_hqdata_2020_5_derive.pkl']
    for i in file_index:
        print(i[17:21])
        hml = Hml(file_indir, i)
        hml.runflow()
