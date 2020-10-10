import pandas as pd
import time


# 5分钟频率
# 市值因子：每日，根据股票市值，取市值前1/3股票作小市值组合，取市值后1/3股票作大市值组合，两组股票收益率均值之差作市值因子
class TwoFactor(object):

    def __init__(self, file_indir, save_indir, file_name):
        self.file_indir = file_indir
        self.save_indir = save_indir
        self.file_name = file_name

    def filein(self):
        t = time.time()
        self.all_data = pd.read_pickle(self.file_indir + self.file_name)
        print('filein running time:%10.4fs' % (time.time() - t))

    def datamanage(self):
        pass

    def minute_factor(self, data, index):
        num = int(len(data) / 3)
        # 取流通市值/估值最小1/3股票的收益率均值
        low = data.sort_values(by=index).iloc[:num, :].change.mean()
        # 取流通市值/估值最大1/3股票的收益率均值
        high = data.sort_values(by=index).iloc[-num:, :].change.mean()
        # 市值因子
        return low - high

    def compute(self):
        t = time.time()
        # 每5分钟市值因子
        self.result1 = self.all_data.groupby(['trade_dt', 'bargaintime'])\
                                    .apply(self.minute_factor, 'mv')\
                                    .reset_index()\
                                    .rename(columns={0: 'smb'})
        # 每5分钟估值因子
        self.result2 = self.all_data.groupby(['trade_dt', 'bargaintime'])\
                                    .apply(self.minute_factor, 'pe')\
                                    .reset_index()\
                                    .rename(columns={0: 'hml'})
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 数据输出
        self.result1.to_pickle(self.save_indir + 'factor_smb_5min_' + self.file_name[17:21] + '.pkl')
        self.result2.to_pickle(self.save_indir + 'factor_hml_5min_' + self.file_name[17:21] + '.pkl')
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        print('start')
        t = time.time()
        self.filein()
        self.datamanage()
        self.compute()
        self.fileout()
        print('finish using time:%10.4f' % (time.time()-t))
        return self


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\mktfactor\\'
    file_names = ['all_store_hqdata_2012_5_derive.pkl', 'all_store_hqdata_2013_5_derive.pkl',
                  'all_store_hqdata_2014_5_derive.pkl', 'all_store_hqdata_2015_5_derive.pkl',
                  'all_store_hqdata_2016_5_derive.pkl', 'all_store_hqdata_2017_5_derive.pkl',
                  'all_store_hqdata_2018_5_derive.pkl', 'all_store_hqdata_2019_5_derive.pkl']

    for file_name in file_names:
        print(file_name)
        tf = TwoFactor(file_indir, save_indir, file_name)
        tf.runflow()
