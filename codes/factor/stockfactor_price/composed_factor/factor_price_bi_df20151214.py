import pandas as pd
import time


# 交易热度：特异度和调整换手率的结合
class Bi(object):

    def __init__(self, file_indir, factor_indir, save_indir, file_name, factor_names):
        self.file_indir = file_indir
        self.factor_indir = factor_indir
        self.save_indir = save_indir
        self.file_name = file_name
        self.factor_names = factor_names

    def filein(self):
        t = time.time()
        self.all_data = pd.read_pickle(self.file_indir + self.file_name)
        self.ivr = pd.read_pickle(self.factor_indir + self.factor_names[0])
        self.adt = pd.read_pickle(self.factor_indir + self.factor_names[1])
        print('filein running time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        self.data = pd.merge(self.ivr, self.adt, how='left')
        self.data_dropna = self.data.dropna().copy()
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def method(self, data, factor1, factor2):
        temp = data.copy()
        num = len(temp)
        q_ivr = temp[factor1].rank()/num
        q_adt = temp[factor2].rank()/num
        result = pd.DataFrame({'s_info_windcode': temp.s_info_windcode, 'bi': (q_ivr + q_adt)/2})
        return result

    def compute(self):
        t = time.time()
        self.temp_result = self.data_dropna.groupby('trade_dt')\
                                           .apply(self.method, 'ivr', 'adjturnover')\
                                           .reset_index()
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        self.result = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.temp_result, how='left')
        item = ['trade_dt', 's_info_windcode', 'bi']
        self.result[item].to_pickle(self.save_indir + 'factor_price_bi.pkl')
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        t = time.time()
        print('start')
        self.filein()
        self.datamanage()
        self.compute()
        self.fileout()
        print('finish running time:%10.4fs' % (time.time()-t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    factor_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    file_name = 'all_dayindex.pkl'
    factor_names = ['factor_price_ivr.pkl', 'factor_price_adjturnover.pkl']

    bi = Bi(file_indir, factor_indir, save_indir, file_name, factor_names)
    bi.runflow()
