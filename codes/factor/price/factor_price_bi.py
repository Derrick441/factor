import pandas as pd
import time


# 交易热度：特异度和调整换手率的结合
class Bi(object):

    def __init__(self, file_indir, factor_indir, save_indir, file_names):
        self.file_indir = file_indir
        self.factor_indir = factor_indir
        self.save_indir = save_indir
        self.file_names = file_names

    def filein(self):
        t = time.time()
        # 从stockfactor文件夹中取ivr、adjturnover因子数据
        self.ivr = pd.read_pickle(self.factor_indir + self.file_names[0])
        self.adt = pd.read_pickle(self.factor_indir + self.file_names[1])
        print('filein running time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        # 数据合并
        self.data = pd.merge(self.ivr, self.adt, how='left')
        # 去除空值
        self.data_dropna = self.data.dropna().copy()
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def quantiles_mean(self, data, factor1, factor2):
        temp = data.copy()
        num = len(temp)

        q_ivr = temp[factor1].rank()/num
        q_adt = temp[factor2].rank()/num

        result = pd.DataFrame({'s_info_windcode': temp.s_info_windcode, 'bi': (q_ivr + q_adt)/2})
        return result

    def compute(self):
        t = time.time()
        # 计算bi
        self.temp_result = self.data_dropna.groupby('trade_dt')\
                                           .apply(self.quantiles_mean, 'ivr', 'adjturnover')\
                                           .reset_index()
        print('compute running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 数据对齐
        self.all_data = pd.read_pickle(self.file_indir + 'all_dayindex.pkl')
        self.result = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.temp_result, how='left')
        # 输出到factor文件夹的stockfactor中
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
        print('end running time:%10.4fs' % (time.time()-t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    factor_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    file_names = ['factor_price_ivr.pkl', 'factor_price_adjturnover.pkl']

    bi = Bi(file_indir, factor_indir, save_indir, file_names)
    bi.runflow()

# bi.filein()
# bi.datamanage()
# bi.compute()
