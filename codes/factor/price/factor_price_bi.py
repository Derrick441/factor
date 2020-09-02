import pandas as pd
import time


# 日频率
# 交易热度：特异度和调整换手率的结合
class Bi(object):

    def __init__(self, file_indir, file_name):
        self.file_indir = file_indir
        self.file_name = file_name

    def filein(self):
        t = time.time()
        # 从stockfactor文件夹中取ivr、turnoveradj因子数据
        self.ivr = pd.read_pickle(self.file_indir + file_name[0])
        self.turnoveradj = pd.read_pickle(self.file_indir + file_name[1])
        print('filein running time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        # 数据合并
        self.data = pd.merge(self.ivr, self.turnoveradj, how='left')
        self.data.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        self.data.dropna(inplace=True)
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def quantile_add(self, data):
        data_len = len(data)
        q_ivr = data.ivr.rank()/data_len
        q_turnoveradj = data.turnoveradj/data_len
        result = (q_ivr + q_turnoveradj)/2
        return result

    def compute_bi(self):
        t = time.time()
        # 根据两个因子的分位数计算bi
        self.data['bi'] = self.data.groupby(level=0).apply(self.quantile_add).values
        self.data_reset = self.data.reset_index()
        print('compute_bi running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 数据对齐
        self.result = pd.merge(self.ivr[['trade_dt', 's_info_windcode']], self.data_reset, how='left')
        # 输出到factor文件夹的stockfactor中
        item = ['trade_dt', 's_info_windcode', 'bi']
        indir_factor = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
        self.result[item].to_pickle(indir_factor + 'factor_price_bi.pkl')
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        t = time.time()
        print('start')
        self.filein()
        self.datamanage()
        self.compute_bi()
        self.fileout()
        print('end running time:%10.4fs' % (time.time()-t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    file_name = ['factor_price_ivr.pkl', 'factor_price_turnoveradj.pkl']
    bi = Bi(file_indir, file_name)
    bi.runflow()
