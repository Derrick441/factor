import pandas as pd
import time


# 日频率
# 交易热度：特质波动率和特异度的组合
class Bi(object):

    # def __init__(self, indir, index):
    #     self.indir = indir
    #     self.index = index

    def filein(self):
        t = time.time()
        # 从factor文件夹中取因子数据
        indir_factor = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
        self.ivr = pd.read_pickle(indir_factor + 'factor_price_ivr.pkl')
        self.turnoveradj = pd.read_pickle(indir_factor + 'factor_price_turnoveradj.pkl')
        print('filein running time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        # 数据合并
        self.data = pd.merge(self.ivr, self.turnoveradj, how='left')
        # 设index
        self.data.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        # 去除nan
        self.data.dropna(inplace=True)
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def quantile_add(self, data):
        data_len = len(data)
        q_ivr = data.ivr.rank()/data_len
        q_turnoveradj = data.turnoveradj/data_len
        bi = (q_ivr + q_turnoveradj)/2
        return bi

    def compute_bi(self):
        t = time.time()
        self.data['bi'] = self.data.groupby(level=0).apply(self.quantile_add).values
        self.result = self.data.reset_index()
        print('compute_bi running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 存在factor文件夹的stockfactor中
        item = ['trade_dt', 's_info_windcode', 'bi']
        indir_factor = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
        self.result[item].to_pickle(indir_factor + 'factor_price_bi.pkl')
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        self.filein()
        self.datamanage()
        self.compute_bi()
        self.fileout()


if __name__ == '__main__':
    bi = Bi()
    bi.runflow()
