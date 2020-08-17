import pandas as pd
import time

# 日频率
# 估值因子：每日，根据股票估值，取估值前1/3股票作低估值组合，取估值后1/3股票作高估值组合，两组股票收益率均值之差作估值因子
class Hml(object):

    def __init__(self, indir, INDEX):
        self.indir = indir
        self.INDEX = INDEX

    def filein(self):
        t = time.time()
        self.all_data = pd.read_pickle(self.indir + self.INDEX + '/' + self.INDEX + '_dayindex.pkl')
        print('filein running time:%10.4fs' % (time.time() - t))

    def datamanage(self):
        pass

    def dayhml(self, data):
        len_3 = int(len(data) / 3)
        # 取当日流通估值最小1/3股票的收益率均值
        low = data.sort_values(by='s_val_pe_ttm').iloc[:len_3, :].change.mean()
        # 取当日流通估值最大1/3股票的收益率均值
        high = data.sort_values(by='s_val_pe_ttm').iloc[-len_3:, :].change.mean()
        # 当日估值因子
        return low - high

    def compute_hml(self):
        t = time.time()
        # 计算每日估值因子
        self.result = self.all_data.groupby('trade_dt').apply(self.dayhml).reset_index().rename(columns={0:'hml'})
        print('fileout running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        self.result.to_pickle(self.indir + 'factor' + '/f6_' + self.INDEX + '_hml.pkl')
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        print('compute start')
        t = time.time()
        self.filein()
        self.datamanage()
        self.compute_hml()
        self.fileout()
        print('compute end, running time:%10.4f' %(time.time()-t))
        return self

if __name__ == '__main__':
    indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    INDEX = 'all'
    hml = Hml(indir, INDEX)
    hml.runflow()