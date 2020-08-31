import pandas as pd
import time

# 日频率
# 动量因子：每日，根据股票收益率，取收益率前1/3股票作低收益组合，取收益率后1/3股票作高收益组合，两组股票收益率均值之差作动量因子


class Mom(object):

    def __init__(self, indir, index):
        self.indir = indir
        self.index = index

    def filein(self):
        t = time.time()
        self.all_data = pd.read_pickle(self.indir + self.index + '/' + self.index + '_dayindex.pkl')
        print('filein running time:%10.4fs' % (time.time() - t))

    def datamanage(self):
        pass

    def daymom(self, data):
        len_3 = int(len(data) / 3)
        # 取当日收益率最小1/3股票的收益率均值
        low = data.sort_values(by='change').iloc[:len_3, :].change.mean()
        # 取当日收益率最大1/3股票的收益率均值
        high = data.sort_values(by='change').iloc[-len_3:, :].change.mean()
        # 当日动量因子
        return low - high

    def compute_mom(self):
        t = time.time()
        # 计算每日动量因子
        self.result = self.all_data.groupby('trade_dt').apply(self.daymom).reset_index().rename(columns={0: 'mom'})
        print('fileout running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 存在factor文件夹的basicfactor中
        indir_factor = 'D:\\wuyq02\\develop\\python\\data\\factor\\mktfactor\\'
        self.result.to_pickle(indir_factor + 'factor_mom.pkl')
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        print('compute start')
        t = time.time()
        self.filein()
        self.datamanage()
        self.compute_mom()
        self.fileout()
        print('compute end, running time:%10.4f' % (time.time()-t))
        return self


if __name__ == '__main__':
    indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    index = 'all'
    mom = Mom(indir, index)
    mom.runflow()
