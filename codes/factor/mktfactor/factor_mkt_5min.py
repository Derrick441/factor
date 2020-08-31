import pandas as pd
import time

# 日频率
# 市场因子：中证500日收益率


class Mkt(object):

    def __init__(self, indir, index):
        self.indir = indir
        self.index = index

    def filein(self):
        self.mkt_price = pd.read_pickle(self.indir + self.index + '/' + self.index + '_indexprice.pkl')

    def mkt(self):
        self.mkt_reset = self.mkt_price.reset_index()
        # 指数点位 转 指数涨跌幅
        self.mkt_reset['mkt'] = self.mkt_reset.s_dq_change / self.mkt_reset.s_dq_preclose * 100
        # 结果
        item = ['trade_dt', 'mkt']
        self.result = self.mkt_reset[item]

    def fileout(self):
        t = time.time()
        # 存在factor文件夹的basicfactor中
        indir_factor = 'D:\\wuyq02\\develop\\python\\data\\factor\\mktfactor\\'
        self.result.to_pickle(indir_factor + 'factor_mkt.pkl')
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        print('compute start')
        t = time.time()
        self.filein()
        self.mkt()
        self.fileout()
        print('compute end, running time:%10.4f' % (time.time()-t))
        return self


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    file_index = 'zz500'
    mkt = Mkt(file_indir, file_index)
    mkt.runflow()
