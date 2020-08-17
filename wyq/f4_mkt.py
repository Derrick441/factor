
import pandas as pd
import time

# 日频率
# 市场因子：中证500日收益率
class Mkt(object):

    def __init__(self, indir, INDEX):
        self.indir = indir
        self.INDEX = INDEX

    def filein(self):
        self.mkt_price = pd.read_pickle(self.indir + self.INDEX + '/' + self.INDEX + '_indexprice.pkl')

    def mkt(self):
        self.mkt_reset = self.mkt_price.reset_index()
        # 指数点位 转 指数涨跌幅
        self.mkt_reset['mkt'] = self.mkt_reset.s_dq_change / self.mkt_reset.s_dq_preclose * 100

    def fileout(self):
        self.mkt_reset[['trade_dt', 'mkt']].to_pickle(self.indir + 'factor' + '/f4_' + self.INDEX + '_mkt.pkl')

    def runflow(self):
        print('compute start')
        t=time.time()
        self.filein()
        self.mkt()
        self.fileout()
        print('compute end, running time:%10.4f' %(time.time()-t))
        return self

if __name__ == '__main__':
    indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    INDEX = 'zz500'
    mkt = Mkt(indir, INDEX)
    mkt.runflow()

