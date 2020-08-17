import pandas as pd
import time

# 日频率
# 市值因子：每日，根据股票市值，取市值前1/3股票作小市值组合，取市值后1/3股票作大市值组合，两组股票收益率均值之差作市值因子
class Smb(object):

    def __init__(self, indir, INDEX):
        self.indir = indir
        self.INDEX = INDEX

    def filein(self):
        t = time.time()
        self.all_data = pd.read_pickle(self.indir + self.INDEX + '/' + self.INDEX + '_dayindex.pkl')
        print('filein running time:%10.4fs' % (time.time() - t))

    def datamanage(self):
        pass

    def daysmb(self, data):
        len_3 = int(len(data) / 3)
        # 取当日流通市值最小1/3股票的收益率均值
        low = data.sort_values(by='s_dq_mv').iloc[:len_3, :].change.mean()
        # 取当日流通市值最大1/3股票的收益率均值
        high = data.sort_values(by='s_dq_mv').iloc[-len_3:, :].change.mean()
        # 当日市值因子
        return low - high

    def compute_smb(self):
        t = time.time()
        # 计算每日市值因子
        self.result = self.all_data.groupby('trade_dt').apply(self.daysmb).reset_index().rename(columns={0:'smb'})
        print('fileout running time:%10.4fs' % (time.time() - t))


    def fileout(self):
        t = time.time()
        self.result.to_pickle(self.indir + 'factor' + '/f5_' + self.INDEX + '_smb.pkl')
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        print('compute start')
        t=time.time()
        self.filein()
        self.datamanage()
        self.compute_smb()
        self.fileout()
        print('compute end, running time:%10.4f' %(time.time()-t))
        return self

if __name__ == '__main__':
    indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    INDEX = 'all'
    smb = Smb(indir, INDEX)
    smb.runflow()