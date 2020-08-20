import pandas as pd
import time


class RetManage(object):

    def __init__(self, indir_r):
        self.indir_r = indir_r

    def filein(self):
        t = time.time()
        # 从dataflow文件夹中读取return数据
        self.ret_1 = pd.read_pickle(self.indir_r + 'all_band_price_label1.pkl')
        self.ret_5 = pd.read_pickle(self.indir_r + 'all_band_price_label5.pkl')
        self.ret_10 = pd.read_pickle(self.indir_r + 'all_band_price_label10.pkl')
        self.ret_20 = pd.read_pickle(self.indir_r + 'all_band_price_label20.pkl')
        self.ret_60 = pd.read_pickle(self.indir_r + 'all_band_price_label60.pkl')
        print('filein running time:%10.4fs' % (time.time() - t))

    def datamange(self):
        t = time.time()
        temp1 = self.ret_1.reset_index().rename(columns={0: 'ret_1'})
        temp5 = self.ret_5.reset_index().rename(columns={0: 'ret_5'})
        temp10 = self.ret_10.reset_index().rename(columns={0: 'ret_10'})
        temp20 = self.ret_20.reset_index().rename(columns={0: 'ret_20'})
        temp60 = self.ret_60.reset_index().rename(columns={0: 'ret_60'})

        data = pd.merge(temp1, temp5, how='left')
        data = pd.merge(data, temp10, how='left')
        data = pd.merge(data, temp20, how='left')
        data = pd.merge(data, temp60, how='left')

        self.out = data
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        self.out.to_pickle(self.indir_r + 'all_ret_sum.pkl')
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        self.filein()
        self.datamange()
        self.fileout()


if __name__ == '__main__':
    indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    ret_manage = RetManage(indir)
    ret_manage.runflow()
