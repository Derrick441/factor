import pandas as pd
import time
import os


class FactorCombine(object):

    def __init__(self, factor_indir, factor_names, ic_indir):
        self.factor_indir = factor_indir
        self.factor_names = factor_names
        self.ic_indir = ic_indir

    def filein(self):

        # 读入因子数据
        t = time.time()
        for i in range(len(self.factor_names)):
            # 读入第一个因子
            if i == 0:
                self.factor = pd.read_pickle(self.factor_indir + self.factor_names[i])
                self.factor.set_index(['trade_dt', 's_info_windcode'], inplace=True)
            # 合并后续的因子
            else:
                temp = pd.read_pickle(self.factor_indir + self.factor_names[i])
                temp.set_index(['trade_dt', 's_info_windcode'], inplace=True)
                self.factor = pd.concat([self.factor, temp], axis=1)
        print('factors read in:%10.4fs' % (time.time()-t))

        # 读入IC数据
        t = time.time()
        ic_1 = []
        ic_5 = []
        ic_10 = []
        ic_20 = []
        ic_60 = []
        for i in range(len(self.factor_names)):
            temp = pd.read_pickle(self.ic_indir + 'IC_' + self.factor_names[i])
            ic_1.append(temp.ret_1)
            ic_5.append(temp.ret_5)
            ic_10.append(temp.ret_10)
            ic_20.append(temp.ret_20)
            ic_60.append(temp.ret_60)
        self.ic_1 = pd.concat(ic_1, axis=1).sort_index().reset_index().rename(columns={'index': 'trade_dt'})
        self.ic_5 = pd.concat(ic_5, axis=1).sort_index().reset_index().rename(columns={'index': 'trade_dt'})
        self.ic_10 = pd.concat(ic_10, axis=1).sort_index().reset_index().rename(columns={'index': 'trade_dt'})
        self.ic_20 = pd.concat(ic_20, axis=1).sort_index().reset_index().rename(columns={'index': 'trade_dt'})
        self.ic_60 = pd.concat(ic_60, axis=1).sort_index().reset_index().rename(columns={'index': 'trade_dt'})
        print('ics read in:%10.4fs' % (time.time() - t))

    def compute(self):
        t = time.time()
        # ***等权均值**
        self.mean = self.factor.mean(axis=1).reset_index().rename(columns={0: 'mean'})
        print('mean compute running:%10.4fs' % (time.time() - t))

        # ***加权均值***
        self.factor_reset = self.factor.reset_index()
        # ret1的ic作为权重
        t = time.time()
        self.ic_1_merge = pd.merge(self.factor_reset[['trade_dt', 's_info_windcode']], self.ic_1, how='left')
        self.ic_1_merge.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        self.mean_weight_ic1 = (self.factor * self.ic_1_merge.values).mean(axis=1)\
                                                                     .reset_index()\
                                                                     .rename(columns={0: 'mean_ic_ret1'})
        print('mean1 compute running:%10.4fs' % (time.time() - t))

        # ret5的ic作为权重
        t = time.time()
        self.ic_5_merge = pd.merge(self.factor_reset[['trade_dt', 's_info_windcode']], self.ic_5, how='left')
        self.ic_5_merge.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        self.mean_weight_ic5 = (self.factor * self.ic_5_merge.values).mean(axis=1)\
                                                                     .reset_index()\
                                                                     .rename(columns={0: 'mean_ic_ret5'})
        print('mean5 compute running:%10.4fs' % (time.time() - t))

        # ret10的ic作为权重
        t = time.time()
        self.ic_10_merge = pd.merge(self.factor_reset[['trade_dt', 's_info_windcode']], self.ic_10, how='left')
        self.ic_10_merge.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        self.mean_weight_ic10 = (self.factor * self.ic_10_merge.values).mean(axis=1)\
                                                                       .reset_index()\
                                                                       .rename(columns={0: 'mean_ic_ret10'})
        print('mean10 compute running:%10.4fs' % (time.time() - t))

        # ret20的ic作为权重
        t = time.time()
        self.ic_20_merge = pd.merge(self.factor_reset[['trade_dt', 's_info_windcode']], self.ic_20, how='left')
        self.ic_20_merge.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        self.mean_weight_ic20 = (self.factor * self.ic_20_merge.values).mean(axis=1)\
                                                                       .reset_index()\
                                                                       .rename(columns={0: 'mean_ic_ret20'})
        print('mean20 compute running:%10.4fs' % (time.time() - t))

        # ret60的ic作为权重
        t = time.time()
        self.ic_60_merge = pd.merge(self.factor_reset[['trade_dt', 's_info_windcode']], self.ic_60, how='left')
        self.ic_60_merge.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        self.mean_weight_ic60 = (self.factor * self.ic_60_merge.values).mean(axis=1)\
                                                                       .reset_index()\
                                                                       .rename(columns={0: 'mean_ic_ret60'})
        print('mean60 compute running:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 输出到factor文件夹的combfactor中
        indir_factor = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor_combine\\'
        self.mean.to_pickle(indir_factor + 'mean.pkl')
        self.mean_weight_ic1.to_pickle(indir_factor + 'mean_ic_ret1.pkl')
        self.mean_weight_ic5.to_pickle(indir_factor + 'mean_ic_ret5.pkl')
        self.mean_weight_ic10.to_pickle(indir_factor + 'mean_ic_ret10.pkl')
        self.mean_weight_ic20.to_pickle(indir_factor + 'mean_ic_ret20.pkl')
        self.mean_weight_ic60.to_pickle(indir_factor + 'mean_ic_ret60.pkl')
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        print('start')
        t = time.time()
        self.filein()
        self.compute()
        self.fileout()
        print('end running time:%10.4fs' % (time.time()-t))


if __name__ == '__main__':
    # 因子地址
    # factor_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor_neutral\\'
    # 因子文件名
    file_names = os.listdir(file_indir)
    # ic地址
    ic_indir = 'D:\\wuyq02\\develop\\python\\data\\performance\\ic\\'
    fc = FactorCombine(file_indir, file_names, ic_indir)
    fc.runflow()
