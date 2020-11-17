import pandas as pd
import numpy as np
import time


class RetrRevMom(object):

    def __init__(self, factor_indir, factor_name, perid):
        self.factor_indir = factor_indir
        self.factor_name = factor_name
        self.perid = perid

    def filein(self):
        t = time.time()
        self.factor = pd.read_pickle(self.factor_indir + self.factor_name)
        print('filein using time:%10.4fs' % (time.time() - t))

    def datamanage(self):
        t = time.time()
        self.factor.sort_values(['s_info_windcode', 'trade_dt'], inplace=True)
        self.fac_name = self.factor.columns[-1]
        print('datamanage using time:%10.4fs' % (time.time() - t))

    def mom_of_n(self, data):
        temp = data.copy()
        temp_result = 1
        for i in temp:
            temp_result = temp_result * (1+i/100)
        return (temp_result-1)*100

    def compute(self):
        t = time.time()
        self.name = self.fac_name + str(self.perid)
        self.factor[self.name] = self.factor.groupby('s_info_windcode').rolling(self.perid).apply(self.mom_of_n).values
        print('compute using time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        item = ['trade_dt', 's_info_windcode', self.name]
        self.factor[item].to_pickle(self.factor_indir + self.factor_name[:-4] + str(self.perid) + '.pkl')
        print('fileout using time:%10.4fs' % (time.time() - t))

    def runflow(self):
        print('start')
        t = time.time()
        self.filein()
        self.datamanage()
        self.compute()
        self.fileout()
        print('finish using time:%10.4fs' % (time.time() - t))


if __name__ == '__main__':
    factor_indir = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'

    # rrev
    factor_name = 'factor_hq_rrev.pkl'
    perid = 5
    rrm = RetrRevMom(factor_indir, factor_name, perid)
    rrm.runflow()

    factor_name = 'factor_hq_rrev.pkl'
    perid = 10
    rrm = RetrRevMom(factor_indir, factor_name, perid)
    rrm.runflow()

    factor_name = 'factor_hq_rrev.pkl'
    perid = 20
    rrm = RetrRevMom(factor_indir, factor_name, perid)
    rrm.runflow()

    factor_name = 'factor_hq_rrev.pkl'
    perid = 60
    rrm = RetrRevMom(factor_indir, factor_name, perid)
    rrm.runflow()

    # mild
    factor_name = 'factor_hq_mild.pkl'
    perid = 20
    rrm = RetrRevMom(factor_indir, factor_name, perid)
    rrm.runflow()

    factor_name = 'factor_hq_mild.pkl'
    perid = 60
    rrm = RetrRevMom(factor_indir, factor_name, perid)
    rrm.runflow()

    factor_name = 'factor_hq_mild.pkl'
    perid = 180
    rrm = RetrRevMom(factor_indir, factor_name, perid)
    rrm.runflow()

    # rmom
    factor_name = 'factor_hq_rmom.pkl'
    perid = 20
    rrm = RetrRevMom(factor_indir, factor_name, perid)
    rrm.runflow()

    factor_name = 'factor_hq_rmom.pkl'
    perid = 60
    rrm = RetrRevMom(factor_indir, factor_name, perid)
    rrm.runflow()

    factor_name = 'factor_hq_rmom.pkl'
    perid = 180
    rrm = RetrRevMom(factor_indir, factor_name, perid)
    rrm.runflow()
