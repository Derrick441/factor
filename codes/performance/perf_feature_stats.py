import pandas as pd
import gc
import numpy as np


class PerfFeatureStats(object):

    def __init__(self, indir, index, startdate, enddate, loadname, cutnum):
        self.indir = indir
        self.index = index
        self.startdate = startdate
        self.enddate = enddate
        self.loadname = loadname
        self.cutnum = cutnum
        self.features = pd.DataFrame()

    def datafeaturereorder(self):
        self.feature_row0 = self.feature_row[self.reordername].copy()
        self.feature_row = self.feature_row0
        del self.feature_row0
        gc.collect()

    def datafeaturemergeload(self, yearname, concattype, loadname):
        if yearname == '2011':
            self.feature_row = pd.read_pickle(self.indir+self.index+'/'+self.index+'_2005-2011_'+loadname+'.pkl')
        else:
            self.feature_row = pd.read_pickle(self.indir+self.index+'/'+self.index+'_'+yearname+'_'+loadname+'.pkl')

        # ----------------------- special feature add ------------------
        if concattype == 'firstconcat':
            self.reordername = self.feature_row.columns.to_list()
        else:
            self.datafeaturereorder()

        self.feature_row.sort_index(inplace=True, level=0, axis=0)
        self.feature_row.fillna(value=0, inplace=True)

    def filefoad(self):
        startyear = int(self.startdate[0:4])
        endyear = int(self.enddate[0:4])

        concattype = 'firstconcat'
        for yearnum in range(startyear, endyear+1):
            print('running year:' + str(yearnum))
            # 加载数据
            yearname = str(yearnum)
            self.datafeaturemergeload(yearname, concattype, self.loadname)

            self.features = pd.concat([self.features, self.feature_row], axis=0)

            concattype = 'normalconcat'

    def featurezerobincount(self, x):
        quartiles = pd.cut(x, self.cutnum)
        grouped = x.groupby(quartiles)
        cr = ((grouped.count() == 0).sum())/self.cutnum
        return cr

    def featurequantile2max(self, x):
        qr = np.abs(np.quantile(x, 0.995))/np.abs(x).max()
        return qr

    def featuremaxdistance(self, x, result):
        stdr = x.std()
        medianr = x.median()
        maxv = x.max()
        minv = x.min()
        maxd = (maxv - medianr)/stdr
        mind = (medianr - minv)/stdr
        result['maxd'] = maxd
        result['mind'] = mind
        result['maxv'] = maxv
        result['minv'] = minv
        result['medianv'] = medianr

    def featurestats(self):
        cr = self.features.apply(self.featurezerobincount).to_frame('zero_bin_count')
        cr['qr'] = self.features.apply(self.featurequantile2max)
        self.featuremaxdistance(self.features, cr)

        pass

    def runflow(self):
        self.filefoad()
        self.featurestats()


if __name__ == '__main__':
    indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    index = 'all'
    loadname = 'feature7'
    pfs = PerfFeatureStats(indir, index, startdate='20120102', enddate='20121231', loadname=loadname, cutnum=256)
    pfs.runflow()
