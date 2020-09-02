import time
import pandas as pd
import sqlconn


class DataflowPricelist(object):

    def __init__(self, index, indir, startdate, enddate):
        self.index = index
        self.indir = indir
        self.startdate = startdate
        self.enddate = enddate

    # def filein(self):
    #     self.dates = np.loadtxt(self.indir+self.index+'\\'+self.index+'_dates.csv',dtype=str)

    def sqlin(self):
        conn = sqlconn.sqlconn()
        starttime = time.time()
        sqlquery = 'select s_info_windcode,trade_dt,s_dq_close,s_dq_open,s_dq_high,s_dq_low,' \
                   's_dq_volume,s_dq_amount,s_dq_adjfactor from wind.AShareEODPrices ' \
                   'where trade_dt>=' + self.startdate + ' and trade_dt<=' + self.enddate
        self.data = pd.read_sql(sqlquery, conn)
        self.data.rename(columns=lambda x: x.lower(), inplace=True)
        endtime = time.time()
        print('sql running time:%10.4fs' % (endtime-starttime))

    # @decorators_runtime
    def data_fillna(self):
        factor = ['s_dq_close', 's_dq_open', 's_dq_high', 's_dq_low', 's_dq_adjfactor']
        self.data.sort_values(by=['s_info_windcode', 'trade_dt'], inplace=True)
        grouped = self.data.groupby('s_info_windcode')
        self.data[factor] = grouped[factor].ffill()[factor]
        self.data.loc[:, ['s_dq_volume', 's_dq_amount']].fillna(0, inplace=True)

    # @decorators_runtime
    def data_fillna_pivot(self):
        factor = ['s_dq_open', 's_dq_high', 's_dq_low', 's_dq_adjfactor']
        self.data.sort_values(by=['s_info_windcode', 'trade_dt'], inplace=True)
        self.mergedata = self.data_fillna_pivot_sub('s_dq_close')
        for item in factor:
            mergetemp = self.data_fillna_pivot_sub(item)
            self.mergedata[item] = pd.merge(self.mergedata, mergetemp,
                                            how='left', on=['trade_dt', 's_info_windcode'])[item]

    def data_fillna_pivot_sub(self, fname):
        pivotdata = self.data.pivot(index='trade_dt', columns='s_info_windcode', values=fname)
        pivotdata.ffill(inplace=True)
        pivotstackdata = pivotdata.stack().reset_index()
        pivotstackdata.rename(columns={0: fname}, inplace=True)
        pivotstackdata.dropna(subset=[fname], inplace=True)
        return pivotstackdata

        # grouped = self.data.groupby('s_info_windcode')
        # self.data[factor] = grouped[factor].ffill()[factor]
        # self.data.loc[:,['s_dq_volume','s_dq_amount']].fillna(0,inplace=True)

    # @decorators_runtime
    def fileout(self):
        # self.mergedata.to_pickle(self.indir+self.index+'\\'+self.index+'_band_price.pkl')
        # self.mergedata[['trade_dt','s_info_windcode','s_dq_close']].to_pickle(self.indir+self.index+'\\'+self.index+'_band_dates_stocks_closep.pkl')
        self.data.to_pickle(self.indir+self.index+'/'+self.index+'_band_price.pkl')
        self.data[['trade_dt', 's_info_windcode', 's_dq_close']]\
            .to_pickle(self.indir+self.index+'/'+self.index+'_band_dates_stocks_closep.pkl')
        self.data['trade_dt'].to_pickle(self.indir+self.index+'/'+self.index+'_dates.pkl')

    def runflow(self):
        # self.filein()
        self.sqlin()
        # self.data_fillna_pivot()
        self.data_fillna()
        self.fileout()


if __name__ == '__main__':
    file_index = 'all'
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    data_startdate = '20050101'
    data_enddate = '20191231'
    pricelist = DataflowPricelist(file_index, file_indir, data_startdate, data_enddate)
    pricelist.runflow()
