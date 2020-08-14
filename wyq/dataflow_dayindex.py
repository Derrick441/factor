import pandas as pd
import sqlconn
import time

class DataflowDayindex(object):

    def __init__(self, INDEX, indir, startdate, enddate):
        self.INDEX = INDEX
        self.indir = indir
        self.startdate = startdate
        self.enddate = enddate

    def filein(self):
        t = time.time()
        self.price = pd.read_pickle(self.indir + self.INDEX + '/' + self.INDEX + '_band_dates_stocks_closep.pkl')
        print('filein running time:%10.4fs' % (time.time()-t))

    def sqlin(self):
        t = time.time()
        # wind日行情估值指标数据：当日流通市值、TTM市盈率、市净率、换手率（自由流通市值）、收盘价
        conn = sqlconn.sqlconn()
        sqlquery = 'select s_info_windcode,trade_dt,s_dq_mv,s_val_pe_ttm,s_val_pb_new,s_dq_freeturnover,s_dq_close_today'\
                   ' from wind.AShareEODDerivativeIndicator where trade_dt>= ' + startdate + ' and trade_dt<= ' + enddate
        self.data = pd.read_sql(sqlquery, conn)
        conn.close()
        print('sqlin running time: %10.4fs' % (time.time() - t))

    def datamanage(self):
        t = time.time()
        self.data.rename(columns=lambda x: x.lower(), inplace=True)
        self.data.drop_duplicates(subset=['trade_dt', 's_info_windcode'], keep='first', inplace=True)

        # 计算收益率，并添加进数据集
        self.data_change_pivot = self.price.pivot('trade_dt', 's_info_windcode', 's_dq_close').pct_change()*100
        self.data_change = self.data_change_pivot.stack().reset_index().rename(columns={0:'change'})
        self.result = pd.merge(self.data_change, self.data, how='left')

        self.result.sort_values(by=['trade_dt', 's_info_windcode'], inplace=True)
        self.result.reset_index(drop=True, inplace=True)
        print('datamanage running time: %10.4fs' % (time.time() - t))

    def fileOut(self):
        t = time.time()
        self.result.to_pickle(self.indir + self.INDEX + '/' + self.INDEX + '_dayindex.pkl')
        print('fileout running time: %10.4fs' %(time.time() - t))

    def runflow(self):
        t = time.time()
        self.filein()
        self.sqlin()
        self.datamanage()
        self.fileOut()
        print('all running time: %10.4fs' %(time.time() - t))

if __name__=='__main__':
    INDEX = 'all'
    indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    startdate = '20050701'
    enddate = '20200630'
    dayindex = DataflowDayindex(INDEX, indir, startdate, enddate)
    dayindex.runflow()
