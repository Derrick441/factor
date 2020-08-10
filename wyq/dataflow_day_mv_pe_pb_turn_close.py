import pandas as pd
import sqlconn
import time

class DataflowTurnover(object):

    def __init__(self, INDEX, indir, startdate, enddate):
        self.INDEX = INDEX
        self.indir = indir
        self.startdate = startdate
        self.enddate = enddate

    def sqlIn(self):
        # wind日行情估值指标数据：当日流通市值、TTM市盈率、市净率、换手率（自由流通股票）、收盘价
        conn = sqlconn.sqlconn()
        t = time.time()
        sqlquery = 'select s_info_windcode,trade_dt,s_dq_mv,s_val_pe_ttm,s_val_pb_new,s_dq_freeturnover,s_dq_close_today' \
                   ' from wind.AShareEODDerivativeIndicator where trade_dt>= ' + startdate + ' and trade_dt<= ' + enddate
        self.data = pd.read_sql(sqlquery,conn)
        print('sqlin running time: %10.4fs' %(time.time() - t))
        conn.close()

    def datamanage(self):
        self.data.rename(columns=lambda x: x.lower(), inplace=True)
        self.data.drop_duplicates(subset=['trade_dt', 's_info_windcode'], keep='first', inplace=True)
        self.data = self.data.sort_values(by=['trade_dt', 's_info_windcode'])
        self.data.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        self.data = self.data.groupby(level=1).fillna(method='ffill')

    def fileOut(self):
        t = time.time()
        self.data.to_pickle(self.indir+self.INDEX+'/'+self.INDEX+'_day_mv_pe_pb_turn_close.pkl')
        print('fileout running time: %10.4fs' %(time.time() - t))

    def runflow(self):
        self.sqlIn()
        self.datamanage()
        self.fileOut()

if __name__=='__main__':
    INDEX = 'all'
    indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    startdate = '20050408'
    enddate = '20200801'
    turnover = DataflowTurnover(INDEX,indir,startdate,enddate)
    turnover.runflow()
