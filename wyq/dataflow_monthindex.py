import pandas as pd
import sqlconn
import time

class DataflowMonthpct(object):

    def __init__(self, INDEX, indir, startdate, enddate):
        self.INDEX = INDEX
        self.indir = indir
        self.startdate = startdate
        self.enddate = enddate

    def sqlin(self):
        # wind月收益率数据：月收益率，月换手率，月成交金额
        conn = sqlconn.sqlconn()
        t = time.time()
        sqlquery = 'select s_info_windcode, trade_dt, s_mq_pctchange, s_mq_turn, s_mq_amount'\
                   ' from wind.AShareMonthlyYield where trade_dt>= ' + startdate + ' and trade_dt<= ' + enddate
        self.data = pd.read_sql(sqlquery, conn)
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
        self.data.to_pickle(self.indir+self.INDEX+'/'+self.INDEX+'_monthindex.pkl')
        print('fileout running time: %10.4fs' %(time.time() - t))

    def runflow(self):
        self.sqlin()
        self.datamanage()
        self.fileOut()

if __name__=='__main__':
    INDEX = 'all'
    indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    startdate = '20050701'
    enddate = '20200630'
    monthpct = DataflowMonthpct(INDEX, indir, startdate, enddate)
    monthpct.runflow()
