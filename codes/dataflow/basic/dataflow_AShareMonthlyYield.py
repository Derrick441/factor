import pandas as pd
import sqlconn
import time


class MonthIndex(object):

    def __init__(self, index, indir, startdate, enddate):
        self.index = index
        self.indir = indir
        self.startdate = startdate
        self.enddate = enddate

    def sqlin(self):
        # wind月收益率数据：月收益率，以及月换手率，月成交金额
        conn = sqlconn.sqlconn()
        t = time.time()
        sqlquery = 'select s_info_windcode, trade_dt, s_mq_pctchange, s_mq_turn, s_mq_amount '\
                   'from wind.AShareMonthlyYield where trade_dt>= ' + self.startdate + ' and trade_dt<= ' + self.enddate
        self.data = pd.read_sql(sqlquery, conn)
        print('sqlin running time: %10.4fs' % (time.time() - t))
        conn.close()

    def datamanage(self):
        self.data.rename(columns=lambda x: x.lower(), inplace=True)
        self.data.drop_duplicates(subset=['trade_dt', 's_info_windcode'], keep='first', inplace=True)
        self.data.sort_values(by=['trade_dt', 's_info_windcode'], inplace=True)
        # self.data.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        # self.data.groupby(level=1).fillna(method='ffill', inplace=True)

    def fileout(self):
        t = time.time()
        self.data.to_pickle(self.indir+self.index+'/'+self.index+'_monthindex.pkl')
        print('fileout running time: %10.4fs' % (time.time() - t))

    def runflow(self):
        self.sqlin()
        self.datamanage()
        self.fileout()


if __name__ == '__main__':
    file_index = 'all'
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    data_startdate = '20050701'
    data_enddate = '20200630'
    monthindex = MonthIndex(file_index, file_indir, data_startdate, data_enddate)
    monthindex.runflow()
