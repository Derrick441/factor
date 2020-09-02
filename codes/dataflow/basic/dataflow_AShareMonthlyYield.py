import pandas as pd
import time
import sqlconn


# 月指标：月收益率，月换手率，月成交金额
class MonthIndex(object):

    def __init__(self, file_indir, file_name, startdate, enddate):
        self.file_indir = file_indir
        self.file_name = file_name
        self.startdate = startdate
        self.enddate = enddate

    def sqlin(self):
        t = time.time()
        # wind月数据：月收益率，月换手率，月成交金额
        conn = sqlconn.sqlconn()
        sqlquery = 'select trade_dt, s_info_windcode, ' \
                   's_mq_pctchange, s_mq_turn, s_mq_amount '\
                   'from wind.AShareMonthlyYield ' \
                   'where trade_dt>= ' + self.startdate + ' and trade_dt<= ' + self.enddate
        self.data = pd.read_sql(sqlquery, conn)
        conn.close()
        print('sqlin running time:%10.4fs' % (time.time() - t))

    def datamanage(self):
        t = time.time()
        # 重命名、去重、数据排序
        self.data.rename(columns=lambda x: x.lower(), inplace=True)
        self.data.drop_duplicates(subset=['trade_dt', 's_info_windcode'], keep='first', inplace=True)
        self.data.sort_values(by=['trade_dt', 's_info_windcode'], inplace=True)
        # self.data.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        print('datamanage running time:%10.4fs' % (time.time()-t))

    def fileout(self):
        t = time.time()
        self.data.to_pickle(self.file_indir + 'all_monthindex.pkl')
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        t = time.time()
        print('start')
        self.sqlin()
        self.datamanage()
        self.fileout()
        print('end running time:%10.4fs' % (time.time()-t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    file_name = 'all_band_dates_stocks_closep.pkl'
    startdate = '20050101'
    enddate = '20191231'
    monthindex = MonthIndex(file_indir, file_name, startdate, enddate)
    monthindex.runflow()
