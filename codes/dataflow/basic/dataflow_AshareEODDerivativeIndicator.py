import pandas as pd
import sqlconn
import time


# wind日行情估值指标数据（包含市值、市盈率、市净率、换手率、股价、收益率）
class DerivativeIndicator(object):

    def __init__(self, index, indir, startdate, enddate):
        self.index = index
        self.indir = indir
        self.startdate = startdate
        self.enddate = enddate

    def filein(self):
        t = time.time()
        self.price = pd.read_pickle(self.indir + self.index + '/' + self.index + '_band_dates_stocks_closep.pkl')
        print('filein running time:%10.4fs' % (time.time()-t))

    def sqlin(self):
        t = time.time()
        # wind日行情估值指标数据：当日流通市值、TTM市盈率、市净率、换手率（自由流通市值）、收盘价
        conn = sqlconn.sqlconn()
        sqlquery = 'select s_info_windcode, trade_dt, ' \
                   's_dq_mv, s_val_pe_ttm, s_val_pb_new, s_dq_freeturnover, s_dq_close_today ' \
                   'from wind.AShareEODDerivativeIndicator ' \
                   'where trade_dt>= ' + self.startdate + ' and trade_dt<= ' + self.enddate
        self.data = pd.read_sql(sqlquery, conn)
        conn.close()
        print('sqlin running time: %10.4fs' % (time.time() - t))

    def datamanage(self):
        t = time.time()
        self.data.rename(columns=lambda x: x.lower(), inplace=True)
        self.data.drop_duplicates(subset=['trade_dt', 's_info_windcode'], keep='first', inplace=True)

        # 根据all_band_dates_stocks_closep计算收益率，并将日收益率与dayindex合并
        self.data_change_pivot = self.price.pivot('trade_dt', 's_info_windcode', 's_dq_close').pct_change()*100
        self.data_change = self.data_change_pivot.stack().reset_index().rename(columns={0: 'change'})
        self.result = pd.merge(self.data_change, self.data, how='left')

        # 数据对齐
        self.result = pd.merge(self.price[['trade_dt', 's_info_windcode']], self.result, how='left')
        # 数据排序
        self.result.sort_values(by=['trade_dt', 's_info_windcode'], inplace=True)
        print('datamanage running time: %10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        self.result.to_pickle(self.indir + self.index + '/' + self.index + '_dayindex.pkl')
        print('fileout running time: %10.4fs' % (time.time() - t))

    def runflow(self):
        t = time.time()
        self.filein()
        self.sqlin()
        self.datamanage()
        self.fileout()
        print('all running time: %10.4fs' % (time.time() - t))


if __name__ == '__main__':
    file_index = 'all'
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    data_startdate = '20050701'
    data_enddate = '20200630'
    dayindex = DerivativeIndicator(file_index, file_indir, data_startdate, data_enddate)
    dayindex.runflow()
