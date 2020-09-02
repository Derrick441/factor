import pandas as pd
import time
import sqlconn


# wind日行情估值指标数据（包含日期、股票代码、收益率、市值、市盈率、市净率、换手率、收盘价）
class DayIndex(object):

    def __init__(self, file_indir, file_name, startdate, enddate):
        self.file_indir = file_indir
        self.file_name = file_name
        self.startdate = startdate
        self.enddate = enddate

    def filein(self):
        t = time.time()
        # 读入股价数据
        # 日期、股票代码、收盘价
        self.price = pd.read_pickle(self.file_indir + self.file_name)
        print('filein running time:%10.4fs' % (time.time()-t))

    def sqlin(self):
        t = time.time()
        # SQL调取wind日行情估值指标数据
        # 日期、股票代码、市值（流通市值）、市盈率（TTM）、市净率、换手率（自由流通市值）、收盘价
        conn = sqlconn.sqlconn()
        sqlquery = 'select trade_dt, s_info_windcode, ' \
                   's_dq_mv, s_val_pe_ttm, s_val_pb_new, s_dq_freeturnover, s_dq_close_today ' \
                   'from wind.AShareEODDerivativeIndicator ' \
                   'where trade_dt>= ' + self.startdate + ' and trade_dt<= ' + self.enddate
        self.data = pd.read_sql(sqlquery, conn)
        conn.close()
        print('sqlin running time: %10.4fs' % (time.time() - t))

    def datamanage(self):
        t = time.time()
        # 重命名、去重
        self.data.rename(columns=lambda x: x.lower(), inplace=True)
        self.data.drop_duplicates(subset=['trade_dt', 's_info_windcode'], keep='first', inplace=True)
        # 根据all_band_dates_stocks_closep计算收益率，并将收益率与dayindex合并
        self.change_pivot = self.price.pivot('trade_dt', 's_info_windcode', 's_dq_close').pct_change()*100
        self.change = self.change_pivot.stack().reset_index().rename(columns={0: 'change'})
        self.result = pd.merge(self.change, self.data, how='left')
        # 数据对齐
        self.result = pd.merge(self.price[['trade_dt', 's_info_windcode']], self.result, how='left')
        # 数据排序
        self.result.sort_values(by=['trade_dt', 's_info_windcode'], inplace=True)
        print('datamanage running time: %10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        self.result.to_pickle(self.file_indir + 'all_dayindex.pkl')
        print('fileout running time: %10.4fs' % (time.time() - t))

    def runflow(self):
        t = time.time()
        print('start')
        self.filein()
        self.sqlin()
        self.datamanage()
        self.fileout()
        print('end running time: %10.4fs' % (time.time() - t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    file_name = 'all_band_dates_stocks_closep.pkl'
    startdate = '20050101'
    enddate = '20191231'
    dayindex = DayIndex(file_indir, file_name, startdate, enddate)
    dayindex.runflow()
