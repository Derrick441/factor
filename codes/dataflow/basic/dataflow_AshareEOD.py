import pandas as pd
import time
import sqlconn


# wind日行情（调取：日期、股票代码；涨跌幅（%））
# wind日行情估值指标（调取：日期、股票代码；自由流通市值、TTM市盈率、市净率、自由流通市值换手率）
class DayIndex(object):

    def __init__(self, file_indir, save_indir, file_name, stadate, enddate):
        self.file_indir = file_indir
        self.save_indir = save_indir
        self.file_name = file_name
        self.stadate = stadate
        self.enddate = enddate

    def filein(self):
        pass

    def sqlin(self):
        t = time.time()
        # 日行情估值指标
        conn = sqlconn.sqlconn()
        sqlquery = 'select trade_dt, s_info_windcode, ' \
                   's_dq_preclose, s_dq_open, s_dq_high, s_dq_low, s_dq_close, s_dq_pctchange, ' \
                   's_dq_volume, s_dq_amount, s_dq_adjfactor, S_dq_avgprice ' \
                   'from wind.AShareEODPrices ' \
                   'where trade_dt>= ' + self.stadate + ' and trade_dt<= ' + self.enddate
        self.data1 = pd.read_sql(sqlquery, conn)
        conn.close()
        print('sqlin1 running time: %10.4fs' % (time.time() - t))

        t = time.time()
        # 日行情估值指标
        conn = sqlconn.sqlconn()
        sqlquery = 'select trade_dt, s_info_windcode, ' \
                   's_val_pe_ttm, s_val_pb_new, s_dq_freeturnover, free_shares_today ' \
                   'from wind.AShareEODDerivativeIndicator ' \
                   'where trade_dt>= ' + self.stadate + ' and trade_dt<= ' + self.enddate
        self.data2 = pd.read_sql(sqlquery, conn)
        conn.close()
        print('sqlin2 running time: %10.4fs' % (time.time() - t))

    def datamanage(self):
        t = time.time()
        # 命名调整
        self.data1.rename(columns=lambda x: x.lower(), inplace=True)
        self.data2.rename(columns=lambda x: x.lower(), inplace=True)
        # 去重
        self.data1.drop_duplicates(subset=['trade_dt', 's_info_windcode'], keep='first', inplace=True)
        self.data2.drop_duplicates(subset=['trade_dt', 's_info_windcode'], keep='first', inplace=True)
        # 合并
        self.data_sum = pd.merge(self.data1, self.data2, how='left')
        # 自由流通市值
        self.data_sum['s_dq_freemv'] = self.data_sum['s_dq_close'] * self.data_sum['free_shares_today']
        print('datamanage running time: %10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 数据对齐
        self.all_data = pd.read_pickle(self.file_indir + self.file_name)
        self.result = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.data_sum, how='left')
        # 数据排序
        self.result.sort_values(by=['trade_dt', 's_info_windcode'], inplace=True)
        # 数据输出
        self.result.to_pickle(self.save_indir + 'all_dayindex.pkl')
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
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    file_name = 'all_band_dates_stocks_closep.pkl'
    stadate = '20050101'
    enddate = '20191231'
    dayindex = DayIndex(file_indir, save_indir, file_name, stadate, enddate)
    dayindex.runflow()
