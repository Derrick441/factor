import pandas as pd
import time

# 日频率

# 市值因子：根据月底市值，将市值前1/3股票划分为低市值股票，后1/3股票划分为高市值股票，两组股票日收益率之差即为市值因子
class Smb(object):

    def __init__(self, indir, INDEX):
        self.indir = indir
        self.INDEX = INDEX

    def filein(self):
        t = time.time()
        self.all_data = pd.read_pickle(self.indir + self.INDEX + '/' + self.INDEX + '_day_mv_pe_pb_turn_close.pkl')
        print('filein running time:%10.4fs' % (time.time() - t))

    def datamanage(self):
        t = time.time()
        self.all_data_reset = self.all_data.reset_index()
        self.all_data_reset['year_month'] = self.all_data_reset.trade_dt.apply(lambda x: x[:6])
        print('data_manage running time:%10.4fs' % (time.time() - t))

    def month_mv_divid_sum(self, data):
        #将code设为index
        data.index = data.s_info_windcode
        #找出月底市值最低1/3股票和最高1/3股票
        end_date = data.trade_dt.drop_duplicates().iloc[-1]
        end_data = data[data.trade_dt == end_date]
        len_3 = int(len(end_data)/3)
        mv_low_index = end_data.sort_values(by='s_dq_mv').index.to_list()[:len_3]
        mv_high_index = end_data.sort_values(by='s_dq_mv').index.to_list()[-len_3:]
        #计算每日市值最低1/3股票和最高1/3股票总价
        result = data.loc[mv_low_index].groupby('trade_dt')\
                                       .sum()['s_dq_close_today']\
                                       .reset_index()\
                                       .rename(columns={'s_dq_close_today':'mv_low'})
        result.loc[:, 'mv_high'] = data.loc[mv_high_index].groupby('trade_dt')\
                                                          .sum()['s_dq_close_today']\
                                                          .values
        return result

    def compute_smb(self):
        t0 = time.time()
        t = time.time()
        #计算月底市值前1/3股票和后1/3股票的月内每日总股价
        self.result = self.all_data_reset.groupby('year_month').apply(self.month_mv_divid_sum).reset_index()
        print(time.time()-t)

        t = time.time()
        self.result.drop('level_1', axis=1, inplace=True)
        self.result.set_index(['year_month', 'trade_dt'], inplace=True)
        #计算两个组合收益率
        self.result.loc[:, ['mv_low_rate', 'mv_high_rate']] = (self.result.pct_change()*100).values
        self.result.reset_index(inplace=True)
        #计算smb
        self.result['smb'] = self.result.mv_low_rate-self.result.mv_high_rate
        print(time.time()-t)
        print('compute_smb running time:%10.4fs' % (time.time() - t0))

    def fileout(self):
        t=time.time()
        self.result[['trade_dt', 'smb']].to_pickle(self.indir + 'factor' + '/f5_' + self.INDEX + '_smb.pkl')
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        print('compute start')
        t=time.time()
        self.filein()
        self.datamanage()
        self.compute_smb()
        self.fileout()
        print('compute end, running time:%10.4f' %(time.time()-t))
        return self

if __name__ == '__main__':
    indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    INDEX = 'all'
    smb = Smb(indir, INDEX)
    smb.runflow()