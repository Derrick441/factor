import pandas as pd
import time

# 动量因子:暂未完成
class Mom(object):

    def __init__(self, indir, INDEX):
        self.indir = indir
        self.INDEX = INDEX

    def filein(self):
        t = time.time()
        self.all_data = pd.read_pickle(self.indir + self.INDEX + '/' + self.INDEX + '_day_mv_pe_pb_turn_close.pkl')
        self.all_data_month = pd.read_pickle(self.indir + self.INDEX + '/' + self.INDEX + '_month_pct.pkl')
        print('filein running time:%10.4fs' % (time.time() - t))

    def datamanage(self):
        t = time.time()
        self.all_data_reset = self.all_data.reset_index()
        self.all_data_month_reset = self.all_data_month.reset_index()
        self.all_data_reset['year_month'] = self.all_data_reset.trade_dt.apply(lambda x: x[:6])
        self.all_data_month_reset['year_month'] = self.all_data_month_reset.trade_dt.apply(lambda x: x[:6])
        print('data_manage running time:%10.4fs' % (time.time() - t))

    # def month_pct_divid_sum(self, data):
    #     #将code设为index
    #     data.index = data.s_info_windcode
    #     #找出月底市净率最低1/3股票和最高1/3股票
    #     end_date = data.trade_dt.drop_duplicates().iloc[-1]
    #     end_data = data[data.trade_dt == end_date]
    #     len_3 = int(len(end_data)/3)
    #     pb_low_index = end_data.sort_values(by='s_val_pb_new').index.to_list()[:len_3]
    #     pb_high_index = end_data.sort_values(by='s_val_pb_new').index.to_list()[-len_3:]
    #     #计算每日市净率最低1/3股票和最高1/3股票总价
    #     result = data.loc[pb_low_index].groupby('trade_dt')\
    #                                    .sum()['s_dq_close_today']\
    #                                    .reset_index()\
    #                                    .rename(columns={'s_dq_close_today':'pb_low'})
    #     result.loc[:, 'pb_high'] = data.loc[pb_high_index].groupby('trade_dt')\
    #                                                       .sum()['s_dq_close_today']\
    #                                                       .values
    #     return result
    #
    # def compute_mom(self):
    #     t0 = time.time()
    #     t = time.time()
    #     #计算月底估值前1/3股票和后1/3股票的月内每日总股价
    #     self.result = self.all_data_reset.groupby('year_month').apply(self.month_pb_divid_sum).reset_index()
    #     print(time.time()-t)
    #
    #     t = time.time()
    #     self.result.drop('level_1', axis=1, inplace=True)
    #     self.result.set_index(['year_month', 'trade_dt'], inplace=True)
    #     #计算两个组合收益率
    #     self.result.loc[:, ['pb_low_rate', 'pb_high_rate']] = (self.result.pct_change()*100).values
    #     self.result.reset_index(inplace=True)
    #     #计算mom
    #     self.result['mom'] = self.result.pb_low_rate-self.result.pb_high_rate
    #     print(time.time()-t)
    #     print('compute_mom running time:%10.4fs' % (time.time() - t0))
    #
    # def fileout(self):
    #     t=time.time()
    #     self.result[['trade_dt', 'mom']].to_pickle(self.indir + 'factor' + '/f6_' + self.INDEX + '_mom.pkl')
    #     print('fileout running time:%10.4fs' % (time.time() - t))
    #
    # def runflow(self):
    #     print('compute start')
    #     t=time.time()
    #     self.filein()
    #     self.datamanage()
    #     self.compute_mom()
    #     self.fileout()
    #     print('compute end, running time:%10.4f' %(time.time()-t))
    #     return self

if __name__ == '__main__':
    indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    INDEX = 'all'
    mom = Mom(indir, INDEX)

    mom.filein()
    mom.datamanage()
    mom.all_data_reset
    mom.all_data_month_reset

    mom.runflow()