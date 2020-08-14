import pandas as pd
import time

# 日频率

# 动量因子:根据月底收益率，将收益率前1/3股票划分为赢家股票，后1/3股票划分为输家股票，两组股票日收益率之差即为动量因子
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

    def month_pct_divid_sum(self, data):
        temp = self.all_data_month_reset.copy()
        # 将code设为index
        data.index = data.s_info_windcode
        temp.index = temp.s_info_windcode
        month = data.year_month[0]

        # 找出月底收益率最低1/3股票和最高1/3股票
        end_data = temp[temp.year_month == month]
        len_3 = int(len(end_data) / 3)
        pct_low_index = end_data.sort_values(by='s_mq_pctchange').index.to_list()[:len_3]
        pct_high_index = end_data.sort_values(by='s_mq_pctchange').index.to_list()[-len_3:]

        #计算一个月内每日输家股票和赢家股票的总价
        result = data.loc[pct_low_index].groupby('trade_dt')\
                                        .sum()['s_dq_close_today']\
                                        .reset_index()\
                                        .rename(columns={'s_dq_close_today':'pct_low'})

        result.loc[:, 'pct_high'] = data.loc[pct_high_index].groupby('trade_dt')\
                                                            .sum()['s_dq_close_today']\
                                                            .values
        return result

    def compute_mom(self):
        t0 = time.time()
        t = time.time()
        #计算输家股票和赢家股票月内每日总股价
        self.result = self.all_data_reset.groupby('year_month').apply(self.month_pct_divid_sum).reset_index()
        self.result.drop('level_1', axis=1, inplace=True)
        print(time.time()-t)

        t = time.time()
        # 计算两个组合收益率
        self.result.set_index(['year_month', 'trade_dt'], inplace=True)
        self.result.loc[:, ['pct_low_rate', 'pct_high_rate']] = (self.result.pct_change()*100).values
        #计算mom
        self.result.reset_index(inplace=True)
        self.result['mom'] = self.result.pct_low_rate-self.result.pct_high_rate
        print(time.time()-t)
        print('compute_mom running time:%10.4fs' % (time.time() - t0))

    def fileout(self):
        t=time.time()
        self.result[['trade_dt', 'mom']].to_pickle(self.indir + 'factor' + '/f7_' + self.INDEX + '_mom.pkl')
        print('fileout running time:%10.4fs' % (time.time() - t))

    def runflow(self):
        print('compute start')
        t=time.time()
        self.filein()
        self.datamanage()
        self.compute_mom()
        self.fileout()
        print('compute end, running time:%10.4f' %(time.time()-t))
        return self

if __name__ == '__main__':
    indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    INDEX = 'all'
    mom = Mom(indir, INDEX)
    mom.runflow()

