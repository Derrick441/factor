import pandas as pd
import time
import statsmodels.regression.rolling as regroll


# 日频率
# 价格时滞：股价对市场信息的反应存在时滞，计算过去市场收益率对股票收益率的解释程度
# 每日取20日收益率数据，回归t0股票收益率与t0市场收益率，得到r_squared解释系数R1
# 每日取20日收益率数据，回归t0股票收益率与t0、t-1、t-2和t-3市场收益率，得到r_squared解释系数R3
# 1-(R1/R3)
class PriceDelay(object):

    def __init__(self, file_indir, file_name):
        self.file_indir = file_indir
        self.file_name = file_name

    def filein(self):
        t = time.time()
        # 从dataflow文件夹中取股票日行情数据
        self.all_data = pd.read_pickle(self.file_indir + self.file_name)
        # 从factor文件夹中取市场因子数据
        indir_factor = 'D:\\wuyq02\\develop\\python\\data\\factor\\mktfactor\\'
        self.mkt = pd.read_pickle(indir_factor + 'factor_mkt.pkl')
        print('filein running time:%10.4fs' % (time.time()-t))

    def datamanage(self):
        t = time.time()
        # 股票收益率数据
        self.all_change = self.all_data[['trade_dt', 's_info_windcode', 'change']]
        # 市场收益率数据
        mkt_rate = self.mkt.copy()
        mkt_rate['mkt1'] = mkt_rate['mkt'].shift(1)
        mkt_rate['mkt2'] = mkt_rate['mkt'].shift(2)
        mkt_rate['mkt3'] = mkt_rate['mkt'].shift(3)
        # 汇总数据
        self.data_sum = pd.merge(self.all_change,
                                 mkt_rate[['trade_dt', 'mkt', 'mkt1', 'mkt2', 'mkt3']],
                                 how='left')
        print('datamanage running time:%10.4fs' % (time.time() - t))

    def rolling_regress(self, data):
        t = time.time()
        if len(data) >= 20:
            temp_data = data[data.change != 0]
            temp_data['intercept'] = 1
            item = ['intercept', 'mkt', 'mkt1', 'mkt2', 'mkt3']
            model1 = regroll.RollingOLS(temp_data['change'], temp_data[['intercept', 'mkt']], window=20).fit()
            model2 = regroll.RollingOLS(temp_data['change'], temp_data[item], window=20).fit()
            temp = 1-(model1.rsquared / model2.rsquared)
            result = pd.DataFrame({'trade_dt': temp_data.trade_dt.values, 'pricedelay': temp.values})
            print(time.time() - t)
            return result
        else:
            result = pd.DataFrame({'trade_dt': data.trade_dt.values, 'ivff': [None for i in range(len(data))]})
            return result

    def compute_pricedelay(self):
        t = time.time()
        # 去除na
        temp_sum_data = self.data_sum.copy().dropna()
        # 每股滚动回归计算价格时滞
        self.result = temp_sum_data.groupby(['s_info_windcode'])\
                                   .apply(self.rolling_regress)\
                                   .reset_index()
        print('compute_pricedelay running time:%10.4fs' % (time.time() - t))

    def fileout(self):
        t = time.time()
        # 数据对齐
        self.final = pd.merge(self.all_data[['trade_dt', 's_info_windcode']], self.result, how='left')
        # 输出到factor文件夹的stockfactor中
        item = ['trade_dt', 's_info_windcode', 'pricedelay']
        indir_factor = 'D:\\wuyq02\\develop\\python\\data\\factor\\stockfactor\\'
        self.final[item].to_pickle(indir_factor + 'factor_price_pricedelay.pkl')
        print('fileout running time:%10.4fs' % (time.time()-t))

    def runflow(self):
        t = time.time()
        print('start')
        self.filein()
        self.datamanage()
        self.compute_pricedelay()
        self.fileout()
        print('end running time:%10.4fs' % (time.time() - t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    file_name = 'all_dayindex.pkl'
    pricedelay = PriceDelay(file_indir, file_name)
    pricedelay.runflow()
