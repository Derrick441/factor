import pandas as pd


class PerfReturn2Perf(object):
    def __init__(self, index, indir, info, iprice, balanced_pct):
        self.index = index
        self.indir = indir
        self.balanced_pct = balanced_pct
        self.info = info
        self.iprice = iprice

    def return2perf(self):
        self.info.index = self.info.index.astype('str')
        # self.info.set_index(['trade_dt'],inplace=True)
        self.info['turnover'] = (self.info['sellmoney']+self.info['buymoney'])/self.info['total']
        # 指数价格及日收益率
        self.info['iprice'] = self.iprice['s_dq_close']
        self.info['iprice_pct'] = self.info['iprice'].pct_change(1)
        self.info['iprice_pct'] = self.info['iprice_pct'].fillna(value=0)
        # 策略日收益率
        self.info['total_pct'] = self.info['total'].pct_change(1)
        self.info['total_pct'].fillna(value=0, inplace=True)
        # 策略超额收益
        self.info['Alpha'] = self.info['total_pct']-self.info['iprice_pct']

    def cal_return(self):
        # 加法累计超额收益
        self.info['Alpha(add)'] = self.info['Alpha'].cumsum()+1

        # 乘法累计超额收益
        self.info['Alpha(mul)'] = (self.info['Alpha']+1).cumprod()
        self.lastret = 0
        self.lastcalret = 1
        dates = self.info.index.to_list()

        # 再平衡超额收益，超额超过balanced_pct后再平衡
        for item in dates:
            self.balanced_return_singleday(item, self.balanced_pct)

        # 收益最大回撤
        retmax = self.info['total'].cummax()
        minusmax = self.info['total']-retmax
        minusmax[minusmax > 0] = 0
        self.info['MDD(total)'] = minusmax/retmax

        # 超额收益最大回撤
        extretmax = self.info['Alpha(balanced)'].cummax()
        minusmax = self.info['Alpha(balanced)']-extretmax
        minusmax[minusmax > 0] = 0
        self.info['MDD(alpha)'] = minusmax/extretmax

    def balanced_return_singleday(self, item, balanced_pct):
        # 超额收益超过balanced_pct就再平衡
        if abs(self.lastret) > balanced_pct:
            self.curret = self.info.loc[item, 'Alpha']
        else:
            self.curret = self.lastret+self.info.loc[item, 'Alpha']
        self.lastret = self.curret

        self.info.loc[item, 'Alpha(balanced)'] = self.lastcalret*(1+self.curret)
        if abs(self.curret) > balanced_pct:
            self.lastcalret = self.info.loc[item, 'Alpha(balanced)']

    def info_stats(self):
        # 获取年份
        self.info.reset_index(inplace=True)
        self.info['year'] = self.info['trade_dt'].apply(lambda x: x[0:4])

        # 分组获取统计值
        grouped = self.info.groupby(by=['year'])

        # 历年超额收益
        stats = grouped['Alpha(balanced)'].last().to_frame()
        stats['Alpha_shift'] = stats['Alpha(balanced)'].shift(periods=1, axis=0, fill_value=1)
        stats['Alpha(year)'] = (stats['Alpha(balanced)']/stats['Alpha_shift']-1)
        stats.drop(['Alpha(balanced)', 'Alpha_shift'], axis=1, inplace=True)

        stats['MDD(alpha)'] = grouped['MDD(alpha)'].min()
        stats['MDD(total)'] = grouped['MDD(total)'].min()
        stats['Vol(Alpha)'] = (252 ** (1/2))*grouped['Alpha'].std()
        stats['Vol(total)'] = (252 ** (1/2))*grouped['total_pct'].std()
        stats['TurnOver'] = grouped['turnover'].sum()
        stats['IR'] = (252 ** (1/2)) * grouped['Alpha'].mean() / grouped['Alpha'].std()
        stats['Sharp'] = (252 ** (1/2)) * grouped['total_pct'].mean() / grouped['total_pct'].std()

        yearnum = self.info.shape[0]/252
        annalpha = self.info['Alpha(balanced)'].iloc[-1] ** (1/yearnum)-1
        mddalpha = self.info['MDD(alpha)'].min()
        mddtotal = self.info['MDD(total)'].min()
        ir = (252 ** (1/2)) * self.info['Alpha'].mean() / self.info['Alpha'].std()
        sharp = (252 ** (1/2)) * self.info['total_pct'].mean() / self.info['total_pct'].std()
        volalpha = (252 ** (1/2)) * self.info['Alpha'].std()
        voltotal = (252 ** (1/2)) * self.info['total_pct'].std()
        turnover = (self.info['turnover'].sum())/yearnum
        stats.loc['All annual'] = [annalpha, mddalpha, mddtotal, volalpha, voltotal, turnover, ir, sharp]
        self.stats = stats.round(4)
        self.stats[['IR', 'Sharp']] = self.stats[['IR', 'Sharp']].round(2)

    def runflow(self):
        self.return2perf()
        self.cal_return()
        self.info_stats()
        return self.info, self.stats


if __name__ == '__main__':
    file_index = 'zz500'
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    filename = 'aifactor1_feature0123_rollN1_cost0.0_scale0.1'
    balanced_pct = 0.03
    info = pd.read_pickle(file_indir+file_index+'/'+file_index+'_'+filename+'.pkl')
    iprice = pd.read_pickle(file_indir+file_index+'/'+file_index+'_indexprice.pkl')
    perf = PerfReturn2Perf(file_index, file_indir, balanced_pct, info, iprice)
    perf.runflow()
