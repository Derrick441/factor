import pandas as pd
import numpy as np


class TradeModuleAdj(object):
    def __init__(self, closep, openp, vol, weight, enddate, initmoney, costsell, costbuy, volconstrain):
        self.closep = closep
        self.openp = openp
        self.vol = vol
        self.weight = weight
        self.enddate = enddate
        self.cash = initmoney
        self.costsell = costsell
        self.costbuy = costbuy
        self.volconstrain = volconstrain
        self.tradeflag = -1   # 当日是否需要交易，-1：初始交易日；0:无换仓需求；1：换仓日
        self.positionlimit = 0.98   # 最高仓位限制
        self.debug = False

    def trade_sellstocks(self, curopenp, curvol):
        leftstock = self.sellstocks-curvol*100
        leftstock[leftstock < 0] = 0
        selledstock = self.sellstocks-leftstock
        sellmoney = (selledstock*(curopenp*(1-self.costsell))).sum()
        self.cash = self.cash+np.floor(sellmoney)
        self.sellmoney = sellmoney
        return leftstock, selledstock

    def trade_buystocks(self, curopenp, curvol):
        leftstock = self.buystocks-curvol*100
        leftstock[leftstock < 0] = 0
        needstock = self.buystocks-leftstock    # 不考虑资金的情况下将买入的股票
        estimate_buymoney = (needstock*curopenp*(1+self.costbuy)).sum()   # 预计将使用的资金
        buymoney = min(estimate_buymoney, self.cash)    # 实际会使用的资金
        if estimate_buymoney == 0:
            buyscale = 0
        else:
            buyscale = buymoney/estimate_buymoney
        buyedstock = np.floor((buyscale*needstock)/100)*100
        realbuymoney = (buyedstock*curopenp*(1+self.costbuy)).sum()
        leftstock = self.buystocks-buyedstock
        self.cash = self.cash-np.floor(realbuymoney)
        self.buymoney = realbuymoney
        return leftstock, buyedstock

    def trade_changeport(self, curopenp, curvol):
        if self.sellstocks.sum() > 0:
            self.leftsellstocks, selledstocks = self.trade_sellstocks(curopenp, curvol)
            self.stocks = self.stocks-selledstocks
            # ?
            self.sellstocks = self.leftsellstocks

        if self.buystocks.sum() > 0:
            self.leftbuystocks, buyedstocks = self.trade_buystocks(curopenp, curvol)
            self.stocks = self.stocks+buyedstocks
            # ?
            self.buystocks = self.leftbuystocks
        self.tradeflag = 0

    def prepare_trade_changeport(self, caltradecash, curweight, curclosep):
        np.seterr(divide='ignore', invalid='ignore')   # for 0 in curclosep
        # 根据权重给每股分配现金
        caleachcash = curweight*caltradecash
        # 根据分配现金/股价数据计算每股需要配置的股数
        needstocks = caleachcash/curclosep
        needstocks[np.isnan(needstocks)] = 0

        # 需要卖出的股票
        sellstocks = self.stocks-needstocks
        idx = (self.stocks-sellstocks) != 0   # 处理零股
        sellstocks[sellstocks <= 0] = 0
        sellstocks[idx] = np.floor(sellstocks[idx]/100)*100
        self.sellstocks = sellstocks

        # 需要买入的股票
        buystocks = needstocks-self.stocks
        buystocks[buystocks <= 0] = 0
        self.buystocks = np.floor(buystocks/100)*100
        self.tradeflag = 1

    def dateend_info(self, tradedate, curclosep):
        stockvalue = (self.stocks*curclosep).sum()
        self.tradeinfo.at[tradedate, 'cash'] = self.cash
        self.tradeinfo.at[tradedate, 'stockvalue'] = int(stockvalue)
        self.tradeinfo.at[tradedate, 'sellmoney'] = self.sellmoney
        self.tradeinfo.at[tradedate, 'buymoney'] = self.buymoney
        self.tradeinfo.at[tradedate, 'total'] = int(stockvalue + self.cash)
        self.tradeinfo.at[tradedate, 'stocknum'] = (self.stocks > 0).sum()

    def trade_singleday(self, item, tradedate):
        curclosep = self.closep[item]
        curopenp = self.openp[item]
        curvol = self.vol[item]*self.volconstrain
        curweight = self.weight[item]
        self.sellmoney = 0
        self.buymoney = 0

        # 检查当天是否需要换仓
        if self.tradeflag != -1:
            self.trade_changeport(curopenp, curvol)

        # 检查当天是否需要准备换仓
        if curweight.sum() > 0:
            if self.tradeflag == -1:
                caltradecash = (self.cash*self.positionlimit)/(1+self.costbuy)
            else:
                stockvalue = (self.stocks*curclosep).sum()
                caltradecash = ((self.cash+stockvalue)*self.positionlimit)/(1+self.costbuy)
            self.prepare_trade_changeport(caltradecash, curweight, curclosep)

        # 每日信息记录
        self.dateend_info(tradedate, curclosep)

    def tradeflow(self):
        # 初始持仓（即：第一天当日持仓，后续当日持仓在此存储空间内存放）
        self.stocklist = self.closep.columns
        self.stocks = np.zeros((len(self.stocklist),))
        self.curstocks = pd.Series(index=self.stocklist, data=self.stocks)

        # 组合第一天作为开始日期，结合传入的结束日期，确定模拟交易的时间范围
        startdate = self.weight.index[0]
        # 时间范围内：日期、收盘价、开盘价、成交量和权重数据
        didx = (self.closep.index >= startdate) & (self.closep.index <= self.enddate)
        self.datelist = self.closep.index[didx]
        self.closep = self.closep[didx].values
        self.openp = self.openp[didx].values
        self.vol = self.vol[didx].values
        self.weight = self.weight[self.weight.index <= self.enddate].values

        # 交易信息
        item = ['cash', 'stockvalue', 'sellmoney', 'buymoney', 'total', 'stocknum']
        self.tradeinfo = pd.DataFrame(data=None, index=self.datelist, columns=item)

        for i in range(len(self.datelist)):
            self.trade_singleday(i, self.datelist[i])    # 逐日交易


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    closep = pd.read_pickle(file_indir+'all_adjclosep.pkl')
    openp = pd.read_pickle(file_indir+'all_adjopenp.pkl')
    vol = pd.read_pickle(file_indir+'all_vol.pkl')
    name = 'combine_factor_ic20_10'
    weight = pd.read_pickle(file_indir+name+'.pkl')
    temp_weight = weight.iloc[::5, :].copy()
    weight_5 = temp_weight.reindex(index=weight.index).ffill()

    initmoney = 100000000
    costbuy = 0.001
    costsell = 0.003
    enddate = '202000101'
    volconstrain = 10000000000

    tradem = TradeModuleAdj(closep, openp, vol, weight_5, enddate, initmoney, costsell, costbuy, volconstrain)
    self = tradem
    tradem.tradeflow()

    trade = tradem.tradeinfo[tradem.tradeinfo.index >= '20070115'].copy().reset_index()
    zz500 = pd.read_pickle('D:\\wuyq02\\develop\\python\\data\\developflow\\zz500\\zz500_indexprice.pkl').reset_index()
    result = pd.merge(trade, zz500[['trade_dt', 's_dq_close']], how='left')
    result['basic'] = result['s_dq_close'] / result['s_dq_close'][0] * initmoney

    result['extra_ret'] = result['total'] - result['basic']
    result.to_csv('D:\\wuyq02\\develop\\python\\data\\performance\\simulated_trading\\'+name+'.csv')
