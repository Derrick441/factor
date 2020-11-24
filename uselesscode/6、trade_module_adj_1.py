import pandas as pd
import numpy as np


class TradeModuleAdj(object):
    def __init__(self, closep, openp, vol, weight, startdate, enddate, initmoney, costsell, costbuy, volconstrain):
        self.closep = closep
        self.openp = openp
        self.vol = vol
        self.weight = weight

        self.startdate = startdate
        self.enddate = enddate
        self.cash = initmoney
        self.costsell = costsell
        self.costbuy = costbuy
        self.volconstrain = volconstrain

        self.tradeflag = -1   # 当日是否需要交易，-1：初始交易日；0:无换仓需求；1：换仓日
        self.positionlimit = 0.98   # 最高仓位限制

    def trade_sellstocks(self, curprice, curvol):
        leftstock = self.sellstocks-curvol*100
        leftstock[leftstock < 0] = 0
        selledstock = self.sellstocks-leftstock
        sellmoney = (selledstock*(curprice*(1-self.costsell))).sum()
        self.cash = self.cash+np.floor(sellmoney)
        self.sellmoney = sellmoney
        return leftstock, selledstock

    def trade_buystocks(self, curprice, curvol):
        leftstock = self.buystocks-curvol*100
        leftstock[leftstock < 0] = 0
        needstock = self.buystocks-leftstock    # 不考虑资金的情况下将买入的股票
        estimate_buymoney = (needstock*curprice*(1+self.costbuy)).sum()   # 预计将使用的资金
        buymoney = min(estimate_buymoney, self.cash)    # 实际会使用的资金
        if estimate_buymoney == 0:
            buyscale = 0
        else:
            buyscale = buymoney/estimate_buymoney
        buyedstock = np.floor((buyscale*needstock)/100)*100
        realbuymoney = (buyedstock*curprice*(1+self.costbuy)).sum()
        leftstock = self.buystocks-buyedstock
        self.cash = self.cash-np.floor(realbuymoney)
        self.buymoney = realbuymoney
        return leftstock, buyedstock

    def trade_changeport(self, curprice, curvol):
        if self.sellstocks.sum() > 0:
            self.leftsellstocks, selledstocks = self.trade_sellstocks(curprice, curvol)
            self.stocks = self.stocks-selledstocks
            self.sellstocks = self.leftsellstocks

        if self.buystocks.sum() > 0:
            self.leftbuystocks, buyedstocks = self.trade_buystocks(curprice, curvol)
            self.stocks = self.stocks+buyedstocks
            self.buystocks = self.leftbuystocks
        self.tradeflag = 0

    def prepare_trade_changeport(self, caltradecash, curport, curprice):
        np.seterr(divide='ignore', invalid='ignore')   # for 0 in curprice
        caleachcash = curport*caltradecash
        needstocks = caleachcash/curprice
        needstocks[np.isnan(needstocks)] = 0
        # 存在数据缺失，一些股票股价为0，可能是停牌，对此，将其（需要持有的股票数量）设置为0
        needstocks[np.isinf(needstocks)] = 0

        sellstocks = self.stocks-needstocks
        idx = (self.stocks-sellstocks) != 0   # 处理零股
        sellstocks[sellstocks <= 0] = 0
        sellstocks[idx] = np.floor(sellstocks[idx]/100)*100
        self.sellstocks = sellstocks

        buystocks = needstocks-self.stocks
        buystocks[buystocks <= 0] = 0
        self.buystocks = np.floor(buystocks/100)*100
        self.tradeflag = 1

    def dateend_info(self, tradedate, curprice):
        stockvalue = (self.stocks*curprice).sum()
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
        self.sellmoney = 0
        self.buymoney = 0
        # 检查当天是否需要换仓
        if self.tradeflag != -1:
            self.trade_changeport(curopenp, curvol)
        # 检查当天是否需要准备换仓
        curport = self.weight[item]
        if curport.sum() > 0:
            if self.tradeflag == -1:
                caltradecash = (self.cash*self.positionlimit)/(1+self.costbuy)
            else:
                stockvalue = (self.stocks*curclosep).sum()
                caltradecash = ((self.cash+stockvalue)*self.positionlimit)/(1+self.costbuy)
            self.prepare_trade_changeport(caltradecash, curport, curclosep)
        # 每日信息记录
        self.dateend_info(tradedate, curclosep)

    def tradeflow(self):
        # 生成初始全0持仓
        self.stocks = np.zeros((len(self.closep.columns),))
        self.curstocks = pd.Series(index=self.closep.columns, data=self.stocks)

        # 限制日期范围
        didx = (self.closep.index >= self.startdate) & (self.closep.index <= self.enddate)
        self.datelist = self.closep.index[didx]
        didx = (self.closep.index >= self.startdate) & (self.closep.index <= self.enddate)
        self.closep = self.closep[didx].values
        didx = (self.openp.index >= self.startdate) & (self.openp.index <= self.enddate)
        self.openp = self.openp[didx].values
        didx = (self.vol.index >= self.startdate) & (self.vol.index <= self.enddate)
        self.vol = self.vol[didx].values
        didx = (self.weight.index >= self.startdate) & (self.weight.index <= self.enddate)
        self.weight = self.weight[didx].values

        # 每日交易信息
        item_info = ['cash', 'stockvalue', 'sellmoney', 'buymoney', 'total', 'stocknum']
        self.tradeinfo = pd.DataFrame(data=None, index=self.datelist, columns=item_info)  # 交易信息

        for item in range(len(self.datelist)):
            self.trade_singleday(item, self.datelist[item])    # 逐日交易


if __name__ == '__main__':
    startdate = '20120101'
    enddate = '20200101'
    initmoney = 100000000
    costbuy = 0.001
    costsell = 0.003

    # 价格、成交量
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    name = 'combine_factor_ic1'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\performance\\'+name+'_test\\'
    closep = pd.read_pickle(file_indir+'all_adjclosep.pkl')
    openp = pd.read_pickle(file_indir+'all_adjopenp.pkl')
    vol = pd.read_pickle(file_indir+'all_vol.pkl')
    volconstrain = 1000000000000000000000000
    weight = pd.read_pickle(file_indir+name + '_10.pkl')

    # 1日换仓
    save_name = name + '_' + startdate + '_' + enddate +'_1.csv'
    temp_weight = weight.iloc[::1, :].copy()
    weight_adj = temp_weight.reindex(index=weight.index).ffill().copy()
    tradem = TradeModuleAdj(closep, openp, vol, weight_adj, startdate, enddate, initmoney, costsell, costbuy, volconstrain)
    tradem.tradeflow()
    # 添加基准
    trade = tradem.tradeinfo.copy().reset_index()
    zz500 = pd.read_pickle('D:\\wuyq02\\develop\\python\\data\\developflow\\zz500\\zz500_indexprice.pkl').reset_index()
    result = pd.merge(trade, zz500[['trade_dt', 's_dq_close']], how='left')
    result['basic'] = result['s_dq_close'] / result['s_dq_close'][0] * initmoney
    result['extra_ret'] = result['total'] - result['basic']
    result.to_csv(save_indir + save_name)

    # 5日换仓
    save_name = name + '_' + startdate + '_' + enddate +'_5.csv'
    temp_weight = weight.iloc[::5, :].copy()
    weight_adj = temp_weight.reindex(index=weight.index).ffill().copy()
    tradem = TradeModuleAdj(closep, openp, vol, weight_adj, startdate, enddate, initmoney, costsell, costbuy, volconstrain)
    tradem.tradeflow()
    # 添加基准
    trade = tradem.tradeinfo.copy().reset_index()
    zz500 = pd.read_pickle('D:\\wuyq02\\develop\\python\\data\\developflow\\zz500\\zz500_indexprice.pkl').reset_index()
    result = pd.merge(trade, zz500[['trade_dt', 's_dq_close']], how='left')
    result['basic'] = result['s_dq_close'] / result['s_dq_close'][0] * initmoney
    result['extra_ret'] = result['total'] - result['basic']
    result.to_csv(save_indir + save_name)

    # 10日换仓
    save_name = name + '_' + startdate + '_' + enddate +'_10.csv'
    temp_weight = weight.iloc[::10, :].copy()
    weight_adj = temp_weight.reindex(index=weight.index).ffill().copy()
    tradem = TradeModuleAdj(closep, openp, vol, weight_adj, startdate, enddate, initmoney, costsell, costbuy, volconstrain)
    tradem.tradeflow()
    # 添加基准
    trade = tradem.tradeinfo.copy().reset_index()
    zz500 = pd.read_pickle('D:\\wuyq02\\develop\\python\\data\\developflow\\zz500\\zz500_indexprice.pkl').reset_index()
    result = pd.merge(trade, zz500[['trade_dt', 's_dq_close']], how='left')
    result['basic'] = result['s_dq_close'] / result['s_dq_close'][0] * initmoney
    result['extra_ret'] = result['total'] - result['basic']
    result.to_csv(save_indir + save_name)

    # 20日换仓
    save_name = name + '_' + startdate + '_' + enddate +'_20.csv'
    temp_weight = weight.iloc[::20, :].copy()
    weight_adj = temp_weight.reindex(index=weight.index).ffill().copy()
    tradem = TradeModuleAdj(closep, openp, vol, weight_adj, startdate, enddate, initmoney, costsell, costbuy, volconstrain)
    tradem.tradeflow()
    # 添加基准
    trade = tradem.tradeinfo.copy().reset_index()
    zz500 = pd.read_pickle('D:\\wuyq02\\develop\\python\\data\\developflow\\zz500\\zz500_indexprice.pkl').reset_index()
    result = pd.merge(trade, zz500[['trade_dt', 's_dq_close']], how='left')
    result['basic'] = result['s_dq_close'] / result['s_dq_close'][0] * initmoney
    result['extra_ret'] = result['total'] - result['basic']
    result.to_csv(save_indir + save_name)

    # 60日换仓
    save_name = name + '_' + startdate + '_' + enddate +'_60.csv'
    temp_weight = weight.iloc[::60, :].copy()
    weight_adj = temp_weight.reindex(index=weight.index).ffill().copy()
    tradem = TradeModuleAdj(closep, openp, vol, weight_adj, startdate, enddate, initmoney, costsell, costbuy, volconstrain)
    tradem.tradeflow()
    # 添加基准
    trade = tradem.tradeinfo.copy().reset_index()
    zz500 = pd.read_pickle('D:\\wuyq02\\develop\\python\\data\\developflow\\zz500\\zz500_indexprice.pkl').reset_index()
    result = pd.merge(trade, zz500[['trade_dt', 's_dq_close']], how='left')
    result['basic'] = result['s_dq_close'] / result['s_dq_close'][0] * initmoney
    result['extra_ret'] = result['total'] - result['basic']
    result.to_csv(save_indir + save_name)
