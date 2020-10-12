# -*- coding: utf-8 -*-
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

    def dateend_info(self, tradedate, curprice):
        stockvalue = (self.stocks*curprice).sum()
        self.tradeinfo.at[tradedate, 'cash'] = self.cash
        self.tradeinfo.at[tradedate, 'stockvalue'] = int(stockvalue)
        self.tradeinfo.at[tradedate, 'sellmoney'] = self.sellmoney
        self.tradeinfo.at[tradedate, 'buymoney'] = self.buymoney
        self.tradeinfo.at[tradedate, 'total'] = int(stockvalue + self.cash)
        self.tradeinfo.at[tradedate, 'stocknum'] = (self.stocks > 0).sum()

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

        sellstocks = self.stocks-needstocks
        idx = (self.stocks-sellstocks) != 0   # 处理零股
        sellstocks[sellstocks <= 0] = 0
        sellstocks[idx] = np.floor(sellstocks[idx]/100)*100
        self.sellstocks = sellstocks

        buystocks = needstocks-self.stocks
        buystocks[buystocks <= 0] = 0
        self.buystocks = np.floor(buystocks/100)*100
        self.tradeflag = 1

    def trade_singleday(self, item, tradedate):
        # 检查当天是否需要换仓
        curclosep = self.closep[item]
        curopenp = self.openp[item]
        curvol = self.vol[item]*self.volconstrain
        self.sellmoney = 0
        self.buymoney = 0

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

    def dateend_holdinfo(self, tradedate):
        self.holdinfo.loc[tradedate] = self.stocks

    def tradeflow(self):
        datelist = self.closep.index    # 生成日期列表
        stocklist = self.closep.columns    # 生成股票列表

        self.stocks = np.zeros((len(stocklist),))   # 生成初始全0持仓
        self.curstocks = pd.Series(index=stocklist, data=self.stocks)
        startdate = self.weight.index[0]      # 组合第一天作为起始日期
        datelist = datelist[(datelist >= startdate) & (datelist <= self.enddate)]   # 限制日期范围
        didx = (self.closep.index >= startdate) & (self.closep.index <= self.enddate)

        self.closep = self.closep[didx].values
        self.openp = self.openp[didx].values
        self.vol = self.vol[didx].values

        self.weight = self.weight[self.weight.index <= self.enddate].values

        self.tradeinfo = pd.DataFrame(data=None,
                                      index=datelist,
                                      columns=['cash',
                                               'stockvalue',
                                               'sellmoney',
                                               'buymoney',
                                               'total',
                                               'stocknum'])  # 交易信息

        self.holdinfo = pd.DataFrame(data=None, index=datelist, columns=stocklist)  # 持仓信息

        for item in range(len(datelist)):
            self.trade_singleday(item, datelist[item])    # 逐日交易
            if self.debug == True:
                self.dateend_holdinfo(datelist[item])  # 持仓信息


if __name__ == '__main__':
    initmoney = 100000000
    costsell = 0.0025
    costbuy = 0.0015
    enddate = '20180125'
    dir = 'D:\\develop\\python\\data\\tradeflow\\test\\'

    # 价格数据
    closep = pd.read_pickle(dir+'all_adjclosep.pkl')
    openp = pd.read_pickle(dir+'all_adjopenp.pkl')
    vol = pd.read_pickle(dir+'all_vol.pkl')
    weight = pd.read_pickle(dir+'all_weight.pkl')
    volconstrain = 1000000

    tradem = TradeModuleAdj(closep, openp, vol, weight, enddate, initmoney, costsell, costbuy, volconstrain)
    tradem.tradeflow()
    tradem.tradeinfo.to_excel('D:\\develop\\python\\data\\tradeflow\\test\\tradeinfo_numpy_test_adj.xlsx')
