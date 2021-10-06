import pandas as pd
import numpy as np


class TradeModule(object):
    def __init__(self, closep, openp, vol, weight, enddate, initmoney, costsell, costbuy,
                 ex_dt, dvd_payout_dt, listing_dt_of_dvd_shr, stk_dvd_per_sh, cash_dvd_per_sh_after_tax,
                 ri_ex_dt, ri_price, ri_ratio):
        self.closep = closep
        self.openp = openp
        self.vol = vol
        self.weight = weight
        self.enddate = enddate
        self.cash = initmoney
        self.costsell = costsell
        self.costbuy = costbuy
        self.ex_dt = ex_dt
        self.dvd_payout_dt = dvd_payout_dt
        self.listing_dt_of_dvd_shr = listing_dt_of_dvd_shr
        self.stk_dvd_per_sh = stk_dvd_per_sh
        self.cash_dvd_per_sh_after_tax = cash_dvd_per_sh_after_tax
        self.ri_ex_dt = ri_ex_dt
        self.ri_price = ri_price
        self.ri_ratio = ri_ratio
        self.tradeflag = -1   # 当日是否需要交易，-1：初始交易日；0:无换仓需求；1：换仓日
        self.positionlimit = 0.98   # 最高仓位限制

    def dateend_info(self, tradedate, curprice):
        stockvalue = (self.stocks*curprice).sum()
        exstockvalue = (self.exinfo_d[:, 0]*self.exinfo_d[:, 2]*curprice).sum()
        exdvdcash = (self.exinfo_d[:, 0]*self.exinfo_d[:, 1]).sum()
        self.tradeinfo.at[tradedate, 'cash'] = self.cash
        self.tradeinfo.at[tradedate, 'exdvdcash'] = int(exdvdcash)
        self.tradeinfo.at[tradedate, 'stockvalue'] = int(stockvalue)
        self.tradeinfo.at[tradedate, 'exstockvalue'] = int(exstockvalue)
        self.tradeinfo.at[tradedate, 'sellmoney'] = self.sellmoney
        self.tradeinfo.at[tradedate, 'buymoney'] = self.buymoney
        self.tradeinfo.at[tradedate, 'total'] = int(stockvalue + self.cash+exstockvalue+exdvdcash)

    def dateend_dividend_exinfo_cal(self, tradedate):
        # 派息日，将利息计算进现金
        idx1 = self.exinfo_s[:, 1] == tradedate
        if idx1.any():
            self.cash = self.cash+(self.exinfo_d[idx1, 0]*self.exinfo_d[idx1, 1]).sum()
            self.exinfo_s[idx1, 1] = None
            self.exinfo_d[idx1, 1] = 0

        # 红股上市日，将红股加入现有股票
        idx2 = self.exinfo_s[:, 2] == tradedate
        if idx2.any():
            self.stocks[idx2] = self.stocks[idx2] + (self.exinfo_d[idx2, 0]*self.exinfo_d[idx2, 2])
            self.exinfo_s[idx2, 2] = None
            self.exinfo_d[idx2, 2] = 0

    def dateend_dividend(self, tradedate, curex_dt, curpayout_dt, curshrlist_dt, curstk_dvd, curcash_dvd):
        # 计算被除权除息的信息，避免被新移入的覆盖
        self.dateend_dividend_exinfo_cal(tradedate)

        # 除权除息日，将股权登记信息移入除权除息表
        idx = self.recordinfo_s[:, 0] == tradedate
        if idx.any():
            self.exinfo_s[idx, :] = self.recordinfo_s[idx, :]
            self.exinfo_d[idx, :] = self.recordinfo_d[idx, :]
            self.recordinfo_s[idx, :] = np.array([None, None, None])
            self.recordinfo_d[idx, :] = np.array([0, 0, 0])

        # 计算被除权除息的信息，主要是针对新移入的表可能当日派息
        self.dateend_dividend_exinfo_cal(tradedate)

        # 股权登记日，且有持仓的个股，将其信息加入recordinfo
        dvdidx = (curstk_dvd > 0) | (curcash_dvd > 0)
        stidx = self.stocks > 0
        idx = dvdidx & stidx
        if idx.any():
            self.recordinfo_s[idx, :] = np.transpose(np.array([curex_dt[idx], curpayout_dt[idx], curshrlist_dt[idx]]))
            self.recordinfo_d[idx, :] = np.transpose(np.array([self.stocks[idx], curcash_dvd[idx], curstk_dvd[idx]]))

    def dateend_rightissue(self, tradedate, curprice, curri_ex_dt, curri_price, curri_ratio):
        # 除权除息日，将股权登记信息移除，对持仓股票做配股复权
        idx = self.ri_recordinfo_s[:, 0] == tradedate
        if idx.any():
            # 多于股权登记日的部分不能复权，因为无法配股；低于股权登记日的部分按该部分复权
            self.stocks[idx] = np.maximum(0, self.stocks[idx]-self.ri_recordinfo_d[idx, 0])\
                               + np.minimum(self.stocks[idx], self.ri_recordinfo_d[idx, 0])\
                               / self.ri_recordinfo_d[idx, 1]
            self.ri_recordinfo_s[idx, :] = np.array([None])
            self.ri_recordinfo_d[idx, :] = np.array([0, 0])

        # 股权登记日，且有持仓的个股，将其信息加入recordinfo
        riidx = curri_ratio > 0
        stidx = self.stocks > 0
        idx = riidx & stidx
        if idx.any():
            adjfactor = (1+curri_price[idx]*curri_ratio[idx]/curprice[idx])/(1+curri_ratio[idx])
            self.ri_recordinfo_s[idx, :] = np.transpose(np.array([curri_ex_dt[idx]]))
            self.ri_recordinfo_d[idx, :] = np.transpose(np.array([self.stocks[idx], adjfactor]))

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
        curvol = self.vol[item]
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
                exstockvalue = (self.exinfo_d[:, 0]*self.exinfo_d[:, 2]*curclosep).sum()
                exdvdcash = (self.exinfo_d[:, 0]*self.exinfo_d[:, 1]).sum()
                caltradecash = ((exdvdcash+exstockvalue+self.cash+stockvalue)*self.positionlimit)/(1+costbuy)
            self.prepare_trade_changeport(caltradecash, curport, curclosep)

        # 分红事项
        curex_dt = self.ex_dt[item]
        curpayout_dt = self.dvd_payout_dt[item]
        curshrlist_dt = self.listing_dt_of_dvd_shr[item]
        curstk_dvd = self.stk_dvd_per_sh[item]
        curcash_dvd = self.cash_dvd_per_sh_after_tax[item]
        self.dateend_dividend(tradedate, curex_dt, curpayout_dt, curshrlist_dt, curstk_dvd, curcash_dvd)

        # 配股事项
        curri_ex_dt = self.ri_ex_dt[item]
        curri_price = self.ri_price[item]
        curri_ratio = self.ri_ratio[item]
        self.dateend_rightissue(tradedate, curclosep, curri_ex_dt, curri_price, curri_ratio)

        # 每日信息记录
        self.dateend_info(tradedate, curclosep)

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

        self.ex_dt = self.ex_dt[didx].values
        self.dvd_payout_dt = self.dvd_payout_dt[didx].values
        self.listing_dt_of_dvd_shr = self.listing_dt_of_dvd_shr[didx].values
        self.stk_dvd_per_sh = self.stk_dvd_per_sh[didx].values
        self.cash_dvd_per_sh_after_tax = self.cash_dvd_per_sh_after_tax[didx].values

        self.ri_ex_dt = self.ri_ex_dt[didx].values
        self.ri_price = self.ri_price[didx].values
        self.ri_ratio = self.ri_ratio[didx].values

        self.weight = self.weight[self.weight.index <= self.enddate].values

        self.tradeinfo = pd.DataFrame(data=None,
                                      index=datelist,
                                      columns=['cash',
                                               'exdvdcash',
                                               'exstockvalue',
                                               'stockvalue',
                                               'sellmoney',
                                               'buymoney',
                                               'total'])  # 交易信息

        self.recordinfo_s = np.empty(shape=(len(stocklist), 3), dtype=object)
        self.exinfo_s = np.empty(shape=(len(stocklist), 3), dtype=object)
        self.ri_recordinfo_s = np.empty(shape=(len(stocklist), 1), dtype=object)

        self.recordinfo_d = np.zeros((len(stocklist), 3))
        self.exinfo_d = np.zeros((len(stocklist), 3))
        self.ri_recordinfo_d = np.zeros((len(stocklist), 2))

        for item in range(len(datelist)):
            self.trade_singleday(item, datelist[item])    # 逐日交易


if __name__ == '__main__':
    initmoney = 100000000
    costsell = 0.0025
    costbuy = 0.0015
    enddate = '20180125'
    dir = 'D:\\develop\\python\\data\\tradeflow\\test\\'

    # 价格数据
    # closep = pd.read_pickle(dir+'all_adjclosep.pkl')
    # openp = pd.read_pickle(dir+'all_adjopenp.pkl')
    closep = pd.read_pickle(dir+'all_closep.pkl')
    openp = pd.read_pickle(dir+'all_openp.pkl')
    vol = pd.read_pickle(dir+'all_vol.pkl')
    weight = pd.read_pickle(dir+'all_weight.pkl')

    # 分红数据
    ex_dt = pd.read_pickle(dir+'all_ex_dt.pkl')
    dvd_payout_dt = pd.read_pickle(dir+'all_dvd_payout_dt.pkl')
    listing_dt_of_dvd_shr = pd.read_pickle(dir+'all_listing_dt_of_dvd_shr.pkl')
    stk_dvd_per_sh = pd.read_pickle(dir+'all_stk_dvd_per_sh.pkl')
    cash_dvd_per_sh_after_tax = pd.read_pickle(dir+'all_cash_dvd_per_sh_after_tax.pkl')

    # 配股数据
    ri_ex_dt = pd.read_pickle(dir+'all_s_rightsissue_exdividenddate.pkl')
    ri_price = pd.read_pickle(dir+'all_s_rightsissue_price.pkl')
    ri_ratio = pd.read_pickle(dir+'all_s_rightsissue_ratio.pkl')

    tradem = TradeModule(closep, openp, vol, weight, enddate, initmoney, costsell, costbuy,
                         ex_dt, dvd_payout_dt, listing_dt_of_dvd_shr, stk_dvd_per_sh, cash_dvd_per_sh_after_tax,
                         ri_ex_dt, ri_price, ri_ratio)
    tradem.tradeflow()
    tradem.tradeinfo.to_excel('D:\\develop\\python\\data\\tradeflow\\test\\tradeinfo_numpy_dvd_ri.xlsx')
