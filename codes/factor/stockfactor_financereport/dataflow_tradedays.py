import time
import pandas as pd
import numpy as np
import sqlconn


def lastTradeDateFromSQL(startdate, enddate):
    conn = sqlconn.sqlconn()
    sqlquery = 'select distinct trade_days from wind.AShareCalendar ' \
               'where trade_days>='+startdate+' ' \
               'and trade_days<='+enddate+' ' \
               'order by trade_days desc'
    data = pd.read_sql(sqlquery, conn)
    return data.iloc[0, 0]


def yearFirstTradeDate(startdate, enddate):
    conn = sqlconn.sqlconn()
    sqlquery = 'select distinct trade_days from wind.AShareCalendar ' \
               'where trade_days>='+startdate+' ' \
               'and trade_days<='+enddate + ' ' \
               'order by trade_days'
    data = pd.read_sql(sqlquery, conn)
    data['year'] = data.TRADE_DAYS.str.slice(0, 4).astype('int16')
    data.drop_duplicates(['year'], keep='first', inplace=True)
    return data.set_index(['year']).TRADE_DAYS.to_dict()


def stockFirstTradeDate(datef):
    datef.sort_values(['s_info_windcode', 'trade_dt'], inplace=True)
    datef.drop_duplicates(['s_info_windcode'], keep='first', inplace=True)
    return datef.set_index(['s_info_windcode']).trade_dt.to_dict()


def fixIntervalTradeDate(startdate, enddate, fillmiss, periods, freq):
    # 选取指定区间的间隔交易日,periods先发生作用，freq与pd.date_range的参数一致

    # 获取交易日
    conn = sqlconn.sqlconn()
    sqlquery = 'select distinct trade_days as trade_dt from wind.AShareCalendar ' \
               'where trade_days>='+startdate+' ' \
               'and trade_days<='+enddate + ' ' \
               'order by trade_days'
    tradedates = pd.read_sql(sqlquery, conn)
    tradedates.rename(columns=lambda x: x.lower(), inplace=True)
    tddf = tradedates.set_index(['trade_dt'], drop=False)

    if periods > 0:
        iddf = tddf.iloc[range(0, tddf.shape[0], periods)]
        selectday = pd.Series(index=iddf.index, data=True)
    else:
        businessdays = pd.date_range(start=startdate, end=enddate, freq='B')
        bd0 = businessdays.strftime('%Y%m%d')
        bddf = pd.DataFrame(index=bd0)

        bddf['trade_dt'] = tddf['trade_dt']

        if fillmiss == 'last':
            bddf.ffill(inplace=True)
        elif fillmiss == 'next':
            bddf.bfill(inplace=True)

        interday0 = pd.date_range(start=startdate, end=enddate, freq=freq)
        interday = interday0.strftime('%Y%m%d')
        iddf = pd.DataFrame(index=interday)

        iddf['trade_dt'] = bddf['trade_dt']
        iddf.dropna(subset=['trade_dt'], inplace=True)
        iddf.drop_duplicates(subset=['trade_dt'], keep='last', inplace=True)
        selectday = pd.Series(index=iddf['trade_dt'].values, data=True)
    return selectday


if __name__ == '__main__':
    # lasttradedate = lastTradeDateFromSQL('20191125','20191214')
    # yftd = yearFirstTradeDate('20181125','20191214')
    indir = 'D:\\lirx\\data\\developflow\\'
    INDEX = 'all'
    selectday = fixIntervalTradeDate(startdate='20191231', enddate='20200425', periods=5, freq='W-FRI', fillmiss='last')
