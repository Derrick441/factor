#!/usr/bin/python
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

def perfFactor2Stocks(holdstock,stflag,weight,benchweight,price,indu,factor):
    sweight = weight.shift(axis=0).stack()
    stocks = holdstock.stack()
    stocks = stocks.to_frame('stocks')
    stocks['stflag'] = stflag['stflag']
    stocks['portweight'] = sweight
    stocks['benchweight'] = benchweight['weightedratio']/100
    stocks['s_dq_open'] = price['s_dq_open']
    stocks['s_dq_close'] = price['s_dq_close']
    stocks['s_dq_adjfactor'] = price['s_dq_adjfactor']
    stocks['vwap_hh'] = price['vwap_hh']
    stocks['induname1'] = indu['induname1']
    stocks['induname2'] = indu['induname2']
    stocks['induname3'] = indu['induname3']
    stocks['factor'] = factor
    stocks.fillna(value=0,inplace=True)

    stocks['holdmv'] = stocks['s_dq_close']*stocks['stocks']*stocks['s_dq_adjfactor']

    # 计算个股偏离
    datesum = stocks.groupby(level=0)['holdmv'].sum().to_frame('holdmv')
    datesingle = stocks.reset_index(level=1)
    datesumf = datesingle.join(datesum, how='left', lsuffix='', rsuffix='_sum')
    datesumf['holdweight'] = datesumf['holdmv']/datesumf['holdmv_sum']
    datesumf['stflagweight'] = datesumf['holdweight']*datesumf['stflag']
    datesumf['notstflagweight'] = datesumf['holdweight']*(1-datesumf['stflag'])
    datesumf['phdiff'] = (datesumf['portweight'] - datesumf['holdweight']).abs()
    stockdiff = datesumf.groupby(level=0)[['phdiff','holdweight','portweight','benchweight','stflagweight','notstflagweight']].sum()

    # 计算行业偏离
    datesumf.reset_index(inplace=True)
    datesumf.set_index(['trade_dt','induname1'],inplace=True)
    grouped = datesumf.groupby(level=[0,1])
    wg = grouped[['holdweight','portweight','benchweight']].sum()
    wg['iphdiff'] = (wg['portweight'] - wg['holdweight']).abs()
    wg['ibpdiff'] = (wg['benchweight'] - wg['portweight']).abs()
    wg['ibhdiff'] = (wg['benchweight'] - wg['holdweight']).abs()
    indudiff = wg.groupby(level=0).sum()

    return stockdiff,indudiff
