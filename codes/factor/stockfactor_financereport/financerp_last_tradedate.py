import pandas as pd


def dict_gen(startdate, enddate, dateslist):
    # 生成节假日对应的上一交易日字典
    dr = pd.date_range(startdate, enddate)
    dr0 = dr.strftime('%Y%m%d')
    alldate = pd.DataFrame(index=dr0, data=dr0, columns=['alldate'])
    tradedate = pd.DataFrame(index=dateslist, data=dateslist, columns=['tradedate'])
    alldate['tradedate'] = tradedate['tradedate']
    idx = alldate['tradedate'].isnull()
    alldate.ffill(inplace=True)
    return alldate[idx]['tradedate'].to_dict()


def reportdates_replace_simpledict(datedata, lastdatedict):
    # 用字典中的上一交易日替换Dataframe中的节假日
    # tabledate = data[tradedate_name].unique()
    # simpledict = {key:value for key,value in lastdatedict.items() if key in tabledate}
    # data[tradedate_name] = data[tradedate_name].replace(simpledict)
    tabledate = datedata.unique()
    simpledict = {key: value for key, value in lastdatedict.items() if key in tabledate}
    datedata = datedata.replace(simpledict)
    return datedata
