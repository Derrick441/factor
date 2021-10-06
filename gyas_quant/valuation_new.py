import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime
from WindPy import w


def pe_new(code):
    if code[0] == '6':
        code = code + '.SH'
    else:
        code = code + '.SZ'
    w.start()
    # code = '000002'
    name = w.wss(code, "sec_name").Data[0][0]
    tday = datetime.date.today()
    todayyear = tday.year
    ipodate = w.wss(code, "ipo_date").Data[0][0].date()
    ipoyear = ipodate.year

    currdf = pd.DataFrame()
    for i in range(ipoyear, todayyear+1):
        begin_date = str(i)+"-01-01"
        end_date = str(i)+"-12-31"
        currdata = w.wsd(code, "ev,est_mediannetprofit", begin_date, end_date, "unit=1;Period=W;PriceAdj=F;year="+str(i))
        df = pd.DataFrame(currdata.Data, index=currdata.Fields, columns=currdata.Times).T
        currdf = currdf.append(df)

    currdf = currdf[(currdf.index >= ipodate) & (currdf.index <= tday)].rename(columns={'EST_MEDIANNETPROFIT': 'curr'})

    nextdf=pd.DataFrame()
    for i in range(ipoyear, todayyear+1):
        begin_date = str(i)+"-01-01"
        end_date = str(i)+"-12-31"
        nextdata = w.wsd(code, "est_mediannetprofit", begin_date, end_date, "unit=1;Period=W;PriceAdj=F;year="+str(i+1))
        df = pd.DataFrame(nextdata.Data, index=nextdata.Fields, columns=nextdata.Times).T
        nextdf = nextdf.append(df)

    nextdf = nextdf[(nextdf.index >= ipodate) & (nextdf.index <= tday)].rename(columns={'EST_MEDIANNETPROFIT': 'next'})

    finadf = currdf.join(nextdf)

    finadf['date'] = finadf.index
    finadf['day'] = finadf['date'].apply(lambda x: int(datetime.date.strftime(x, '%d')))
    finadf['mon'] = finadf['date'].apply(lambda x: datetime.date.strftime(x, '%m'))
    finadf['year'] = finadf['date'].apply(lambda x: datetime.date.strftime(x, '%Y'))
    finadf['adjust'] = 0
    finadf['adjust'][(finadf['mon'] != '10')] = finadf['curr']
    finadf['adjust'][(finadf['mon'] == '11') | (finadf['mon'] == '12')] = finadf['next']
    finadf['adjust'][(finadf['mon'] == '10')] = finadf['next']*finadf['day']/31+finadf['curr']*(31-finadf['day'])/31
    finadf['PE'] = finadf.EV/finadf.adjust

    fig, ax = plt.subplots(1, 1, figsize=(6, 5))
    ax.plot(finadf.PE)
    plt.title(name+'动态PE估值')
    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False
    plt.show()
    # plt.savefig(name+'动态PE.png')

if __name__ == '__main__':
    code = input('股票代码：\n')
    print('计算开始：')
    t = datetime.datetime.now()
    pe_new(code)
    print(datetime.datetime.now() - t)

    again = input('是否继续?(yes or no)\n')
    while again == 'yes':
        code = input('股票代码：\n')
        print('计算开始：')
        t = datetime.datetime.now()
        pe_new(code)
        print(datetime.datetime.now() - t)

        again = input('是否继续?(yes or no)\n')

