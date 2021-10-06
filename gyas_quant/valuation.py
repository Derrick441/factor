# -*- coding: utf-8 -*-
"""
Created on Thu May 28 13:25:03 2020

@author: Administrator
"""

import datetime
import numpy as np
import pandas as pd
from WindPy import w
import matplotlib.pyplot as plt

code="300327.SZ"


w.start()

name=w.wss(code, "sec_name").Data[0][0]
name
tday=datetime.date.today()
todayyear=tday.year
ipodate=w.wss(code, "ipo_date").Data[0][0].date()
ipoyear=ipodate.year

currdf=pd.DataFrame()

for i in range(ipoyear,todayyear+1):
    begin_date=str(i)+"-01-01"
    end_date=str(i)+"-12-31"
    currdata=w.wsd(code, "ev,est_mediannetprofit", begin_date, end_date, "unit=1;Period=W;PriceAdj=F;year="+str(i))
    df=pd.DataFrame(currdata.Data,index=currdata.Fields,columns=currdata.Times).T
    currdf=currdf.append(df)
currdf=currdf[(currdf.index>=ipodate)&(currdf.index<=tday)].rename(columns = {'EST_MEDIANNETPROFIT':'curr'}) 

nextdf=pd.DataFrame()

for i in range(ipoyear,todayyear+1):
    begin_date=str(i)+"-01-01"
    end_date=str(i)+"-12-31"
    nextdata=w.wsd(code, "est_mediannetprofit", begin_date, end_date, "unit=1;Period=W;PriceAdj=F;year="+str(i+1))
    df=pd.DataFrame(nextdata.Data,index=nextdata.Fields,columns=nextdata.Times).T
    nextdf=nextdf.append(df)
nextdf=nextdf[(nextdf.index>=ipodate)&(nextdf.index<=tday)].rename(columns = {'EST_MEDIANNETPROFIT':'next'}) 

finadf=currdf.join(nextdf)
 

finadf['date']=finadf.index 
finadf['day']=finadf['date'].apply(lambda x:int(datetime.date.strftime(x,'%d')))
finadf['mon']=finadf['date'].apply(lambda x:datetime.date.strftime(x,'%m')) 
finadf['year']=finadf['date'].apply(lambda x:datetime.date.strftime(x,'%Y')) 
finadf['adjust']=finadf[finadf['mon']!='10']['curr']
finadf['adjust'][(finadf['mon']=='11')|(finadf['mon']=='12')]=finadf['next']
finadf['adjust'][finadf['mon']=='10']=finadf['next']*finadf['day']/31+finadf['curr']*(31-finadf['day'])/31
finadf['PE']=finadf.EV/finadf.adjust   


fig,ax=plt.subplots(figsize=(12,8))
ax.plot(finadf.PE)
plt.title(name+'动态PE估值')
plt.rcParams['font.sans-serif']=['SimHei']
plt.rcParams['axes.unicode_minus'] = False
fig.show()
fig.savefig(name+'动态PE.png')

