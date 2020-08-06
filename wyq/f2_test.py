import pandas as pd
import numpy as np
import time

INDEX = 'zz500'
indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'

#数据导入
zz500_indexprice=pd.read_pickle(indir+INDEX+'/'+INDEX+'_indexprice.pkl')
zz500_freeweight=pd.read_pickle(indir+INDEX+'/'+INDEX+'_freeweight.pkl')
zz500_freeweight_curdate=pd.read_pickle(indir+INDEX+'/'+INDEX+'_freeweight_curdate.pkl')

#市场收益率
zz500_indexprice['s_dq_rate']=zz500_indexprice.s_dq_change/zz500_indexprice.s_dq_preclose*100






zz500_freeweight.loc['20200731'].sort_values(by='weightedratio')
zz500_freeweight_curdate.loc['20200731'].sort_values(by='weightedratio')