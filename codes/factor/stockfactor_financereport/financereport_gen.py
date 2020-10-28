import pandas as pd
import numpy as np
import time
from sqlconn import sqlconn
import financerp_last_tradedate as flt
from reduce_mem_usage_df import reduce_mem_usage
from dataflow_tradedays import yearFirstTradeDate
from dataflow_tradedays import stockFirstTradeDate


class FinanceReportGen(object):
    def __init__(self, INDEX, indir, ftable, keep_report, enddate, longtimeago, QuarterNum):
        self.INDEX = INDEX
        self.indir = indir
        self.ftable = ftable
        self.keep_report = keep_report   # drop duplicated report data, keep 'first' or 'last'
        self.longtimeago = longtimeago
        self.enddate = enddate
        self.QuarterNum = QuarterNum

    def fileIn(self):
        t = time.time()
        self.datefactor = pd.read_pickle(self.indir+self.INDEX+'/'+self.INDEX+'_band_dates_stocks_closep.pkl')
        # self.datefactor = self.datefactor.loc[self.datefactor['trade_dt']>'20180102']
        self.datefactor.drop(['s_dq_close'], axis=1, inplace=True)
        self.datefactor['trade_year'] = self.datefactor.trade_dt.str.slice(0, 4)
        print('filein using time:%10.4fs' % (time.time() - t))

    def sqlGetName(self, conn):
        sqlq = "select COLUMN_NAME from all_tab_columns " \
               "where owner='WIND' and " \
               "table_name='"+self.ftable+"'"
        factornames = pd.read_sql(sqlq, conn)
        return factornames['COLUMN_NAME'].apply(lambda x: x.lower())

    def sqlDropName(self, factornames):
        dropfactors = ['s_info_windcode',
                       'wind_code',
                       'ann_dt',
                       'report_period',
                       'crncy_code',
                       'object_id',
                       'opdate',
                       'opmode']
        factornames = factornames[~factornames.isin(dropfactors)]
        return factornames

    def sqlIn(self):
        t = time.time()
        # 取财务因子的因子名
        conn = sqlconn()
        self.factornames = self.sqlGetName(conn)
        self.factornames = self.sqlDropName(self.factornames)
        self.factornames = self.factornames.values.tolist()
        factorstr = ','.join(self.factornames)

        # 取合并报表(408001000)和合并报表调整(4008004000)，不取单季度
        sqlq = "select s_info_windcode,report_period,ann_dt,"+factorstr+" from wind."+self.ftable+" " \
               "where report_period>'"+self.longtimeago+"' " \
               "and statement_type in ('408001000','408004000') " \
               "and s_info_windcode<='699999.SH' " \
               "and ann_dt<='"+self.enddate+"' " \
               "order by s_info_windcode,report_period,ann_dt"
        self.data = pd.read_sql(sqlq, conn)
        conn.close()
        print('sqlin using time:%10.4fs' % (time.time() - t))

    def findYearFirstTradeDate(self):
        # 添加未来年份
        # 将startdate所在年份的第一个交易日设为起始日
        yfd = yearFirstTradeDate(self.longtimeago, self.enddate)

        endyear = int(self.enddate[0:4])
        for item in range(endyear + 1, endyear + 10):
            yfd[item] = str(item) + '0101'

        startdate = self.datefactor.trade_dt.iloc[0]
        startyear = int(startdate[0:4])
        yfd[startyear] = startdate
        return yfd

    def replaceLastTradeDate(self, factor):
        # 替换节假日到上一个交易日
        datelist = self.datefactor['trade_dt'].unique()
        lastdatedict = flt.dict_gen(datelist[0], datelist[-1], datelist)
        factor['ann_dt'] = flt.reportdates_replace_simpledict(factor['ann_dt'], lastdatedict)
        return factor

    def seasonShift(self):
        t = time.time()
        # 名称变小写
        self.data.rename(columns=lambda x: x.lower(), inplace=True)
        # 替换节假日
        self.replaceLastTradeDate(self.data)

        # 每一季财报，其需要用到当年(year0，作为当年财报),一年后(year1，作为去年财报),year2,year3,year4 等5个日期
        # 此处生成这5个日期
        yfd = self.findYearFirstTradeDate()
        sfd = stockFirstTradeDate(self.datefactor.copy())
        self.data['reportyear'] = self.data['report_period'].str.slice(0, 4).astype('int16')
        self.data['reportseason'] = self.data['report_period'].str.slice(4, 8)
        self.data['listdate'] = self.data.s_info_windcode.replace(sfd)
        self.data['year+0'] = self.data.reportyear.replace(yfd)
        self.data['year+1'] = (self.data.reportyear+1).replace(yfd)
        self.data['year+2'] = (self.data.reportyear+2).replace(yfd)
        self.data['year+3'] = (self.data.reportyear+3).replace(yfd)
        self.data['year+4'] = (self.data.reportyear+4).replace(yfd)
        self.data['year+5'] = (self.data.reportyear+5).replace(yfd)
        print('seanshift using time:%10.4fs' % (time.time() - t))

    def dataSeperateYearOut(self, factor, name):
        startdate = self.datefactor.trade_dt.iloc[0]
        for year in range(int(startdate[0:4]), int(self.enddate[0:4])+1):
            print('Out table: '+self.ftable+' Out year :'+str(year)+' Out name:'+name)
            yf = factor.loc[factor['trade_year'] == str(year)]
            save_indir = self.indir+self.INDEX+'/'+self.INDEX+'_' + self.ftable+name+'_'+str(year)+'.pkl'
            yf.drop(['trade_year'], axis=1).to_pickle(save_indir)

    def seasonMapSub(self, setyear, setyearp1, setseason, setname):
        print(setyear+' '+setseason+' '+setname)

        # 将季报映射到datefactor上面
        datef = self.datefactor.copy()
        self.data['trade_dt'] = self.data[setyear]
        seasonf = self.data[self.data['reportseason'] == setseason].copy()

        # ann_dt和listdate超过setyear+1年的数据没有用，因为当年用不上
        seasonf = seasonf.loc[seasonf[setyearp1] > seasonf['ann_dt']]
        seasonf = seasonf.loc[seasonf[setyearp1] > seasonf['listdate']]

        # 公布日如果大于映射日，将映射日替换成公布日
        idx = seasonf['ann_dt'] > seasonf['trade_dt']
        seasonf.loc[idx, 'trade_dt'] = seasonf.loc[idx, 'ann_dt']

        # 上市日如果大于映射日，将映射日替换成上市日
        idx = seasonf['listdate'] > seasonf['trade_dt']
        seasonf.loc[idx, 'trade_dt'] = seasonf.loc[idx, 'listdate']

        # 排序,存在重复公布日的调整前后数据，因ann_dt已处理过比trade_dt小，故保留最后更新的
        seasonf.sort_values(by=['trade_dt', 's_info_windcode', 'report_period', 'ann_dt'], inplace=True)
        seasonf.drop_duplicates(['trade_dt', 's_info_windcode'], keep='last', inplace=True)

        # 映射到每日数据
        seasonf.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        datef.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        datef[seasonf.columns.to_list()] = seasonf

        # 前填
        datef = datef.groupby(level=1).ffill()

        # 需要取出trade_dt用于比较
        datef.reset_index(inplace=True)

        # 前填之后，可能有当年数据进入下一年了，需要删除
        idx = datef.trade_dt >= datef[setyearp1]
        datef.loc[idx, self.factornames+['ann_dt', 'report_period']] = np.nan

        # 交易日小于发布日的数据设为nan
        idx = datef.trade_dt < datef.ann_dt
        datef.loc[idx, self.factornames+['ann_dt', 'report_period']] = np.nan

        # 还原日期和代码为index
        datef.set_index(['trade_dt', 's_info_windcode'], inplace=True)

        # 删除冗余数据
        item = ['reportyear', 'reportseason', 'listdate', 'year+0', 'year+1', 'year+2', 'year+3', 'year+4']
        datef.drop(labels=item, axis=1, inplace=True)

        # 推导新列名字典
        namedict = {item: item+setname for item in self.factornames}
        namedict['ann_dt'] = 'ann_dt'+setname
        namedict['report_period'] = 'report_period'+setname

        # 更改当前因子的名字，加入后缀
        datef.rename(columns=namedict, inplace=True)

        # 输出结果
        self.dataSeperateYearOut(datef, setname)

    def seasonMap(self):
        # 当年没有年报
        self.seasonMapSub(setyear='year+0', setyearp1='year+1', setseason='0331', setname='_y-0s1')
        self.seasonMapSub(setyear='year+0', setyearp1='year+1', setseason='0630', setname='_y-0s2')
        self.seasonMapSub(setyear='year+0', setyearp1='year+1', setseason='0930', setname='_y-0s3')

        # 一年前数据
        self.seasonMapSub(setyear='year+1', setyearp1='year+2', setseason='0331', setname='_y-1s1')
        self.seasonMapSub(setyear='year+1', setyearp1='year+2', setseason='0630', setname='_y-1s2')
        self.seasonMapSub(setyear='year+1', setyearp1='year+2', setseason='0930', setname='_y-1s3')
        self.seasonMapSub(setyear='year+1', setyearp1='year+2', setseason='1231', setname='_y-1s4')

        # 两年前数据
        self.seasonMapSub(setyear='year+2', setyearp1='year+3', setseason='0331', setname='_y-2s1')
        self.seasonMapSub(setyear='year+2', setyearp1='year+3', setseason='0630', setname='_y-2s2')
        self.seasonMapSub(setyear='year+2', setyearp1='year+3', setseason='0930', setname='_y-2s3')
        self.seasonMapSub(setyear='year+2', setyearp1='year+3', setseason='1231', setname='_y-2s4')

        # 三年前数据
        self.seasonMapSub(setyear='year+3', setyearp1='year+4', setseason='0331', setname='_y-3s1')
        self.seasonMapSub(setyear='year+3', setyearp1='year+4', setseason='0630', setname='_y-3s2')
        self.seasonMapSub(setyear='year+3', setyearp1='year+4', setseason='0930', setname='_y-3s3')
        self.seasonMapSub(setyear='year+3', setyearp1='year+4', setseason='1231', setname='_y-3s4')

        # 四年前数据
        self.seasonMapSub(setyear='year+4', setyearp1='year+5', setseason='0331', setname='_y-4s1')
        self.seasonMapSub(setyear='year+4', setyearp1='year+5', setseason='0630', setname='_y-4s2')
        self.seasonMapSub(setyear='year+4', setyearp1='year+5', setseason='0930', setname='_y-4s3')
        self.seasonMapSub(setyear='year+4', setyearp1='year+5', setseason='1231', setname='_y-4s4')

    def factorClassificate(self):
        # 预处理数据，主要用于减低存储空间，增长率变成0-1000的int型，资产变成int型
        # 保留原值的因子
        self.toorifactor = [
            ]

        # 乘以10000以后转成int，即保留到bp
        self.tomulfactor = [
            ]

        # 转成 int 型的变量
        self.tointfactor = [
            ]

        # # 预处理时，将所有nan都填充为0，因为考虑到缺失值不应该用上一季的值，直接取0时一种处理方法
        # self.data.rename(columns=lambda x:x.lower(),inplace=True)
        # self.data.fillna(value=0,inplace=True)
        # start_mem = self.data.memory_usage().sum() / 1024 ** 2
        # self.data[tointfactor] = self.data[tointfactor].astype(int)
        # self.data[tomulfactor] = (10000*self.data[tomulfactor]).astype(int)
        # end_mem = self.data.memory_usage().sum() / 1024 ** 2
        # print('Mem pre cut usage decreased to {:5.2f} Mb ({:.1f}% reduction)'.format(end_mem,
        #                                                                   100 * (start_mem - end_mem) / start_mem))

    def self_reduce_mem_usage(self, factors, postfix):
        intfname = [x+postfix for x in self.tointfactor]
        mulfname = [x+postfix for x in self.tomulfactor]

        # 整型金额数据都以万为单位
        intfactor = np.abs(factors[intfname])/10000
        # 小于1万的金额当成1万
        intfactor[intfactor < 1] = 1
        # 取对数，然后保留4位小数，由于10^0.0001只有0.0023，对数据影响不大，而且lgb的max_bin默认在255左右，所以对最后结果影响不大
        factors[intfname] = ((np.sign(factors[intfname])*np.log10(intfactor))).round(4)
        # 对于财务比值，大于10000的都设成10000
        mulfactor = factors[mulfname].copy()
        mulfactor[mulfactor > 10000] = 10000
        factors[mulfname] = mulfactor
        # 对于财务比值来说，千分之一的精度已经足够了
        factors[mulfname] = factors[mulfname].round(3)
        return reduce_mem_usage(factors)

    def run_flow(self):
        self.fileIn()
        self.sqlIn()
        self.seasonShift()
        self.seasonMap()
        # self.factorClassificate()
        # self.seperateflag_get()
        # self.quarter_data_gen()
        # self.quarter_data_to_date()


if __name__ == '__main__':
    indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\'
    INDEX = 'all'
    ftable = 'ASHAREINCOME'
    # ftable = 'AShareIncome'
    keep_report = 'last'
    enddate = '20191231'
    longtimeago = '20030101'
    QuarterNum = 12

    financereport = FinanceReportGen(INDEX, indir, ftable, keep_report, enddate, longtimeago, QuarterNum)
    financereport.run_flow()
