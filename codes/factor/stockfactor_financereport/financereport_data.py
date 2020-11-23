import pandas as pd
import numpy as np
import time
from sqlconn import sqlconn


# 财务数据处理
class FinanceData(object):

    def __init__(self, file_indir, save_indir, file_name, starttime, endtime, table_name):
        self.file_indir = file_indir
        self.save_indir = save_indir
        self.file_name = file_name

        self.starttime = starttime
        self.endtime = endtime

        self.table_name = table_name

    # 1
    def filein(self):
        t = time.time()
        # 读入交易日期数据
        self.datefactor = pd.read_pickle(self.file_indir + self.file_name)
        # self.datefactor = self.datefactor[self.datefactor['trade_dt'] >= '20180101']
        self.datefactor.drop(['s_dq_close'], axis=1, inplace=True)
        self.datefactor['trade_year'] = self.datefactor.trade_dt.str.slice(0, 4)
        print('filein using time:%10.4fs' % (time.time() - t))

    def sqlGetName(self, conn):
        # 取表中所有列名
        sqlq = "select COLUMN_NAME from all_tab_columns " \
               "where owner='WIND' and table_name='"+self.table_name+"'"
        factornames = pd.read_sql(sqlq, conn)
        return factornames['COLUMN_NAME'].apply(lambda x: x.lower())

    def sqlDropName(self, factornames):
        # 删去一些列名
        dropfactors = ['s_info_windcode', 'wind_code', 'ann_dt', 'report_period',
                       'crncy_code', 'object_id', 'opdate', 'opmode']
        factornames = factornames[~factornames.isin(dropfactors)]
        return factornames

    # 2
    def sqlin(self):
        t = time.time()
        # 取财务因子的因子名
        conn = sqlconn()
        self.factornames = self.sqlGetName(conn)
        self.factornames = self.sqlDropName(self.factornames)
        self.factornames = self.factornames.values.tolist()
        factorstr = ','.join(self.factornames)

        # 取合并报表(408001000)和合并报表调整(4008004000)，不取单季度
        sqlq = "select s_info_windcode,report_period,ann_dt,"+factorstr+" from wind."+self.table_name+" " \
               "where report_period>'"+self.starttime+"' and statement_type in ('408001000','408004000') " \
               "and s_info_windcode<='699999.SH' and ann_dt<='"+self.endtime+"' " \
               "order by s_info_windcode,report_period,ann_dt"
        self.data = pd.read_sql(sqlq, conn)
        conn.close()

        # 名称变小写
        self.data.rename(columns=lambda x: x.lower(), inplace=True)
        print('sqlin using time:%10.4fs' % (time.time() - t))

    def holidayLastTradeDate(self, startdate, enddate, tradedatelist):
        # 所有日期
        alldatelist = list(pd.date_range(startdate, enddate).strftime('%Y%m%d'))
        # 生成两个日期dataframe
        alldate = pd.DataFrame(index=alldatelist, data=alldatelist, columns=['alldate'])
        tradedate = pd.DataFrame(index=tradedatelist, data=tradedatelist, columns=['tradedate'])
        alldate['tradedate'] = tradedate['tradedate']
        # 节假日地址
        idx = alldate['tradedate'].isnull()
        # 交易日向前填充？
        alldate.ffill(inplace=True)
        # 返回的字典：{节假日：节假日前交易日}
        return alldate[idx]['tradedate'].to_dict()

    def replaceHolidayLastTradeDate(self, datefactor, data):
        datelist = datefactor['trade_dt'].unique()  # 所有交易日
        hld = self.holidayLastTradeDate(datelist[0], datelist[-1], datelist)  # hld:{节假日：节假日前交易日}
        # 节假日替换成前一交易日
        anndate = data['ann_dt'].unique()
        simpledict = {key: value for key, value in hld.items() if key in anndate}
        data['ann_dt'] = data['ann_dt'].replace(simpledict)
        return data

    def stockFirstTradeDate(self, datefactor):
        datefactor.sort_values(['s_info_windcode', 'trade_dt'], inplace=True)
        datefactor.drop_duplicates(['s_info_windcode'], keep='first', inplace=True)
        # 返回字典：{股票代码：第一个交易日}
        return datefactor.set_index(['s_info_windcode']).trade_dt.to_dict()

    def yearFirstTradeDate(self, startdate, enddate):
        # 取所有交易日数据
        conn = sqlconn()
        sqlquery = "select distinct trade_days from wind.AShareCalendar " \
                   "where trade_days>='"+startdate+"' and trade_days<='"+enddate+"' " \
                   'order by trade_days'
        data = pd.read_sql(sqlquery, conn)
        data['year'] = data.TRADE_DAYS.str.slice(0, 4).astype('int16')
        # 保留每年的第一个交易日
        data.drop_duplicates(['year'], keep='first', inplace=True)
        # 返回的字典：{年：第一个交易日}
        return data.set_index(['year']).TRADE_DAYS.to_dict()

    def findYearFirstTradeDate(self, data, starttime, endtime):
        # {年：第一个交易日}
        yfd = self.yearFirstTradeDate(starttime, endtime)
        # 添加未来年份？
        endyear = int(endtime[0:4])
        for item in range(endyear + 1, endyear + 10):
            yfd[item] = str(item) + '0101'
        # 更新第一年的第一个交易日？
        startdate = data.trade_dt.iloc[0]
        startyear = int(startdate[0:4])
        yfd[startyear] = startdate
        return yfd

    # 3
    def seasonShift(self):
        t = time.time()
        # 节假日替换前一交易日
        self.data = self.replaceHolidayLastTradeDate(self.datefactor.copy(), self.data.copy())
        # 报告年、季节
        self.data['reportyear'] = self.data['report_period'].str.slice(0, 4).astype('int16')
        self.data['reportseason'] = self.data['report_period'].str.slice(4, 8)
        # 股票交易首日
        sfd = self.stockFirstTradeDate(self.datefactor.copy())
        self.data['listdate'] = self.data['s_info_windcode'].replace(sfd)
        # 每一季财报，其需要用到当年(year0),一年后（year1）,year2,year3,year4 等5个日期,此处生成这5个日期
        yfd = self.findYearFirstTradeDate(self.datefactor.copy(), self.starttime, self.endtime)
        self.data['year+0'] = self.data.reportyear.replace(yfd)
        self.data['year+1'] = (self.data.reportyear+1).replace(yfd)
        self.data['year+2'] = (self.data.reportyear+2).replace(yfd)
        self.data['year+3'] = (self.data.reportyear+3).replace(yfd)
        self.data['year+4'] = (self.data.reportyear+4).replace(yfd)
        self.data['year+5'] = (self.data.reportyear+5).replace(yfd)
        print('seanshift using time:%10.4fs' % (time.time() - t))

    def dataSeperateYearOut(self, factor, name):
        startdate = self.datefactor.trade_dt.iloc[0]
        for year in range(int(startdate[0:4]), int(self.endtime[0:4])+1):
            print('Out table: '+self.table_name+' Out year :'+str(year)+' Out name:'+name)
            yf = factor[factor['trade_year'] == str(year)]
            save_indir = self.save_indir+'all_' + self.table_name+name+'_'+str(year)+'.pkl'
            yf.drop(['trade_year'], axis=1).to_pickle(save_indir)

    # 将季报映射到datefactor上面
    def seasonMapSub(self, setyear, setyearp1, setseason, setname):
        print(setyear+' '+setseason+' '+setname)

        # 交易日数据
        datef = self.datefactor.copy()

        # 所有财报数据，添加trade_dt（初始设其为当年的第一个交易日）
        self.data['trade_dt'] = self.data[setyear]
        # 取某一季度的所有财报
        seasonf = self.data[self.data['reportseason'] == setseason].copy()
        # 公告日期ann_dt和股票交易首日listdate超过下一年第一个交易日的数据没有用，因为当年用不上
        seasonf = seasonf[seasonf[setyearp1] > seasonf['ann_dt']]
        seasonf = seasonf[seasonf[setyearp1] > seasonf['listdate']]

        # 公布日如果大于映射日（当年第一个交易日），将映射日替换成公布日（大部分日期都被替换）
        idx = seasonf['ann_dt'] > seasonf['trade_dt']
        seasonf.loc[idx, 'trade_dt'] = seasonf.loc[idx, 'ann_dt']

        # 上市日如果大于映射日，将映射日替换成上市日
        idx = seasonf['listdate'] > seasonf['trade_dt']
        seasonf.loc[idx, 'trade_dt'] = seasonf.loc[idx, 'listdate']

        # 排序
        seasonf.sort_values(by=['trade_dt', 's_info_windcode', 'report_period', 'ann_dt'], inplace=True)
        # 存在重复公布日的调整前后数据，因ann_dt已处理过比trade_dt小，故保留最后更新的
        seasonf.drop_duplicates(['trade_dt', 's_info_windcode'], keep='last', inplace=True)

        # 映射到每日数据
        seasonf.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        datef.set_index(['trade_dt', 's_info_windcode'], inplace=True)
        datef[seasonf.columns.to_list()] = seasonf

        # 财报数据前填
        datef = datef.groupby(level=1).ffill()

        # 需要取出trade_dt用于比较
        datef.reset_index(inplace=True)

        # 前填之后，可能有当年数据进入下一年了，需要设为nan
        idx = datef.trade_dt >= datef[setyearp1]
        datef.loc[idx, self.factornames+['ann_dt', 'report_period']] = np.nan

        # 交易日小于发布日的数据设为nan
        idx = datef.trade_dt < datef.ann_dt
        datef.loc[idx, self.factornames+['ann_dt', 'report_period']] = np.nan

        # 设日期和股票代码为index
        datef.set_index(['trade_dt', 's_info_windcode'], inplace=True)

        # 删除冗余数据
        item = ['reportyear', 'reportseason', 'listdate', 'year+0', 'year+1', 'year+2', 'year+3', 'year+4', 'year+5']
        datef.drop(labels=item, axis=1, inplace=True)

        # 推导新列名字典
        namedict = {item: item+setname for item in self.factornames}
        namedict['ann_dt'] = 'ann_dt'+setname
        namedict['report_period'] = 'report_period'+setname

        # 更改当前因子的名字，加入后缀
        datef.rename(columns=namedict, inplace=True)

        # 输出结果
        self.dataSeperateYearOut(datef, setname)

    # 4
    def seasonMap(self):
        # 当年没有年报
        self.seasonMapSub(setyear='year+0', setyearp1='year+1', setseason='0331', setname='_y-0s1')
        # self.seasonMapSub(setyear='year+0', setyearp1='year+1', setseason='0630', setname='_y-0s2')
        # self.seasonMapSub(setyear='year+0', setyearp1='year+1', setseason='0930', setname='_y-0s3')
        #
        # # 一年前数据
        # self.seasonMapSub(setyear='year+1', setyearp1='year+2', setseason='0331', setname='_y-1s1')
        # self.seasonMapSub(setyear='year+1', setyearp1='year+2', setseason='0630', setname='_y-1s2')
        # self.seasonMapSub(setyear='year+1', setyearp1='year+2', setseason='0930', setname='_y-1s3')
        # self.seasonMapSub(setyear='year+1', setyearp1='year+2', setseason='1231', setname='_y-1s4')
        #
        # # 两年前数据
        # self.seasonMapSub(setyear='year+2', setyearp1='year+3', setseason='0331', setname='_y-2s1')
        # self.seasonMapSub(setyear='year+2', setyearp1='year+3', setseason='0630', setname='_y-2s2')
        # self.seasonMapSub(setyear='year+2', setyearp1='year+3', setseason='0930', setname='_y-2s3')
        # self.seasonMapSub(setyear='year+2', setyearp1='year+3', setseason='1231', setname='_y-2s4')
        #
        # # 三年前数据
        # self.seasonMapSub(setyear='year+3', setyearp1='year+4', setseason='0331', setname='_y-3s1')
        # self.seasonMapSub(setyear='year+3', setyearp1='year+4', setseason='0630', setname='_y-3s2')
        # self.seasonMapSub(setyear='year+3', setyearp1='year+4', setseason='0930', setname='_y-3s3')
        # self.seasonMapSub(setyear='year+3', setyearp1='year+4', setseason='1231', setname='_y-3s4')
        #
        # # 四年前数据
        # self.seasonMapSub(setyear='year+4', setyearp1='year+5', setseason='0331', setname='_y-4s1')
        # self.seasonMapSub(setyear='year+4', setyearp1='year+5', setseason='0630', setname='_y-4s2')
        # self.seasonMapSub(setyear='year+4', setyearp1='year+5', setseason='0930', setname='_y-4s3')
        # self.seasonMapSub(setyear='year+4', setyearp1='year+5', setseason='1231', setname='_y-4s4')

    def runflow(self):
        print('start')
        t = time.time()
        self.filein()
        self.sqlin()
        self.seasonShift()
        self.seasonMap()
        print('finish using time:%10.4fs' % (time.time() - t))


if __name__ == '__main__':
    file_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\'
    save_indir = 'D:\\wuyq02\\develop\\python\\data\\developflow\\all\\finance\\'
    file_name = 'all_band_dates_stocks_closep.pkl'
    starttime = '20030101'
    endtime = '20191231'
    table_name = 'ASHAREINCOME'

    fd = FinanceData(file_indir, save_indir, file_name, starttime, endtime, table_name)
    # fd.runflow()
    self = fd
    self.filein()
    self.sqlin()
    self.seasonShift()
    self.seasonMap()
