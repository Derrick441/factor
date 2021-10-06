import numpy as np
import pandas as pd
from data_pre import vpd_data_read, factor_read
from operation_rolling import pd_mean


def data_20170101_20180601_20210924():
    # 时间设定
    t1, t2 = '20170101', '20180601'

    # 数据读取
    basic_data = vpd_data_read(t1, t2, 0)
    rets = factor_read('future_ret', t1, t2, 0)
    fac1s = factor_read('fac_vpd_mom', t1, t2, 0)
    fac2s = factor_read('fac_vpd_vol', t1, t2, 0)

    # 数据整理
    data_temp = basic_data.copy()
    data_temp.loc[data_temp.volume == 0.0, ['close_adj', 'volume', 'changepct']] = np.nan
    temp_data = data_temp[~np.isnan(data_temp.volume)].copy()

    rets_p = rets.pivot(index=['stock', 'date'], columns='factor_name', values='factor_value')
    fac1s_p = fac1s.pivot(index=['stock', 'date'], columns='factor_name', values='factor_value')
    fac2s_p = fac2s.pivot(index=['stock', 'date'], columns='factor_name', values='factor_value')
    temp_data = pd.merge(temp_data, rets_p.reset_index(), how='left')
    temp_data = pd.merge(temp_data, fac1s_p.reset_index(), how='left')
    temp_data = pd.merge(temp_data, fac2s_p.reset_index(), how='left')

    temp_data['sp005'] = temp_data['mom005'] / temp_data['std005'] * np.sqrt(48)
    temp_data['sp010'] = temp_data['mom010'] / temp_data['std010'] * np.sqrt(24)
    temp_data['sp020'] = temp_data['mom020'] / temp_data['std020'] * np.sqrt(12)
    temp_data['sp060'] = temp_data['mom060'] / temp_data['std060'] * np.sqrt(4)
    temp_data['sp120'] = temp_data['mom120'] / temp_data['std120'] * np.sqrt(2)
    temp_data['sp240'] = temp_data['mom240'] / temp_data['std240'] * np.sqrt(1)

    newhigh240 = temp_data[['stock', 'date', 'newhigh240']].pivot('date', 'stock', 'newhigh240')
    newhigh240_mean120 = pd_mean(newhigh240, 120)
    nh240_120_mean = newhigh240_mean120.stack().reset_index().rename(columns={0: 'nh240_120_mean'})
    temp_data = pd.merge(temp_data, nh240_120_mean, how='left')

    newhigh240_mean005 = pd_mean(newhigh240, 5)
    nh240_005_mean = newhigh240_mean005.stack().reset_index().rename(columns={0: 'nh240_005_mean'})
    temp_data = pd.merge(temp_data, nh240_005_mean, how='left')

    result = temp_data.dropna(how='any')

    # 数据临时保存
    result.to_pickle('C:\\Users\\Administrator\\Desktop\\data_' + t1 + '_' + t2 + '_.pkl')


if __name__ == '__main__':

    data_20170101_20180601_20210924()
