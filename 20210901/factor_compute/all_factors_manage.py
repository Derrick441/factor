from data_pre import vpd_data_read, vpl1_data_read, vpl2_data_read, fin_data_read, exp_data_read, esg_data_read
from operation_time import print_execute_time
from operation_database import data_to_mysql

from future_ret.ret import ret_sql_compute_sql
from factor_vpd.inv_spreadbias import spreadbias_compute
from factor_vpd.mom_ma import ma_compute
from factor_vpd.mom_mom import mom_compute
from factor_vpd.mom_newhigh import newhigh_compute
from factor_vpd.vol_minret import minret_compute
from factor_vpd.vol_std import std_compute
from factor_vpd.vol_pmd import pmd_compute


@print_execute_time
def all_factors_compute(mode, fac_functions, data_functions):
    # 时间设置
    if mode == 'recompute':
        ret_t1, ret_t2 = '20100101', '20190601'
        fac_sta_list = ['20100101', '20130101', '20160101', '20190101']
        fac_end_list = ['20121231', '20151231', '20181231', '20190601']
        vpd_type1_time_list = zip(fac_sta_list, fac_end_list)
    elif mode == 'update':
        today = '20190610'
        # today = datetime.strftime(datetime.now(), '%Y%m%d')
        ret_t1, ret_t2 = today, today
        vpd_type1_time_list = zip([today], [today])
    else:
        print('请确认全部因子计算模式：recompute or update')
        return 0

    # 未来收益率计算
    print('未来收益率计算：', ret_t1, ret_t2)
    ret_sql_compute_sql('ret', ret_t1, ret_t2, 1, 'future_ret')

    # 因子计算
    # 分时段
    for t1, t2 in vpd_type1_time_list:

        print('因子计算：', t1, t2)
        # 分大类
        for fac_1 in fac_functions.keys():

            # 大类因子数据读取
            data_read_list = data_functions[fac_1]
            data_raw = data_read_list[0](t1, t2, data_read_list[1])

            # 大类因子逐个计算
            fac_dict = fac_functions[fac_1]
            for fac_2 in fac_dict.keys():

                # 因子计算、存储
                fac_list = fac_dict[fac_2]
                try:
                    factors = fac_list[0](data_raw, t1, fac_2, fac_1)
                    data_to_mysql(fac_list[1], factors, fac_list[2], fac_2)
                except Exception as e:
                    print(fac_2 + 'compute failed\nthe problem is:', e)

    return 1


if __name__ == '__main__':

    vpd_functions = {
        'spreadbias': [spreadbias_compute, 1, 'fac_vpd_inv'],
        'ma': [ma_compute, 1, 'fac_vpd_mom'],
        'mom': [mom_compute, 1, 'fac_vpd_mom'],
        'newhigh': [newhigh_compute, 1, 'fac_vpd_mom'],
        'minret': [minret_compute, 1, 'fac_vpd_vol'],
        'std': [std_compute, 1, 'fac_vpd_vol'],
        'pmd': [pmd_compute, 1, 'fac_vpd_vol']
    }

    vpl1_functions = {

    }

    vpl2_functions = {

    }

    fin_functions = {

    }

    exp_functions = {

    }

    esg_functions = {

    }

    fac_functions = {
        'vpd': vpd_functions,
        'vpl1': vpl1_functions,
        'vpl2': vpl2_functions,
        'fin': fin_functions,
        'exp': exp_functions,
        'esg': esg_functions,
    }

    data_functions = {
        'vpd': [vpd_data_read, 19],
        'vpl1': [vpl1_data_read, 1],
        'vpl2': [vpl2_data_read, 1],
        'fin': [fin_data_read, 1],
        'exp': [exp_data_read, 1],
        'esg': [esg_data_read, 1],
    }

    mode = 'recompute'
    # mode = 'update'

    all_factors_compute(mode, fac_functions, data_functions)
