def perf_single_ic(f, method):
    """
    calculate IC or rank IC with concat data
    :param f: column1 is return of stocks, column2 is alpha
    :param method: pearson for IC and spearman for rankIC
    :return:
    """
    if method == 'IC':
        ic = f.corr(method='pearson', mini_periods=1)
    # for rank IC
    else:
        ic = f.corr(method='spearman', mini_periods=1)
    return ic


def perf_evaluation_ic_raw(ret, alpha, method):
    """
    calculate IC or rank IC with raw data, and the data need Groupby
    :param ret: return of stocks
    :param alpha: alpha factor
    :param method: pearson for IC and spearman for rankIC
    :return:
    """
    f = alpha.to_frame('alpha')
    f['ret'] = ret
    ic = f.groupby(level=[0, 1]).apply(perf_single_ic, method=method)
    return ic


def perf_evaluation_ic(f, method):
    """
    calculate IC or rank IC with concat data, and the data need Groupby
    :param f: column1 is return of stocks, column2 is alpha
    :param method: pearson for IC and spearman for rankIC
    :return:
    """
    ic = f.groupby(level=[0, 1]).apply(perf_single_ic, method=method)
    return ic
