import numpy as np


def reduce_mem_usage(df):
    df = df.replace([np.inf, -np.inf], np.nan)
    start_mem = df.memory_usage().sum() / 1024 ** 2
    int8min = np.iinfo(np.int8).min
    int8max = np.iinfo(np.int8).max
    int16min = np.iinfo(np.int16).min
    int16max = np.iinfo(np.int16).max
    int32min = np.iinfo(np.int32).min
    int32max = np.iinfo(np.int32).max
    int64min = np.iinfo(np.int64).min
    int64max = np.iinfo(np.int64).max
    float16min = np.finfo(np.float16).min
    float16max = np.finfo(np.float16).max
    float32min = np.finfo(np.float32).min
    float32max = np.finfo(np.float32).max
    for col in df.columns:
        # print(col)
        col_type = df[col].dtypes
        if (col_type != object) & (col_type != str):
            c_min = df[col].min()
            c_max = df[col].max()
            if str(col_type)[:3] == 'int':
                # if c_min >= int8min and c_max <= int8max:
                #     # df[col] = df[col].astype(np.int8)
                #     df[col] = df[col].astype(dtype='int8',copy=True)
                # elif c_min >= int16min and c_max <= int16max:
                #     df[col] = df[col].astype(dtype='int16',copy=True)
                # elif c_min >= int32min and c_max <= int32max:
                if c_min >= int32min and c_max <= int32max:
                    df[col] = df[col].astype(dtype='int32', copy=True)
                elif c_min >= int64min and c_max <= int64max:
                    df[col] = df[col].astype(dtype='int64', copy=True)
            else:
                # if c_min >= float16min and c_max <= float16max:
                #     # df[col] = df[col].astype(np.float16)
                #     df[col] = df[col].astype(dtype='float16',copy=True)
                # elif c_min >= float32min and c_max <= float32max:
                if c_min >= float32min and c_max <= float32max:
                    df[col] = df[col].astype(dtype='float32', copy=True)
                else:
                    df[col] = df[col].astype(dtype='float64', copy=True)
    end_mem = df.memory_usage().sum() / 1024 ** 2
    print('Mem. usage decreased to {:5.2f} Mb ({:.1f}% reduction)'.format(end_mem,
                                                                          100 * (start_mem - end_mem) / start_mem))
    return df
