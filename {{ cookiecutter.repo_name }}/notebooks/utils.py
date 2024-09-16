import numpy as np
import pandas as pd

from typing import Optional


def reduce_memory(X: pd.DataFrame, y: Optional[pd.Series] = None, verbose: bool = True):
    int64df = X.select_dtypes(pd.Int64Dtype())
    X[int64df.columns] = int64df.astype(float)

    if verbose:
        start_mem = X.memory_usage().sum() / 1024**2

    def _reduce_numeric_series(col: pd.Series) -> pd.Series:
        c_min, c_max = col.min(), col.max()
        col_type = col.dtype

        if str(col_type) == "bool":
            return col
        elif str(col_type)[:3] == "int":
            if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                return col.astype(np.int8)
            elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                return col.astype(np.int16)
            elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                return col.astype(np.int32)
            elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                return col.astype(np.int64)
        else:
            if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                return col.astype(np.float16)
            elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                return col.astype(np.float32)
            else:
                return col.astype(np.float64)

    for col in X.columns:
        if pd.api.types.is_numeric_dtype(X[col]):
            X[col] = _reduce_numeric_series(X[col])

    # if (y is not None) and (pd.api.types.is_numeric_dtype(y)):
    #     y = _reduce_numeric_series(y)

    if verbose:
        end_mem = X.memory_usage().sum() / 1024**2
        pct = (start_mem - end_mem) / start_mem  # type: ignore
        print(f"Mem. usage decreased to {end_mem:5.2f} Mb ({pct:.1%}% reduction)")

    return X
