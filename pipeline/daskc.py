import dask.array as da
import numpy as np
np.random.seed(0) # for reproducibility

import dask.array as da
x = da.random.random((10000, 10000), chunks=(1000, 1000))
x


def foo(a):
    print(a.shape, a.size)
    return a*2
result = x.map_blocks(foo, drop_axis=0)
r  =result.compute()  # this works
