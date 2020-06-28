# -*- coding: utf-8 -*-
import time
import functools


def timeit(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        ts = time.perf_counter()
        result = func(*args, **kwargs)
        te = time.perf_counter()
        run_time = te - ts
        print("-"*20)
        print(f"Finished <{func.__name__!r}> in {run_time:.4f} sec.")
        return result
    return wrapper
