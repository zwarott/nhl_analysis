import functools
from time import time


def timer(func):
    "Decorator for recording the duration of function process."
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time()
        result = func(*args, **kwargs)
        duration = time() - start_time
        print(f"Process time: {duration:.2f} s.")
        return result

    return wrapper
