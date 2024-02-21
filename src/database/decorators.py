import functools
from time import time


def timer(func):
    "Decorator for recording the duration of function process."
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time()
        result = func(*args, **kwargs)
        duration = time() - start_time
        # Calculate minutes and remaining seconds
        minutes, seconds = divmod(duration, 60)
        minutes = int(minutes)
        print(f"Process time: {minutes} minutes {seconds:.2f} seconds.")
        return result

    return wrapper

