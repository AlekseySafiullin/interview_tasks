import time

from functools import wraps


def time_counter(fn):
    @wraps(fn)
    def callback(*args, **kwargs):
        start = time.time()

        result = fn(*args, **kwargs)

        end = time.time()

        print(f'Duration {fn.__module__}.{fn.__name__}: {end - start}s')

        return result

    return callback
