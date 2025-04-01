import datetime
from collections import OrderedDict
from typing import Any, Optional

"""Heavily influenced by (lifted from) async-cache (https://pypi.org/project/async-cache/)
"""


class AsyncTTLCache:
    """This class will keep a cached copy of the return values of async coroutines. It
    should only be used for coroutines that retrieve values. Any modification of values
    by the coroutine would not actually be run while a return value is stored in the
    cache.
    """

    class _CacheKey:
        """This class is used to create the hash key for the cache, with the basic idea
        being that the arguments passed to the cached function will be used as the
        basis for the hash so that each call with different arguments will be stored
        separately.
        """

        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

        def __eq__(self, obj):
            return hash(self) == hash(obj)

        def __hash__(self):
            def _hash(param: Any):
                if isinstance(param, tuple):
                    return tuple(map(_hash, param))
                if isinstance(param, dict):
                    return tuple(map(_hash, param.items()))
                elif hasattr(param, "__dict__"):
                    return str(vars(param))
                else:
                    return str(param)

            return hash(_hash(self.args) + _hash(self.kwargs))

    class _InnerCache(OrderedDict):
        """This class is used to store the returned values. It is an inner class to
        hide the Dictionary interface from the caller. It extends from an OrderedDict
        so that the data can be sorted in a least recently used order. The reason to
        do this is so that when the maximum size is reached, the least recently
        used entry can easily be removed. The idea behind this is that if a value
        hasn't been used in a while, it is less likely to be needed any time soon.
        """

        def __init__(
            self, time_to_live: Optional[int] = 60, max_size: Optional[int] = 1024
        ):
            self.time_to_live = (
                datetime.timedelta(seconds=time_to_live) if time_to_live else None
            )
            self.max_size = max_size
            super().__init__()

        def __contains__(self, key):
            if key not in self.keys():
                return False
            else:
                key_expiration = super().__getitem__(key)[1]
                if key_expiration and key_expiration < datetime.datetime.now():
                    del self[key]
                    return False
                else:
                    return True

        def __getitem__(self, key):
            value = super().__getitem__(key)[0]
            self.move_to_end(key)
            return value

        def __setitem__(self, key, value):
            time_to_live_value = self._get_time_to_live_value()
            super().__setitem__(key, (value, time_to_live_value))
            if self.max_size and len(self) > self.max_size:
                oldest = next(iter(self))
                del self[oldest]

        def _get_time_to_live_value(self):
            return (
                (datetime.datetime.now() + self.time_to_live)
                if self.time_to_live
                else None
            )

    def __init__(
        self, time_to_live: Optional[int] = 60, max_size: Optional[int] = 1024
    ):
        """If an integer value of time_to_live is provided, the cached copy will expire in
        that many seconds. If it is set to None, it will not expire.

        If an integer value of max_size is provided, the cache will only keep that many
        returned values in memory. If it is set to None, the cache size will not be
        limited. When the max size has been reached, the least recently used values will
        be pushed out.
        """
        self._inner_cache = self._InnerCache(
            time_to_live=time_to_live, max_size=max_size
        )

    def __call__(self, func):
        def reset():
            """This was added to allow resetting of the cache between tests."""
            self._inner_cache.clear()

        async def wrapper(*args, **kwargs):
            key = self._CacheKey(args, kwargs)
            if key in self._inner_cache:
                val = self._inner_cache[key]
            else:
                self._inner_cache[key] = await func(*args, **kwargs)
                val = self._inner_cache[key]

            return val

        wrapper.__name__ += func.__name__
        wrapper.reset = reset

        return wrapper
