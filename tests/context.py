# -*- coding: utf-8 -*-

import sys
import os
_parent_dp = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# _src_dp = os.path.join(_parent_dp, "src")
_src_dp = _parent_dp
sys.path.insert(0, _src_dp)

# import yfinance_cache
from yfinance_cache import yfc_cache_manager, yfc_dat, yfc_prices_manager, yfc_financials_manager, yfc_ticker, yfc_time, yfc_utils, yfc_logging, yfc_options


import numpy as np ; np.seterr(divide='raise', over='raise', under='raise', invalid='raise')


# Setup a session to rate-limit and cache persistently:
import datetime as _dt
import os
import appdirs as _ad
from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass
from pyrate_limiter import Duration, RequestRate, Limiter
history_rate = RequestRate(1, Duration.SECOND*2)
limiter = Limiter(history_rate)
cache_fp = os.path.join(_ad.user_cache_dir(),'yfinance.cache.testing')
if os.path.isfile(cache_fp):
    # Delete local cache if older than 1 day:
    mod_dt = _dt.datetime.fromtimestamp(os.path.getmtime(cache_fp))
    if mod_dt.date() < _dt.date.today():
        os.remove(cache_fp)
session_gbl = CachedLimiterSession(
    limiter=limiter,
    bucket_class=MemoryQueueBucket,
    backend=SQLiteCache(cache_fp, expire_after=_dt.timedelta(hours=1)),
)
session_gbl.headers['User-agent'] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0"
# Use this instead if only want rate-limiting:
# from requests_ratelimiter import LimiterSession
# session_gbl = LimiterSession(limiter=limiter)


