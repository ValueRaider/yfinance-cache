#!/usr/bin/env python

import os
with open(os.path.join(os.path.dirname(__file__), 'VERSION'), 'r') as f:
    __version__ = f.read().strip()

from .yfc_dat import Period, Interval, AmbiguousComparisonException
from .yfc_ticker import Ticker, verify_cached_tickers_prices
from .yfc_multi import download
from .yfc_logging import EnableLogging, DisableLogging
from .yfc_cache_manager import _option_manager as options


from .yfc_upgrade import _tidy_upgrade_history
_tidy_upgrade_history()

from .yfc_upgrade import _recommend_verify
_recommend_verify()

# Wait for Pandas 3 to be stable, because YFC will have to require Pandas 3.
# Remember to switch calendars FetchDate to UTC tz
# from .yfc_upgrade import _migrate_dfs_to_pandas3
# _migrate_dfs_to_pandas3()

