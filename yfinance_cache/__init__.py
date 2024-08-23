#!/usr/bin/env python

from .yfc_dat import Period, Interval, AmbiguousComparisonException
from .yfc_ticker import Ticker, verify_cached_tickers_prices
from .yfc_multi import download
from .yfc_logging import EnableLogging, DisableLogging
from .yfc_cache_manager import _option_manager as options

# Define the public API
__all__ = [
    'Period',
    'Interval',
    'AmbiguousComparisonException',
    'Ticker',
    'verify_cached_tickers_prices',
    'download',
    'EnableLogging',
    'DisableLogging',
    'options'
]

# Upgrade the data in cache, e.g. fix errors, update structures:
from .yfc_upgrade import _init_options, _reset_cached_cals
_init_options()
_reset_cached_cals()

