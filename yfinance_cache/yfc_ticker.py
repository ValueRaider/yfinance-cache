import yfinance as yf

from . import yfc_cache_manager as yfcm
from . import yfc_dat as yfcd
from . import yfc_utils as yfcu
from . import yfc_logging as yfcl
from . import yfc_time as yfct
from . import yfc_prices_manager as yfcp
from . import yfc_financials_manager as yfcf

import numpy as np
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo
import os
import re
from collections import namedtuple
# from time import perf_counter

# TODO: Ticker: add method to delete ticker from cache


class Ticker:
    def __init__(self, ticker, session=None):
        self._ticker = ticker.upper()

        self._session = session
        self._dat = yf.Ticker(self._ticker, session=self._session)

        self._yf_lag = None

        self._histories_manager = None

        self._attributes = {}

        self._info = None
        self._info_age = None
        self._fast_info = None

        self._splits = None

        self._shares = None

        self._major_holders = None

        self._institutional_holders = None

        self._sustainability = None

        self._recommendations = None

        self._calendar = None

        self._isin = None

        self._options = None
        self._options_age = None
        self._option_chain = None

        self._news = None

        self._debug = False
        # self._debug = True

        self._tz = None
        self._exchange = None
        self._listing_day = None

        exchange, tz_name, lday = self._getExchangeAndTzAndListingDay()
        self._financials_manager = yfcf.FinancialsManager(ticker, exchange, tz_name, session=self._session)

    def history(self,
                interval="1d",
                max_age=None,  # defaults to half of interval
                period=None,
                start=None, end=None, prepost=False, actions=True,
                adjust_splits=True, adjust_divs=True,
                keepna=False,
                proxy=None, rounding=False,
                debug=True, quiet=False,
                trigger_at_market_close=False
    ):

        # t0 = perf_counter()

        if prepost:
            raise Exception("pre and post-market caching currently not implemented. If you really need it raise an issue on Github")

        debug_yfc = self._debug
        # debug_yfc = True

        if start is not None or end is not None:
            log_msg = f"Ticker::history(tkr={self._ticker} interval={interval} start={start} end={end} max_age={max_age} trigger_at_market_close={trigger_at_market_close} adjust_splits={adjust_splits}, adjust_divs={adjust_divs})"
        else:
            log_msg = f"Ticker::history(tkr={self._ticker} interval={interval} period={period} max_age={max_age} trigger_at_market_close={trigger_at_market_close} adjust_splits={adjust_splits}, adjust_divs={adjust_divs})"
        yfcl.TraceEnter(log_msg)

        td_1d = datetime.timedelta(days=1)
        exchange, tz_name, lday = self._getExchangeAndTzAndListingDay()
        if exchange == 'YHD':
            raise Exception(f"Ticker symbol '{self._ticker}' not on Yahoo Finance.")
        tz_exchange = ZoneInfo(tz_name)
        try:
            yfct.SetExchangeTzName(exchange, tz_name)
        except Exception as e:
            if "Need to add mapping" in str(e):
                raise Exception(f"Need to add mapping of exchange {exchange} to xcal (ticker={self._ticker})")
            else:
                raise
        dt_now = pd.Timestamp.utcnow()

        # Type checks
        if max_age is not None:
            if isinstance(max_age, str):
                if max_age.endswith("wk"):
                    max_age = re.sub("wk$", "w", max_age)
                max_age = pd.Timedelta(max_age)
            if not isinstance(max_age, (datetime.timedelta, pd.Timedelta)):
                raise Exception("Argument 'max_age' must be Timedelta or equivalent string")
        if period is not None:
            if start is not None or end is not None:
                raise Exception("Don't set both 'period' and 'start'/'end'' arguments")
            if isinstance(period, str):
                if period in ["max", "ytd"]:
                    period = yfcd.periodStrToEnum[period]
                else:
                    if period.endswith("wk"):
                        period = re.sub("wk$", "w", period)
                    if period.endswith("y"):
                        period = relativedelta(years=int(re.sub("y$", "", period)))
                    elif period.endswith("mo"):
                        period = relativedelta(months=int(re.sub("mo", "", period)))
                    else:
                        period = pd.Timedelta(period)
            if not isinstance(period, (yfcd.Period, datetime.timedelta, pd.Timedelta, relativedelta)):
                raise Exception(f"Argument 'period' must be one of: 'max', 'ytd', Timedelta or equivalent string. Not {type(period)}")
        if isinstance(interval, str):
            if interval not in yfcd.intervalStrToEnum.keys():
                raise Exception("'interval' if str must be one of: {}".format(yfcd.intervalStrToEnum.keys()))
            interval = yfcd.intervalStrToEnum[interval]
        if not isinstance(interval, yfcd.Interval):
            raise Exception("'interval' must be yfcd.Interval")

        start_d = None ; end_d = None
        start_dt = None ; end_dt = None
        interday = interval in [yfcd.Interval.Days1, yfcd.Interval.Week]#, yfcd.Interval.Months1, yfcd.Interval.Months3]
        if start is not None:
            start_dt, start_d = self._process_user_dt(start)
            if start_dt > dt_now:
                return None
            if interval == yfcd.Interval.Week:
                # Note: if start is on weekend then Yahoo can return weekly data starting
                #       on Saturday. This breaks YFC, start must be Monday! So fix here:
                if start_dt is None:
                    # Working with simple dates, easy
                    if start_d.weekday() in [5, 6]:
                        start_d += datetime.timedelta(days=7-start_d.weekday())
                else:
                    wd = start_d.weekday()
                    if wd in [5, 6]:
                        start_d += datetime.timedelta(days=7-wd)
                        start_dt = datetime.datetime.combine(start_d, datetime.time(0), tz_exchange)

        if end is not None:
            end_dt, end_d = self._process_user_dt(end)

        if start_dt is not None and end_dt is not None and start_dt >= end_dt:
            raise ValueError("start must be < end")

        if debug_yfc:
            print("- start_dt={} , end_dt={}".format(start_dt, end_dt))

        if (start_dt is not None) and start_dt == end_dt:
            return None

        if max_age is None:
            if interval == yfcd.Interval.Days1:
                max_age = datetime.timedelta(hours=4)
            elif interval == yfcd.Interval.Week:
                max_age = datetime.timedelta(hours=60)
            # elif interval == yfcd.Interval.Months1:
            #     max_age = datetime.timedelta(days=15)
            # elif interval == yfcd.Interval.Months3:
            #     max_age = datetime.timedelta(days=45)
            else:
                max_age = 0.5*yfcd.intervalToTimedelta[interval]
            if start is not None:
                max_age = min(max_age, dt_now-start_dt)

        if period is not None:
            if isinstance(period, (datetime.timedelta, pd.Timedelta)):
                if (dt_now - max_age) < (dt_now - period):
                    raise Exception(f"max_age={max_age} must be less than period={period}")
            elif period == yfcd.Period.Ytd:
                dt_now_ex = dt_now.tz_convert(tz_exchange)
                dt_year_start = pd.Timestamp(year=dt_now_ex.year, month=1, day=1).tz_localize(tz_exchange)
                if (dt_now - max_age) < dt_year_start:
                    raise Exception(f"max_age={max_age} must be less than days since this year start")
        elif start is not None:
            if (dt_now - max_age) < start_dt:
                raise Exception(f"max_age={max_age} must be closer to now than start={start}")


        if start_dt is not None:
            try:
                sched_14d = yfct.GetExchangeSchedule(exchange, start_dt.date(), start_dt.date()+14*td_1d)
            except Exception as e:
                if "Need to add mapping" in str(e):
                    raise Exception("Need to add mapping of exchange {} to xcal (ticker={})".format(exchange, self._ticker))
                else:
                    raise
            if sched_14d is None:
                raise Exception("sched_14d is None for date range {}->{} and ticker {}".format(start_dt.date(), start_dt.date()+14*td_1d, self._ticker))
            if sched_14d["open"].iloc[0] > dt_now:
                # Requested date range is in future
                return None
        else:
            sched_14d = None

        # All date checks passed so can begin fetching

        if ((start_d is None) or (end_d is None)) and (start_dt is not None) and (end_dt is not None):
            # if start_d/end_d not set then start/end are datetimes, so need to inspect
            # schedule opens/closes to determine days
            if sched_14d is not None:
                sched = sched_14d.iloc[0:1]
            else:
                sched = yfct.GetExchangeSchedule(exchange, start_dt.date(), end_dt.date()+td_1d)
            n = sched.shape[0]
            start_d = start_dt.date() if start_dt < sched["open"].iloc[0] else start_dt.date()+td_1d
            end_d = end_dt.date()+td_1d if end_dt >= sched["close"].iloc[n-1] else end_dt.date()
        else:
            if exchange not in yfcd.exchangeToXcalExchange:
                raise Exception("Need to add mapping of exchange {} to xcal (ticker={})".format(exchange, self._ticker))

        if self._histories_manager is None:
            self._histories_manager = yfcp.HistoriesManager(self._ticker, exchange, tz_name, lday, self._session, proxy)

        # t1_setup = perf_counter()

        hist = self._histories_manager.GetHistory(interval)
        if period is not None:
            h = hist.get(start=None, end=None, period=period, max_age=max_age, trigger_at_market_close=trigger_at_market_close, quiet=quiet)
        elif interday:
            h = hist.get(start_d, end_d, period=None, max_age=max_age, trigger_at_market_close=trigger_at_market_close, quiet=quiet)
        else:
            h = hist.get(start_dt, end_dt, period=None, max_age=max_age, trigger_at_market_close=trigger_at_market_close, quiet=quiet)
        if (h is None) or h.shape[0] == 0:
            return pd.DataFrame()

        # t2_sync = perf_counter()

        f_dups = h.index.duplicated()
        if f_dups.any():
            raise Exception("{}: These timepoints have been duplicated: {}".format(self._ticker, h.index[f_dups]))

        # Present table for user:
        h_copied = False
        if (start_dt is not None) and (end_dt is not None):
            h = h.loc[start_dt:end_dt-datetime.timedelta(milliseconds=1)].copy()
            h_copied = True

        if not keepna:
            price_data_cols = [c for c in yfcd.yf_data_cols if c in h.columns]
            mask_nan_or_zero = (np.isnan(h[price_data_cols].to_numpy()) | (h[price_data_cols].to_numpy() == 0)).all(axis=1)
            if mask_nan_or_zero.any():
                h = h.drop(h.index[mask_nan_or_zero])
                h_copied = True
        # t3_filter = perf_counter()

        if h.shape[0] == 0:
            return h

        if adjust_splits:
            if not h_copied:
                h = h.copy()
            for c in ["Open", "Close", "Low", "High", "Dividends"]:
                h[c] = np.multiply(h[c].to_numpy(), h["CSF"].to_numpy())
            h["Volume"] = np.round(np.divide(h["Volume"].to_numpy(), h["CSF"].to_numpy()), 0).astype('int')
        if adjust_divs:
            if not h_copied:
                h = h.copy()
            for c in ["Open", "Close", "Low", "High"]:
                h[c] = np.multiply(h[c].to_numpy(), h["CDF"].to_numpy())
        else:
            if not h_copied:
                h = h.copy()
            h["Adj Close"] = np.multiply(h["Close"].to_numpy(), h["CDF"].to_numpy())
        h = h.drop(["CSF", "CDF"], axis=1)

        if rounding:
            # Round to 4 sig-figs
            if not h_copied:
                h = h.copy()
            f_na = h["Close"].isna()
            na = f_na.any()
            if na:
                f_nna = ~f_na
                if not f_nna.any():
                    raise Exception(f"{self._ticker}: price table is entirely NaNs. Delisted?" +" \n" + log_msg)
                last_close = h["Close"][f_nna].iloc[-1]
            else:
                last_close = h["Close"].iloc[-1]
            rnd = yfcu.CalculateRounding(last_close, 4)
            for c in ["Open", "Close", "Low", "High"]:
                if na:
                    h.loc[f_nna, c] = np.round(h.loc[f_nna, c].to_numpy(), rnd)
                else:
                    h[c] = np.round(h[c].to_numpy(), rnd)

        if debug_yfc:
            print("- h:")
            cols = [c for c in ["Close", "Dividends", "Volume", "CDF", "CSF"] if c in h.columns]
            print(h[cols])
            if "Dividends" in h.columns:
                f = h["Dividends"] != 0.0
                if f.any():
                    print("- dividends:")
                    print(h.loc[f, cols])
            print("")
        yfcl.TraceExit("Ticker::history() returning")

        # t4_adjust = perf_counter()
        # t_setup = t1_setup - t0
        # t_sync = t2_sync - t1_setup
        # t_filter = t3_filter - t2_sync
        # t_adjust = t4_adjust - t3_filter
        # t_sum = t_setup + t_sync + t_filter + t_adjust
        # print("TIME: {:.4f}s: setup={:.4f} sync={:.4f} filter={:.4f} adjust={:.4f}".format(t_sum, t_setup, t_sync, t_filter, t_adjust))
        # t_setup *= 100/t_sum
        # t_sync *= 100/t_sum
        # t_cache *= 100/t_sum
        # t_filter *= 100/t_sum
        # t_adjust *= 100/t_sum
        # print("TIME %:        setup={:.1f}%  sync={:.1f}%  filter={:.1f}%  adjust={:.1f}%".format(t_setup, t_sync, t_filter, t_adju

        return h

    @property
    def history_metadata(self):
        return yfcm.ReadCacheDatum(self._ticker, "history_metadata")

    def _getCachedPrices(self, interval, proxy=None):
        if self._histories_manager is None:
            exchange, tz_name, lday = self._getExchangeAndTzAndListingDay()
            self._histories_manager = yfcp.HistoriesManager(self._ticker, exchange, tz_name, lday, self._session, proxy)

        if isinstance(interval, str):
            if interval not in yfcd.intervalStrToEnum.keys():
                raise Exception("'interval' if str must be one of: {}".format(yfcd.intervalStrToEnum.keys()))
            interval = yfcd.intervalStrToEnum[interval]

        return self._histories_manager.GetHistory(interval).h

    def _getExchangeAndTzAndListingDay(self):
        if self._tz is not None and self._exchange is not None and self._listing_day is not None:
            return self._exchange, self._tz, self._listing_day

        exchange, tz_name, lday = None, None, None
        i = None
        try:
            i = self.get_info('9999d')
            exchange = i['exchange']
            if "exchangeTimezoneName" in i:
                tz_name = i["exchangeTimezoneName"]
            else:
                tz_name = i["timeZoneFullName"]
            if tz_name is not None:
                if 'firstTradeDateEpochUtc' in i:
                    lday = pd.Timestamp(i['firstTradeDateEpochUtc'], unit='s').tz_localize("UTC")
                elif 'firstTradeDateMilliseconds' in i:
                    lday = pd.Timestamp(i['firstTradeDateMilliseconds'], unit='ms').tz_localize("UTC")
                if lday is not None:
                    lday = lday.tz_convert(tz_name).date()
        except Exception:
            md = yf.Ticker(self._ticker, session=self._session).history_metadata
            if 'exchangeName' in md:
                exchange = md['exchangeName']
            if 'exchangeTimezoneName' in md:
                tz_name = md['exchangeTimezoneName']
            if 'firstTradeDate' in md and tz_name is not None:
                lday = pd.Timestamp(md['firstTradeDate'], unit='s').tz_localize("UTC").tz_convert(tz_name).date()
            if i is not None:
                # info fetch was successful, just missing some keys
                n = len(i)
                if exchange is not None:
                    i['exchange'] = exchange
                if tz_name is not None:
                    i['exchangeTimezoneName'] = tz_name
                if 'firstTradeDate' in md:
                    i['firstTradeDateEpochUtc'] = md['firstTradeDate']
                if len(i) > n:
                    yfcm.StoreCacheDatum(self._ticker, "info", i)

        if exchange is None or tz_name is None:
            print("- info:") ; print(i)
            raise Exception(f"{self._ticker}: exchange and timezone not available")
        self._tz = tz_name
        self._exchange = exchange
        self._listing_day = lday
        return self._exchange, self._tz, self._listing_day

    def verify_cached_prices(self, rtol=0.0001, vol_rtol=0.005, correct='none', discard_old=False, quiet=True, debug=False, debug_interval=None):
        if debug:
            quiet = False
        if debug_interval is not None and isinstance(debug_interval, str):
            debug_interval = yfcd.intervalStrToEnum[debug_interval]

        fn_locals = locals()
        del fn_locals["self"]

        interval = yfcd.Interval.Days1
        cache_key = "history-"+yfcd.intervalToString[interval]
        if not yfcm.IsDatumCached(self._ticker, cache_key):
            return True

        yfcl.TraceEnter(f"Ticker::verify_cached_prices(tkr={self._ticker} {fn_locals})")

        if self._histories_manager is None:
            exchange, tz_name, lday = self._getExchangeAndTzAndListingDay()
            self._histories_manager = yfcp.HistoriesManager(self._ticker, exchange, tz_name, lday, self._session, proxy=None)

        v = True

        # First verify 1d
        dt0 = self._histories_manager.GetHistory(interval)._getCachedPrices().index[0]
        try:
            self.history(start=dt0.date(), quiet=quiet, trigger_at_market_close=True)  # ensure have all dividends
        except yfcd.NoPriceDataInRangeException:
            pass
        v = self._verify_cached_prices_interval(interval, rtol, vol_rtol, correct, discard_old, quiet, debug)
        if debug_interval == yfcd.Interval.Days1:
            yfcl.TraceExit(f"Ticker::verify_cached_prices() returning {v} (1st pass)")
            return v
        if not v:
            if debug or not correct:
                yfcl.TraceExit(f"Ticker::verify_cached_prices() returning {v} (1st pass)")
                return v
        if correct in ['one', 'all']:
            # Rows were removed so re-fetch. Only do for 1d data
            self.history(start=dt0.date(), quiet=quiet)

            # repeat verification, because 'fetch backporting' may be buggy
            v2 = self._verify_cached_prices_interval(interval, rtol, vol_rtol, correct, discard_old, quiet, debug)
            if not v2 and debug:
                yfcl.TraceExit(f"Ticker::verify_cached_prices() returning {v2} (post-correction)")
                return v2
            if not v2:
                yfcl.TraceExit(f"Ticker::verify_cached_prices() returning {v2} (post-correction)")
                return v2

            if not v:
                # Stop after correcting first problem, because user won't have been shown the next problem yet
                yfcl.TraceExit(f"Ticker::verify_cached_prices() returning {v} (corrected but user should review next problem)")
                return v

        if debug_interval is not None:
            if debug_interval == yfcd.Interval.Days1:
                intervals = []
            else:
                intervals = [debug_interval]
            debug = True
        else:
            intervals = yfcd.Interval
        for interval in intervals:
            if interval == yfcd.Interval.Days1:
                continue
            istr = yfcd.intervalToString[interval]
            cache_key = "history-"+istr
            if not yfcm.IsDatumCached(self._ticker, cache_key):
                continue
            vi = self._verify_cached_prices_interval(interval, rtol, vol_rtol, correct, discard_old, quiet, debug)
            yfcl.TracePrint(f"{istr}: vi={vi}")

            if not vi and correct != 'all':
                # Stop after correcting first problem, because user won't have been shown the next problem yet
                yfcl.TraceExit(f"Ticker::verify_cached_prices() returning {vi}")
                return vi

            v = v and vi

        yfcl.TraceExit(f"Ticker::verify_cached_prices() returning {v}")

        return v

    def _verify_cached_prices_interval(self, interval, rtol=0.0001, vol_rtol=0.005, correct=False, discard_old=False, quiet=True, debug=False):
        if debug:
            quiet = False

        fn_locals = locals()
        del fn_locals["self"]

        if isinstance(interval, str):
            if interval not in yfcd.intervalStrToEnum.keys():
                raise Exception("'interval' if str must be one of: {}".format(yfcd.intervalStrToEnum.keys()))
            interval = yfcd.intervalStrToEnum[interval]

        istr = yfcd.intervalToString[interval]
        cache_key = "history-"+istr
        if not yfcm.IsDatumCached(self._ticker, cache_key):
            return True

        yfcl.TraceEnter(f"Ticker::_verify_cached_prices_interval(tkr={self._ticker}, {fn_locals})")

        if self._histories_manager is None:
            exchange, tz_name, lday = self._getExchangeAndTzAndListingDay()
            self._histories_manager = yfcp.HistoriesManager(self._ticker, exchange, tz_name, lday, self._session, proxy=None)

        v = self._histories_manager.GetHistory(interval)._verifyCachedPrices(rtol, vol_rtol, correct, discard_old, quiet, debug)

        yfcl.TraceExit(f"Ticker::_verify_cached_prices_interval() returning {v}")
        return v

    def _process_user_dt(self, dt):
        exchange, tz_name, lday = self._getExchangeAndTzAndListingDay()
        return yfcu.ProcessUserDt(dt, tz_name)

    def get_attribute(self, name, max_age=None, merge=False, metadata=False):
        if max_age is None:
            max_age = pd.Timedelta('365d')
        if not isinstance(max_age, (datetime.timedelta, pd.Timedelta)):
            max_age = pd.Timedelta(max_age)
        if max_age < pd.Timedelta(0):
            raise Exception(f"'max_age' must be positive timedelta not {max_age}")

        if name in self._attributes:
            a = self._attributes[name]
        else:
            a = None

        if (a is not None) and (max_age > a['age']):
            if metadata:
                return a['data'], a['md']
            return a['data']

        md = None
        if yfcm.IsDatumCached(self._ticker, name):
            data, md = yfcm.ReadCacheDatum(self._ticker, name, True)
            if data is not None:
                if a is None:
                    a = {}
                a['data'] = data
                if md is None:
                    md = {}
                a['md'] = md
                if isinstance(data, dict) and 'FetchDate' in data:
                    fetchDate = data['FetchDate']
                    md['FetchDate'] = fetchDate
                    del data['FetchDate']
                    yfcm.StoreCacheDatum(self._ticker, name, data, metadata=md)
                else:
                    fetchDate = md['FetchDate']
                age = pd.Timestamp.now() - max(fetchDate, md['LastCheck'])
                a['age'] = age
                if age < max_age:
                    if metadata:
                        return data, md
                    return data

        if name in dir(self._dat):
            newd = getattr(self._dat, name)
        else:
            raise NotImplementedError(f"Implement fetching from YF: {name}")
        if md is None:
            md = {}
        md['FetchDate'] = pd.Timestamp.now()

        if a is not None and len(a['data']) > 0 and (not merge):
            # Check new data is not downgrade
            d = a['data']
            if isinstance(d, dict):
                diff = len(newd) - len(d)
                diff_pct = float(diff) / float(len(d))
                if diff_pct < -0.1 and diff < -10:
                    msg = f"When fetching new '{name}', significant amount of data has disappeared\n"
                    missing_keys = [k for k in d.keys() if k not in data.keys()]
                    new_keys = [k for k in data.keys() if k not in d.keys()]
                    msg += "- missing: "
                    msg += str({k:d[k] for k in missing_keys}) + '\n'
                    msg += "- new: "
                    msg += str({k:newd[k] for k in new_keys}) + '\n'
                    msg += f"- diff={diff} diff_pct={diff_pct:.2f}"
                    msg += f"\nDiscarding fetched '{name}'."
                    print(f'{self._ticker}: {msg}')
                    print("- cached:")
                    pprint(d)
                    print("- fetched:")
                    pprint(newd)
                    raise Exception('review info diffs')
                    yfcm.WriteCacheMetadata(self._ticker, name, 'LastCheck', md['FetchDate'])
                    if metadata:
                        return d, a['md']
                    return d
            elif isinstance(d, pd.DataFrame):
                diff = len(newd) - len(d)
                diff_pct = float(diff) / float(len(d))
                if diff_pct < -0.1 and diff < -10:
                    msg = f"When fetching new '{name}', significant amount of data has disappeared\n"
                    msg += f"- cache had {len(d)} rows\n"
                    msg += f"- fetched has {len(newd)} rows\n"
                    msg += f"- diff={diff} diff_pct={diff_pct:.2f}"
                    msg += f"\nDiscarding fetched '{name}'."
                    print(f'{self._ticker}: {msg}')
                    yfcm.WriteCacheMetadata(self._ticker, name, 'LastCheck', md['FetchDate'])
                    if metadata:
                        return d, a['md']
                    return d
            elif isinstance(d, str):
                diff = len(newd) - len(d)
                diff_pct = float(diff) / float(len(d))
                if diff_pct < -0.1 and diff < -10:
                    msg = f"When fetching new '{name}', significant amount of data has disappeared\n"
                    msg += "- cached: "
                    msg += f"{d}\n"
                    msg += "- new: "
                    msg += f"{newd}\n"
                    msg += f"- diff={diff} diff_pct={diff_pct:.2f}"
                    msg += f"\nDiscarding fetched '{name}'."
                    print(f'{self._ticker}: {msg}')
                    yfcm.WriteCacheMetadata(self._ticker, name, 'LastCheck', md['FetchDate'])
                    if metadata:
                        return d, a['md']
                    return d
            else:
                raise Exception(f"Implement check for lost data for type {type(d)}")

        if a is None:
            a = {}
        if merge and 'data' in a.keys():
            raise NotImplementedError(f"Implement merging cache+fetch for type {type(a['data'])}")
        a['data'] = newd
        a['age'] = pd.Timestamp.now() - md['FetchDate']
        if md is None:
            md = {}
        md['LastCheck'] = md['FetchDate']
        a['md'] = md
        yfcm.StoreCacheDatum(self._ticker, name, a['data'], metadata=md)

        exchange, tz_name, lday = self._getExchangeAndTzAndListingDay()
        yfct.SetExchangeTzName(exchange, tz_name)

        if metadata:
            return a['data'], a['md']
        return a['data']

    @property
    def info(self):
        return self.get_info()

    def get_info(self, max_age=None):
        if max_age is None:
            if yfcm._option_manager.max_ages.info is None:
                raise Exception('why is yfcm._option_manager.max_ages.info None?')
            max_age = pd.Timedelta(yfcm._option_manager.max_ages.info)
        return self.get_attribute("info", max_age)

    @property
    def fast_info(self):
        if self._fast_info is not None:
            return self._fast_info

        if yfcm.IsDatumCached(self._ticker, "fast_info"):
            try:
                self._fast_info = yfcm.ReadCacheDatum(self._ticker, "fast_info")
            except Exception:
                pass
            else:
                return self._fast_info

        # self._fast_info = self._dat.fast_info
        self._fast_info = {}
        for k in self._dat.fast_info.keys():
            try:
                self._fast_info[k] = self._dat.fast_info[k]
            except Exception as e:
                if "decrypt" in str(e):
                    pass
                else:
                    print(f"TICKER = {self._ticker}")
                    raise
        yfcm.StoreCacheDatum(self._ticker, "fast_info", self._fast_info)

        yfct.SetExchangeTzName(self._fast_info["exchange"], self._fast_info["timezone"])

        return self._fast_info

    def get_shares(self, start=None, end=None, max_age='30d'):
        debug = False
        # debug = True

        max_age = pd.Timedelta(max_age)

        # Process dates
        exchange, tz, lday = self._getExchangeAndTzAndListingDay()
        dt_now = pd.Timestamp.utcnow().tz_convert(tz)
        if start is not None:
            start_dt, start_d = self._process_user_dt(start)
            start = start_d
        if end is not None:
            end_dt, end_d = self._process_user_dt(end)
            end = end_d
        if end is None:
            end_dt = dt_now
            end = dt_now.date()
        if start is None:
            start = end - pd.Timedelta(days=548)  # 18 months
            start_dt = end_dt - pd.Timedelta(days=548)
        if start >= end:
            raise Exception("Start date must be before end")
        if debug:
            print("- start =", start, " end =", end)

        if self._shares is None:
            if yfcm.IsDatumCached(self._ticker, "shares"):
                if debug:
                    print("- init shares from cache")
                self._shares = yfcm.ReadCacheDatum(self._ticker, "shares")
        if self._shares is None or self._shares.empty:
            # Loaded from cache. Either re-fetch or return None
            lastFetchDt = yfcm.ReadCacheMetadata(self._ticker, "shares", 'LastFetch')
            do_fetch = (lastFetchDt is None) or ((dt_now - lastFetchDt) > max_age)
            if do_fetch:
                self._shares = self._fetch_shares(start, end)
                if self._shares is None:
                    self._shares = pd.DataFrame()
                yfcm.StoreCacheDatum(self._ticker, "shares", self._shares, metadata={'LastFetch':pd.Timestamp.utcnow().tz_convert(tz)})
                if self._shares.empty:
                    return None
            else:
                return None

        if debug:
            print("- self._shares:", self._shares.index[0], '->', self._shares.index[-1])

        td_1d = datetime.timedelta(days=1)
        last_row = self._shares.iloc[-1]
        if pd.isna(last_row['Shares']):# and last_row['FetchDate'].date() == last_row.name:
            if debug:
                print("- dropping last row from cached")
            self._shares = self._shares.drop(self._shares.index[-1])

        if not isinstance(self._shares.index, pd.DatetimeIndex):
            self._shares.index = pd.to_datetime(self._shares.index).tz_localize(tz)
        if self._shares['Shares'].dtype == 'float':
            # Convert to Int, and add a little to avoid rounding errors
            self._shares['Shares'] = (self._shares['Shares']+0.01).round().astype('Int64')

        if start < self._shares.index[0].date():
            df_pre = self._fetch_shares(start, self._shares.index[0])
            yfcm.WriteCacheMetadata(self._ticker, "shares", 'LastFetch', pd.Timestamp.utcnow().tz_convert(tz))
            if df_pre is not None:
                self._shares = pd.concat([df_pre, self._shares])
        if (end-td_1d) > self._shares.index[-1].date() and \
            (end - self._shares.index[-1].date()) > max_age:
            df_post = self._fetch_shares(self._shares.index[-1] + td_1d, end)
            yfcm.WriteCacheMetadata(self._ticker, "shares", 'LastFetch', pd.Timestamp.utcnow().tz_convert(tz))
            if df_post is not None:
                self._shares = pd.concat([self._shares, df_post])

        self._shares = self._shares
        yfcm.StoreCacheDatum(self._ticker, "shares", self._shares)

        f_na = self._shares['Shares'].isna()
        shares = self._shares[~f_na]
        if start is not None:
            i0 = np.searchsorted(shares.index, start_dt)
        else:
            i0 = None
        if end is not None:
            i1 = np.searchsorted(shares.index, end_dt)
        else:
            i1 = None
        if i0 is not None and i1 is not None:
            return shares.iloc[i0:i1]
        elif i0 is not None:
            return shares[i0:]
        elif i1 is not None:
            return shares[:i1]
        return shares

    def _fetch_shares(self, start, end):
        td_1d = datetime.timedelta(days=1)

        exchange, tz, lday = self._getExchangeAndTzAndListingDay()
        if isinstance(end, datetime.datetime):
            end_dt = end
            end_d = end.date()
        else:
            end_dt = pd.Timestamp(end).tz_localize(tz)
            end_d = end
        if isinstance(start, datetime.datetime):
            start_dt = start
            start_d = start.date()
        else:
            start_dt = pd.Timestamp(start).tz_localize(tz)
            start_d = start

        end_d = min(end_d, datetime.date.today() + td_1d)

        df = self._dat.get_shares_full(start_d, end_d)
        if df is None:
            return df
        if df.empty:
            return None

        # Convert to Pandas Int for NaN support
        df = df.astype('Int64')

        # Currently, yfinance uses ceil(end), so fix:
        if df.index[-1].date() == end_d:
            df.drop(df.index[-1])
            if df.empty:
                return None

        fetch_dt = pd.Timestamp.utcnow().tz_convert(tz)
        df = pd.DataFrame(df, columns=['Shares'])

        if start_d < df.index[0].date():
            df.loc[start_dt, 'Shares'] = np.nan
        if (end_d-td_1d) > df.index[-1].date():
            df.loc[end_dt, 'Shares'] = np.nan
        df = df.sort_index()

        df['FetchDate'] = fetch_dt

        return df

    @property
    def major_holders(self):
        return self.get_attribute("major_holders", yfcm._option_manager.max_ages.holdings)

    @property
    def institutional_holders(self):
        return self.get_attribute("institutional_holders", yfcm._option_manager.max_ages.holdings)

    @property
    def mutualfund_holders(self):
        return self.get_attribute("mutualfund_holders", yfcm._option_manager.max_ages.holdings)

    @property
    def insider_purchases(self) -> pd.DataFrame:
        return self.get_attribute("insider_purchases", yfcm._option_manager.max_ages.holdings)

    @property
    def insider_transactions(self) -> pd.DataFrame:
        return self.get_attribute("insider_transactions", yfcm._option_manager.max_ages.holdings)

    @property
    def insider_roster_holders(self) -> pd.DataFrame:
        return self.get_attribute("insider_roster_holders", yfcm._option_manager.max_ages.holdings)

    @property
    def earnings(self):
        return self._financials_manager.get_earnings()

    @property
    def quarterly_earnings(self):
        return self._financials_manager.get_quarterly_earnings()

    @property
    def income_stmt(self):
        return self._financials_manager.get_income_stmt()

    @property
    def quarterly_income_stmt(self):
        return self._financials_manager.get_quarterly_income_stmt()

    @property
    def financials(self):
        return self._financials_manager.get_income_stmt()

    @property
    def quarterly_financials(self):
        return self._financials_manager.get_quarterly_income_stmt()

    @property
    def balance_sheet(self):
        return self._financials_manager.get_balance_sheet()

    @property
    def quarterly_balance_sheet(self):
        return self._financials_manager.get_quarterly_balance_sheet()

    @property
    def cashflow(self):
        return self._financials_manager.get_cashflow()

    @property
    def quarterly_cashflow(self):
        return self._financials_manager.get_quarterly_cashflow()

    def get_earnings_dates(self, limit=12):
        return self._financials_manager.get_earnings_dates(limit)

    def get_release_dates(self, period='quarterly', as_df=False, check=True):
        if period not in ['annual', 'quarterly']:
            raise ValueError(f'period argument must be "annual" or "quarterly", not "{period}"')
        if period == 'annual':
            period = yfcd.ReportingPeriod.Full
        else:
            period = yfcd.ReportingPeriod.Interim

        releases = self._financials_manager.get_release_dates(period, as_df=as_df, refresh=True, check=check)
        if releases is None:
            return releases

        if as_df:
            # Format:
            releases['Period end uncertainty'] = '0d'
            f = releases['PE confidence'] == yfcd.Confidence.Medium
            if f.any():
                releases.loc[f, 'Period end uncertainty'] = '+-7d'
            f = releases['PE confidence'] == yfcd.Confidence.Low
            if f.any():
                releases.loc[f, 'Period end uncertainty'] = '+-45d'
            releases = releases.drop('PE confidence', axis=1)

            releases['Release date uncertainty'] = '0d'
            f = releases['RD confidence'] == yfcd.Confidence.Medium
            if f.any():
                releases.loc[f, 'Release date uncertainty'] = '+/-7d'
            f = releases['RD confidence'] == yfcd.Confidence.Low
            if f.any():
                releases.loc[f, 'Release date uncertainty'] = '+/-45d'
            releases = releases.drop('RD confidence', axis=1)

            releases = releases.drop('Delay', axis=1)

        return releases

    @property
    def sustainability(self):
        return self.get_attribute("sustainability", yfcm._option_manager.max_ages.analysis)

    @property
    def recommendations(self):
        return self.get_attribute("recommendations", yfcm._option_manager.max_ages.analysis)

    @property
    def recommendations_summary(self):
        return self.get_attribute("recommendations_summary", yfcm._option_manager.max_ages.analysis)

    @property
    def upgrades_downgrades(self):
        return self.get_attribute("upgrades_downgrades", yfcm._option_manager.max_ages.analysis)

    @property
    def analyst_price_targets(self) -> dict:
        return self.get_attribute("analyst_price_targets", yfcm._option_manager.max_ages.analysis)

    @property
    def earnings_estimate(self) -> pd.DataFrame:
        return self.get_attribute("earnings_estimate", yfcm._option_manager.max_ages.analysis)

    @property
    def revenue_estimate(self) -> pd.DataFrame:
        return self.get_attribute("revenue_estimate", yfcm._option_manager.max_ages.analysis)

    @property
    def earnings_history(self) -> pd.DataFrame:
        return self.get_attribute("earnings_history", yfcm._option_manager.max_ages.analysis)

    @property
    def eps_trend(self) -> pd.DataFrame:
        return self.get_attribute("eps_trend", yfcm._option_manager.max_ages.analysis)

    @property
    def eps_revisions(self) -> pd.DataFrame:
        return self.get_attribute("eps_revisions", yfcm._option_manager.max_ages.analysis)

    @property
    def growth_estimates(self) -> pd.DataFrame:
        return self.get_attribute("growth_estimates", yfcm._option_manager.max_ages.analysis)

    @property
    def calendar(self):
        return self._financials_manager.get_calendar()

    @property
    def isin(self):
        return self.get_attribute("isin", yfcm._option_manager.max_ages.info)

    @property
    def options(self):
        return self.get_options()

    def get_options(self, exclude_expired_contracts=True, max_age=None):
        if not exclude_expired_contracts:
            raise NotImplementedError(f'Not implement exclude_expired_contracts={exclude_expired_contracts}')

        if max_age is None:
            max_age = pd.Timedelta(yfcm._option_manager.max_ages.options)
        if not isinstance(max_age, (datetime.timedelta, pd.Timedelta)):
            max_age = pd.Timedelta(max_age)
        if max_age < pd.Timedelta(0):
            raise Exception(f"'max_age' must be positive timedelta not {max_age}")

        if (self._options is not None) and (max_age > self._options_age):
            return self._options

        md = None
        if yfcm.IsDatumCached(self._ticker, "options"):
            self._options, md = yfcm.ReadCacheDatum(self._ticker, "options", True)
            if 'FetchDate' not in md.keys():
                raise Exception(f'{self._ticker}: why YFC options[] still missing FetchDate?!')

            if self._options is not None:
                if md is None:
                    md = {}
                self._options_age = pd.Timestamp.now() - md['FetchDate']
                if self._options_age < max_age:
                    return self._options

        o = self._dat.options
        if md is None:
            md = {}
        md['FetchDate'] = pd.Timestamp.now()

        if self._options is None:
            self._options = o
        else:
            # Merge
            merged = tuple(sorted(set(self._options).union(o)))
            self._options = merged

        if md is None:
            md = {}
        yfcm.StoreCacheDatum(self._ticker, "options", self._options, metadata=md)

        self._options_age = pd.Timestamp.now() - md['FetchDate']

        return self._options

    def option_chain(self, expiry_date, max_age=None):
        if expiry_date not in self.options:
            raise Exception(f"expiry_date='{expiry_date}' not found in: {self.options}")

        if max_age is None:
            max_age = pd.Timedelta(yfcm._option_manager.max_ages.options)
        if not isinstance(max_age, (datetime.timedelta, pd.Timedelta)):
            max_age = pd.Timedelta(max_age)
        if max_age < pd.Timedelta(0):
            raise Exception(f"'max_age' must be positive timedelta not {max_age}")

        if self._option_chain is None:
            if yfcm.IsDatumCached(self._ticker, "option_chain"):
                self._option_chain = yfcm.ReadCacheDatum(self._ticker, "option_chain", False)
            else:
                self._option_chain = {}

        if expiry_date in self._option_chain:
            md = self._option_chain[expiry_date]['metadata']
            age = pd.Timestamp.now() - md['FetchDate']
            # If 'expiry_date' in past, don't bother re-fetching from YF
            if age < max_age or pd.Timestamp(expiry_date) < pd.Timestamp.now():
                return namedtuple('Options', ['calls', 'puts', 'underlying'])(**{
                    "calls": self._option_chain[expiry_date]['calls'],
                    "puts": self._option_chain[expiry_date]['puts'], 
                    "underlying": self._option_chain[expiry_date]['underlying']
                })

        oc = self._dat.option_chain(expiry_date)
        md = {'FetchDate': pd.Timestamp.now()}
        self._option_chain[expiry_date] = {
            'calls': oc.calls,
            'puts': oc.puts,
            'underlying': oc.underlying,
            'metadata': md
        }
        yfcm.StoreCacheDatum(self._ticker, "option_chain", self._option_chain)
        return oc
        

    @property
    def news(self):
        if self._news is not None:
            return self._news

        if yfcm.IsDatumCached(self._ticker, "news"):
            self._news = yfcm.ReadCacheDatum(self._ticker, "news")
            return self._news

        self._news = self._dat.news
        yfcm.StoreCacheDatum(self._ticker, "news", self._news)
        return self._news

    @property
    def yf_lag(self):
        if self._yf_lag is not None:
            return self._yf_lag

        exchange, tz_name, lday = self._getExchangeAndTzAndListingDay()
        exchange_str = "exchange-{0}".format(exchange)
        if yfcm.IsDatumCached(exchange_str, "yf_lag"):
            self._yf_lag = yfcm.ReadCacheDatum(exchange_str, "yf_lag")
            if self._yf_lag:
                return self._yf_lag

        # Just use specified lag
        specified_lag = yfcd.exchangeToYfLag[exchange]
        self._yf_lag = specified_lag
        return self._yf_lag


def verify_cached_tickers_prices(session=None, rtol=0.0001, vol_rtol=0.005, correct=False, halt_on_fail=True, resume_from_tkr=None, debug_tkr=None, debug_interval=None):
    """
    :Parameters:
        session:
            Recommend providing a 'requests_cache' session, in case
            you have to abort and resume verification (likely).
        correct:
            False, 'one', 'all'
        resume_from_tkr: str
            Resume verification from this ticker (alphabetical order).
            Because maybe you had to abort verification partway.
        debug_tkr: str
            Only verify this ticker.
            Because maybe you want to investigate a difference.
    """

    if debug_interval is not None and isinstance(debug_interval, str):
        if debug_interval not in yfcd.intervalStrToEnum.keys():
            raise Exception("'debug_interval' if str must be one of: {}".format(yfcd.intervalStrToEnum.keys()))
        debug_interval = yfcd.intervalStrToEnum[debug_interval]

    d = yfcm.GetCacheDirpath()
    tkrs = [x for x in os.listdir(d) if not x.startswith("exchange-") and os.path.isdir(os.path.join(d, x)) and '_' not in x]

    debug = debug_tkr is not None
    if debug_tkr is not None:
        debug_tkr = debug_tkr.upper()
        tkrs = [debug_tkr]
    else:
        tkrs = sorted(tkrs)
        if resume_from_tkr is not None:
            resume_from_tkr = resume_from_tkr.upper()
            resume_after_tkr = False
            if resume_from_tkr.endswith("+1"):
                resume_after_tkr = True
                resume_from_tkr = resume_from_tkr.replace("+1", "")
            i = np.searchsorted(np.array(tkrs), resume_from_tkr, side="left")
            if resume_after_tkr:
                i += 1
            tkrs = tkrs[i:]

    if debug_tkr is not None:
        tkrs = [debug_tkr]
    tqdm_loaded = False
    try:
        from tqdm import tqdm
        t = tqdm(range(len(tkrs)))
        tqdm_loaded = True
    except ModuleNotFoundError:
        print("Install Python module 'tqdm' to print progress bar + estimated time to completion")
        t = range(len(tkrs))
    for i in t:
        tkr = tkrs[i]
        if tqdm_loaded:
            t.set_description("Verifying " + tkr)
        else:
            print(f"{tkr} : {i+1}/{len(tkrs)}")

        dat = Ticker(tkr, session=session)

        try:
            discard_old = correct in ['one', 'all']
            v = dat.verify_cached_prices(rtol=rtol, vol_rtol=vol_rtol, correct=correct, discard_old=discard_old, quiet=not debug, debug=debug, debug_interval=debug_interval)
        except yfcd.NoPriceDataInRangeException as e:
            print(str(e) + " - is it delisted? Aborting verification so you can investigate.")
            return
        if debug:
            return

        if correct in ['one', 'all']:
            v = dat.verify_cached_prices(rtol=rtol, vol_rtol=vol_rtol, correct=correct, discard_old=False, quiet=True, debug=debug, debug_interval=debug_interval)

        if not v and correct != 'all':
            v = dat.verify_cached_prices(rtol=rtol, vol_rtol=vol_rtol, correct=False, discard_old=False, quiet=False, debug=True, debug_interval=debug_interval)
            if halt_on_fail:
                raise Exception(f"{tkr}: verify failing")
            else:
                print(f"{tkr}: verify failing")
