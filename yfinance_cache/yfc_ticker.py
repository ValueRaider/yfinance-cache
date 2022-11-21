import yfinance as yf

from . import yfc_cache_manager as yfcm
from . import yfc_dat as yfcd
from . import yfc_utils as yfcu
from . import yfc_time as yfct

import pandas as pd
import numpy as np
from scipy import ndimage as _ndimage
import datetime
from zoneinfo import ZoneInfo

from pprint import pprint

# from time import perf_counter


class Ticker:
    def __init__(self, ticker, session=None):
        self.ticker = ticker.upper()

        self.session = session
        self.dat = yf.Ticker(self.ticker, session=self.session)

        self._yf_lag = None

        self._history = {}

        self._info = None

        self._splits = None

        self._financials = None
        self._quarterly_financials = None

        self._major_holders = None

        self._institutional_holders = None

        self._balance_sheet = None
        self._quarterly_balance_sheet = None

        self._cashflow = None
        self._quarterly_cashflow = None

        self._earnings = None
        self._quarterly_earnings = None

        self._sustainability = None

        self._recommendations = None

        self._calendar = None

        self._isin = None

        self._options = None

        self._news = None

        self._debug = False
        # self._debug = True
        self._trace = False
        # self._trace = True
        self._trace_depth = 0

        # Manage potential for infinite recursion during price repair:
        self._record_stack_trace = True
        # self._record_stack_trace = False
        self._stack_trace = []
        self._infinite_recursion_detected = False

    def history(self,
                interval="1d",
                max_age=None,  # defaults to half of interval
                period=None,
                start=None, end=None, prepost=False, actions=True,
                adjust_splits=True, adjust_divs=True,
                keepna=False,
                proxy=None, rounding=False,
                debug=True):

        # t0 = perf_counter()

        if prepost:
            raise Exception("pre and post-market caching currently not implemented. If you really need it raise an issue on Github")

        debug_yfc = self._debug
        # debug_yfc = True

        log_msg = "YFC: history(tkr={}, interval={}, period={}, start={}, end={}, max_age={}, adjust_splits={}, adjust_divs={})".format(self.ticker, interval, period, start, end, max_age, adjust_splits, adjust_divs)
        if debug_yfc:
            print("")
            print(log_msg)
        elif self._trace:
            self._trace_depth += 1
            print(" "*self._trace_depth + log_msg)

        td_1d = datetime.timedelta(days=1)
        exchange = self.info['exchange']
        tz_exchange = ZoneInfo(self.info["exchangeTimezoneName"])
        yfct.SetExchangeTzName(exchange, self.info["exchangeTimezoneName"])
        dt_now = datetime.datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))

        # Type checks
        if (max_age is not None) and (not isinstance(max_age, datetime.timedelta)):
            raise Exception("Argument 'max_age' must be timedelta")
        if period is not None:
            if start is not None or end is not None:
                raise Exception("Don't set both 'period' and 'start'/'end'' arguments")
            if isinstance(period, str):
                if period not in yfcd.periodStrToEnum.keys():
                    raise Exception("'period' if str must be one of: {}".format(yfcd.periodStrToEnum.keys()))
                period = yfcd.periodStrToEnum[period]
            if not isinstance(period, yfcd.Period):
                raise Exception("'period' must be a yfcd.Period")
        if isinstance(interval, str):
            if interval not in yfcd.intervalStrToEnum.keys():
                raise Exception("'interval' if str must be one of: {}".format(yfcd.intervalStrToEnum.keys()))
            interval = yfcd.intervalStrToEnum[interval]
        if not isinstance(interval, yfcd.Interval):
            raise Exception("'interval' must be yfcd.Interval")

        start_d = None ; end_d = None
        if start is not None:
            start, start_d = self._process_user_dt(start)
            if start > dt_now:
                return None
            if interval == yfcd.Interval.Week:
                # Note: if start is on weekend then Yahoo can return weekly data starting
                #       on Saturday. This breaks YFC, start must be Monday! So fix here:
                if start is None:
                    # Working with simple dates, easy
                    if start_d.weekday() in [5, 6]:
                        start_d += datetime.timedelta(days=7-start_d.weekday())
                else:
                    wd = start_d.weekday()
                    if wd in [5, 6]:
                        start_d += datetime.timedelta(days=7-wd)
                        start = datetime.datetime.combine(start_d, datetime.time(0), tz_exchange)

        if end is not None:
            end, end_d = self._process_user_dt(end)

        # 'prepost' not doing anything in yfinance

        if max_age is None:
            if interval == yfcd.Interval.Days1:
                max_age = datetime.timedelta(hours=4)
            elif interval == yfcd.Interval.Week:
                max_age = datetime.timedelta(hours=60)
            elif interval == yfcd.Interval.Months1:
                max_age = datetime.timedelta(days=15)
            elif interval == yfcd.Interval.Months3:
                max_age = datetime.timedelta(days=45)
            else:
                max_age = 0.5*yfcd.intervalToTimedelta[interval]

        if (interval in self._history) and (self._history[interval] is not None):
            h = self._history[interval]
        else:
            h = self._getCachedPrices(interval)
            if h is not None:
                self._history[interval] = h
        h_cache_key = "history-"+yfcd.intervalToString[interval]

        # Handle missing dates, or dependencies between date arguments
        if period is not None:
            if h is None:
                # Use period
                pstr = yfcd.periodToString[period]
                start = None ; end = None
            else:
                # Map period to start->end range so logic can intelligently fetch missing data
                pstr = None
                d_now = dt_now.astimezone(tz_exchange).date()
                sched = yfct.GetExchangeSchedule(exchange, d_now-(7*td_1d), d_now+td_1d)
                dt_now_sub_lag = dt_now-self.yf_lag
                if sched["open"].iloc[-1] > dt_now_sub_lag:
                    # Discard days that haven't opened yet
                    opens = pd.DatetimeIndex(sched["open"])
                    x = opens.get_indexer([dt_now_sub_lag], method="bfill")[0]  # If not exact match, search forward
                    sched = sched.iloc[:x]
                last_open_day = sched["open"][-1].date()
                end = datetime.datetime.combine(last_open_day+td_1d, datetime.time(0), tz_exchange)
                end_d = end.date()
                if period == yfcd.Period.Max:
                    start = datetime.datetime.combine(datetime.date(yfcd.yf_min_year, 1, 1), datetime.time(0), tz_exchange)
                else:
                    start = yfct.DtSubtractPeriod(end, period)
                while not yfct.ExchangeOpenOnDay(exchange, start.date()):
                    start += td_1d
                start_d = start.date()
        else:
            pstr = None
            if end is None:
                end = datetime.datetime.combine(dt_now.date() + td_1d, datetime.time(0), tz_exchange)
            if start is None:
                start = datetime.datetime.combine(end.date()-td_1d, datetime.time(0), tz_exchange)

        if debug_yfc:
            print("- start={} , end={}".format(start, end))

        if (start is not None) and start == end:
            return None

        if start is not None:
            try:
                sched_7d = yfct.GetExchangeSchedule(exchange, start.date(), start.date()+7*td_1d)
            except Exception as e:
                if "Need to add mapping" in str(e):
                    raise Exception("Need to add mapping of exchange {} to xcal (ticker={})".format(self.info["exchange"], self.ticker))
                else:
                    raise
            if sched_7d is None:
                raise Exception("sched_7d is None for date range {}->{} and ticker {}".format(start.date(), start.date()+4*td_1d, self.ticker))
            if sched_7d["open"][0] > dt_now:
                # Requested date range is in future
                return None
        else:
            sched_7d = None

        # All date checks passed so can begin fetching

        if self._record_stack_trace:
            # Log function calls to detect and manage infinite recursion
            if len(self._stack_trace) == 0:
                fn_tuple = ("{}: history()".format(self.ticker), "interval={}".format(interval), "start={}".format(start), "end={}".format(end), "period={}".format(period))
            else:
                fn_tuple = ("history()", "interval={}".format(interval), "start={}".format(start), "end={}".format(end), "period={}".format(period))
            if fn_tuple in self._stack_trace:
                # Detected a potential recursion loop
                reconstruct_detected = False
                for i in range(len(self._stack_trace)-1, -1, -1):
                    if "_reconstructInterval" in str(self._stack_trace[i]):
                        reconstruct_detected = True
                        break
                if reconstruct_detected:
                    self._stack_trace.append(fn_tuple)
                    for i in range(len(self._stack_trace)):
                        print("  "*i + str(self._stack_trace[i]))
                    raise Exception("YFC detected recursion loop during price repair. Probably best to exclude '{}' until Yahoo fix their end.".format(self.ticker))
            self._stack_trace.append(fn_tuple)

        if ((start_d is None) or (end_d is None)) and (start is not None) and (end is not None):
            # if start_d/end_d not set then start/end are datetimes, so need to inspect
            # schedule opens/closes to determine days
            if sched_7d is not None:
                sched = sched_7d.iloc[0:1]
            else:
                sched = yfct.GetExchangeSchedule(exchange, start.date(), end.date()+td_1d)
            n = sched.shape[0]
            start_d = start.date() if start < sched["open"][0] else start.date()+td_1d
            end_d = end.date()+td_1d if end >= sched["close"][n-1] else end.date()

        if (start is not None) and (end is not None):
            listing_date = yfcm.ReadCacheDatum(self.ticker, "listing_date")
            if listing_date is not None:
                if not isinstance(listing_date, datetime.date):
                    raise Exception("listing_date = {} ({}) should be a date".format(listing_date, type(listing_date)))
                if start_d < listing_date:
                    start_d = listing_date
                    start = datetime.datetime.combine(listing_date, datetime.time(0), tz_exchange)

        interday = interval in [yfcd.Interval.Days1, yfcd.Interval.Week, yfcd.Interval.Months1, yfcd.Interval.Months3]

        # Trigger an estimation of Yahoo data delay:
        self.yf_lag

        # t1 = perf_counter()

        d_tomorrow = dt_now.astimezone(tz_exchange).date() + td_1d
        h_lastAdjustD = None
        if h is None:
            if period is not None:
                h = self._fetchYfHistory(pstr, interval, None, None, prepost, proxy, debug)
            else:
                if interval == yfcd.Interval.Days1:
                    # Ensure daily always up-to-now
                    h = self._fetchYfHistory(pstr, interval, start_d, d_tomorrow, prepost, proxy, debug)
                else:
                    if interday:
                        h = self._fetchYfHistory(pstr, interval, start_d, end_d, prepost, proxy, debug)
                    else:
                        h = self._fetchYfHistory(pstr, interval, start, end, prepost, proxy, debug)
            if h is None:
                raise Exception("{}: Failed to fetch date range {}->{}".format(self.ticker, start, end))

            # Adjust
            if interval == yfcd.Interval.Days1:
                h = self._processYahooAdjustment(h, interval)
                h_lastAdjustD = h.index[-1].date()
            else:
                # Ensure h is split-adjusted. Sometimes Yahoo returns unadjusted data
                h_lastDt = h.index[-1].to_pydatetime()
                next_day = yfct.GetTimestampNextInterval(exchange, h_lastDt, yfcd.Interval.Days1)["interval_open"]
                if next_day > dt_now.astimezone(tz_exchange).date():
                    h = self._processYahooAdjustment(h, interval)
                    h_lastAdjustD = h_lastDt.date()
                else:
                    try:
                        df_daily = self.history(start=next_day, interval=yfcd.Interval.Days1, max_age=td_1d, keepna=True)
                    except yfcd.NoPriceDataInRangeException:
                        df_daily = None
                    except Exception as e:
                        if "Failed to fetch date range" in str(e):
                            df_daily = None
                        else:
                            raise
                    h = self._processYahooAdjustment(h, interval)
                    if (df_daily is None) or (df_daily.shape[0] == 0):
                        h_lastAdjustD = h_lastDt.date()
                    else:
                        h_lastAdjustD = df_daily.index[-1].date()

        else:
            n = h.shape[0]
            if n > 0:
                if debug_yfc:
                    print("- h lastDt = {}".format(h.index[-1]))
                elif self._trace:
                    print(" "*self._trace_depth + "- h lastDt = {}".format(h.index[-1]))

            if interday:
                h_interval_dts = h.index.date if isinstance(h.index[0], pd.Timestamp) else h.index
            else:
                h_interval_dts = np.array([yfct.ConvertToDatetime(dt, tz=tz_exchange) for dt in h.index])
            h_interval_dts = np.array(h_interval_dts)
            if interval == yfcd.Interval.Days1:
                # Daily data is always contiguous so only need to check last row
                h_interval_dt = h_interval_dts[-1]
                fetch_dt = yfct.ConvertToDatetime(h["FetchDate"].iloc[-1], tz=tz_exchange)
                last_expired = yfct.IsPriceDatapointExpired(h_interval_dt, fetch_dt, max_age, exchange, interval, yf_lag=self.yf_lag)
                if last_expired:
                    # Drop last row because expired
                    h = h.drop(h.index[-1])
                    h_interval_dts = h_interval_dts[0: n-1]
                    n -= 1
            else:
                expired = np.array([False]*n)
                f_final = h["Final?"].values
                for idx in np.where(~f_final)[0]:
                    h_interval_dt = h_interval_dts[idx]
                    fetch_dt = yfct.ConvertToDatetime(h["FetchDate"][idx], tz=tz_exchange)
                    expired[idx] = yfct.IsPriceDatapointExpired(h_interval_dt, fetch_dt, max_age, exchange, interval, yf_lag=self.yf_lag)
                if expired.any():
                    h = h.drop(h.index[expired])
                    h_interval_dts = h_interval_dts[~expired]

            # Performance TODO: tag rows as fully contiguous to avoid searching for gaps

            if interval == yfcd.Interval.Days1:
                if h is None or h.shape[0] == 0:
                    if interday:
                        ranges_to_fetch = yfct.IdentifyMissingIntervalRanges(exchange, start_d, end_d, interval, [], weeklyUseYahooDef=True, minDistanceThreshold=5)
                    else:
                        ranges_to_fetch = yfct.IdentifyMissingIntervalRanges(exchange, start, end, interval, [], weeklyUseYahooDef=True, minDistanceThreshold=5)
                else:
                    # Ensure that daily data always up-to-date to now
                    dt_start = yfct.ConvertToDatetime(h.index[0], tz=tz_exchange)
                    dt_end = yfct.ConvertToDatetime(h.index[-1], tz=tz_exchange)
                    h_start = yfct.GetTimestampCurrentInterval(exchange, dt_start, interval, weeklyUseYahooDef=True)["interval_open"]
                    h_end = yfct.GetTimestampCurrentInterval(exchange, dt_end, interval, weeklyUseYahooDef=True)["interval_close"]

                    rangePre_to_fetch = None
                    if interday:
                        if start_d < h_start:
                            try:
                                rangePre_to_fetch = yfct.IdentifyMissingIntervalRanges(exchange, start_d, h_start, interval, None, weeklyUseYahooDef=True, minDistanceThreshold=5)
                            except yfcd.NoIntervalsInRangeException:
                                rangePre_to_fetch = None
                    else:
                        if start < h_start:
                            try:
                                rangePre_to_fetch = yfct.IdentifyMissingIntervalRanges(exchange, start, h_start, interval, None, weeklyUseYahooDef=True, minDistanceThreshold=5)
                            except yfcd.NoIntervalsInRangeException:
                                rangePre_to_fetch = None
                    if rangePre_to_fetch is not None:
                        if len(rangePre_to_fetch) > 1:
                            raise Exception("Expected only one element in rangePre_to_fetch[], but = {}".format(rangePre_to_fetch))
                        rangePre_to_fetch = rangePre_to_fetch[0]
                    #
                    target_end_d = dt_now.astimezone(tz_exchange).date() + td_1d
                    rangePost_to_fetch = None
                    if interday:
                        if h_end < target_end_d:
                            try:
                                rangePost_to_fetch = yfct.IdentifyMissingIntervalRanges(exchange, h_end, target_end_d, interval, None, weeklyUseYahooDef=True, minDistanceThreshold=5)
                            except yfcd.NoIntervalsInRangeException:
                                rangePost_to_fetch = None
                    else:
                        target_end_dt = dt_now
                        d = target_end_dt.astimezone(tz_exchange).date()
                        sched = yfct.GetExchangeSchedule(exchange, d, d + td_1d)
                        if (sched is not None) and (sched.shape[0] > 0) and (dt_now > sched["open"].iloc[0]):
                            target_end_dt = sched["close"].iloc[0]+datetime.timedelta(hours=2)
                        if h_end < target_end_dt:
                            try:
                                rangePost_to_fetch = yfct.IdentifyMissingIntervalRanges(exchange, h_end, target_end_dt, interval, None, weeklyUseYahooDef=True, minDistanceThreshold=5)
                            except yfcd.NoIntervalsInRangeException:
                                rangePost_to_fetch = None
                    ranges_to_fetch = []
                    if rangePost_to_fetch is not None:
                        if len(rangePost_to_fetch) > 1:
                            raise Exception("Expected only one element in rangePost_to_fetch[], but = {}".format(rangePost_to_fetch))
                        rangePost_to_fetch = rangePost_to_fetch[0]
                    if rangePre_to_fetch is not None:
                        ranges_to_fetch.append(rangePre_to_fetch)
                    if rangePost_to_fetch is not None:
                        ranges_to_fetch.append(rangePost_to_fetch)
            else:
                h_intervals = yfct.GetTimestampCurrentInterval_batch(exchange, h_interval_dts, interval, weeklyUseYahooDef=True)
                if h_intervals is None:
                    h_intervals = pd.DataFrame(data={"interval_open": [], "interval_close": []})
                f_na = h_intervals["interval_open"].isna().values
                if f_na.any():
                    print(h[f_na])
                    raise Exception("Bad rows found in prices table")
                    if debug_yfc:
                        print("- found bad rows, deleting:")
                        print(h[f_na])
                    h = h[~f_na].copy()
                    h_intervals = h_intervals[~f_na]
                if h_intervals.shape[0] > 0 and isinstance(h_intervals["interval_open"][0], datetime.datetime):
                    h_interval_opens = [x.to_pydatetime().astimezone(tz_exchange) for x in h_intervals["interval_open"]]
                else:
                    h_interval_opens = h_intervals["interval_open"].values

                try:
                    if interday:
                        ranges_to_fetch = yfct.IdentifyMissingIntervalRanges(exchange, start_d, end_d, interval, h_interval_opens, weeklyUseYahooDef=True, minDistanceThreshold=5)
                    else:
                        ranges_to_fetch = yfct.IdentifyMissingIntervalRanges(exchange, start, end, interval, h_interval_opens, weeklyUseYahooDef=True, minDistanceThreshold=5)
                    if ranges_to_fetch is None:
                        ranges_to_fetch = []
                except yfcd.NoIntervalsInRangeException:
                    ranges_to_fetch = []
                except Exception:
                    print("Ticker =", self.ticker)
                    raise
            # Prune ranges in future:
            for i in range(len(ranges_to_fetch)-1, -1, -1):
                x = ranges_to_fetch[i][0]
                delete_range = False
                if isinstance(x, (datetime.datetime, pd.Timestamp)):
                    if x > dt_now:
                        delete_range = True
                    else:
                        sched = yfct.GetExchangeSchedule(exchange, x.date(), x.date() + 3*td_1d)
                        delete_range = dt_now < (sched["open"].iloc[0] + self.yf_lag)
                else:
                    if datetime.datetime.combine(x, datetime.time(0), tzinfo=tz_exchange) > dt_now:
                        delete_range = True
                    else:
                        sched = yfct.GetExchangeSchedule(exchange, x, x + 3*td_1d)
                        delete_range = dt_now < (sched["open"].iloc[0] + self.yf_lag)
                if delete_range:
                    if debug_yfc:
                        print("- deleting future range:", ranges_to_fetch[i])
                    del ranges_to_fetch[i]
            # Important that ranges_to_fetch in reverse order!
            ranges_to_fetch.sort(key=lambda x: x[0], reverse=True)
            if debug_yfc:
                print("- ranges_to_fetch:")
                pprint(ranges_to_fetch)

            if len(ranges_to_fetch) > 0:
                if h.shape[0] > 0:
                    # Ensure only one range max is after cached data:
                    h_last_dt = h.index[-1].to_pydatetime()
                    if not isinstance(ranges_to_fetch[0][0], datetime.datetime):
                        h_last_dt = h_last_dt.astimezone(tz_exchange).date()
                    n = 0
                    for r in ranges_to_fetch:
                        if r[0] > h_last_dt:
                            n += 1
                    if n > 1:
                        print("ranges_to_fetch:")
                        pprint(ranges_to_fetch)
                        raise Exception("ranges_to_fetch contains {} ranges that occur after h_last_dt={}, expected 1 max".format(n, h_last_dt))

                quiet = period is not None  # YFC generated date range so don't print message
                if debug_yfc:
                    quiet = False
                # quiet = not debug_yfc
                if interval == yfcd.Interval.Days1:
                    h = self._fetchAndAddRanges_contiguous(h, pstr, interval, ranges_to_fetch, prepost, proxy, debug, quiet=quiet)
                    h_lastAdjustD = h.index[-1].date()
                else:
                    h = self._fetchAndAddRanges_sparse(h, pstr, interval, ranges_to_fetch, prepost, proxy, debug, quiet=quiet)
                    h_lastAdjustD = self._history[yfcd.Interval.Days1].index[-1].date()

        # t2 = perf_counter()

        if (h is None) or h.shape[0] == 0:
            raise Exception("history() is exiting without price data")

        f_dups = h.index.duplicated()
        if f_dups.any():
            raise Exception("{}: These timepoints have been duplicated: {}".format(self.ticker, h.index[f_dups]))

        # Cache
        self._history[interval] = h
        yfcm.StoreCacheDatum(self.ticker, h_cache_key, self._history[interval])
        if h_lastAdjustD is not None:
            h_lastAdjustD_cached = yfcm.ReadCacheMetadata(self.ticker, h_cache_key, "LastAdjustD")
            if h_lastAdjustD_cached is None or h_lastAdjustD > h_lastAdjustD_cached:
                if debug_yfc:
                    print("- writing LastAdjustD={} to md of {}/{}".format(h_lastAdjustD, self.ticker, h_cache_key))
                yfcm.WriteCacheMetadata(self.ticker, h_cache_key, "LastAdjustD", h_lastAdjustD)

        # t3 = perf_counter()

        # Present table for user:
        h_copied = False
        if (start is not None) and (end is not None):
            h = h.loc[start:end-datetime.timedelta(milliseconds=1)].copy()
            h_copied = True

        # t4 = perf_counter()

        if not keepna:
            price_data_cols = [c for c in yfcd.yf_data_cols if c in h.columns]
            mask_nan_or_zero = (np.isnan(h[price_data_cols].to_numpy()) | (h[price_data_cols].to_numpy() == 0)).all(axis=1)
            if mask_nan_or_zero.any():
                h = h.drop(h.index[mask_nan_or_zero])
                h_copied = True
        if h.shape[0] == 0:
            h = None
        else:
            if not actions:
                h = h.drop(["Dividends", "Stock Splits"], axis=1)
            else:
                if "Dividends" not in h.columns:
                    raise Exception("Dividends column missing from table")

            if adjust_splits:
                if not h_copied:
                    h = h.copy()
                for c in ["Open", "Close", "Low", "High", "Dividends"]:
                    h[c] = np.multiply(h[c].to_numpy(), h["CSF"].to_numpy())
                h["Volume"] = np.divide(h["Volume"].to_numpy(), h["CSF"].to_numpy())
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
                rnd = yfcu.CalculateRounding(h["Close"].iloc[-1], 4)
                for c in ["Open", "Close", "Low", "High"]:
                    h[c] = np.round(h[c].to_numpy(), rnd)

            if debug_yfc:
                print("YFC: history() returning")
                cols = [c for c in ["Close", "Dividends", "Volume", "CDF", "CSF"] if c in h.columns]
                print(h[cols])
                if "Dividends" in h.columns:
                    f = h["Dividends"] != 0.0
                    if f.any():
                        print("- dividends:")
                        print(h.loc[f, cols])
                print("")
            elif self._trace:
                print(" "*self._trace_depth + "YFC: history() returning")
                self._trace_depth -= 1

        # t5 = perf_counter()
        # t_setup = t1-t0
        # t_sync = t2-t1
        # t_cache = t3-t2
        # t_filter = t4-t3
        # t_adjust = t5-t4
        # t_sum = t_setup + t_sync + t_cache + t_filter + t_adjust
        # print("TIME: {:.4f}s: setup={:.4f} sync={:.4f} cache={:.4f} filter={:.4f} adjust={:.4f}".format(t_sum, t_setup, t_sync, t_cache, t_filter, t_adjust))
        # t_setup *= 100/t_sum
        # t_sync *= 100/t_sum
        # t_cache *= 100/t_sum
        # t_filter *= 100/t_sum
        # t_adjust *= 100/t_sum
        # print("TIME %:        setup={:.1f}%  sync={:.1f}%  cache={:.1f}%  filter={:.1f}%  adjust={:.1f}%".format(t_setup, t_sync, t_cache, t_filter, t_adjust))

        if self._record_stack_trace:
            # Pop stack trace
            if len(self._stack_trace) == 0:
                raise Exception("Failing to correctly push/pop stack trace (is empty too early)")
            if not self._stack_trace[-1] == fn_tuple:
                for i in range(len(self._stack_trace)):
                    print("  "*i + str(self._stack_trace[i]))
                raise Exception("Failing to correctly push/pop stack trace (see above)")
            self._stack_trace.pop(len(self._stack_trace)-1)
            if len(self._stack_trace) == 0:
                # Reset:
                self._infinite_recursion_detected = False

        return h

    def _fetchYfHistory(self, pstr, interval, start, end, prepost, proxy, debug):
        debug_yfc = self._debug
        # debug_yfc = True

        log_msg = "YFC: {}: _fetchYfHistory(interval={} , pstr={} , start={} , end={})".format(self.ticker, interval, pstr, start, end)
        if debug_yfc:
            print("")
            print(log_msg)
        elif self._trace:
            self._trace_depth += 1
            print(" "*self._trace_depth + log_msg)
        # else:
        #   print(log_msg)

        if pstr is not None:
            if (start is not None) and (end is not None):
                # start/end take precedence over pstr
                pstr = None

        exchange = self.info["exchange"]
        tz_exchange = ZoneInfo(self.info["exchangeTimezoneName"])
        istr = yfcd.intervalToString[interval]
        itd = yfcd.intervalToTimedelta[interval]
        interday = interval in [yfcd.Interval.Days1, yfcd.Interval.Week, yfcd.Interval.Months1, yfcd.Interval.Months3]
        td_1d = datetime.timedelta(days=1)

        fetch_start = start
        fetch_end = end
        if end is not None:
            # If 'fetch_end' in future then cap to exchange midnight
            dtnow = datetime.datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
            dtnow_exchange = dtnow.astimezone(tz_exchange)
            if isinstance(end, datetime.datetime):
                end_dt = end
                end_d = end.astimezone(tz_exchange).date()
            else:
                end_d = end
                end_dt = datetime.datetime.combine(end, datetime.time(0), tz_exchange)
            if end_dt > dtnow:
                exchange_midnight_dt = datetime.datetime.combine(dtnow_exchange.date()+td_1d, datetime.time(0), tz_exchange)
                if isinstance(end, datetime.datetime):
                    fetch_end = exchange_midnight_dt
                else:
                    fetch_end = exchange_midnight_dt.date()
        if start is not None:
            if isinstance(start, datetime.datetime):
                start_dt = start
                start_d = start.astimezone(tz_exchange).date()
            else:
                start_d = start
                start_dt = datetime.datetime.combine(start, datetime.time(0), tz_exchange)

            if (fetch_start is not None) and (fetch_end <= fetch_start):
                return None
        if isinstance(start, datetime.datetime) and (start.time() > datetime.time(3)) and (not interday):
            # if start = start time of trading day, then Volume will be incorrect.
            # Need to be 20 minutes earlier to get Yahoo to return correct volume.
            # And rather than lookup trading day start, just shift any datetime
            fetch_start -= datetime.timedelta(minutes=20)
            if debug_yfc:
                print("- fetch_start shifted back to:", fetch_start)
        # # Update: 20 minutes not enough! Need to go back to last trading day!
        # if isinstance(start, datetime.datetime) and (not interday):
        #     if yfct.IsTimestampInActiveSession(exchange, fetch_start):
        #         s = yfct.GetTimestampCurrentSession(exchange, fetch_start)
        #         fetch_start = s["market_open"] - datetime.timedelta(hours=6)
        #     s = yfct.GetTimestampMostRecentSession(exchange, fetch_start)
        #     fetch_start = s["market_close"] - datetime.timedelta(hours=1)
        #     if debug_yfc:
        #         print("- fetch_start shifted back to:", fetch_start)
        # Update: but shifting back too far for 1-minute data increase risk of
        # hitting Yahoo's limit (7-days worth only).
        if interval == yfcd.Interval.Week:
            # Ensure aligned to week start:
            fetch_start -= datetime.timedelta(days=fetch_start.weekday())
        if fetch_start is not None:
            if not isinstance(fetch_start, (datetime.datetime, pd.Timestamp)):
                fetch_start_dt = datetime.datetime.combine(fetch_start, datetime.time(0), tz_exchange)
            else:
                fetch_start_dt = fetch_start
        if fetch_end is not None:
            if not isinstance(fetch_end, (datetime.datetime, pd.Timestamp)):
                fetch_end_dt = datetime.datetime.combine(fetch_end, datetime.time(0), tz_exchange)
            else:
                fetch_end_dt = fetch_end

        if interday:
            if interval == yfcd.Interval.Days1:
                listing_date_check_tol = datetime.timedelta(days=7)
            elif interval == yfcd.Interval.Week:
                listing_date_check_tol = datetime.timedelta(days=14)
            elif interval == yfcd.Interval.Months1:
                listing_date_check_tol = datetime.timedelta(days=35)
            elif interval == yfcd.Interval.Months3:
                listing_date_check_tol = datetime.timedelta(days=35*3)

        if debug_yfc:
            if pstr is not None:
                print("YFC: {}: fetching {} period".format(self.ticker, pstr))
            else:
                if (not isinstance(fetch_start, datetime.datetime)) or fetch_start.time() == datetime.time(0):
                    start_str = fetch_start.strftime("%Y-%m-%d")
                else:
                    start_str = fetch_start.strftime("%Y-%m-%d %H:%M:%S")
                if (not isinstance(fetch_end, datetime.datetime)) or fetch_end.time() == datetime.time(0):
                    end_str = fetch_end.strftime("%Y-%m-%d")
                else:
                    end_str = fetch_end.strftime("%Y-%m-%d %H:%M:%S")
                print("YFC: {}: fetching {} {} -> {}".format(self.ticker, yfcd.intervalToString[interval], start_str, end_str))

        first_fetch_failed = False ; ex = None
        df = None
        try:
            if debug_yfc:
                print("- fetch_start={} ; fetch_end={}".format(fetch_start, fetch_end))
            df = self.dat.history(period=pstr,
                                  interval=istr,
                                  start=fetch_start, end=fetch_end,
                                  prepost=prepost,
                                  actions=True,  # Always fetch
                                  keepna=True,
                                  auto_adjust=False,  # store raw data, adjust myself
                                  back_adjust=False,  # store raw data, adjust myself
                                  proxy=proxy,
                                  rounding=False,  # store raw data, round myself
                                  raise_errors=True,
                                  debug=debug)
            if debug_yfc:
                if df is None:
                    print("- YF returned None")
                else:
                    print("- YF returned table:")
                    print(df[["Close", "Dividends", "Volume"]])
            if df is None or df.shape[0] == 0:
                raise Exception("No data found for this date range")
        except Exception as e:
            first_fetch_failed = True
            if "Data doesn't exist for startDate" in str(e):
                ex = yfcd.NoPriceDataInRangeException(self.ticker, istr, start, end)
            elif "No data found for this date range" in str(e):
                ex = yfcd.NoPriceDataInRangeException(self.ticker, istr, start, end)
            else:
                print("df:")
                print(df)
                raise e
        if not first_fetch_failed and fetch_start is not None:
            # if isinstance(fetch_start, (datetime.datetime, pd.Timestamp)):
            #   df = df[df.index>=fetch_start]
            # else:
            #   df = df[df.index.date>=fetch_start]
            df = df.loc[fetch_start_dt:]
            if df.shape[0] == 0:
                first_fetch_failed = True
                ex = yfcd.NoPriceDataInRangeException(self.ticker, istr, start, end)

        fetch_dt_utc = datetime.datetime.utcnow()

        second_fetch_failed = False
        df_wider = None
        if interday:
            if first_fetch_failed and (fetch_end is not None):
                # Try with wider date range, maybe entire range is just before listing date
                if debug_yfc:
                    print("- retrying YF fetch with wider date range")

                fetch_start -= 2*listing_date_check_tol
                fetch_end += 2*listing_date_check_tol
                if not isinstance(fetch_start, (datetime.datetime, pd.Timestamp)):
                    fetch_start_dt = datetime.datetime.combine(fetch_start, datetime.time(0), tz_exchange)
                else:
                    fetch_start_dt = fetch_start
                if not isinstance(fetch_end, (datetime.datetime, pd.Timestamp)):
                    fetch_end_dt = datetime.datetime.combine(fetch_end, datetime.time(0), tz_exchange)
                else:
                    fetch_end_dt = fetch_end
                if debug_yfc:
                    print("- first fetch failed, trying again with longer range: {} -> {}".format(fetch_start, fetch_end))
                try:
                    df_wider = self.dat.history(period=pstr,
                                                interval=istr,
                                                start=fetch_start, end=fetch_end,
                                                prepost=prepost,
                                                actions=True,  # Always fetch
                                                keepna=True,
                                                auto_adjust=False,  # store raw data, adjust myself
                                                back_adjust=False,  # store raw data, adjust myself
                                                proxy=proxy,
                                                rounding=False,  # store raw data, round myself
                                                raise_errors=True,
                                                debug=debug)
                    if debug_yfc:
                        print("- second fetch returned:")
                        print(df_wider)
                    if df_wider is None or df_wider.shape[0] == 0:
                        raise Exception("No data found for this date range")
                except Exception as e:
                    if "Data doesn't exist for startDate" in str(e):
                        second_fetch_failed = True
                    elif "No data found for this date range" in str(e):
                        second_fetch_failed = True
                    else:
                        raise e

                if df_wider is not None:
                    df = df_wider
                    if fetch_start is not None:
                        df = df.loc[fetch_start_dt:]
                    if fetch_end is not None:
                        df = df.loc[:fetch_end_dt-datetime.timedelta(milliseconds=1)]

            if first_fetch_failed:
                if second_fetch_failed:
                    # Hopefully code never comes here
                    raise ex
                else:
                    # Requested date range was just before stock listing date,
                    # but wider range crosses over so can continue
                    pass

            # Detect listing day
            found_listing_day = False
            listing_day = None
            if df.shape[0] > 0:
                if pstr == "max":
                    found_listing_day = True
                else:
                    tol = listing_date_check_tol
                    if fetch_start is not None:
                        fetch_start_d = fetch_start.date() if isinstance(fetch_start, datetime.datetime) else fetch_start
                        if (df.index[0].date() - fetch_start_d) > tol:
                            # Yahoo returned data starting significantly after requested start date, indicates
                            # request is before stock listed on exchange
                            found_listing_day = True
                    else:
                        start_expected = yfct.DtSubtractPeriod(fetch_dt_utc.date()+td_1d, yfcd.periodStrToEnum[pstr])
                        if interval == yfcd.Interval.Week:
                            start_expected -= datetime.timedelta(days=start_expected.weekday())
                        if (df.index[0].date() - start_expected) > tol:
                            found_listing_day = True
                if debug_yfc:
                    print("- found_listing_day = {}".format(found_listing_day))
                if found_listing_day:
                    listing_day = df.index[0].date()
                    if debug_yfc:
                        print("YFC: inferred listing_date = {}".format(listing_day))
                    yfcm.StoreCacheDatum(self.ticker, "listing_date", listing_day)

                if (listing_day is not None) and first_fetch_failed:
                    if end <= listing_day:
                        # Aha! Requested date range was entirely before listing
                        if debug_yfc:
                            print("- requested date range was before listing date")
                        return None

            # Check that weekly aligned to Monday. If not, shift start date back
            # and re-fetch
            if interval == yfcd.Interval.Week and df.shape[0] > 0 and (df.index[0].weekday() != 0):
                # Despite fetch_start aligned to Monday, sometimes Yahoo returns weekly
                # data starting a different day. Shifting back a little fixes
                fetch_start -= datetime.timedelta(days=2)
                if debug_yfc:
                    print("- weekly data not aligned to Monday, re-fetching from {}".format(fetch_start))
                df = self.dat.history(period=pstr, interval=istr,
                                      start=fetch_start, end=fetch_end,
                                      prepost=prepost, actions=True, keepna=True,
                                      auto_adjust=False, back_adjust=False,
                                      proxy=proxy,
                                      rounding=False,  # store raw data, round myself
                                      raise_errors=True,
                                      debug=debug)

                if interval == yfcd.Interval.Week and (df.index[0].weekday() != 0):
                    print("Date range requested: {} -> {}".format(fetch_start, fetch_end))
                    print(df)
                    raise Exception("Weekly data returned by YF doesn't begin Monday but {}".format(df.index[0].weekday()))

        if (df is not None) and (df.shape[0] > 0) and (df.index.tz is not None) and (not isinstance(df.index.tz, ZoneInfo)):
            # Convert to ZoneInfo
            df.index = df.index.tz_convert(tz_exchange)

        if debug_yfc:
            if df is None:
                print("YFC: YF returned None")
            else:
                # pass
                print("YFC: YF returned table:")
                print(df[["Close", "Dividends", "Volume"]])

        if pstr is None:
            if df is None:
                received_interval_starts = None
            else:
                if interday:
                    received_interval_starts = df.index.date
                else:
                    received_interval_starts = df.index.to_pydatetime()
            try:
                intervals_missing_df = yfct.IdentifyMissingIntervals(exchange, start, end, interval, received_interval_starts)
            except yfcd.NoIntervalsInRangeException:
                intervals_missing_df = None
            # Remove missing intervals today:
            if (intervals_missing_df is not None) and intervals_missing_df.shape[0] > 0:
                # If very few intervals and not today (so Yahoo should have data),
                # then assume no trading occurred and insert NaN rows.
                # Normally Yahoo has already filled with NaNs but sometimes they forget/are late
                nm = intervals_missing_df.shape[0]
                if interday:
                    threshold = 1
                else:
                    if itd <= datetime.timedelta(2):
                        threshold = 10
                    elif itd <= datetime.timedelta(5):
                        threshold = 3
                    else:
                        threshold = 2
                if nm <= threshold:
                    if debug_yfc:
                        print("- found missing intervals, inserting nans:")
                        print(intervals_missing_df)
                    df_missing = pd.DataFrame(data={k: [np.nan]*nm for k in yfcd.yf_data_cols}, index=intervals_missing_df["open"])
                    df_missing.index = pd.to_datetime(df_missing.index)
                    if interday:
                        df_missing.index = df_missing.index.tz_localize(tz_exchange)
                    for c in ["Volume", "Dividends", "Stock Splits"]:
                        df_missing[c] = 0
                    if df is None:
                        df = df_missing
                    else:
                        df = pd.concat([df, df_missing], sort=True).sort_index()
                        df.index = pd.to_datetime(df.index, utc=True).tz_convert(tz_exchange)

        # Improve tolerance to calendar missing a recent new holiday:
        if (df is None) or df.shape[0] == 0:
            return None

        n = df.shape[0]

        fetch_dt = fetch_dt_utc.replace(tzinfo=ZoneInfo("UTC"))

        df = self._repairZeroPrices(df, interval)
        df = self._repairUnitMixups(df, interval)

        if (n > 0) and (pstr is None):
            # Remove any out-of-range data:
            # NOTE: YF has a bug-fix pending merge: https://github.com/ranaroussi/yfinance/pull/1012
            if end is not None:
                if interday:
                    df = df[df.index.date < end_d]
                else:
                    df = df[df.index < end_dt]
                n = df.shape[0]
            #
            # And again for pre-start data:
            if start is not None:
                if interday:
                    df = df[df.index.date >= start_d]
                else:
                    df = df[df.index >= start_dt]
                n = df.shape[0]

        if n == 0:
            raise yfcd.NoPriceDataInRangeException(self.ticker, istr, start, end)
        else:
            # Verify that all datetimes match up with actual intervals:
            if interday:
                if not (df.index.time == datetime.time(0)).all():
                    print(df)
                    raise Exception("Interday data contains times in index")
                intervalStarts = df.index.date
            else:
                intervalStarts = df.index.to_pydatetime()
            #
            intervals = yfct.GetTimestampCurrentInterval_batch(exchange, intervalStarts, interval, weeklyUseYahooDef=True)
            f_na = intervals["interval_open"].isna().values
            if f_na.any():
                if not interday:
                    # For some exchanges (e.g. JSE) Yahoo returns intraday timestamps right on market close. Remove them.
                    df2 = df.copy() ; df2["_date"] = df2.index.date ; df2["_intervalStart"] = df2.index
                    sched = yfct.GetExchangeSchedule(exchange, df2["_date"].min(), df2["_date"].max()+td_1d)
                    rename_cols = {"open": "market_open", "close": "market_close"}
                    sched.columns = [rename_cols[c] if c in rename_cols else c for c in sched.columns]
                    sched_df = sched.copy()
                    sched_df["_date"] = sched_df.index.date
                    df2 = df2.merge(sched_df, on="_date", how="left")
                    f_drop = (df2["Volume"] == 0).values & ((df2["_intervalStart"] == df2["market_close"]).values)
                    if f_drop.any():
                        if debug_yfc:
                            print("- dropping 0-volume rows starting at market close")
                        intervalStarts = intervalStarts[~f_drop]
                        intervals = intervals[~f_drop]
                        df = df[~f_drop]
                        n = df.shape[0]
                        f_na = intervals["interval_open"].isna().values
                if f_na.any():
                    # For some national holidays when exchange closed, Yahoo fills in row. Clue is 0 volume.
                    # Solution = drop:
                    f_na_zeroVol = f_na & (df["Volume"] == 0).values
                    if f_na_zeroVol.any():
                        if debug_yfc:
                            print("- dropping {} 0-volume rows with no matching interval".format(sum(f_na_zeroVol)))
                        f_drop = f_na_zeroVol
                        intervalStarts = intervalStarts[~f_drop]
                        intervals = intervals[~f_drop]
                        df = df[~f_drop]
                        n = df.shape[0]
                        f_na = intervals["interval_open"].isna().values
                    # TODO ... another clue is row is identical to previous trading day
                    if f_na.any():
                        f_drop = np.array([False]*n)
                        for i in np.where(f_na)[0]:
                            if i > 0:
                                dt = df.index[i]
                                last_dt = df.index[i-1]
                                if (df.loc[dt, yfcd.yf_data_cols] == df.loc[last_dt, yfcd.yf_data_cols]).all():
                                    f_drop[i] = True
                        if f_drop.any():
                            if debug_yfc:
                                print("- dropping rows with no interval that are identical to previous row")
                            intervalStarts = intervalStarts[~f_drop]
                            intervals = intervals[~f_drop]
                            df = df[~f_drop]
                            n = df.shape[0]
                            f_na = intervals["interval_open"].isna().values
                if f_na.any() and interval==yfcd.Interval.Mins1:
                    # If 1-minute interval at market close, then merge with previous minute
                    indices = sorted(np.where(f_na)[0], reverse=True)
                    for idx in indices:
                        dt = df.index[idx]
                        sched = yfct.GetExchangeSchedule(exchange, dt.date(), dt.date()+td_1d)
                        if dt.time() == sched["close"].iloc[0].time():
                            if idx==0:
                                # Discard
                                print("discarding")
                                pass
                            else:
                                print("merging")
                                # Merge with previous
                                dt1 = df.index[idx-1]
                                df.loc[dt1,"Close"] = df["Close"].iloc[idx]
                                df.loc[dt1,"High"] = df["High"].iloc[idx-1:idx+1].max()
                                df.loc[dt1,"Low"] = df["Low"].iloc[idx-1:idx+1].min()
                                df.loc[dt1,"Volume"] = df["Volume"].iloc[idx-1:idx+1].sum()
                            df = df.drop(dt)
                            intervals = intervals.drop(dt)
                            intervalStarts = np.delete(intervalStarts, idx)
                    f_na = intervals["interval_open"].isna().values

                if f_na.any():
                    ctr = 0
                    for idx in np.where(f_na)[0]:
                        dt = df.index[idx]
                        ctr += 1
                        if ctr < 10:
                            print("Failed to map: {} (exchange={}, xcal={})".format(dt, exchange, yfcd.exchangeToXcalExchange[exchange]))
                            print(df.loc[dt])
                        elif ctr == 10:
                            print("- stopped printing at 10 failures")
                    raise Exception("Problem with dates returned by Yahoo, see above")

        df = df.copy()

        lastDataDts = yfct.CalcIntervalLastDataDt_batch(exchange, intervalStarts, interval)
        data_final = fetch_dt >= lastDataDts
        df["Final?"] = data_final

        df["FetchDate"] = pd.Timestamp(fetch_dt_utc).tz_localize("UTC")

        if debug_yfc:
            print(df)
            print("_fetchYfHistory() returning")
        elif self._trace:
            print(" "*self._trace_depth + "_fetchYfHistory() returning")
            self._trace_depth -= 1

        return df

    def _getCachedPrices(self, interval):
        if isinstance(interval, str):
            if interval not in yfcd.intervalStrToEnum.keys():
                raise Exception("'interval' if str must be one of: {}".format(yfcd.intervalStrToEnum.keys()))
            interval = yfcd.intervalStrToEnum[interval]

        istr = yfcd.intervalToString[interval]

        h = None

        if yfcm.IsDatumCached(self.ticker, "history-"+istr):
            h = yfcm.ReadCacheDatum(self.ticker, "history-"+istr)

        if h is not None and h.shape[0] == 0:
            h = None
        elif h is not None:
            h_modified = False

            f_na = np.isnan(h["CDF"].to_numpy())
            if f_na.any():
                h["CDF"] = h["CDF"].fillna(method="bfill").fillna(method="ffill")
                f_na = h["CDF"].isna()
                if f_na.any():
                    raise Exception("CDF NaN repair failed")
                h_modified = True

            # f_na = h["Close"].isna()
            # dtnow = datetime.datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
            # f_old = (dtnow-h["FetchDate"])>datetime.timedelta(minutes=15)
            # f_na = f_na & f_old
            # if f_na.any():
            #   print("Dropping NaNs in cached {}-{}".format(self.ticker, interval))
            #   h = h.drop(h.index[f_na])
            #   h_modified = True

            if h_modified:
                h_cache_key = "history-"+yfcd.intervalToString[interval]
                yfcm.StoreCacheDatum(self.ticker, h_cache_key, h)

        return h

    def _fetchAndAddRanges_contiguous(self, h, pstr, interval, ranges_to_fetch, prepost, proxy, debug, quiet=False):
        # Fetch each range, appending/prepending to cached data
        if (ranges_to_fetch is None) or len(ranges_to_fetch) == 0:
            return h

        debug_yfc = self._debug
        # debug_yfc = True

        if debug_yfc:
            print("_fetchAndAddRanges_contiguous()")
            print("- ranges_to_fetch:")
            pprint(ranges_to_fetch)
        elif self._trace:
            self._trace_depth += 1
            print(" "*self._trace_depth + "_fetchAndAddRanges_contiguous()")

        tz_exchange = ZoneInfo(self.info["exchangeTimezoneName"])
        if h.shape[0] == 0:
            h_first_dt = None ; h_last_dt = None
        else:
            h_first_dt = h.index[0].to_pydatetime()
            h_last_dt = h.index[-1].to_pydatetime()
            if not isinstance(ranges_to_fetch[0][0], datetime.datetime):
                h_first_dt = h_first_dt.astimezone(tz_exchange).date()
                h_last_dt = h_last_dt.astimezone(tz_exchange).date()
        td_1d = datetime.timedelta(days=1)
        istr = yfcd.intervalToString[interval]

        # Because data should be contiguous, then ranges should meet some conditions:
        if len(ranges_to_fetch) > 2:
            pprint(ranges_to_fetch)
            raise Exception("For contiguous data generated {}>2 ranges".format(len(ranges_to_fetch)))
        if h.shape[0] == 0 and len(ranges_to_fetch) > 1:
            raise Exception("For contiguous data generated {} ranges, but h is empty".format(len(ranges_to_fetch)))
        range_pre = None ; range_post = None
        if h.shape[0] == 0 and len(ranges_to_fetch) == 1:
            range_pre = ranges_to_fetch[0]
        else:
            n_pre = 0 ; n_post = 0
            for r in ranges_to_fetch:
                if r[0] > h_last_dt:
                    n_post += 1
                    range_post = r
                elif r[0] < h_first_dt:
                    n_pre += 1
                    range_pre = r
            if n_pre > 1:
                pprint(ranges_to_fetch)
                raise Exception("For contiguous data generated {}>1 ranges before h_first_dt".format(n_pre))
            if n_post > 1:
                pprint(ranges_to_fetch)
                raise Exception("For contiguous data generated {}>1 ranges after h_last_dt".format(n_post))

        h2_pre = None ; h2_post = None
        if range_pre is not None:
            r = range_pre
            check_for_listing = False
            try:
                h2_pre = self._fetchYfHistory(pstr, interval, r[0], r[1], prepost, proxy, debug)
            except yfcd.NoPriceDataInRangeException:
                if interval == yfcd.Interval.Days1 and r[1] - r[0] == td_1d:
                    # If only trying to fetch 1 day of 1d data, then print warning instead of exception.
                    # Could add additional condition of dividend previous day (seems to mess up table).
                    if not quiet:
                        print("WARNING: No {}-price data fetched for ticker {} between dates {} -> {}".format(yfcd.intervalToString[interval], self.ticker, r[0], r[1]))
                    h2_pre = None
                elif (range_post is None) and (r[1]-r[0] < td_1d*7) and (r[1]-r[0] > td_1d*3):
                    # Small date range, potentially trying to fetch before listing data
                    check_for_listing = True
                    h2_pre = None
                else:
                    raise

            if check_for_listing:
                df = None
                try:
                    df = self._fetchYfHistory(pstr, interval, r[0], r[1]+td_1d*7, prepost, proxy, debug)
                except Exception:
                    # Discard
                    pass
                if df is not None:
                    # Then the exception above occurred because trying to fetch before listing dated!
                    yfcm.StoreCacheDatum(self.ticker, "listing_date", h.index[0].date())
                else:
                    # Then the exception above was genuine and needs to be propagated
                    raise yfcd.NoPriceDataInRangeException(self.ticker, istr, r[0], r[1])
        if range_post is not None:
            r = range_post
            try:
                h2_post = self._fetchYfHistory(pstr, interval, r[0], r[1], prepost, proxy, debug)
            except yfcd.NoPriceDataInRangeException:
                # If only trying to fetch 1 day of 1d data, then print warning instead of exception.
                # Could add additional condition of dividend previous day (seems to mess up table).
                if interval == yfcd.Interval.Days1 and r[1] - r[0] == td_1d:
                    if not quiet:
                        print("WARNING: No {}-price data fetched for ticker {} between dates {} -> {}".format(yfcd.intervalToString[interval], self.ticker, r[0], r[1]))
                    h2_post = None
                else:
                    raise

        if h2_post is not None:
            # UPDATE: only need duplicate check for noncontiguous data
            # # If a timepoint is in both h and h2, drop from h. This is possible because of
            # # threshold in IdentifyMissingIntervalRanges(), allowing re-fetch of cached data
            # # if it reduces total number of web requests
            # f_duplicate = h.index.isin(h2_pre.index)
            # h = h[~f_duplicate]

            # De-adjust the new data, and backport any new events in cached data
            # Note: Yahoo always returns split-adjusted price, so reverse it

            if debug_yfc:
                print("- appending new data")

            # Simple append to bottom of table
            # 1) adjust h2_post
            h2_post = self._processYahooAdjustment(h2_post, interval)
            if debug_yfc:
                print("- h2_post:")
                print(h2_post)

            # 2) backport h2_post splits across entire h table
            h2_csf = yfcu.GetCSF0(h2_post)
            if h2_csf != 1.0:
                if debug_yfc:
                    print("- backporting new data CSF={} across cached".format(h2_csf))
                h["CSF"] *= h2_csf
                if h2_pre is not None:
                    h2_pre["CSF"] *= h2_csf

            # 2) backport h2_post divs across entire h table
            close_day_before = h["Close"].iloc[-1]
            h2_cdf = yfcu.GetCDF0(h2_post, close_day_before)
            if h2_cdf != 1.0:
                if debug_yfc:
                    print("- backporting new data CDF={} across cached".format(h2_cdf))
                h["CDF"] *= h2_cdf
                # Note: don't need to backport across h2_pre because already
                #       contains dividend adjustment (via 'Adj Close')

            try:
                h = pd.concat([h, h2_post])
            except Exception:
                print(self.ticker)
                print("h:")
                print(h.iloc[h.shape[0]-10:])
                print("h2_post:")
                print(h2_post)
                raise

        if h2_pre is not None:
            if debug_yfc:
                print("- prepending new data")

            # Simple prepend to top of table
            if h.shape[0] == 0:
                post_csf = 1.0
            else:
                post_csf = yfcu.GetCSF0(h)
            h2_pre = self._processYahooAdjustment(h2_pre, interval, post_csf=post_csf)

            try:
                h = pd.concat([h, h2_pre])
            except Exception:
                print(self.ticker)
                print("h:")
                print(h.iloc[h.shape[0]-10:])
                print("h2_pre:")
                print(h2_pre)
                raise

        h.index = pd.to_datetime(h.index, utc=True).tz_convert(tz_exchange)
        h = h.sort_index()

        if debug_yfc:
            print("- h:")
            print(h)
            print("_fetchAndAddRanges_contiguous() returning")
        elif self._trace:
            print(" "*self._trace_depth + "_fetchAndAddRanges_contiguous() returning")
            self._trace_depth -= 1

        return h

    def _fetchAndAddRanges_sparse(self, h, pstr, interval, ranges_to_fetch, prepost, proxy, debug, quiet=False):
        # Fetch each range, but can be careless regarding de-adjust because
        # getting events from the carefully-managed daily data
        if (ranges_to_fetch is None) or len(ranges_to_fetch) == 0:
            return h

        debug_yfc = self._debug
        # debug_yfc = True

        if debug_yfc:
            print("_fetchAndAddRanges_sparse()")
        elif self._trace:
            self._trace_depth += 1
            print(" "*self._trace_depth + "_fetchAndAddRanges_sparse()")

        tz_exchange = ZoneInfo(self.info["exchangeTimezoneName"])
        td_1d = datetime.timedelta(days=1)

        h_cache_key = "history-"+yfcd.intervalToString[interval]
        h_lastAdjustD = yfcm.ReadCacheMetadata(self.ticker, h_cache_key, "LastAdjustD")
        if h_lastAdjustD is None:
            raise Exception("h_lastAdjustD is None")

        h_lastAdjustDt = datetime.datetime.combine(h_lastAdjustD+td_1d, datetime.time(0), tzinfo=tz_exchange)

        # Calculate cumulative adjustment factors for events that occurred since last adjustment,
        # and apply to h:
        dt_now = datetime.datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
        first_day_since_adjust = yfct.GetTimestampNextInterval(self.info["exchange"], h_lastAdjustDt, yfcd.Interval.Days1)["interval_open"]
        if first_day_since_adjust > dt_now.astimezone(tz_exchange).date():
            cdf = 1.0 ; csf = 1.0
        else:
            if debug_yfc:
                print("- first_day_since_adjust = {}".format(first_day_since_adjust))
            df_since = self.history(start=first_day_since_adjust, interval=yfcd.Interval.Days1, max_age=td_1d)  # auto_adjust=False)
            if df_since is None:
                cdf = 1.0 ; csf = 1.0
            else:
                df_since = self._getCachedPrices(interval) ; df_since = df_since[df_since.index.date >= first_day_since_adjust]
                if df_since.shape[0] == 0:
                    cdf = 1.0 ; csf = 1.0
                else:
                    close_day_before = self.history(start=h_lastAdjustD, end=h_lastAdjustD+td_1d)["Close"].iloc[-1]
                    cdf = yfcu.GetCDF0(df_since, close_day_before)
                    csf = yfcu.GetCSF0(df_since)

        # Backport adjustment factors to h:
        h["CSF"] *= csf
        h["CDF"] *= cdf

        # Ensure have daily data covering all ranges_to_fetch, so they can be de-splitted
        r_start_earliest = ranges_to_fetch[0][0]
        for rstart, rend in ranges_to_fetch:
            r_start_earliest = min(rstart, r_start_earliest)
        r_start_earliest_d = r_start_earliest.date() if isinstance(r_start_earliest, datetime.datetime) else r_start_earliest
        if debug_yfc:
            print("- r_start_earliest = {}".format(r_start_earliest))
        # Trigger price sync:
        self.history(start=r_start_earliest_d, interval=yfcd.Interval.Days1, max_age=td_1d)

        # Fetch each range, and adjust for splits that occurred after
        for rstart, rend in ranges_to_fetch:
            if debug_yfc:
                print("- fetching {} -> {}".format(rstart, rend))
            try:
                h2 = self._fetchYfHistory(pstr, interval, rstart, rend, prepost, proxy, debug)
            except yfcd.NoPriceDataInRangeException:
                # If only trying to fetch 1 day of 1d data, then print warning instead of exception.
                # Could add additional condition of dividend previous day (seems to mess up table).
                if interval == yfcd.Interval.Days1 and rend - rstart == td_1d:
                    ignore = True
                elif interval == yfcd.Interval.Mins1 and rend - rstart <= datetime.timedelta(minutes=10):
                    ignore = True
                if ignore:
                    if not quiet:
                        print("WARNING: No {}-price data fetched for ticker {} between dates {} -> {}".format(yfcd.intervalToString[interval], self.ticker, rstart, rend))
                    h2 = None
                    continue
                else:
                    raise

            if h2 is None:
                raise Exception("YF returned None for: tkr={}, interval={}, start={}, end={}".format(self.ticker, interval, rstart, rend))

            # Ensure h2 is split-adjusted. Sometimes Yahoo returns unadjusted data
            h2 = self._processYahooAdjustment(h2, interval)
            if debug_yfc:
                print("- h2 adjusted:")
                print(h2[["Close", "Dividends", "Volume", "CSF", "CDF"]])

            h = pd.concat([h, h2])
            h.index = pd.to_datetime(h.index, utc=True).tz_convert(tz_exchange)

        h = h.sort_index()

        if debug_yfc:
            print("_fetchAndAddRanges_sparse() returning")
        elif self._trace:
            print(" "*self._trace_depth + "_fetchAndAddRanges_sparse() returning")
            self._trace_depth -= 1

        return h

    def _processYahooAdjustment(self, df, interval, post_csf=None):
        # Yahoo returns data split-adjusted so reverse that.
        #
        # Except for hourly/minute data, Yahoo isn't consistent with adjustment:
        # - prices only split-adjusted if date range contains a split
        # - dividend appears split-adjusted
        # Easy to fix using daily data:
        # - divide prices by daily to determine if split-adjusted
        # - copy dividends from daily

        # Finally, add 'CSF' & 'CDF' columns to allow cheap on-demand adjustment

        if not isinstance(df, pd.DataFrame):
            raise Exception("'df' must be pd.DataFrame not {}".format(type(df)))
        if not isinstance(interval, yfcd.Interval):
            raise Exception("'interval' must be yfcd.Interval not {}".format(type(interval)))
        if (post_csf is not None) and not isinstance(post_csf, (float, int, np.int64)):
            raise Exception("'post_csf' if set must be scalar numeric not {}".format(type(post_csf)))

        debug_yfc = False
        debug_yfc = self._debug
        # debug_yfc = True

        if debug_yfc:
            print("")
            print("_processYahooAdjustment(interval={}, post_csf={}), {}->{}".format(interval, post_csf, df.index[0], df.index[-1]))
            print(df[["Close", "Dividends", "Volume"]])
        elif self._trace:
            self._trace_depth += 1
            print(" "*self._trace_depth + "_processYahooAdjustment(interval={}, post_csf={}), {}->{}".format(interval, post_csf, df.index[0], df.index[-1]))

        cdf = None
        csf = None

        # Step 1: ensure intraday price data is always split-adjusted
        interday = interval in [yfcd.Interval.Days1, yfcd.Interval.Week, yfcd.Interval.Months1, yfcd.Interval.Months3]
        td_7d = datetime.timedelta(days=7)
        if not interday:
            # Get daily price data during and after 'df'
            df_daily = self.history(start=df.index[0].date(), interval=yfcd.Interval.Days1, adjust_divs=False)
            if df_daily is None or df_daily.shape[0] == 0:
                df = df.drop("Adj Close", axis=1)
                df["CSF"] = 1.0
                df["CDF"] = 1.0
                return df

            f_post = df_daily.index.date > df.index[-1].date()
            df_daily_during = df_daily[~f_post].copy()
            df_daily_post = df_daily[f_post].copy()
            df_daily_during.index = df_daily_during.index.date ; df_daily_during.index.name = "_date"

            # Also get raw daily data from cache
            df_daily_raw = self._history[yfcd.Interval.Days1] ; df_daily_raw = df_daily_raw[df_daily_raw.index.date >= df.index[0].date()]
            f_post = df_daily_raw.index.date > df.index[-1].date()
            df_daily_raw_during = df_daily_raw[~f_post].copy()
            df_daily_raw_during_d = df_daily_raw_during.copy()
            df_daily_raw_during_d.index = df_daily_raw_during_d.index.date ; df_daily_raw_during_d.index.name = "_date"

            if df_daily_post.shape[0] == 0:
                csf_post = 1.0
            else:
                csf_post = yfcu.GetCSF0(df_daily_post)
            expectedRatio = 1.0 / csf_post

            # Merge 'df' with daily data to compare and infer adjustment
            df_aggByDay = df.copy()
            df_aggByDay["_date"] = df_aggByDay.index.date
            df_aggByDay = df_aggByDay.groupby("_date").agg(
                Low=("Low", "min"),
                High=("High", "max"),
                Open=("Open", "first"),
                Close=("Close", "last"))
            data_cols = ["Open", "Close", "Low", "High"]
            df2 = pd.merge(df_aggByDay, df_daily_during, how="left", on="_date", validate="one_to_one", suffixes=("", "_day"))
            # If 'df' has not been split-adjusted by Yahoo, but it should have been,
            # then the inferred split-adjust ratio should be close to 1.0/post_csf.
            # Apply a few sanity tests against inferred ratio - not NaN, low variance
            df3 = df2[~df2["Close"].isna()]
            if df3.shape[0] == 0:
                ss_ratio = expectedRatio
                stdev_pct = 0.0
            elif df.shape[0] == 1:
                ss_ratio = df2["Close"].iloc[0] / df2["Close_day"].iloc[0]
                stdev_pct = 0.0
            else:
                ratios = df2[data_cols].values / df2[[dc + "_day" for dc in data_cols]].values
                ratios[df2[data_cols].isna()] = 1.0
                ss_ratio = np.mean(ratios)
                stdev_pct = np.std(ratios) / ss_ratio
            #
            if stdev_pct > 0.05:
                cols_to_print = []
                for dc in data_cols:
                    df2[dc + "_r"] = df2[dc] / df2[dc + "_day"]
                    cols_to_print.append(dc)
                    cols_to_print.append(dc + "_day")
                    cols_to_print.append(dc + "_r")
                print(df2[cols_to_print])
                raise Exception("STDEV % of estimated stock-split ratio is {}%, should be near zero".format(round(stdev_pct * 100, 1)))

            if abs(1.0 - ss_ratio / expectedRatio) > 0.05:
                cols_to_print = []
                for dc in data_cols:
                    df2[dc + "_r"] = df2[dc] / df2[dc + "_day"]
                    cols_to_print.append(dc)
                    cols_to_print.append(dc + "_day")
                    cols_to_print.append(dc + "_r")
                print(df2[cols_to_print])
                raise Exception("ss_ratio={} != expected_ratio={}".format(ss_ratio, expectedRatio))
            ss_ratio = expectedRatio
            ss_ratioRcp = 1.0 / ss_ratio
            #
            price_data_cols = ["Open", "Close", "Adj Close", "Low", "High"]
            if ss_ratio > 1.01:
                for c in price_data_cols:
                    df[c] *= ss_ratioRcp
                if debug_yfc:
                    # print("Applying 1:{.2f} stock-split".format(ss_ratio))
                    print("Applying 1:{} stock-split".format(round(ss_ratio, 2)))
            elif ss_ratioRcp > 1.01:
                for c in price_data_cols:
                    df[c] *= ss_ratio
                if debug_yfc:
                    print("Applying {.2f}:1 reverse-split-split".format(ss_ratioRcp))
            # Note: volume always returned unadjusted

            # Yahoo messes up dividend adjustment too so copy correct dividend from daily,
            # but only to first time periods of each day:
            df = df.drop("Dividends", axis=1)
            df["_date"] = df.index.date
            # - get first times
            df["_time"] = df.index.time
            df_openTimes = df[["_date", "_time"]].groupby("_date", as_index=False, group_keys=False).min().rename(columns={"_time": "_open_time"})
            df = df.drop("_time", axis=1)
            # - merge
            df["_indexBackup"] = df.index
            df = pd.merge(df, df_daily_during[["Dividends"]], how="left", on="_date", validate="many_to_one")
            df = pd.merge(df, df_openTimes, how="left", on="_date")
            df.index = df["_indexBackup"] ; df.index.name = None
            # - correct dividends
            df.loc[df.index.time != df["_open_time"], "Dividends"] = 0.0
            df = df.drop("_open_time", axis=1)
            # Copy over CSF and CDF too from daily
            df = pd.merge(df, df_daily_raw_during_d[["CDF", "CSF"]], how="left", on="_date", validate="many_to_one")
            df.index = df["_indexBackup"] ; df.index.name = None ; df = df.drop(["_indexBackup", "_date"], axis=1)
            cdf = df["CDF"]
            df["Adj Close"] = df["Close"] * cdf
            csf = df["CSF"]

            if df_daily_post.shape[0] > 0:
                post_csf = yfcu.GetCSF0(df_daily_post)

        elif interval == yfcd.Interval.Week:
            df_daily = self.history(start=df.index[-1].date()+td_7d, interval=yfcd.Interval.Days1)
            if (df_daily is not None) and df_daily.shape[0] > 0:
                post_csf = yfcu.GetCSF0(df_daily)
                if debug_yfc:
                    print("- post_csf of daily date range {}->{} = {}".format(df_daily.index[0], df_daily.index[-1], post_csf))

        elif interval in [yfcd.Interval.Months1, yfcd.Interval.Months3]:
            raise Exception("not implemented")

        if debug_yfc:
            print("- post_csf =", post_csf)

        # If 'df' does not contain all stock splits until present, then
        # set 'post_csf' to cumulative stock split factor just after last 'df' date
        last_dt = df.index[-1]
        dt_now = datetime.datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
        thr = 5
        if interval == yfcd.Interval.Week:
            thr = 10  # Extend threshold for weekly data
        if (dt_now-last_dt) > datetime.timedelta(days=thr):
            if post_csf is None:
                raise Exception("Data is older than {} days, need to set 'post_csf' arg to capture all stock splits since".format(thr))

        # Cumulative dividend factor
        if cdf is None:
            f_nna = ~df["Close"].isna()
            if not f_nna.any():
                cdf = 1.0
            else:
                cdf = np.full(df.shape[0], np.nan)
                cdf[f_nna] = df.loc[f_nna, "Adj Close"] / df.loc[f_nna, "Close"]
                cdf = pd.Series(cdf).fillna(method="bfill").fillna(method="ffill").values

        # Cumulative stock-split factor
        if csf is None:
            ss = df["Stock Splits"].copy()
            ss[(ss == 0.0) | ss.isna()] = 1.0
            ss_rcp = 1.0 / ss
            csf = ss_rcp.sort_index(ascending=False).cumprod().sort_index(ascending=True).shift(-1, fill_value=1.0)
            if post_csf is not None:
                csf *= post_csf
        csf_rcp = 1.0 / csf

        # Reverse Yahoo's split adjustment:
        data_cols = ["Open", "High", "Low", "Close", "Dividends"]
        for dc in data_cols:
            df[dc] = df[dc] * csf_rcp
        if not interday:
            # Don't need to de-split volume data because Yahoo always returns interday volume unadjusted
            pass
        else:
            df["Volume"] *= csf

        # Drop 'Adj Close', replace with scaling factors:
        df = df.drop("Adj Close", axis=1)
        df["CSF"] = csf
        df["CDF"] = cdf

        if debug_yfc:
            print("- unadjusted:")
            print(df[["Close", "Dividends", "Volume", "CSF", "CDF"]])
            f = df["Dividends"] != 0.0
            if f.any():
                print("- dividends:")
                print(df.loc[f, ["Close", "Dividends", "Volume", "CSF", "CDF"]])
            print("")

        if debug_yfc:
            print("_processYahooAdjustment() returning")
            print(df[["Close", "Dividends", "Volume", "CSF"]])
        elif self._trace:
            print(" "*self._trace_depth + "_processYahooAdjustment() returning")

        return df

    def _reconstructInterval(self, df_row, interval, bad_fields):
        if isinstance(df_row, pd.DataFrame) or not isinstance(df_row, pd.Series):
            raise Exception("'df_row' must be a Pandas Series not", type(df_row))
        if not isinstance(bad_fields, (list, set, np.ndarray)):
            raise Exception("'bad_fields' must be a list/set not", type(bad_fields))

        idx = df_row.name
        start = idx.date()

        if self._trace:
            self._trace_depth += 1
            print(" "*self._trace_depth + "_reconstructInterval(interval={}, idx={}, bad_fields={})".format(interval, idx, bad_fields))

        data_cols = [c for c in ["Open", "High", "Low", "Close", "Adj Close"] if c in df_row.index]

        # If interval is weekly then can construct with daily. But if smaller intervals then
        # restricted to recent times:
        # - daily = hourly restricted to last 730 days
        sub_interval = None
        td_range = None
        if interval == yfcd.Interval.Week:
            # Correct by fetching week of daily data
            sub_interval = yfcd.Interval.Days1
            td_range = datetime.timedelta(days=7)
        elif interval == yfcd.Interval.Days1:
            # Correct by fetching day of hourly data
            sub_interval = yfcd.Interval.Hours1
            td_range = datetime.timedelta(days=1)
        else:
            print("WARNING: Have not implemented repair for '{}' interval. Contact developers".format(interval))
            return df_row

        if sub_interval == yfcd.Interval.Hours1 and (datetime.date.today()-start) > datetime.timedelta(days=729):
            # Don't bother requesting more price data, Yahoo will reject
            return None
        else:
            new_vals = {}

            if self._record_stack_trace:
                # Log function calls to detect and manage infinite recursion
                fn_tuple = ("_reconstructInterval()", "dt={}".format(idx), "interval={}".format(interval))
                if fn_tuple in self._stack_trace:
                    # Detected a potential recursion loop
                    reconstruct_detected = False
                    for i in range(len(self._stack_trace)-1, -1, -1):
                        if "_reconstructInterval" in str(self._stack_trace[i]):
                            reconstruct_detected = True
                            break
                    if reconstruct_detected:
                        self._infinite_recursion_detected = True
                self._stack_trace.append(fn_tuple)

            # Infinite loop potential here via repair:
            # - request fine-grained data e.g. 1H
            # - 1H requires accurate dividend data
            # - triggers fetch of 1D data which must be kept contiguous
            # - triggers fetch of older 1D data which requires repair using 1H data -> recursion loop
            # Solution:
            # 1) add tuple to fn stack buy with YF=True
            # 2) if that tuple already in stack then raise Exception
            if sub_interval == yfcd.Interval.Hours1:
                fetch_start = start
            else:
                fetch_start = start - td_range
            if self._infinite_recursion_detected:
                for i in range(len(self._stack_trace)):
                    print("  "*i + str(self._stack_trace[i]))
                raise Exception("WARNING: Infinite recursion detected (see stack trace above). Switch to fetching prices direct from YF")
                # print("WARNING: Infinite recursion detected (see stack trace above). Switch to fetching prices direct from YF")
                df_fine = self.dat.history(start=fetch_start, end=start+td_range, interval=yfcd.intervalToString[sub_interval], auto_adjust=False, repair=True)
            elif interval in [yfcd.Interval.Days1] or self._infinite_recursion_detected:
                # Assume infinite recursion will happen
                df_fine = self.dat.history(start=fetch_start, end=start+td_range, interval=yfcd.intervalToString[sub_interval], auto_adjust=False, repair=True)
            else:
                df_fine = self.history(start=fetch_start, end=start+td_range, interval=sub_interval, adjust_splits=True, adjust_divs=False)

            # First, check whether df_fine has different split-adjustment than df_row.
            # If it is different, then adjust df_fine to match df_row
            good_fields = list(set(data_cols) - set(bad_fields) - set("Adj Close"))
            if len(good_fields) == 0:
                raise Exception("No good fields, so cannot determine whether different split-adjustment. Contact developers")
            # median = df_row.loc[good_fields].median()
            # median_fine = np.median(df_fine[good_fields].values)
            # ratio = median/median_fine
            # Better method to calculate split-adjustment:
            df_fine_from_idx = df_fine[df_fine.index >= idx]
            ratios = []
            for f in good_fields:
                if f == "Low":
                    ratios.append(df_row[f] / df_fine_from_idx[f].min())
                elif f == "High":
                    ratios.append(df_row[f] / df_fine_from_idx[f].max())
                elif f == "Open":
                    ratios.append(df_row[f] / df_fine_from_idx[f].iloc[0])
                elif f == "Close":
                    ratios.append(df_row[f] / df_fine_from_idx[f].iloc[-1])
            ratio = np.mean(ratios)
            #
            ratio_rcp = round(1.0 / ratio, 1) ; ratio = round(ratio, 1)
            if ratio == 1 and ratio_rcp == 1:
                # Good!
                pass
            else:
                if ratio > 1:
                    # data has different split-adjustment than fine-grained data
                    # Adjust fine-grained to match
                    df_fine[data_cols] *= ratio
                elif ratio_rcp > 1:
                    # data has different split-adjustment than fine-grained data
                    # Adjust fine-grained to match
                    df_fine[data_cols] *= 1.0 / ratio_rcp

            if sub_interval != yfcd.Interval.Hours1:
                df_last_week = df_fine[df_fine.index < idx]
                df_fine = df_fine[df_fine.index >= idx]

            if "High" in bad_fields:
                new_vals["High"] = df_fine["High"].max()
            if "Low" in bad_fields:
                new_vals["Low"] = df_fine["Low"].min()
            if "Open" in bad_fields:
                if sub_interval != yfcd.Interval.Hours1 and idx != df_fine.index[0]:
                    # Exchange closed Monday. In this case, Yahoo sets Open to last week close
                    new_vals["Open"] = df_last_week["Close"][-1]
                    if "Low" in new_vals:
                        new_vals["Low"] = min(new_vals["Open"], new_vals["Low"])
                    elif new_vals["Open"] < df_row["Low"]:
                        new_vals["Low"] = new_vals["Open"]
                else:
                    new_vals["Open"] = df_fine["Open"].iloc[0]
            if "Close" in bad_fields:
                new_vals["Close"] = df_fine["Close"].iloc[-1]
                # Assume 'Adj Close' also corrupted, easier than detecting whether true
                new_vals["Adj Close"] = df_fine["Adj Close"].iloc[-1]

            if self._record_stack_trace:
                # Pop stack trace
                if len(self._stack_trace) == 0:
                    raise Exception("Failing to correctly push/pop stack trace (is empty too early)")
                if not self._stack_trace[-1] == fn_tuple:
                    for i in range(len(self._stack_trace)):
                        print("  "*i + str(self._stack_trace[i]))
                    raise Exception("Failing to correctly push/pop stack trace (see above)")
                self._stack_trace.pop(len(self._stack_trace) - 1)

        if self._trace:
            self._trace_depth += 1
            print(" "*self._trace_depth + "_reconstructInterval() returning")

        return new_vals

    def _repairUnitMixups(self, df, interval):
        # Sometimes Yahoo returns few prices in cents/pence instead of $/£
        # I.e. 100x bigger
        # Easy to detect and fix, just look for outliers = ~100x local median

        if df.shape[0] == 0:
            return df
        if df.shape[0] == 1:
            # Need multiple rows to confidently identify outliers
            return df

        if self._trace:
            self._trace_depth += 1
            print(" "*self._trace_depth + "_repairUnitMixups(interval={})".format(interval))

        df2 = df.copy()

        data_cols = ["High", "Open", "Low", "Close"]  # Order important, separate High from Low
        data_cols = [c for c in data_cols if c in df2.columns]
        median = _ndimage.median_filter(df2[data_cols].values, size=(3, 3), mode='wrap')

        if (median == 0).any():
            print("Ticker =", self.ticker)
            print("yf =", yf)
            print("df:")
            print(df)
            raise Exception("median contains zeroes, why?")
        ratio = df2[data_cols].values / median
        ratio_rounded = (ratio / 20).round() * 20  # round ratio to nearest 20
        f = (ratio_rounded) == 100

        # Store each mixup:
        mixups = {}
        for j in range(len(data_cols)):
            fj = f[:, j]
            if fj.any():
                dc = data_cols[j]
                for i in np.where(fj)[0]:
                    idx = df2.index[i]
                    if idx not in mixups:
                        mixups[idx] = {"data": df2.loc[idx, data_cols], "fields": set([dc])}
                    else:
                        mixups[idx]["fields"].add(dc)
        n_mixups = len(mixups)

        if len(mixups) > 0:
            # This first pass will correct all errors in Open/Close/AdjClose columns.
            # It will also attempt to correct Low/High columns, but only if can get price data.
            for idx in sorted(list(mixups.keys())):
                m = mixups[idx]
                new_values = self._reconstructInterval(df2.loc[idx], interval, m["fields"])
                if new_values is not None:
                    for k in new_values:
                        df2.loc[idx, k] = new_values[k]
                    del mixups[idx]

            # This second pass will *crudely* "fix" any remaining errors in High/Low
            # simply by ensuring they don't contradict e.g. Low = 100x High
            if len(mixups) > 0:
                for idx in sorted(list(mixups.keys())):
                    m = mixups[idx]
                    row = df2.loc[idx, ["Open", "Close"]]
                    if "High" in m["fields"]:
                        df2.loc[idx, "High"] = row.max()
                        m["fields"].remove("High")
                    if "Low" in m["fields"]:
                        df2.loc[idx, "Low"] = row.min()
                        m["fields"].remove("Low")

                    if len(m["fields"]) == 0:
                        del mixups[idx]

            n_fixed = n_mixups - len(mixups)
            print("{}: fixed {} currency unit mixups in {} price data".format(self.ticker, n_fixed, interval))
            if len(mixups) > 0:
                print("    ... and failed to correct {}".format(len(mixups)))

        if self._trace:
            print(" "*self._trace_depth + "_repairUnitMixups() returning")
            self._trace_depth -= 1

        return df2

    def _repairZeroPrices(self, df, interval):
        # Sometimes Yahoo returns prices=0 when obviously wrong e.g. Volume>0 and Close>0.
        # Easy to detect and fix

        if df.shape[0] == 0:
            return df
        if df.shape[0] == 1:
            # Need multiple rows to confidently identify outliers
            return df

        if self._trace:
            self._trace_depth += 1
            print(" "*self._trace_depth + "_repairZeroPrices(interval={}, date_range={}->{})".format(interval, df.index[0], df.index[-1]))

        df2 = df.copy()

        data_cols = ["Open", "High", "Low", "Close"]
        data_cols = [c for c in data_cols if c in df2.columns]
        f_zeroes = (df2[data_cols] == 0.0).values.any(axis=1)

        n_fixed = 0
        for i in np.where(f_zeroes)[0]:
            idx = df2.index[i]
            df_row = df2.loc[idx]
            bad_fields = df2.columns[df_row.values == 0.0].values
            new_values = self._reconstructInterval(df2.loc[idx], interval, bad_fields)
            if new_values is not None:
                for k in new_values:
                    df2.loc[idx, k] = new_values[k]
                n_fixed += 1

        if n_fixed > 0:
            print("{}: fixed {} price=0.0 errors in {} price data".format(self.ticker, n_fixed, interval))

        if self._trace:
            print(" "*self._trace_depth + "_repairZeroPrices() returning")
            self._trace_depth -= 1

        return df2

    def _process_user_dt(self, dt):
        d = None
        tz_exchange = ZoneInfo(self.info["exchangeTimezoneName"])
        if isinstance(dt, str):
            d = datetime.datetime.strptime(dt, "%Y-%m-%d").date()
            dt = datetime.datetime.combine(d, datetime.time(0), tz_exchange)
        elif isinstance(dt, datetime.date) and not isinstance(dt, datetime.datetime):
            d = dt
            dt = datetime.datetime.combine(dt, datetime.time(0), tz_exchange)
        elif not isinstance(dt, datetime.datetime):
            raise Exception("Argument 'dt' must be str, date or datetime")
        dt = dt.replace(tzinfo=tz_exchange) if dt.tzinfo is None else dt.astimezone(tz_exchange)

        return dt, d

    @property
    def info(self):
        if self._info is not None:
            return self._info

        if yfcm.IsDatumCached(self.ticker, "info"):
            self._info = yfcm.ReadCacheDatum(self.ticker, "info")
            return self._info

        self._info = self.dat.info
        yfcm.StoreCacheDatum(self.ticker, "info", self._info)

        yfct.SetExchangeTzName(self._info["exchange"], self._info["exchangeTimezoneName"])

        return self._info

    @property
    def splits(self):
        if self._splits is not None:
            return self._splits

        if yfcm.IsDatumCached(self.ticker, "splits"):
            self._splits = yfcm.ReadCacheDatum(self.ticker, "splits")
            return self._splits

        self._splits = self.dat.splits
        yfcm.StoreCacheDatum(self.ticker, "splits", self._splits)
        return self._splits

    @property
    def financials(self):
        if self._financials is not None:
            return self._financials

        if yfcm.IsDatumCached(self.ticker, "financials"):
            self._financials = yfcm.ReadCacheDatum(self.ticker, "financials")
            return self._financials

        self._financials = self.dat.financials
        yfcm.StoreCacheDatum(self.ticker, "financials", self._financials)
        return self._financials

    @property
    def quarterly_financials(self):
        if self._quarterly_financials is not None:
            return self._quarterly_financials

        if yfcm.IsDatumCached(self.ticker, "quarterly_financials"):
            self._quarterly_financials = yfcm.ReadCacheDatum(self.ticker, "quarterly_financials")
            return self._quarterly_financials

        self._quarterly_financials = self.dat.quarterly_financials
        yfcm.StoreCacheDatum(self.ticker, "quarterly_financials", self._quarterly_financials)
        return self._quarterly_financials

    @property
    def major_holders(self):
        if self._major_holders is not None:
            return self._major_holders

        if yfcm.IsDatumCached(self.ticker, "major_holders"):
            self._major_holders = yfcm.ReadCacheDatum(self.ticker, "major_holders")
            return self._major_holders

        self._major_holders = self.dat.major_holders
        yfcm.StoreCacheDatum(self.ticker, "major_holders", self._major_holders)
        return self._major_holders

    @property
    def institutional_holders(self):
        if self._institutional_holders is not None:
            return self._institutional_holders

        if yfcm.IsDatumCached(self.ticker, "institutional_holders"):
            self._institutional_holders = yfcm.ReadCacheDatum(self.ticker, "institutional_holders")
            return self._institutional_holders

        self._institutional_holders = self.dat.institutional_holders
        yfcm.StoreCacheDatum(self.ticker, "institutional_holders", self._institutional_holders)
        return self._institutional_holders

    @property
    def balance_sheet(self):
        if self._balance_sheet is not None:
            return self._balance_sheet

        if yfcm.IsDatumCached(self.ticker, "balance_sheet"):
            self._balance_sheet = yfcm.ReadCacheDatum(self.ticker, "balance_sheet")
            return self._balance_sheet

        self._balance_sheet = self.dat.balance_sheet
        yfcm.StoreCacheDatum(self.ticker, "balance_sheet", self._balance_sheet)
        return self._balance_sheet

    @property
    def quarterly_balance_sheet(self):
        if self._quarterly_balance_sheet is not None:
            return self._quarterly_balance_sheet

        if yfcm.IsDatumCached(self.ticker, "quarterly_balance_sheet"):
            self._quarterly_balance_sheet = yfcm.ReadCacheDatum(self.ticker, "quarterly_balance_sheet")
            return self._quarterly_balance_sheet

        self._quarterly_balance_sheet = self.dat.quarterly_balance_sheet
        yfcm.StoreCacheDatum(self.ticker, "quarterly_balance_sheet", self._quarterly_balance_sheet)
        return self._quarterly_balance_sheet

    @property
    def cashflow(self):
        if self._cashflow is not None:
            return self._cashflow

        if yfcm.IsDatumCached(self.ticker, "cashflow"):
            self._cashflow = yfcm.ReadCacheDatum(self.ticker, "cashflow")
            return self._cashflow

        self._cashflow = self.dat.cashflow
        yfcm.StoreCacheDatum(self.ticker, "cashflow", self._cashflow)
        return self._cashflow

    @property
    def quarterly_cashflow(self):
        if self._quarterly_cashflow is not None:
            return self._quarterly_cashflow

        if yfcm.IsDatumCached(self.ticker, "quarterly_cashflow"):
            self._quarterly_cashflow = yfcm.ReadCacheDatum(self.ticker, "quarterly_cashflow")
            return self._quarterly_cashflow

        self._quarterly_cashflow = self.dat.quarterly_cashflow
        yfcm.StoreCacheDatum(self.ticker, "quarterly_cashflow", self._quarterly_cashflow)
        return self._quarterly_cashflow

    @property
    def earnings(self):
        if self._earnings is not None:
            return self._earnings

        if yfcm.IsDatumCached(self.ticker, "earnings"):
            self._earnings = yfcm.ReadCacheDatum(self.ticker, "earnings")
            return self._earnings

        self._earnings = self.dat.earnings
        yfcm.StoreCacheDatum(self.ticker, "earnings", self._earnings)
        return self._earnings

    @property
    def quarterly_earnings(self):
        if self._quarterly_earnings is not None:
            return self._quarterly_earnings

        if yfcm.IsDatumCached(self.ticker, "quarterly_earnings"):
            self._quarterly_earnings = yfcm.ReadCacheDatum(self.ticker, "quarterly_earnings")
            return self._quarterly_earnings

        self._quarterly_earnings = self.dat.quarterly_earnings
        yfcm.StoreCacheDatum(self.ticker, "quarterly_earnings", self._quarterly_earnings)
        return self._quarterly_earnings

    @property
    def sustainability(self):
        if self._sustainability is not None:
            return self._sustainability

        if yfcm.IsDatumCached(self.ticker, "sustainability"):
            self._sustainability = yfcm.ReadCacheDatum(self.ticker, "sustainability")
            return self._sustainability

        self._sustainability = self.dat.sustainability
        yfcm.StoreCacheDatum(self.ticker, "sustainability", self._sustainability)
        return self._sustainability

    @property
    def recommendations(self):
        if self._recommendations is not None:
            return self._recommendations

        if yfcm.IsDatumCached(self.ticker, "recommendations"):
            self._recommendations = yfcm.ReadCacheDatum(self.ticker, "recommendations")
            return self._recommendations

        self._recommendations = self.dat.recommendations
        yfcm.StoreCacheDatum(self.ticker, "recommendations", self._recommendations)
        return self._recommendations

    @property
    def calendar(self):
        if self._calendar is not None:
            return self._calendar

        if yfcm.IsDatumCached(self.ticker, "calendar"):
            self._calendar = yfcm.ReadCacheDatum(self.ticker, "calendar")
            return self._calendar

        self._calendar = self.dat.calendar
        yfcm.StoreCacheDatum(self.ticker, "calendar", self._calendar)
        return self._calendar

    @property
    def inin(self):
        if self._inin is not None:
            return self._inin

        if yfcm.IsDatumCached(self.ticker, "inin"):
            self._inin = yfcm.ReadCacheDatum(self.ticker, "inin")
            return self._inin

        self._inin = self.dat.inin
        yfcm.StoreCacheDatum(self.ticker, "inin", self._inin)
        return self._inin

    @property
    def options(self):
        if self._options is not None:
            return self._options

        if yfcm.IsDatumCached(self.ticker, "options"):
            self._options = yfcm.ReadCacheDatum(self.ticker, "options")
            return self._options

        self._options = self.dat.options
        yfcm.StoreCacheDatum(self.ticker, "options", self._options)
        return self._options

    @property
    def news(self):
        if self._news is not None:
            return self._news

        if yfcm.IsDatumCached(self.ticker, "news"):
            self._news = yfcm.ReadCacheDatum(self.ticker, "news")
            return self._news

        self._news = self.dat.news
        yfcm.StoreCacheDatum(self.ticker, "news", self._news)
        return self._news

    @property
    def yf_lag(self):
        if self._yf_lag is not None:
            return self._yf_lag

        exchange_str = "exchange-{0}".format(self.info["exchange"])
        if yfcm.IsDatumCached(exchange_str, "yf_lag"):
            self._yf_lag = yfcm.ReadCacheDatum(exchange_str, "yf_lag")
            if self._yf_lag:
                return self._yf_lag

        # # Have to calculate lag from YF data.
        # # To avoid circular logic will call YF directly, not use my cache. Because cache requires knowing lag.
        # dt_now = datetime.datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
        # if not yfct.IsTimestampInActiveSession(self.info["exchange"], dt_now):
        #   # Exchange closed so used hardcoded delay, ...
        #   self._yf_lag = yfcd.exchangeToYfLag[self.info["exchange"]]

        #   # ... but only until next session starts +1H:
        #   s = yfct.GetTimestampNextSession(self.info["exchange"], dt_now)
        #   expiry = s["open"] + datetime.timedelta(hours=1)

        #   yfcm.StoreCacheDatum(exchange_str, "yf_lag", self._yf_lag, expiry=expiry)
        #   return self._yf_lag

        # # Calculate actual delay from live market data, and cache with expiry in 4 weeks

        # specified_lag = yfcd.exchangeToYfLag[self.info["exchange"]]

        # # Because some stocks go days without any volume, need to
        # # be sure today has volume
        # start_d = dt_now.date()-datetime.timedelta(days=7)
        # end_d = dt_now.date()+datetime.timedelta(days=1)
        # df_1d = self.dat.history(interval="1d", start=start_d, end=end_d)
        # start_d = df_1d.index[-1].date()
        # if start_d != dt_now.date():
        #   self._yf_lag = specified_lag
        #   return self._yf_lag

        # # Get last hour of 5m price data:
        # start_dt = dt_now-datetime.timedelta(hours=1)
        # try:
        #   df_5mins = self.dat.history(interval="5m", start=start_dt, end=dt_now, raise_errors=True)
        #   df_5mins = df_5mins[df_5mins["Volume"]>0]
        # except:
        #   df_5mins = None
        # if (df_5mins is None) or (df_5mins.shape[0] == 0):
        #   # raise Exception("Failed to fetch 5m data for tkr={}, start={}".format(self.ticker, start_dt))
        #   # print("WARNING: Failed to fetch 5m data for tkr={} so setting yf_lag to hardcoded default".format(self.ticker, start_dt))
        #   self._yf_lag = specified_lag
        #   return self._yf_lag
        # df_5mins_lastDt = df_5mins.index[df_5mins.shape[0]-1].to_pydatetime()
        # df_5mins_lastDt = df_5mins_lastDt.astimezone(ZoneInfo("UTC"))

        # # Now 15 minutes of 1m price data around the last 5m candle:
        # dt2_start = df_5mins_lastDt - datetime.timedelta(minutes=10)
        # dt2_end = df_5mins_lastDt + datetime.timedelta(minutes=5)
        # df_1mins = self.dat.history(interval="1m", start=dt2_start, end=dt2_end, raise_errors=True)
        # dt_now = datetime.datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
        # df_1mins_lastDt = df_1mins.index[df_1mins.shape[0]-1].to_pydatetime()

        # lag = dt_now - df_1mins_lastDt
        # # Update: ignore all large lags
        # # if lag > datetime.timedelta(minutes=40):
        # #     raise Exception("{}: calculated YF lag as {}, seems excessive".format(self.ticker, lag))
        # if lag < datetime.timedelta(seconds=0):
        #   print("dt_now = {} (tz={})".format(dt_now, dt_now.tzinfo))
        #   print("df_1mins:")
        #   print(df_1mins)
        #   raise Exception("{}: calculated YF lag as {}, seems negative".format(self.ticker, lag))
        # expiry_td = datetime.timedelta(days=28)
        # if (lag > (2*specified_lag)) and (lag-specified_lag)>datetime.timedelta(minutes=2):
        #   if df_1mins["Volume"][df_1mins.shape[0]-1] == 0:
        #       # Ticker has low volume, ignore larger-than-expected lag. Just reduce the expiry, in case tomorrow has more volume
        #       expiry_td = datetime.timedelta(days=1)
        #   else:
        #       #print("df_5mins:")
        #       #print(df_5mins)
        #       #raise Exception("{}: calculated YF lag as {}, greatly exceeds the specified lag {}".format(self.ticker, lag, specified_lag))
        #       self._yf_lag = specified_lag
        #       return self._yf_lag
        # self._yf_lag = lag
        # yfcm.StoreCacheDatum(exchange_str, "yf_lag", self._yf_lag, expiry=dt_now+expiry_td)
        # return self._yf_lag

        # Just use specified lag
        specified_lag = yfcd.exchangeToYfLag[self.info["exchange"]]
        self._yf_lag = specified_lag
        return self._yf_lag
