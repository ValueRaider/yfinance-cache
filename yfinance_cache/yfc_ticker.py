import yfinance as yf

from . import yfc_cache_manager as yfcm
from . import yfc_dat as yfcd
from . import yfc_utils as yfcu
from . import yfc_time as yfct
from . import yfc_prices_manager as yfcp

import numpy as np
import datetime
from zoneinfo import ZoneInfo
import os
# from time import perf_counter

# TODO: Ticker: add method to delete ticker from cache

class Ticker:
    def __init__(self, ticker, session=None):
        self.ticker = ticker.upper()

        self.session = session
        self.dat = yf.Ticker(self.ticker, session=self.session)

        self._yf_lag = None

        self._histories_manager = None

        self._info = None
        self._fast_info = None

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

    def history(self,
                interval="1d",
                max_age=None,  # defaults to half of interval
                period=None,
                start=None, end=None, prepost=False, actions=True,
                adjust_splits=True, adjust_divs=True,
                keepna=False,
                proxy=None, rounding=False,
                debug=True, quiet=False):

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
        exchange = self.fast_info['exchange']
        tz_name = self.fast_info["timezone"]
        tz_exchange = ZoneInfo(self.fast_info["timezone"])
        yfct.SetExchangeTzName(exchange, self.fast_info["timezone"])
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

        if start is not None and end is not None and start >= end:
            raise ValueError("start must be < end")

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

        if debug_yfc:
            print("- start={} , end={}".format(start, end))

        if (start is not None) and start == end:
            return None

        if start is not None:
            try:
                sched_7d = yfct.GetExchangeSchedule(exchange, start.date(), start.date()+7*td_1d)
            except Exception as e:
                if "Need to add mapping" in str(e):
                    raise Exception("Need to add mapping of exchange {} to xcal (ticker={})".format(self.fast_info["exchange"], self.ticker))
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

        if self._histories_manager is None:
            self._histories_manager = yfcp.HistoriesManager(self.ticker, exchange, tz_name, self.session, proxy)

        # t1_setup = perf_counter()

        hist = self._histories_manager.GetHistory(interval)
        interday = interval in [yfcd.Interval.Days1, yfcd.Interval.Week, yfcd.Interval.Months1, yfcd.Interval.Months3]
        if period is not None:
            h = hist.get(start=None, end=None, period=period, max_age=max_age, quiet=quiet)
        elif interday:
            h = hist.get(start_d, end_d, period=None, max_age=max_age, quiet=quiet)
        else:
            h = hist.get(start, end, period=None, max_age=max_age, quiet=quiet)
        if (h is None) or h.shape[0] == 0:
            raise Exception("history() is exiting without price data")

        # t2_sync = perf_counter()

        f_dups = h.index.duplicated()
        if f_dups.any():
            raise Exception("{}: These timepoints have been duplicated: {}".format(self.ticker, h.index[f_dups]))

        # Present table for user:
        h_copied = False
        if (start is not None) and (end is not None):
            h = h.loc[start:end-datetime.timedelta(milliseconds=1)].copy()
            h_copied = True

        if not keepna:
            price_data_cols = [c for c in yfcd.yf_data_cols if c in h.columns]
            mask_nan_or_zero = (np.isnan(h[price_data_cols].to_numpy()) | (h[price_data_cols].to_numpy() == 0)).all(axis=1)
            if mask_nan_or_zero.any():
                h = h.drop(h.index[mask_nan_or_zero])
                h_copied = True
        # t3_filter = perf_counter()

        if h.shape[0] == 0:
            h = None
        else:
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
        # print("TIME %:        setup={:.1f}%  sync={:.1f}%  filter={:.1f}%  adjust={:.1f}%".format(t_setup, t_sync, t_filter, t_adjust))

        return h

    def _getCachedPrices(self, interval, proxy=None):
        if self._histories_manager is None:
            exchange = self.fast_info['exchange']
            tz_name = self.fast_info["timezone"]
            self._histories_manager = yfcp.HistoriesManager(self.ticker, exchange, tz_name, self.session, proxy)

        return self._histories_manager.GetHistory(interval)._getCachedPrices()


    def verify_cached_prices(self, correct=False, discard_old=False, quiet=True, debug=False, debug_interval=None):
        interval = yfcd.Interval.Days1
        cache_key = "history-"+yfcd.intervalToString[interval]
        if not yfcm.IsDatumCached(self.ticker, cache_key):
            return True

        log_msg = f"YFC: _verify_cached_prices(tkr={self.ticker})"
        if self._trace:
            self._trace_depth += 1
            print(" "*self._trace_depth + log_msg)

        if self._histories_manager is None:
            exchange = self.fast_info['exchange']
            tz_name = self.fast_info["timezone"]
            self._histories_manager = yfcp.HistoriesManager(self.ticker, exchange, tz_name, self.session, proxy=None)

        v = True

        # First verify 1d
        # print(f"- {yfcd.intervalToString[interval]}")
        dt0 = self._histories_manager.GetHistory(interval)._getCachedPrices().index[0]
        v = self._verify_cached_prices_interval(interval, correct, discard_old, quiet, debug)
        if correct:
            # self.history(start=dt0.date())
            self.history(start=dt0.date(), quiet=quiet)
        #
        # repeat verification, because 'fetch backporting' may be buggy
        v = self._verify_cached_prices_interval(interval, correct, discard_old, quiet, debug)
        if not v and correct:
            # Rows were removed so re-fetch. Only do for 1d data
            # self.history(start=dt0.date())
            self.history(start=dt0.date(), quiet=quiet)
        #
        # verification should now pass, no exception
        v = self._verify_cached_prices_interval(interval, correct, discard_old, quiet, debug)
        if not v:
            raise Exception(f"{self.ticker}: 1d failing to fetch & verify")

        if debug_interval is not None:
            intervals = [debug_interval]
        else:
            intervals = yfcd.Interval
        for interval in intervals:
            if interval == yfcd.Interval.Days1:
                continue
            istr = yfcd.intervalToString[interval]
            cache_key = "history-"+istr
            if not yfcm.IsDatumCached(self.ticker, cache_key):
                continue
            # print(f"- {istr}")
            vi = self._verify_cached_prices_interval(interval, correct, discard_old, quiet, debug)

            # Try a fetch and ensure re-verify. Ideally wouldn't do this, but it is finding bugs.
            try:
                if istr.endswith('m'):
                    # self.history(interval=interval, period="1wk")
                    self.history(interval=interval, period="1wk", quiet=quiet)
                else:
                    # self.history(interval=interval, period="1mo")
                    self.history(interval=interval, period="1mo", quiet=quiet)
                vi = self._verify_cached_prices_interval(interval, correct, discard_old, quiet, debug)
            except yfcd.NoPriceDataInRangeException as e:
                pass
            
            v = v and vi

        if self._trace:
            print(" "*self._trace_depth + f"YFC: _verify_cached_prices() returning {v}")
            self._trace_depth -= 1

        return v

    def _verify_cached_prices_interval(self, interval, correct=False, discard_old=False, quiet=True, debug=False):
        # TODO: iterate over all intervals, but only once I'm sure verify is 100% solid. 
        #       - I remember needing 1d verified first => re-fetch after correction before verifying other intervals?
        if isinstance(interval, str):
            if interval not in yfcd.intervalStrToEnum.keys():
                raise Exception("'interval' if str must be one of: {}".format(yfcd.intervalStrToEnum.keys()))
            interval = yfcd.intervalStrToEnum[interval]

        istr = yfcd.intervalToString[interval]
        cache_key = "history-"+istr
        if not yfcm.IsDatumCached(self.ticker, cache_key):
            return True

        log_msg = f"YFC: _verify_cached_prices_interval(tkr={self.ticker}, interval={istr})"
        if self._trace:
            self._trace_depth += 1
            print(" "*self._trace_depth + log_msg)

        if self._histories_manager is None:
            exchange = self.fast_info['exchange']
            tz_name = self.fast_info["timezone"]
            self._histories_manager = yfcp.HistoriesManager(self.ticker, exchange, tz_name, self.session, proxy=None)

        v = self._histories_manager.GetHistory(interval)._verifyCachedPrices(correct, discard_old, quiet, debug)

        if self._trace:
            print(" "*self._trace_depth + f"YFC: _verify_cached_prices_interval() returning {v}")
            self._trace_depth -= 1

        return v

    def _process_user_dt(self, dt):
        d = None
        tz_exchange = ZoneInfo(self.fast_info["timezone"])
        if isinstance(dt, str):
            d = datetime.datetime.strptime(dt, "%Y-%m-%d").date()
            dt = datetime.datetime.combine(d, datetime.time(0), tz_exchange)
        elif isinstance(dt, datetime.date) and not isinstance(dt, datetime.datetime):
            d = dt
            dt = datetime.datetime.combine(dt, datetime.time(0), tz_exchange)
        elif not isinstance(dt, datetime.datetime):
            raise Exception("Argument 'dt' must be str, date or datetime")
        dt = dt.replace(tzinfo=tz_exchange) if dt.tzinfo is None else dt.astimezone(tz_exchange)

        if d is None and dt.time() == datetime.time(0):
            d = dt.date()

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
    def fast_info(self):
        if self._fast_info is not None:
            return self._fast_info

        if yfcm.IsDatumCached(self.ticker, "fast_info"):
            try:
                self._fast_info = yfcm.ReadCacheDatum(self.ticker, "fast_info")
            except:
                pass
            else:
                return self._fast_info

        # self._fast_info = self.dat.fast_info
        self._fast_info = {k:self.dat.fast_info[k] for k in self.dat.fast_info.keys()}
        yfcm.StoreCacheDatum(self.ticker, "fast_info", self._fast_info)

        yfct.SetExchangeTzName(self._fast_info["exchange"], self._fast_info["timezone"])

        return self._fast_info

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

        exchange_str = "exchange-{0}".format(self.fast_info["exchange"])
        if yfcm.IsDatumCached(exchange_str, "yf_lag"):
            self._yf_lag = yfcm.ReadCacheDatum(exchange_str, "yf_lag")
            if self._yf_lag:
                return self._yf_lag

        # Just use specified lag
        specified_lag = yfcd.exchangeToYfLag[self.fast_info["exchange"]]
        self._yf_lag = specified_lag
        return self._yf_lag


def verify_cached_tickers_prices(session=None, resume_from_tkr=None, debug_tkr=None, debug_interval=None):
    """
    :Parameters:
        session:
            Recommend providing a 'requests_cache' session, in case
            you have to abort and resume verification (likely).
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
    tkrs = [x for x in os.listdir(d) if not x.startswith("exchange-") and not '_' in x]
    # tkrs = tkrs[:5]
    # tkrs = tkrs[:20]
    # tkrs = tkrs[tkrs.index("DDOG"):]

    if debug_tkr is not None:
        tkrs = [debug_tkr]
    else:
        tkrs = sorted(tkrs)
        if resume_from_tkr is not None:
            tkrs = np.array(tkrs)
            tkrs = tkrs[np.searchsorted(tkrs, resume_from_tkr, side="left"):]

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

        # dat = Ticker(tkr)
        dat = Ticker(tkr, session=session)

        # if debug_tkr is not None:
        #     v = dat._verify_cached_prices(correct=True, discard_old=True, quiet=False, debug=True, debug_interval=debug_interval)
        #     quit()

        # print(">>> FIRST PASS")
        try:
            v = dat.verify_cached_prices(correct=True, discard_old=True, quiet=True, debug_interval=debug_interval)
        except yfcd.NoPriceDataInRangeException as e:
            print(str(e) + f" - is it delisted? Aborting verification so you can investigate.")
            return
        except Exception as e:
            print("FIRST PASS FAILED: " + str(e))
            print("re-running with debug=True")
            v = dat.verify_cached_prices(correct=True, discard_old=True, quiet=False, debug=True, debug_interval=debug_interval)
            quit()
        # sleep(0.5)
        # sleep(1)
        # Second pass is important, because some cached data may have been div-adjusted without 
        # record of that dividend. Well the first verify will re-apply that dividend, so 
        # potential for new mismatches to correct.
        v = dat.verify_cached_prices(correct=True, discard_old=False, quiet=True, debug_interval=debug_interval)
        # sleep(0.5)
        # sleep(1)
        # v = dat.verify_cached_prices(correct=True, discard_old=False, quiet=False, debug=True, debug_interval=debug_interval)
        if not v:
            v = dat.verify_cached_prices(correct=False, discard_old=False, quiet=False, debug=True, debug_interval=debug_interval)
            raise Exception(f"{tkr}: verify failing")


