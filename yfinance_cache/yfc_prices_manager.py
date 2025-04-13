import yfinance as yf

from . import yfc_cache_manager as yfcm
from . import yfc_dat as yfcd
from . import yfc_time as yfct
from . import yfc_utils as yfcu
from . import yfc_logging as yfcl

import numpy as np
import pandas as pd
import pytz
# import itertools
from scipy import ndimage as _ndimage
from datetime import datetime, date, time, timedelta
import dateutil
from zoneinfo import ZoneInfo
from pprint import pprint
import logging


# TODOs:
# - when filling a missing interval with NaNs, try to reconstruct first


class HistoriesManager:
    # Intended as single to class to ensure:
    # - only one History() object exists for each timescale/data type
    # - different History() objects and communicate

    def __init__(self, ticker, exchange, tzName, listingDay, session, proxy):
        self.logger = None

        yfcu.TypeCheckStr(ticker, "ticker")
        yfcu.TypeCheckStr(exchange, "exchange")
        yfcu.TypeCheckStr(tzName, "tzName")
        if listingDay is not None:
            yfcu.TypeCheckDateStrict(listingDay, 'listingDay')

        self.ticker = ticker
        self.exchange = exchange
        self.tzName = tzName
        self.listingDay = listingDay
        self.histories = {}
        self.session = session
        self.proxy = proxy

    def __del__(self):
        if self.logger is not None:
            # Fix OS error "Too many open files"
            self.logger.handlers[0].close()

    def GetHistory(self, key):
        permitted_keys = set(yfcd.intervalToString.keys()) | {"Events"}
        if key not in permitted_keys:
            raise ValueError(f"key='{key}' is invalid, must be one of: {permitted_keys}")

        if key not in self.histories:
            if key in yfcd.intervalToString.keys():
                if key == yfcd.Interval.Days1:
                    self.histories[key] = PriceHistory(self, self.ticker, self.exchange, self.tzName, self.listingDay, key, self.session, self.proxy, repair=True, contiguous=True)
                else:
                    self.histories[key] = PriceHistory(self, self.ticker, self.exchange, self.tzName, self.listingDay, key, self.session, self.proxy, repair=True, contiguous=False)
            elif key == "Events":
                self.histories[key] = EventsHistory(self, self.ticker, self.exchange, self.tzName, self.proxy)
            else:
                raise Exception(f"Not implemented code path for key='{key}'")

        return self.histories[key]

    def LogEvent(self, level, group, msg):
        if not yfcl.IsLoggingEnabled():
            if yfcl.IsTracingEnabled():
                yfcl.TracePrint(msg)
            return

        if not isinstance(level, str) or level not in ["debug", "info"]:
            raise Exception("'level' must be str 'debug' or 'info'")

        if self.logger is None:
            self.logger = yfcl.GetLogger(self.ticker)

        full_msg = f"{group}: {msg}"
        if level == "debug":
            self.logger.debug(full_msg)
        else:
            self.logger.info(full_msg)


class EventsHistory:
    def __init__(self, manager, ticker, exchange, tzName, proxy):
        if not isinstance(manager, HistoriesManager):
            raise TypeError(f"'manager' must be HistoriesManager not {type(manager)}")
        yfcu.TypeCheckStr(ticker, "ticker")
        yfcu.TypeCheckStr(exchange, "exchange")
        yfcu.TypeCheckStr(tzName, "tzName")

        self.manager = manager
        self.ticker = ticker
        self.exchange = exchange
        self.tzName = tzName
        self.proxy = proxy

        self.tz = ZoneInfo(self.tzName)

        if yfcm.IsDatumCached(self.ticker, "dividends"):
            self.divs = yfcm.ReadCacheDatum(self.ticker, "dividends").sort_index()
        else:
            self.divs = None

        if yfcm.IsDatumCached(self.ticker, "splits"):
            self.splits = yfcm.ReadCacheDatum(self.ticker, "splits").sort_index()
        else:
            self.splits = None

    def GetDivs(self, start=None, end=None):
        if start is not None:
            yfcu.TypeCheckDateStrict(start, "start")
        if end is not None:
            yfcu.TypeCheckDateStrict(end, "end")

        if self.divs is None or self.divs.empty:
            return None

        if start is None and end is None:
            return self.divs.copy()

        tz = self.divs.index[0].tz
        if start is not None:
            start = pd.Timestamp(start).tz_localize(tz)
        if end is not None:
            end = pd.Timestamp(end).tz_localize(tz)

        td_1d = timedelta(days=1)
        if end is None:
            slc = self.divs.loc[start:]
        elif start is None:
            slc = self.divs.loc[:end-td_1d]
        else:
            slc = self.divs.loc[start:end-td_1d]
        if slc.empty:
            return None
        else:
            return slc.copy()

    def GetDivsFetchedSince(self, dt):
        yfcu.TypeCheckDatetime(dt, "dt")

        if self.divs is None or self.divs.empty:
            result = None
        else:
            f = self.divs["FetchDate"] > dt
            if f.any():
                result = self.divs[f].copy()
            else:
                result = None

        return result

    def GetSplits(self, start, end=None):
        yfcu.TypeCheckDateStrict(start, "start")
        if end is not None:
            yfcu.TypeCheckDateStrict(end, "end")

        result = None
        if self.splits is not None and not self.splits.empty:
            start = pd.Timestamp(start).tz_localize(self.splits.index[0].tz)
            if end is not None:
                end = pd.Timestamp(end).tz_localize(self.splits.index[0].tz)
            td_1d = timedelta(days=1)
            if end is None:
                slc = self.splits.loc[start:]
            else:
                slc = self.splits.loc[start:end-td_1d]

            if slc.empty:
                result = None
            else:
                result = slc.copy()

        return result

    def GetSplitsFetchedSince(self, dt):
        yfcu.TypeCheckDatetime(dt, "dt")

        if self.splits is None or self.splits.empty:
            result = None
        else:
            f = self.splits["FetchDate"] > dt
            if f.any():
                result = self.splits[f].copy()
            else:
                result = None

        return result

    def UpdateSplits(self, splits_df):
        n = splits_df.shape[0]
        yfcl.TraceEnter(f"PM: UpdateSplits({splits_df.index.date})") if n <= 2 else yfcl.TraceEnter(f"PM: UpdateSplits(n={n})")

        self_splits_modified = False

        debug = False
        # debug = True

        yfcu.TypeCheckDataFrame(splits_df, "splits_df")
        splits_df = splits_df.copy()
        if not splits_df.empty:
            expected_cols = ["Stock Splits", "FetchDate"]
            for c in expected_cols:
                if c not in splits_df.columns:
                    raise ValueError("UpdateSplits() 'splits_df' columns must contain: '{expected_cols}'")

            # Prepare 'splits_df' for append
            splits_df["Superseded split"] = 0.0
            splits_df["Superseded split FetchDate"] = pd.NaT
            if splits_df['Superseded split FetchDate'].dt.tz is None and self.splits is not None:
                splits_df['Superseded split FetchDate'] = splits_df['Superseded split FetchDate'].dt.tz_localize(self.splits['FetchDate'].dt.tz)
            for dt in splits_df.index:
                new_split = splits_df.loc[dt, "Stock Splits"]
                if self.splits is not None and dt in self.splits.index:
                    cached_split = self.splits.loc[dt, "Stock Splits"]
                    if debug:
                        yfcl.TracePrint(f"pre-existing stock-split @ {dt}: {cached_split} vs {new_split}")
                    diff_pct = 100*abs(cached_split-new_split)/cached_split
                    if diff_pct < 0.01:
                        # tiny difference, easier to just keep old value
                        splits_df = splits_df.drop(dt)
                        if debug:
                            yfcl.TracePrint("ignoring")
                    else:
                        splits_df.loc[dt, "Superseded split"] = self.splits.loc[dt, "Stock Splits"]
                        splits_df.loc[dt, "Superseded split FetchDate"] = self.splits.loc[dt, "FetchDate"]
                        self.splits = self.splits.drop(dt)
                        self_splits_modified = True
                        if debug:
                            yfcl.TracePrint("supersede")

            cols = ["Stock Splits", "FetchDate", "Superseded split", "Superseded split FetchDate"]
            if not splits_df.empty:
                splits_pretty = splits_df["Stock Splits"]
                splits_pretty.index = splits_pretty.index.date.astype(str)
                log_msg = f"{splits_pretty.shape[0]} new splits: {splits_pretty.to_dict()}"
                self.manager.LogEvent("info", "SplitManager", log_msg)
                if debug:
                    print(log_msg)

                if self.splits is None:
                    self.splits = splits_df[cols].copy()
                else:
                    f_na = self.splits['Superseded split FetchDate'].isna()
                    if f_na.all():
                        # Drop column. It breaks concat, and anyway 'divs_df' will restore it.
                        self.splits = self.splits.drop('Superseded split FetchDate', axis=1)
                    self.splits = pd.concat([self.splits, splits_df[cols]], sort=True).sort_index()
                yfcm.StoreCacheDatum(self.ticker, "splits", self.splits)
            elif self_splits_modified:
                yfcm.StoreCacheDatum(self.ticker, "splits", self.splits)

        yfcl.TraceExit("UpdateSplits() returning")

    def UpdateDividends(self, divs_df):
        debug = False
        # debug = True

        n = divs_df.shape[0]
        yfcl.TraceEnter(f"PM: UpdateDividends({divs_df.index.date})") if n <= 2 else yfcl.TraceEnter(f"PM: UpdateDividends(n={n})")

        self_divs_modified = False

        yfcu.TypeCheckDataFrame(divs_df, "divs_df")
        divs_df = divs_df.copy()
        if not divs_df.empty:
            expected_cols = ["Dividends", "FetchDate", "Close before"]
            expected_cols.append('Close repaired?')
            for c in expected_cols:
                if c not in divs_df.columns:
                    print(divs_df)
                    raise ValueError(f"{self.ticker}: AddDividends() 'divs_df' is missing column: '{c}'")

            # Prepare 'divs_df' for append
            divs_df["Back Adj."] = np.nan
            divs_df["Superseded div"] = 0.0
            divs_df["Superseded back adj."] = 0.0
            divs_df["Superseded div FetchDate"] = pd.NaT
            if divs_df['Superseded div FetchDate'].dt.tz is None and self.divs is not None:
                divs_df['Superseded div FetchDate'] = divs_df['Superseded div FetchDate'].dt.tz_localize(self.divs['FetchDate'].dt.tz)
            divs_df_dts = divs_df.index.copy()
            for dt in divs_df_dts:
                new_div = divs_df.loc[dt, "Dividends"]

                close_before = divs_df.loc[dt, "Close before"]
                # adj = (close_before - new_div) / close_before
                adj = 1.0 - new_div / close_before
                # # F = P2/(P2+D)
                # # http://marubozu.blogspot.com/2006/09/how-yahoo-calculates-adjusted-closing.html#c8038064975185708856
                # close_today = divs_df.loc[dt, "Close today"]
                # adj = close_today / (close_today + new_div)
                try:
                    if np.isnan(adj):  # todo: remove once confirm YFC bug-free
                        print(f"- divs_df.loc[{dt}]:")
                        print(divs_df.loc[dt])
                        raise Exception(f"Back Adj. is NaN (new_div={new_div} close_before={close_before})")
                except:
                    print("dt=", dt)
                    print("T=", self.ticker)
                    print("divs_df:") ; print(divs_df)
                    raise
                if debug:
                    fetch_dt = divs_df.loc[dt, "FetchDate"]
                    msg = f"new dividend: {new_div} @ {dt.date()} adj={adj:.5f} close_before={close_before:.4f} fetch={fetch_dt.strftime('%Y-%m-%d %H:%M:%S%z')}"
                    yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(f"{self.ticker}: " + msg)
                divs_df.loc[dt, "Back Adj."] = adj

                div_already_in_cache = False
                if self.divs is not None:
                    if dt in self.divs.index:
                        div_already_in_cache = True
                        cached_div_dt = dt
                    else:
                        # Update: maybe Yahoo has shifted the day slighty.
                        for dt2 in self.divs.index:
                            if (abs(dt2-dt).days < 7) and (new_div == self.divs['Dividends'].loc[dt2]):
                                div_already_in_cache = True
                                cached_div_dt = dt2
                                break

                if self.divs is not None and div_already_in_cache:
                    # Replaced cached dividend event if (i) dividend different or (ii) adj different.
                    cached_div = self.divs.loc[cached_div_dt, "Dividends"]
                    cached_adj = self.divs.loc[cached_div_dt, "Back Adj."]
                    if debug:
                        msg = f"- pre-existing dividend @ {cached_div_dt}: {cached_div} vs {new_div}"
                        yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(f"{self.ticker}: " + msg)
                    if cached_div == 0.0:
                        diff_pct = 1.0
                    else:
                        diff_pct = 100*abs(cached_div-new_div)/cached_div
                        diff_pct2 = 100*abs(cached_adj-adj)/cached_adj
                        diff_pct = max(diff_pct, diff_pct2)
                    supersede = diff_pct >= 0.01
                    # Update: and (iii) fetched in past
                    supersede = supersede and (self.divs.loc[cached_div_dt, "FetchDate"] < divs_df.loc[dt, "FetchDate"])
                    supersede = supersede or (cached_div_dt != dt)
                    if not supersede:
                        # tiny difference, easier to just keep old value
                        divs_df = divs_df.drop(dt)
                        if debug:
                            msg = "ignoring new div"
                            yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(f"{self.ticker}: " + msg)
                    else:
                        divs_df.loc[dt, "Superseded div"] = self.divs.loc[cached_div_dt, "Dividends"]
                        divs_df.loc[dt, "Superseded back adj."] = self.divs.loc[cached_div_dt, "Back Adj."]
                        divs_df.loc[dt, "Superseded div FetchDate"] = self.divs.loc[cached_div_dt, "FetchDate"]
                        self.divs = self.divs.drop(cached_div_dt)
                        self_divs_modified = True
                        if debug:
                            msg = "- replacing old div"
                            yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(f"{self.ticker}: " + msg)
                # elif new_div == 0.0:
                #     # Discard, was only sent to deprecate previous dividend on this date
                #     divs_df = divs_df.drop(dt)

            cols = ["Dividends", "Back Adj.", "FetchDate", "Superseded div", "Superseded back adj.", "Superseded div FetchDate"]
            cols.append('Close repaired?')
            if not divs_df.empty:
                divs_pretty = divs_df[["Dividends", "Back Adj.", "Close before"]].copy()
                divs_pretty = divs_pretty.rename(columns={'Dividends':'Div', 'Back Adj.':'Adj', 'Close before':'Close'})
                divs_pretty['Adj'] = divs_pretty['Adj'].round(3)
                divs_pretty['Close'] = divs_pretty['Close'].round(3)
                divs_pretty.index = divs_pretty.index.date.astype(str)
                n = divs_pretty.shape[0]
                n_sup = np.sum((divs_df['Superseded div']>0.0).to_numpy())
                n_new = n - n_sup
                msg = f"stored {n} dividends ({n_new} new, {n_sup} superseded): {divs_pretty.to_dict(orient='index')}"
                self.manager.LogEvent("info", "DividendManager", msg)
                if yfcl.IsTracingEnabled():
                    yfcl.TracePrint(msg)

                if self.divs is None:
                    self.divs = divs_df[cols].copy()
                else:
                    f_na = self.divs['Superseded div FetchDate'].isna()
                    if f_na.all():
                        # Drop column. It breaks concat, and anyway 'divs_df' will restore it.
                        self.divs = self.divs.drop('Superseded div FetchDate', axis=1)
                    self.divs = pd.concat([self.divs, divs_df[cols]], sort=True).sort_index()
                yfcm.StoreCacheDatum(self.ticker, "dividends", self.divs)
            elif self_divs_modified:
                yfcm.StoreCacheDatum(self.ticker, "dividends", self.divs)

        yfcl.TraceExit("UpdateDividends() returning")


class PriceHistory:
    def __init__(self, manager, ticker, exchange, tzName, listingDay, interval, session, proxy, repair=True, contiguous=False):
        if isinstance(interval, str):
            if interval not in yfcd.intervalStrToEnum.keys():
                raise Exception("'interval' if str must be one of: {}".format(yfcd.intervalStrToEnum.keys()))
            interval = yfcd.intervalStrToEnum[interval]
        yfcu.TypeCheckStr(ticker, "ticker")
        yfcu.TypeCheckStr(exchange, "exchange")
        yfcu.TypeCheckStr(tzName, "tzName")
        if listingDay is not None:
            yfcu.TypeCheckDateStrict(listingDay, 'listingDay')
        yfcu.TypeCheckBool(repair, "repair")
        yfcu.TypeCheckBool(contiguous, "contiguous")

        self.manager = manager
        self.ticker = ticker
        self.exchange = exchange
        self.tzName = tzName
        self.listingDay = listingDay
        self.interval = interval
        self.session = session
        self.proxy = proxy
        self.repair = repair
        self.contiguous = contiguous

        self.dat = yf.Ticker(self.ticker, session=self.session)
        self.tz = ZoneInfo(self.tzName)

        self.itd = yfcd.intervalToTimedelta[self.interval]
        self.istr = yfcd.intervalToString[self.interval]
        self.interday = self.interval in [yfcd.Interval.Days1, yfcd.Interval.Week]#, yfcd.Interval.Months1, yfcd.Interval.Months3]
        self.intraday = not self.interday
        self.multiday = self.interday and self.interval != yfcd.Interval.Days1

        # Load from cache
        self.cache_key = "history-"+self.istr
        self.h = self._getCachedPrices()
        self._reviewNewDivs()

        # A place to temporarily store new dividends, until prices have
        # been repaired, then they can be sent to EventsHistory

        self._debug = False
        # self._debug = True

        # Manage potential for infinite recursion during price repair:
        self._record_stack_trace = True
        # self._record_stack_trace = False
        self._stack_trace = []
        self._infinite_recursion_detected = False

    def _getCachedPrices(self):
        h = None
        if yfcm.IsDatumCached(self.ticker, self.cache_key):
            h = yfcm.ReadCacheDatum(self.ticker, self.cache_key)

        if h is not None and h.empty:
            h = None

        return h

    def _updatedCachedPrices(self, df):
        if df is not None:
            yfcu.TypeCheckDataFrame(df, "df")

            expected_cols = ["Open", "High", "Low", "Close", "Volume", "Dividends", "Stock Splits"]
            expected_cols += ["Final?", "C-Check?", "FetchDate", "CSF", "CDF"]
            expected_cols += ["LastDivAdjustDt", "LastSplitAdjustDt"]

            missing_cols = [c for c in expected_cols if c not in df.columns]
            if len(missing_cols) > 0:
                raise Exception(f"DF missing these columns: {missing_cols}")

            if df.empty:
                df = None
        yfcm.StoreCacheDatum(self.ticker, self.cache_key, df)

        self.h = df

    def _reviewNewDivs(self):
        if self.interval != yfcd.Interval.Days1:
            return

        cached_new_divs = yfcm.ReadCacheDatum(self.ticker, "new_divs")
        if cached_new_divs is not None:
            if yfcm.ReadCacheMetadata(self.ticker, "new_divs", "locked") is not None:
                # This is bad, means YFC was killed before 'new_divs' could be processed.
                # Means potentially future new dividends have not been processed
                self.manager.LogEvent("info", '_reviewNewDivs', "'new_divs' was locked. Setting 'new_divs' to all divs in self.h")

                h_divs = self.h.loc[self.h["Dividends"]!=0, ["Dividends", "FetchDate"]]
                h_divs_since = h_divs[h_divs.index > cached_new_divs.index.max()]
                if not h_divs_since.empty:
                    if 'Desplitted?' not in cached_new_divs.columns:
                        cached_new_divs['Desplitted?'] = False  # assume
                    h_divs_since['Desplitted?'] = True
                    if 'Close before' not in cached_new_divs.columns:
                        cached_new_divs['Close before'] = np.nan
                    h_divs_since['Close before'] = self.h['Close'].shift(1).loc[h_divs_since.index]
                    cached_new_divs = pd.concat([cached_new_divs, h_divs_since])
                    yfcm.StoreCacheDatum(self.ticker, "new_divs", cached_new_divs)
                yfcm.WriteCacheMetadata(self.ticker, "new_divs", "locked", None)

    def get(self, start=None, end=None, period=None, max_age=None, trigger_at_market_close=False, repair=True, prepost=False, adjust_splits=False, adjust_divs=False, quiet=False):
        if start is None and end is None and period is None:
            raise ValueError("Must provide value for one of: 'start', 'end', 'period'")
        if start is not None:
            yfcu.TypeCheckIntervalDt(start, self.interval, "start", strict=False)
        if end is not None:
            yfcu.TypeCheckIntervalDt(end, self.interval, "end", strict=False)
        if period is not None:
            yfcu.TypeCheckPeriod(period, "period")
        yfcu.TypeCheckBool(trigger_at_market_close, "trigger_at_market_close")
        yfcu.TypeCheckBool(repair, "repair")
        yfcu.TypeCheckBool(adjust_splits, "adjust_splits")
        yfcu.TypeCheckBool(adjust_divs, "adjust_divs")

        # TODO: enforce 'max_age' value provided. Only 'None' while I dev
        if max_age is None:
            if self.interval == yfcd.Interval.Days1:
                max_age = timedelta(hours=4)
            elif self.interval == yfcd.Interval.Week:
                max_age = timedelta(hours=60)
            # elif self.interval == yfcd.Interval.Months1:
            #     max_age = timedelta(days=15)
            # elif self.interval == yfcd.Interval.Months3:
            #     max_age = timedelta(days=45)
            else:
                max_age = 0.5*yfcd.intervalToTimedelta[self.interval]

        # YFC cannot handle pre- and post-market intraday
        prepost = self.interday

        yfct.SetExchangeTzName(self.exchange, self.tzName)
        td_1d = timedelta(days=1)
        tz_exchange = ZoneInfo(self.tzName)
        dt_now = pd.Timestamp.utcnow().tz_convert(tz_exchange)
        d_now_exchange = dt_now.date()
        tomorrow_d = d_now_exchange + td_1d
        if self.interday:
            tomorrow = tomorrow_d
        else:
            tomorrow = datetime.combine(tomorrow_d, time(0), tz_exchange)
        if start is not None:
            if end is not None and start >= end:
                # raise ValueError(f"start={start} must < end={end}")
                return None
            # if (self.interday and start >= tomorrow) or (not self.interday and start > dt_now):
            if isinstance(start, datetime):
                if start > dt_now:
                    return None
            else:
                if start >= tomorrow_d:
                    return None

        debug_yf = False

        debug_yfc = self._debug
        # debug_yfc = True

        if period is not None:
            log_msg = f"PriceHistory-{self.istr}.get(tkr={self.ticker}, period={period}, max_age={max_age}, trigger_at_market_close={trigger_at_market_close}, prepost={prepost}, repair={repair})"
        else:
            log_msg = f"PriceHistory-{self.istr}.get(tkr={self.ticker}, start={start}, end={end}, max_age={max_age}, trigger_at_market_close={trigger_at_market_close}, prepost={prepost}, repair={repair})"
        if yfcl.IsTracingEnabled():
            yfcl.TraceEnter(log_msg)
        elif debug_yfc:
            print(log_msg)

        self._applyNewEvents()

        try:
            yf_lag = yfcd.exchangeToYfLag[self.exchange]
        except:
            print(f"- ticker = {self.ticker}")
            raise

        pstr = None
        end_d = None ; end_dt = None
        if period is not None:
            start_d, end_d = yfct.MapPeriodToDates(self.exchange, period, self.interval)
            period = None
            if self.interday:
                start = start_d
                end = end_d
                start_dt = datetime.combine(start_d, time(0), tz_exchange)
                end_dt = datetime.combine(end_d, time(0), tz_exchange)
            else:
                start = datetime.combine(start_d, time(0), tz_exchange)
                end = None
                start_dt = start
                end_dt = None
        else:
            if self.interday:
                start_dt = datetime.combine(start, time(0), tz_exchange)
            else:
                if isinstance(start, datetime):
                    start_dt = start
                else:
                    start_dt = datetime.combine(start, time(0), tz_exchange)
                    start = start_dt

        if end is None and pstr is None:
            if self.interday:
                end = d_now_exchange+td_1d
            else:
                sched = yfct.GetExchangeSchedule(self.exchange, d_now_exchange, d_now_exchange+td_1d)
                if sched is None or (dt_now + yf_lag) < sched["open"].iloc[0]:
                    # Before market open
                    end = datetime.combine(d_now_exchange, time(0), tz_exchange)
                else:
                    i = yfct.GetTimestampCurrentInterval(self.exchange, dt_now+yf_lag, self.interval, ignore_breaks=True)
                    if i is None:
                        # After interval
                        end = datetime.combine(d_now_exchange+td_1d, time(0), tz_exchange)
                    else:
                        # During interval
                        end = i["interval_close"]
        if end is not None and end_dt is None:
            if isinstance(end, datetime):
                end_dt = end
            else:
                # end_dt = datetime.combine(end+td_1d, time(0), tz_exchange)
                end_dt = datetime.combine(end, time(0), tz_exchange)
            if not self.interday:
                end = end_dt

        if self.interday:
            if isinstance(start, datetime) or isinstance(end, datetime):
                raise TypeError(f"'start' and 'end' must be date type not {type(start)}, {type(end)}")
        else:
            if (not isinstance(start, datetime)) and (not isinstance(end, datetime)):
                raise TypeError(f"'start' and 'end' must be datetime type not {type(start)}, {type(end)}")

        if self.listingDay is not None:
            listing_date = self.listingDay
            hist_md = yfcm.ReadCacheDatum(self.ticker, "history_metadata")
            if hist_md is None:
                hist_md = {'listingDate': listing_date}
                yfcm.StoreCacheDatum(self.ticker, "history_metadata", hist_md)
            elif 'listingDate' in hist_md and hist_md['listingDate'] != self.listingDay:
                hist_md['listingDate'] = listing_date
                yfcm.StoreCacheDatum(self.ticker, "history_metadata", hist_md)
        else:
            hist_md = yfcm.ReadCacheDatum(self.ticker, "history_metadata")
            if hist_md is not None and 'listingDate' in hist_md:
                listing_date = hist_md['listingDate']
            else:
                listing_date = None
        if listing_date is not None and start is not None:
            listing_date_dt = datetime.combine(listing_date, time(0), tz_exchange)
            if isinstance(start, datetime):
                start = max(start, listing_date_dt)
            else:
                start = max(start, listing_date)

        if self.h is not None:
            if self.h.empty:
                self.h = None

        # Remove expired intervals from cache
        if self.h is not None:
            n = self.h.shape[0]
            if self.interday:
                h_interval_dts = self.h.index.date if isinstance(self.h.index[0], pd.Timestamp) else self.h.index
            else:
                h_interval_dts = np.array([yfct.ConvertToDatetime(dt, tz=tz_exchange) for dt in self.h.index])
            h_interval_dts = np.array(h_interval_dts)
            # if self.interval == yfcd.Interval.Days1:
            if self.contiguous:
                # Daily data is always contiguous so only need to check last row
                h_interval_dt = h_interval_dts[-1]
                fetch_dt = yfct.ConvertToDatetime(self.h["FetchDate"].iloc[-1], tz=tz_exchange)
                f_final = self.h["Final?"].to_numpy()
                f_nfinal = ~f_final
                # - also treat repaired data as non-final, if fetched near to interval timepoint
                #   because Yahoo might now have correct data
                # - and treat NaN data as repaired
                f_repair = self.h["Repaired?"].to_numpy()
                f_na = self.h['Close'].isna().to_numpy()
                f_repair = f_repair | f_na
                cutoff_dts = self.h.index + self.itd + timedelta(days=7)
                # Ignore repaired data if fetched/repaired 7+ days after interval end
                f_repair[self.h['FetchDate'] > cutoff_dts] = False
                if f_repair.any():
                    f_nfinal = f_nfinal | f_repair
                if f_nfinal.any():
                    idx0 = np.where(f_nfinal)[0][0]
                    repaired = f_repair[idx0]
                    h_interval_dt = h_interval_dts[idx0]
                    fetch_dt = yfct.ConvertToDatetime(self.h["FetchDate"].iloc[idx0], tz=tz_exchange)
                    try:
                        expired = yfct.IsPriceDatapointExpired(h_interval_dt, fetch_dt, repaired, max_age, self.exchange, self.interval, yf_lag=yf_lag, triggerExpiryOnClose=trigger_at_market_close)
                    except yfcd.TimestampOutsideIntervalException as e:
                        if f_na[idx0]:
                            # YFC must have inserted a row of NaNs, wrongly thinking exchange should have been open here.
                            expired = True
                        else:
                            raise e
                    if expired:
                        self.h = self.h.iloc[:idx0]
                        h_interval_dts = h_interval_dts[:idx0]

            else:
                expired = np.array([False]*n)
                f_final = self.h["Final?"].to_numpy()
                f_nfinal = ~f_final
                # - also treat repaired data as non-final, if fetched near to interval timepoint
                #   because Yahoo might now have correct data
                # - and treat NaN data as repaired
                cutoff_dt = dt_now - timedelta(days=7)
                idx = self.h.index.get_indexer([cutoff_dt], method='bfill')[0]
                f_repair = self.h["Repaired?"].to_numpy()
                f_na = self.h['Close'].isna().to_numpy()
                f_repair = f_repair | f_na
                cutoff_dts = self.h.index + self.itd + timedelta(days=7)
                # Ignore repaired data if fetched/repaired 7+ days after interval end
                f_repair[self.h['FetchDate'] > cutoff_dts] = False
                if f_repair.any():
                    f_nfinal = f_nfinal | f_repair
                for idx in np.where(f_nfinal)[0]:
                    # repaired = False
                    repaired = f_repair[idx]
                    h_interval_dt = h_interval_dts[idx]
                    fetch_dt = yfct.ConvertToDatetime(self.h["FetchDate"].iloc[idx], tz=tz_exchange)
                    try:
                        expired_idx = yfct.IsPriceDatapointExpired(h_interval_dt, fetch_dt, repaired, max_age, self.exchange, self.interval, yf_lag=yf_lag, triggerExpiryOnClose=trigger_at_market_close)
                    except yfcd.TimestampOutsideIntervalException as e:
                        if f_na[idx0]:
                            # YFC must have inserted a row of NaNs, wrongly thinking exchange should have been open here.
                            expired_idx = True
                        else:
                            raise e
                    expired[idx] = expired_idx
                if expired.any():
                    self.h = self.h.drop(self.h.index[expired])
                    h_interval_dts = h_interval_dts[~expired]
            if self.h.empty:
                self.h = None

        ranges_to_fetch = []
        if self.h is None:
            # Simple, just fetch the requested data

            if self.contiguous:
                # Ensure daily always up-to-now
                h = self._fetchYfHistory(start, tomorrow, prepost, debug_yf)
            else:
                h = self._fetchYfHistory(start, end, prepost, debug_yf)
            if h is None:
                raise yfcd.NoPriceDataInRangeException(self.ticker, self.istr, start, end)

            # Adjust
            h = self._reverseYahooAdjust(h)

            if self.interval == yfcd.Interval.Days1:
                h_splits = h[h["Stock Splits"] != 0]
                if len(h_splits) > 0:
                    self.manager.GetHistory("Events").UpdateSplits(h_splits)

            self._updatedCachedPrices(h)

        else:
            # Compare request against cached data, only fetch missing/expired data

            # Performance TODO: tag rows as fully contiguous to avoid searching for gaps

            # Calculate ranges_to_fetch
            if self.contiguous:
                if self.h is None or self.h.empty:
                    if self.interday:
                        if self.multiday:
                            ranges_to_fetch = yfct.IdentifyMissingIntervalRanges(self.exchange, start_d, end_d+self.itd, self.interval, [], minDistanceThreshold=5)
                        else:
                            ranges_to_fetch = yfct.IdentifyMissingIntervalRanges(self.exchange, start_d, end_d, self.interval, [], minDistanceThreshold=5)
                    else:
                        ranges_to_fetch = yfct.IdentifyMissingIntervalRanges(self.exchange, start, end, self.interval, [], minDistanceThreshold=5)
                else:
                    # Ensure that daily data always up-to-date to now
                    # Update: only necessary to be up-to-date to now if a fetch happens
                    dt_start = yfct.ConvertToDatetime(self.h.index[0], tz=tz_exchange)
                    dt_end = yfct.ConvertToDatetime(self.h.index[-1], tz=tz_exchange)
                    start_interval = yfct.GetTimestampCurrentInterval(self.exchange, dt_start, self.interval, ignore_breaks=True)
                    if start_interval is None:
                        # Possible if Yahoo returned price data when 'exchange_calendars' thinks exchange was closed
                        for i in range(1, 5):
                            start_interval = yfct.GetTimestampCurrentInterval(self.exchange, dt_start + td_1d*i, self.interval, ignore_breaks=True)
                            if start_interval is not None:
                                break
                            start_interval = yfct.GetTimestampCurrentInterval(self.exchange, dt_start - td_1d*i, self.interval, ignore_breaks=True)
                            if start_interval is not None:
                                break
                    h_start = start_interval["interval_open"]
                    last_interval = yfct.GetTimestampCurrentInterval(self.exchange, dt_end, self.interval, ignore_breaks=True)
                    if last_interval is None:
                        # Possible if Yahoo returned price data when 'exchange_calendars' thinks exchange was closed
                        for i in range(1, 5):
                            last_interval = yfct.GetTimestampCurrentInterval(self.exchange, dt_end - td_1d*i, self.interval, ignore_breaks=True)
                            if last_interval is not None:
                                break
                            last_interval = yfct.GetTimestampCurrentInterval(self.exchange, dt_end + td_1d*i, self.interval, ignore_breaks=True)
                            if last_interval is not None:
                                break
                    h_end = last_interval["interval_close"]

                    rangePre_to_fetch = None
                    if start < h_start:
                        if debug_yfc:
                            msg = "checking for rangePre_to_fetch"
                            yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(f"{self.ticker}: " + msg)
                        try:
                            rangePre_to_fetch = yfct.IdentifyMissingIntervalRanges(self.exchange, start, h_start, self.interval, None, ignore_breaks=True, minDistanceThreshold=5)
                        except yfcd.NoIntervalsInRangeException:
                            rangePre_to_fetch = None
                    if rangePre_to_fetch is not None:
                        if len(rangePre_to_fetch) > 1:
                            raise Exception("Expected only one element in rangePre_to_fetch[], but = {}".format(rangePre_to_fetch))
                        rangePre_to_fetch = rangePre_to_fetch[0]
                    #
                    rangePost_to_fetch = None
                    dt_end_lastDataDt = yfct.CalcIntervalLastDataDt(self.exchange, dt_end, self.interval)
                    fetchDt = self.h["FetchDate"].iloc[-1]
                    if dt_end_lastDataDt is None:
                        # hacky fix but should be fine
                        dt_end_lastDataDt = fetchDt
                    more_rows_possible = self.h["Final?"].iloc[-1] and (min(dt_end_lastDataDt, fetchDt)+max_age <= dt_now)
                    if more_rows_possible:
                        # Possible for Yahoo to have new data.
                        # Note: if self.h["Final?"].iloc[-1] == False, then that means above expiry check didn't remove it so don't fetch anything
                        if debug_yfc:
                            msg = "checking for rangePost_to_fetch"
                            yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(f"{self.ticker}: " + msg)
                        if self.interday:
                            if rangePre_to_fetch is not None or end > h_end:
                                target_end_d = tomorrow_d
                            else:
                                target_end_d = end
                            if self.multiday:
                                target_end_d += self.itd  # testing new code
                            if h_end < target_end_d:
                                try:
                                    rangePost_to_fetch = yfct.IdentifyMissingIntervalRanges(self.exchange, h_end, target_end_d, self.interval, None, minDistanceThreshold=5)
                                except yfcd.NoIntervalsInRangeException:
                                    rangePost_to_fetch = None
                        else:
                            if rangePre_to_fetch is not None or end > h_end:
                                target_end_dt = dt_now
                            else:
                                target_end_dt = end
                            d = target_end_dt.astimezone(tz_exchange).date()
                            sched = yfct.GetExchangeSchedule(self.exchange, d, d + td_1d)
                            if (sched is not None) and (not sched.empty) and (dt_now > sched["open"].iloc[0]):
                                target_end_dt = sched["close"].iloc[0]+timedelta(hours=2)
                            if h_end < target_end_dt:
                                try:
                                    rangePost_to_fetch = yfct.IdentifyMissingIntervalRanges(self.exchange, h_end, target_end_dt, self.interval, None, minDistanceThreshold=5)
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
                h_intervals = yfct.GetTimestampCurrentInterval_batch(self.exchange, h_interval_dts, self.interval, ignore_breaks=True)
                if h_intervals is None:
                    h_intervals = pd.DataFrame(data={"interval_open": [], "interval_close": []})
                else:
                    f_na = h_intervals["interval_open"].isna().to_numpy()
                    if f_na.any():
                        # Mapping Yahoo intervals -> xcal can fail now, because sometime xcal is wrong.
                        # Need to tolerate
                        h_intervals.loc[f_na, "interval_open"] = h_interval_dts[f_na]
                        h_intervals.loc[f_na, "interval_close"] = h_interval_dts[f_na]+self.itd
                if (not h_intervals.empty) and isinstance(h_intervals["interval_open"].iloc[0], datetime):
                    h_interval_opens = [x.to_pydatetime().astimezone(tz_exchange) for x in h_intervals["interval_open"]]
                else:
                    h_interval_opens = h_intervals["interval_open"].to_numpy()

                try:
                    target_end = end
                    if self.multiday:
                        target_end += self.itd
                    ranges_to_fetch = yfct.IdentifyMissingIntervalRanges(self.exchange, start, target_end, self.interval, h_interval_opens, ignore_breaks=True, minDistanceThreshold=5)
                    if ranges_to_fetch is None:
                        ranges_to_fetch = []
                except yfcd.NoIntervalsInRangeException:
                    ranges_to_fetch = []
                except Exception:
                    print("Ticker =", self.ticker)
                    raise
            # Prune ranges in future:
            for i in range(len(ranges_to_fetch)-1, -1, -1):
                r = ranges_to_fetch[i]
                x = r[0]
                delete_range = False
                if isinstance(x, (datetime, pd.Timestamp)):
                    if x > dt_now:
                        delete_range = True
                    else:
                        sched = yfct.GetExchangeSchedule(self.exchange, x.date(), x.date() + td_1d)
                        delete_range = (sched is not None) and dt_now < (sched["open"].iloc[0] + yf_lag)
                else:
                    if datetime.combine(x, time(0), tzinfo=tz_exchange) > dt_now:
                        delete_range = True
                    else:
                        sched = yfct.GetExchangeSchedule(self.exchange, x, x + td_1d)
                        delete_range = (sched is not None) and dt_now < (sched["open"].iloc[0] + yf_lag)
                if delete_range:
                    if debug_yfc:
                        print("- deleting future range:", r[i])
                    del ranges_to_fetch[i]
                else:
                    # Check if range ends in future, if yes then adjust to tomorrow max
                    y = r[1]
                    if isinstance(y, (datetime, pd.Timestamp)):
                        if y > dt_now:
                            ranges_to_fetch[i] = (r[0], min(dt_now.ceil('1D'), y))
                    elif y > d_now_exchange:
                        sched = yfct.GetExchangeSchedule(self.exchange, d_now_exchange, y + td_1d)
                        if sched is not None:
                            if debug_yfc:
                                print("- capping last range_to_fetch end to d_now_exchange")
                            if dt_now < sched["open"].iloc[0]:
                                ranges_to_fetch[i] = (r[0], d_now_exchange)
                            else:
                                ranges_to_fetch[i] = (r[0], d_now_exchange + td_1d)

            # Important that ranges_to_fetch in reverse order!
            ranges_to_fetch.sort(key=lambda x: x[0], reverse=True)
            if debug_yfc:
                print("- ranges_to_fetch:")
                pprint(ranges_to_fetch)

            if len(ranges_to_fetch) > 0:
                if not self.h.empty:
                    # Ensure only one range max is after cached data:
                    h_last_dt = self.h.index[-1].to_pydatetime()
                    if not isinstance(ranges_to_fetch[0][0], datetime):
                        h_last_dt = h_last_dt.astimezone(tz_exchange).date()
                    n = 0
                    for r in ranges_to_fetch:
                        if r[0] > h_last_dt:
                            n += 1
                    if n > 1:
                        print("ranges_to_fetch:")
                        pprint(ranges_to_fetch)
                        raise Exception("ranges_to_fetch contains {} ranges that occur after h_last_dt={}, expected 1 max".format(n, h_last_dt))

                # if not quiet:
                #     quiet = period is not None  # YFC generated date range so don't print message
                if debug_yfc:
                    quiet = False
                    # quiet = not debug_yfc
                # if self.interval == yfcd.Interval.Days1:
                if self.contiguous:
                    self._fetchAndAddRanges_contiguous(ranges_to_fetch, prepost, debug_yf, quiet=quiet)
                else:
                    self._fetchAndAddRanges_sparse(ranges_to_fetch, prepost, debug_yf, quiet=quiet)

        # repair after all fetches complete
        if self.repair and repair:
            f_checked = self.h["C-Check?"].to_numpy()
            f_not_checked = ~f_checked
            if f_not_checked.any():
                # Check for 100x errors across ENTIRE table with split-adjustment applied temporarily.
                # Potential problem with very sparse data, but assume most users will
                # fetch "sparse" fairly contiguously.
                # - apply split-adjustment
                OHLC = ['Open', 'High', 'Low', 'Close']
                csf = self.h['CSF'].to_numpy()
                for c in OHLC:
                    self.h[c] *= csf
                self.h['Dividends'] *= csf
                f = self.h['Dividends'] != 0

                # Need the latest/last row to be repaired before 100x/split repair:
                self.h = self.h.sort_index()
                df_last = self.h.iloc[-1:]
                df_last = self._repairZeroPrices(df_last)
                if 'Repaired?' not in self.h.columns:
                    self.h['Repaired?'] = False
                self.h = pd.concat([self.h.drop(self.h.index[-1]), df_last])
                # Must fix bad 'Adj Close' & dividends before 100x/split errors
                self.h = self._repairUnitMixups(self.h)
                # Also repair split errors:
                self.h = self._fixBadStockSplits(self.h)

                # Update new divs, because now I am possibly repairing dividends
                if self.interval == yfcd.Interval.Days1:
                    f = (self.h['Dividends']>0).to_numpy()
                    if f.any():
                        df_divs = self.h[f][['Dividends', 'FetchDate']].copy()
                        df_divs['Close repaired?'] = self.h['Repaired?'].ffill().shift(1).loc[df_divs.index]
                        df_divs['Desplitted?'] = False
                        cached_new_divs = yfcm.ReadCacheDatum(self.ticker, "new_divs")
                        if cached_new_divs is not None:
                            if 'Desplitted?' not in cached_new_divs.columns:
                                cached_new_divs['Desplitted?'] = False
                            for dt in df_divs.index:
                                if dt in cached_new_divs.index:
                                    # Overwrite
                                    cached_new_divs = cached_new_divs.drop(dt)
                            if cached_new_divs.empty:
                                cached_new_divs = None
                        if cached_new_divs is None:
                            yfcm.StoreCacheDatum(self.ticker, "new_divs", df_divs)
                        else:
                            df_divs = df_divs[~df_divs.index.isin(cached_new_divs.index)]
                            if not df_divs.empty:
                                divs_pretty = df_divs['Dividends'].copy()
                                divs_pretty.index = divs_pretty.index.date
                                self.manager.LogEvent("info", "DividendManager", f"detected {divs_pretty.shape[0]} new dividends: {divs_pretty} (before reversing adjust)")
                                if yfcm.ReadCacheMetadata(self.ticker, "new_divs", "locked") is not None:
                                    # locked
                                    pass
                                else:
                                    cached_new_divs = pd.concat([cached_new_divs, df_divs])
                                    yfcm.StoreCacheDatum(self.ticker, "new_divs", cached_new_divs)

                # - reverse split-adjustment
                csf_rcp = 1.0/self.h['CSF'].to_numpy()
                for c in OHLC:
                    self.h[c] *= csf_rcp
                self.h['Dividends'] *= csf_rcp
                ha = self.h[f_not_checked].copy()
                hb = self.h[~f_not_checked]
                ha = self._repairZeroPrices(ha, silent=True)
                ha["C-Check?"] = True
                if not hb.empty:
                    self.h = pd.concat([ha, hb[ha.columns]])
                    self.h.index = pd.to_datetime(self.h.index, utc=True).tz_convert(tz_exchange)
                else:
                    self.h = ha
                self.h = self.h.sort_index()
                self._updatedCachedPrices(self.h)

        # Now prices have been repaired, can send out dividends
        cached_new_divs = yfcm.ReadCacheDatum(self.ticker, "new_divs")
        if self.interval == yfcd.Interval.Days1 and cached_new_divs is not None and not cached_new_divs.empty:
            cached_new_divs_locked = yfcm.ReadCacheMetadata(self.ticker, "new_divs", "locked")
            if cached_new_divs_locked is None:
                f_dups = cached_new_divs.index.duplicated()
                if f_dups.any():
                    print(cached_new_divs)
                    raise Exception('duplicates detected in cached_new_divs')
                yfcm.WriteCacheMetadata(self.ticker, "new_divs", "locked", 1)
                yfcl.TracePrint("sending out new dividends ...")
                # TODO: remove duplicates from _newDivs (possible when restoring file file)
                divs_df = cached_new_divs
                divs_df["Close before"] = np.nan
                for dt in divs_df.index:
                    if dt == self.h.index[0]:
                        hist_before = self.manager.GetHistory(yfcd.Interval.Days1).get(start=dt.date()-timedelta(days=7), end=dt.date(), adjust_splits=False, adjust_divs=False)
                        close_before = hist_before["Close"].iloc[-1]
                        if np.isnan(close_before):
                            raise Exception("'close_before' is NaN")
                    else:
                        if dt not in self.h.index:
                            continue
                        idx = self.h.index.get_loc(dt)
                        close_before = self.h["Close"].iloc[idx-1]
                        if np.isnan(close_before):
                            for idx in range(idx-1, idx-9, -1):
                                close_before = self.h["Close"].iloc[idx-1]
                                if not np.isnan(close_before):
                                    break
                            if np.isnan(close_before):
                                print(f"- idx={idx} dt={dt}")
                                print(self.h.iloc[idx-2:idx+3][["Close", "FetchDate"]])
                                raise Exception("'close_before' is NaN")
                    divs_df.loc[dt, "Close before"] = close_before
                    # De-split div:
                    if not divs_df.loc[dt, 'Desplitted?']:
                        splits_post = self.manager.GetHistory('Events').GetSplits(dt.date())
                        if splits_post is not None:
                            post_csf = 1.0/splits_post["Stock Splits"].prod()
                            divs_df.loc[dt, 'Dividends'] /= post_csf
                        divs_df.loc[dt, 'Desplitted?'] = True
                f_close_nan = divs_df['Close before'].isna()
                if f_close_nan.any():
                    self.manager.GetHistory("Events").UpdateDividends(divs_df[~f_close_nan])
                    self._applyNewEvents()
                    yfcm.StoreCacheDatum(self.ticker, "new_divs", divs_df[f_close_nan])  # store the divs where we couldn't get a close
                else:
                    self.manager.GetHistory("Events").UpdateDividends(divs_df)
                    self._applyNewEvents()
                    if cached_new_divs is not None and not cached_new_divs_locked:
                        yfcm.StoreCacheDatum(self.ticker, "new_divs", None)  # delete

        if "Adj Close" in self.h.columns:
            raise Exception("Adj Close in self.h")

        if (start is not None) and (end is not None):
            h_copy = self.h.loc[start_dt:end_dt-timedelta(milliseconds=1)].copy()
        else:
            h_copy = self.h.copy()

        if adjust_splits:
            for c in ["Open", "High", "Low", "Close", "Dividends"]:
                h_copy[c] *= h_copy["CSF"]
            h_copy["Volume"] = (h_copy["Volume"]/h_copy["CSF"]).round(0).astype('int')
            h_copy = h_copy.drop("CSF", axis=1)
        if adjust_divs:
            for c in ["Open", "High", "Low", "Close"]:
                h_copy[c] *= h_copy["CDF"]
            h_copy = h_copy.drop("CDF", axis=1)

        log_msg = f"PriceHistory-{self.istr}.get() returning"
        if h_copy.empty:
            log_msg += " empty df"
        else:
            log_msg += f" DF {h_copy.index[0]} -> {h_copy.index[-1]}"
        if yfcl.IsTracingEnabled():
            yfcl.TraceExit(log_msg)
        elif debug_yfc:
            print(log_msg)

        return h_copy

    def get_metadata(self):
        return yfcm.ReadCacheDatum(self.ticker, "history_metadata")

    def _fetchAndAddRanges_contiguous(self, ranges_to_fetch, prepost, debug, quiet=False):
        yfcu.TypeCheckIterable(ranges_to_fetch, "ranges_to_fetch")
        yfcu.TypeCheckBool(prepost, "prepost")
        yfcu.TypeCheckBool(debug, "debug")
        yfcu.TypeCheckBool(quiet, "quiet")

        # Fetch each range, appending/prepending to cached data
        if (ranges_to_fetch is None) or len(ranges_to_fetch) == 0:
            # return h
            return

        debug_yfc = self._debug
        # debug_yfc = True

        log_msg = f"_fetchAndAddRanges_contiguous-{self.istr}(n={len(ranges_to_fetch)} prepost={prepost}))"
        if yfcl.IsTracingEnabled():
            yfcl.TraceEnter(log_msg)
        elif debug_yfc:
            print(log_msg)
            print("- ranges_to_fetch:")
            pprint(ranges_to_fetch)

        tz_exchange = ZoneInfo(self.tzName)
        yfct.SetExchangeTzName(self.exchange, self.tzName)

        if self.h is None or self.h.empty:
            h_first_dt = None ; h_last_dt = None
        else:
            h_first_dt = self.h.index[0].to_pydatetime()
            h_last_dt = self.h.index[-1].to_pydatetime()
            if not isinstance(ranges_to_fetch[0][0], datetime):
                h_first_dt = h_first_dt.astimezone(tz_exchange).date()
                h_last_dt = h_last_dt.astimezone(tz_exchange).date()
        td_1d = timedelta(days=1)

        # Because data should be contiguous, then ranges should meet some conditions:
        if len(ranges_to_fetch) > 2:
            pprint(ranges_to_fetch)
            raise Exception("For contiguous data generated {}>2 ranges".format(len(ranges_to_fetch)))
        if self.h.empty and len(ranges_to_fetch) > 1:
            raise Exception("For contiguous data generated {} ranges, but h is empty".format(len(ranges_to_fetch)))
        range_pre = None ; range_post = None
        if self.h.empty and len(ranges_to_fetch) == 1:
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

        # Fetch ranges
        h2_pre = None ; h2_post = None
        if range_post is not None:
            r = range_post
            try:
                h2_post = self._fetchYfHistory(r[0], r[1], prepost, debug, quiet=quiet)
            except yfcd.NoPriceDataInRangeException:
                # If only trying to fetch 1 day of 1d data, then print warning instead of exception.
                # Could add additional condition of dividend previous day (seems to mess up table).
                if self.interval == yfcd.Interval.Days1 and r[1] - r[0] == td_1d:
                    if not quiet:
                        print(f"WARNING: {self.ticker}: No {yfcd.intervalToString[self.interval]}-price data fetched for {r[0]} -> {r[1]}")
                    h2_post = None
                # Allow longer ranges, for low-volume tickers
                elif r[1] - r[0] < timedelta(days=30):
                    if not quiet:
                        print(f"WARNING: {self.ticker}: No {yfcd.intervalToString[self.interval]}-price data fetched for {r[0]} -> {r[1]}")
                    h2_post = None
                else:
                    raise

        hist_md = yfcm.ReadCacheDatum(self.ticker, "history_metadata")
        if hist_md is None or 'listingDate' not in hist_md:
            listing_date = self.dat.history_metadata["firstTradeDate"]
            if isinstance(listing_date, int):
                listing_date = pd.to_datetime(listing_date, unit='s', utc=True).tz_convert(tz_exchange)
            if hist_md is None:
                hist_md = {}
            hist_md = {'listingDate': listing_date}
            yfcm.StoreCacheDatum(self.ticker, "history_metadata", hist_md)

        if h2_post is not None:
            # De-adjust the new data, and backport any new events in cached data
            # Note: Yahoo always returns split-adjusted price, so reverse it

            # Simple append to bottom of table
            # 1) adjust h2_post
            h2_post = self._reverseYahooAdjust(h2_post)
            if debug_yfc:
                print("- h2_post:")
                print(h2_post)

            # TODO: Problem: dividends need correct close
            if self.interval == yfcd.Interval.Days1:
                h2_post_splits = h2_post[h2_post["Stock Splits"] != 0][["Stock Splits", "FetchDate"]].copy()
                if not h2_post_splits.empty:
                    self.manager.GetHistory("Events").UpdateSplits(h2_post_splits)
                # Update: moving UpdateDividends() to after repair

            # Backport new events across entire h table
            self._applyNewEvents()

            dc = 'Capital Gains'
            if dc in self.h.columns and dc not in h2_post.columns:
                # mismatch
                if (self.h[dc]==0.0).all():
                    self.h = self.h.drop(dc, axis=1)
                else:
                    h2_post[dc] = 0.0
            elif dc not in self.h.columns and dc in h2_post.columns:
                # mismatch
                self.h[dc] = 0.0

            self.h = pd.concat([self.h, h2_post[self.h.columns]])
            self.h.index = pd.to_datetime(self.h.index, utc=True).tz_convert(tz_exchange)

        # Update: fetch pre AFTER post, to add any new divs to div cache
        if range_pre is not None:
            r = range_pre
            try:
                h2_pre = self._fetchYfHistory(r[0], r[1], prepost, debug, quiet=quiet)
            except yfcd.NoPriceDataInRangeException:
                if self.interval == yfcd.Interval.Days1 and r[1] - r[0] == td_1d:
                    # If only trying to fetch 1 day of 1d data, then print warning instead of exception.
                    # Could add additional condition of dividend previous day (seems to mess up table).
                    if not quiet:
                        print(f"WARNING: {self.ticker}: No {yfcd.intervalToString[self.interval]}-price data fetched for {r[0]} -> {r[1]}")
                    h2_pre = None
                elif (range_post is None) and (r[1]-r[0] < td_1d*7) and (r[1]-r[0] > td_1d*3):
                    # Small date range, potentially trying to fetch before listing data
                    h2_pre = None
                else:
                    raise

        if h2_pre is not None:
            if debug_yfc:
                print("- prepending new data")
            # Simple prepend to top of table

            h2_pre = self._reverseYahooAdjust(h2_pre)

            if self.interval == yfcd.Interval.Days1:
                h2_pre_splits = h2_pre[h2_pre["Stock Splits"] != 0][["Stock Splits", "FetchDate"]].copy()
                if not h2_pre_splits.empty:
                    self.manager.GetHistory("Events").UpdateSplits(h2_pre_splits)

            dc = 'Capital Gains'
            if dc in self.h.columns and dc not in h2_pre.columns:
                # mismatch
                if (self.h[dc]==0.0).all():
                    self.h = self.h.drop(dc, axis=1)
                else:
                    h2_pre[dc] = 0.0
            elif dc not in self.h.columns and dc in h2_pre.columns:
                # mismatch
                self.h[dc] = 0.0

            self.h = pd.concat([self.h, h2_pre[self.h.columns]])
            self.h.index = pd.to_datetime(self.h.index, utc=True).tz_convert(tz_exchange)

        self.h = self.h.sort_index()
        self._updatedCachedPrices(self.h)

        log_msg = "_fetchAndAddRanges_contiguous() returning"
        if yfcl.IsTracingEnabled():
            yfcl.TraceExit(log_msg)
        elif debug_yfc:
            print("- h:")
            print(self.h)
            print(log_msg)

    def _fetchAndAddRanges_sparse(self, ranges_to_fetch, prepost, debug, quiet=False):
        yfcu.TypeCheckIterable(ranges_to_fetch, "ranges_to_fetch")
        yfcu.TypeCheckBool(prepost, "prepost")
        yfcu.TypeCheckBool(debug, "debug")
        yfcu.TypeCheckBool(quiet, "quiet")

        # Fetch each range, but can be careless regarding de-adjust because
        # getting events from the carefully-managed daily data
        if (ranges_to_fetch is None) or len(ranges_to_fetch) == 0:
            # return h
            return

        debug_yfc = self._debug
        # debug_yfc = True

        log_msg = f"_fetchAndAddRanges_sparse(prepost={prepost})"
        if yfcl.IsTracingEnabled():
            yfcl.TraceEnter(log_msg)
        elif debug_yfc:
            print(log_msg)

        tz_exchange = self.tz
        td_1d = timedelta(days=1)
        dtnow = pd.Timestamp.utcnow().tz_convert(tz_exchange)

        # Backport events that occurred since last adjustment:
        self._applyNewEvents()

        # Ensure have daily data covering all ranges_to_fetch, so they can be de-splitted
        r_start_earliest = ranges_to_fetch[0][0]
        for rstart, rend in ranges_to_fetch:
            r_start_earliest = min(rstart, r_start_earliest)
        r_start_earliest_d = r_start_earliest.date() if isinstance(r_start_earliest, datetime) else r_start_earliest
        if debug_yfc:
            print("- r_start_earliest = {}".format(r_start_earliest))
        # Trigger price sync:
        histDaily = self.manager.GetHistory(yfcd.Interval.Days1)
        # histDaily.get(start=r_start_earliest_d, max_age=td_1d)
        histDaily.get(start=r_start_earliest_d, max_age=td_1d, repair=False)

        # Fetch each range, and adjust for splits that occurred after
        for rstart, rend in ranges_to_fetch:
            fetch_start = rstart
            fetch_end = rend
            # if not self.interday:  # and fetch_start.date() == fetch_end.date():
            #     # Intraday fetches behave better when time = midnight
            #     fetch_start = fetch_start.floor("1D")
            #     fetch_end = fetch_end.ceil("1D")
            # Update: data reliability now fixed by ChunkDatesIntoYfFetches()
            if debug_yfc:
                print("- fetching {} -> {}".format(fetch_start, fetch_end))
            try:
                h2 = self._fetchYfHistory(fetch_start, fetch_end, prepost, debug, quiet=quiet)
            except yfcd.NoPriceDataInRangeException:
                # If only trying to fetch 1 day of 1d data, then print warning instead of exception.
                # Could add additional condition of dividend previous day (seems to mess up table).
                ignore = False
                if self.interval == yfcd.Interval.Days1 and fetch_end - fetch_start == td_1d:
                    ignore = True
                elif self.intraday and fetch_start.date() == dtnow.date():
                    ignore = True
                elif self.interval == yfcd.Interval.Mins1 and fetch_end - fetch_start <= timedelta(minutes=10):
                    ignore = True
                elif self.interday and self.itd > (fetch_end - fetch_start):
                    ignore = True
                if ignore:
                    if not quiet:
                        # print("WARNING: No {}-price data fetched for ticker {} between dates {} -> {}".format(yfcd.intervalToString[self.interval], self.ticker, rstart, rend))
                        print(f"WARNING: {self.ticker}: No {yfcd.intervalToString[self.interval]}-price data fetched for {rstart} -> {rend}")
                    h2 = None
                    continue
                else:
                    raise

            if h2 is None:
                # raise Exception("YF returned None for: tkr={}, interval={}, start={}, end={}".format(self.ticker, self.interval, rstart, rend))
                # raise Exception(f"yfinance.history() returned None ({self.ticker} {self.istr} {rstart}->{rend})")
                raise yfcd.NoPriceDataInRangeException(self.ticker, self.istr, rstart, rend)
                # raise Exception(f"yfinance.history() returned None ({self.ticker} {self.istr} {fetch_start}->{fetch_end})")

            # Ensure h2 is split-adjusted. Sometimes Yahoo returns unadjusted data
            h2 = self._reverseYahooAdjust(h2)
            if debug_yfc:
                print("- h2 adjusted:")
                print(h2[["Close", "Dividends", "Volume", "CSF", "CDF"]])

            if fetch_start != rstart:
                h2 = h2[h2.index >= rstart]
            if fetch_end != rend:
                h2 = h2[h2.index < rend+self.itd]

            if "Adj Close" in h2.columns:
                raise Exception("Adj Close in h2")
            try:
                self.h = self.h[yfcu.np_isin_optimised(self.h.index, h2.index, invert=True)]
            except Exception:
                print("self.h.shape:", self.h.shape)
                print("h2.shape:", h2.shape)
                raise
            self.h = pd.concat([self.h, h2[self.h.columns]])
            self.h.index = pd.to_datetime(self.h.index, utc=True).tz_convert(tz_exchange)

            f_dups = self.h.index.duplicated()
            if f_dups.any():
                raise Exception(f"{self.ticker}: Adding range {rstart}->{rend} has added duplicate timepoints have been duplicated: {self.h.index[f_dups]}")

        self.h = self.h.sort_index()
        self._updatedCachedPrices(self.h)

        log_msg = "_fetchAndAddRanges_sparse() returning"
        if yfcl.IsTracingEnabled():
            yfcl.TraceExit(log_msg)
        elif debug_yfc:
            print(log_msg)

    def _verifyCachedPrices(self, rtol=0.0001, vol_rtol=0.004, correct=False, discard_old=False, quiet=True, debug=False):
        correct_values = [False, 'one', 'all']
        if correct not in correct_values:
            raise TypeError(f"'correct' must be one of: {correct_values}")
        yfcu.TypeCheckBool(discard_old, "discard_old")
        yfcu.TypeCheckBool(quiet, "quiet")
        yfcu.TypeCheckBool(debug, "debug")

        if self.h is None or self.h.empty:
            return True

        if debug:
            quiet = False

        yfcl.TraceEnter(f"PM::_verifyCachedPrices-{self.istr}(correct={correct}, debug={debug})")

        # New code: hopefully this will correct bad CDF in 1wk etc
        self._applyNewEvents()

        h = self.h.copy()  # working copy for comparison with YF
        h_modified = False
        h_new = self.h.copy()  # copy for storing changes
        # Keep track of problems:
        f_diff_all = pd.Series(np.full(h.shape[0], False), h.index, name="None")
        n = h.shape[0]

        # Ignore non-final data because will differ to Yahoo
        h_lastRow = h.iloc[-1]

        # Apply stock-split adjustment to match YF
        for c in ["Open", "Close", "Low", "High", "Dividends"]:
            h[c] = h[c].to_numpy() * h["CSF"].to_numpy()
        h["Volume"] = (h["Volume"].to_numpy() / h["CSF"].to_numpy()).round().astype('int')

        td_1d = pd.Timedelta("1D")
        dt_now = pd.Timestamp.utcnow().tz_convert(ZoneInfo("UTC"))

        def _aggregate_yfdf_daily(df):
            df2 = df.copy()
            df2["_date"] = df2.index.date
            df2.loc[df2["Stock Splits"] == 0, "Stock Splits"] = 1
            if "CDF" in df.columns:
                df2 = df2.groupby("_date").agg(
                    Open=("Open", "first"),
                    Close=("Close", "last"),
                    Low=("Low", "min"),
                    High=("High", "max"),
                    Volume=("Volume", "sum"),
                    Dividends=("Dividends", "sum"),
                    StockSplits=("Stock Splits", "prod"),
                    CDF=("CDF", "prod"),
                    CSF=("CSF", "prod"),
                    FetchDate=("FetchDate", "first")).rename(columns={"StockSplits": "Stock Splits"})
            else:
                df2 = df2.groupby("_date").agg(
                    Open=("Open", "first"),
                    Close=("Close", "last"),
                    AdjClose=("Adj Close", "last"),
                    Low=("Low", "min"),
                    High=("High", "max"),
                    Volume=("Volume", "sum"),
                    Dividends=("Dividends", "sum"),
                    StockSplits=("Stock Splits", "prod")).rename(columns={"StockSplits": "Stock Splits", "AdjClose": "Adj Close"})
            df2.loc[df2["Stock Splits"] == 1, "Stock Splits"] = 0
            df2.index.name = df.index.name
            df2.index = pd.to_datetime(df2.index).tz_localize(df.index.tz)
            return df2

        # For intraday data older than Yahoo limit, compare aggregated against 1d
        if self.intraday:
            dt_now_local = dt_now.tz_convert(self.tzName)
            if self.interval == yfcd.Interval.Hours1:
                max_lookback_days = 365*2
            elif self.interval == yfcd.Interval.Mins1:
                # max_lookback_days = 7
                max_lookback_days = 30
            else:
                max_lookback_days = 60
            max_lookback = timedelta(days=max_lookback_days)
            max_lookback -= timedelta(minutes=5)  # allow time for server processing
            fetch_start_min = dt_now_local - max_lookback
            if self.intraday:
                fetch_start_min = fetch_start_min.ceil("1D")
            if h.index[0] < fetch_start_min:
                # h_old = h.loc[:fetch_start_min-timedelta(seconds=1)]
                # h_old_1d = _aggregate_yfdf_daily(h_old.drop(["Final?", "C-Check?", "LastDivAdjustDt", "LastSplitAdjustDt"], axis=1))

                # if self.interval == yfcd.Interval.Hours1:
                #     df_yf = self.dat.history(interval="1d", start=h_old.index[0].date(), end=h_old.index[-1].date()+td_1d, auto_adjust=False, repair="silent")
                # else:
                #     df_yf = self.dat.history(interval="1h", start=h_old.index[0].date(), end=h_old.index[-1].date()+td_1d, auto_adjust=False, repair="silent")
                #     df_yf = _aggregate_yfdf_daily(df_yf)

                # # Aggregated data will almost always differ from actual daily, so only
                # # look for big errors
                # f_old_diff = yfcu.VerifyPricesDf(h_old_1d, df_yf, self.interval, rtol=0.2, debug=True)
                # raise Exception("- investigate")
                # f_old_diff = yfcu.VerifyPricesDf(h_old_1d, df_yf, self.interval, rtol=0.2)
                # if f_old_diff.any():
                #     msg = f"Significant differences detected between old {self.istr} data and 1d."
                #     print()
                #     # f_diff_all = f_diff_all | f_old_diff
                #     bad_dates = f_old_diff.index.date[f_old_diff]
                #     f_diff_all[np.isin(f_diff_all.index.date, bad_dates)] = True

                if not isinstance(discard_old, bool):
                    msg = f"{self.ticker}: Some {self.istr} data is now older than {max_lookback_days} days" +\
                            " so cannot compare to Yahoo. Discard old data?"
                    discard_old = click.confirm(msg, default=False)
                if discard_old:
                    f_discard = pd.Series(h.index < fetch_start_min, h.index)
                    if f_discard.any():
                        if debug:
                            print(f"Discarding {np.sum(f_discard)}/{n} old rows because can't verify (fetch_start_min={fetch_start_min})")
                        f_diff_all = f_diff_all | f_discard
                        f_diff_all = f_diff_all.rename("Discard")

                h = h.loc[fetch_start_min:]

        if self.interval == yfcd.Interval.Days1:
            # Also verify dividends
            divs_df = self.manager.GetHistory("Events").GetDivs()
            if divs_df is not None:
                divs_df = divs_df[(divs_df['Dividends']!=0.0).to_numpy()]
                if divs_df.empty:
                    divs_df = None

        if not h.empty:
            # Fetch YF data
            start_dt = h.index[0]
            last_dt = h.index[-1]
            end_dt = last_dt + self.itd
            fetch_start = start_dt.date()
            if self.itd > timedelta(days=1):
                fetch_end = last_dt.date()+yfcd.intervalToTimedelta[self.interval]
            else:
                fetch_end = last_dt.date()+td_1d
            # Sometimes Yahoo doesn't return full trading data for last day if end = day after.
            # Add some more days to avoid problem.
            fetch_end += 3*td_1d
            fetch_end = min(fetch_end, dt_now.tz_convert(self.tzName).ceil("D", nonexistent='shift_forward').date())
            history_args_base = {'raise_errors': True, 'repair': True}
            if self.intraday:
                history_args_base_intraday = history_args_base | {'interval': self.istr, 'auto_adjust': False, 'repair': True, 'keepna': True}
                if self.interval == yfcd.Interval.Mins1:
                    # Fetch in 7-day batches
                    df_yf = None
                    td_7d = timedelta(days=7)
                    fetch_end_batch = fetch_end
                    fetch_start_batch = fetch_end - td_7d
                    while fetch_end_batch > fetch_start:
                        history_args = history_args_base_intraday | {'start': fetch_start_batch, 'end': fetch_end_batch}
                        df_yf_batch = self.dat.history(**history_args)
                        if "Repaired?" not in df_yf_batch.columns:
                            df_yf_batch["Repaired?"] = False
                        if df_yf is None:
                            df_yf = df_yf_batch
                        else:
                            df_yf = pd.concat([df_yf, df_yf_batch], axis=0)
                        #
                        fetch_end_batch -= td_7d
                        fetch_start_batch -= td_7d
                        fetch_start_batch = max(fetch_start_batch, fetch_start)
                    #
                    df_yf = df_yf.sort_index()
                else:
                    history_args = history_args_base_intraday | {'start': fetch_start, 'end': fetch_end}
                    df_yf = self.dat.history(**history_args)
                    if "Repaired?" not in df_yf.columns:
                        df_yf["Repaired?"] = False
                    df_yf = df_yf.loc[start_dt:]
                    df_yf = df_yf[df_yf.index < end_dt]

                # Yahoo doesn't div-adjust intraday
                history_args_1d = history_args_base | {'interval': '1d', 'start' :df_yf.index[0].date(), 'end': df_yf.index[-1].date()+td_1d, 'auto_adjust': False}
                df_yf_1d = self.dat.history(**history_args_1d)
                if "Repaired?" not in df_yf_1d.columns:
                    df_yf_1d["Repaired?"] = False
                df_yf["_indexBackup"] = df_yf.index
                df_yf["_date"] = df_yf.index.date
                df_yf_1d["_date"] = df_yf_1d.index.date
                #
                df_yf_1d["Adj"] = df_yf_1d["Adj Close"].to_numpy() / df_yf_1d["Close"].to_numpy()
                df_yf = df_yf.merge(df_yf_1d[["Adj", "_date"]], how="left", on="_date")
                df_yf["Adj Close"] = df_yf["Close"].to_numpy() * df_yf["Adj"].to_numpy()
                df_yf = df_yf.drop("Adj", axis=1)
                #
                df_yf.index = df_yf["_indexBackup"]
                df_yf = df_yf.drop(["_indexBackup", "_date"], axis=1)
            else:
                if self.interval == yfcd.Interval.Days1 and divs_df is not None and not divs_df.empty:
                    # Also use YF data to verify dividends.
                    fetch_start = min(fetch_start, divs_df.index[0].date())
                # Add buffer intervals (1 before, 1 after) so YF can repair dividends
                fetch_start -= self.itd
                fetch_end += self.itd
                history_args = history_args_base | {'interval': self.istr, 'start': fetch_start, 'end': fetch_end, 'auto_adjust': False, 'repair': True, 'keepna': True}
                df_yf = self.dat.history(**history_args)
                if "Repaired?" not in df_yf.columns:
                    df_yf["Repaired?"] = False
                if df_yf.empty:
                    raise Exception(f"{self.ticker}: YF fetch failed for {self.istr} {fetch_start} -> {fetch_end}")
                if self.interval == yfcd.Interval.Week:
                    df_yf_adj = self.dat.history(**(history_args|{'auto_adjust':True}))
                # Make special adjustments for dividends / stock splits released TODAY
                if self.interval == yfcd.Interval.Days1:
                    if df_yf.index[-1] == h_lastRow.name and df_yf["Stock Splits"].iloc[-1] != 0 and h_lastRow["Stock Splits"] == 0:
                        # YFC doesn't have record of today's split yet so remove effect
                        rev_adj = df_yf["Stock Splits"].iloc[-1]
                        if debug:
                            msg = f"- removing split from df_yf-{self.istr}: {df_yf.index[-1]} adj={1.0/rev_adj}"
                            yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(f"{self.ticker}: " + msg)
                        for c in ["Open", "High", "Low", "Close", "Adj Close"]:
                            df_yf[c] *= rev_adj
                        df_yf["Volume"] /= rev_adj
                        df_yf.loc[df_yf.index[-1], "Stock Splits"] = 0.0

            if df_yf is None or df_yf.empty:
                raise Exception(f"Fetching reference yfinance data failed (interval={self.istr}, start_dt={start_dt}, last_dt={last_dt})")
            if self.interval == yfcd.Interval.Week:
                # Ensure data aligned to Monday:
                if not df_yf.index[0].weekday() == 0:
                    n = 0
                    history_args_1wk_base = history_args_base | {'interval': self.istr, 'auto_adjust': False, 'repair': True}
                    while n < 3:
                        fetch_start -= timedelta(days=2)
                        history_args = history_args_1wk_base | {'start': fetch_start, 'end': fetch_end}
                        df_yf = self.dat.history(**history_args)
                        if "Repaired?" not in df_yf.columns:
                            df_yf["Repaired?"] = False
                        n += 1
                        if df_yf.index[0].weekday() == 0:
                            break
                    if not df_yf.index[0].weekday() == 0:
                        raise Exception("Failed to get Monday-aligned weekly data from YF")
                    df_yf = df_yf.loc[h.index[0]:]
                    if self.interval == yfcd.Interval.Week:
                        df_yf_adj = self.dat.history(**(history_args|{'auto_adjust':True}))
                        df_yf_adj = df_yf_adj.loc[h.index[0]:]

            if self.interval == yfcd.Interval.Week:
                df_yf = df_yf.drop('Adj Close', axis=1)
                for c in ['Open', 'High', 'Low', 'Close']:
                    df_yf['Adj '+c] = df_yf_adj[c]

            if self.intraday:
                # Volume not split-adjusted
                ss = df_yf["Stock Splits"].copy()
                ss[(ss == 0.0) | ss.isna()] = 1.0
                ss_rcp = 1.0 / ss
                csf = ss_rcp.sort_index(ascending=False).cumprod().sort_index(ascending=True).shift(-1, fill_value=1.0)
                df_yf["Volume"] = df_yf["Volume"].to_numpy() / csf

            if self.interval != yfcd.Interval.Days1 and correct in ['one', 'all']:
                # Copy over any missing dividends
                c = "Dividends"
                h_divs = h.loc[h[c] != 0.0, c].copy().dropna()
                yf_divs = df_yf.loc[df_yf[c] != 0.0, c]
                dts_missing_from_cache = yf_divs.index[~yf_divs.index.isin(h_divs.index)]
                dts_missing_from_cache = [dt for dt in dts_missing_from_cache if dt in h.index]
                if len(dts_missing_from_cache) > 0:
                    if debug:
                        msg = f"CORRECTING: Cache missing these dividends: {dts_missing_from_cache}"
                        yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(f"{self.ticker}: " + msg)
                    for dt in dts_missing_from_cache:
                        # Correct here
                        h.loc[dt, c] = yf_divs.loc[dt]
                        h_new.loc[dt, c] = yf_divs.loc[dt]
                        h_modified = True

                # Copy over any missing stock splits
                c = "Stock Splits"
                h_ss = h.loc[h[c] != 0.0, c].copy().dropna()
                yf_ss = df_yf.loc[df_yf[c] != 0.0, c]
                dts_missing_from_cache = yf_ss.index[~yf_ss.index.isin(h_ss.index)]
                dts_missing_from_cache = [dt for dt in dts_missing_from_cache if dt in h.index]
                if len(dts_missing_from_cache) > 0:
                    if debug:
                        msg = f"CORRECTING: Cache missing these stock splits: {dts_missing_from_cache}"
                        yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(f"{self.ticker}: " + msg)
                    for dt in dts_missing_from_cache:
                        # Correct here
                        h.loc[dt, c] = yf_ss.loc[dt]
                        h_new.loc[dt, c] = yf_ss.loc[dt]
                        h_modified = True

            if self.interval == yfcd.Interval.Days1 and divs_df is not None and not divs_df.empty:
                # Verify dividends
                c = "Dividends"
                divs = divs_df['Dividends']
                yf_divs = df_yf['Dividends'][df_yf['Dividends']!=0.0]
                f_orphan = ~divs.index.isin(yf_divs.index)
                if f_orphan.any():
                    if correct in ['one', 'all']:
                        print(f'Dropping these orphan dividends: {divs.index.date[f_orphan]}')
                        orphan_divs = divs_df.loc[divs.index[f_orphan], [c, 'FetchDate']].copy().dropna()
                        orphan_divs['Dividends'] = 0.0
                        orphan_divs['Close before'] = 1.0
                        orphan_divs['FetchDate'] = dt_now
                        self.manager.GetHistory("Events").UpdateDividends(orphan_divs)
                    else:
                        if not quiet:
                            orphan_divs = divs[f_orphan]
                            orphan_divs.index = orphan_divs.index.date
                            print(f'- detected orphan dividends: { {str(k):v for k,v in orphan_divs.to_dict().items()} }')
                        for dt in divs.index[f_orphan]:
                            f_diff_all[dt] = True

            f_diff = yfcu.VerifyPricesDf(h, df_yf, self.interval, rtol=rtol, vol_rtol=vol_rtol, quiet=quiet, debug=debug, exit_first_error=True)
            if f_diff.any():
                if not f_diff_all.any():
                    f_diff_all = (f_diff_all | f_diff).rename(f_diff.name)
                else:
                    f_diff_all = f_diff_all | f_diff

        if not f_diff_all.any():
            if h_modified:
                # yfcm.StoreCacheDatum(self.ticker, self.cache_key, h)
                yfcm.StoreCacheDatum(self.ticker, self.cache_key, h_new)
                self.h = self._getCachedPrices()

            yfcl.TraceExit(f"PM::_verifyCachedPrices-{self.istr}() returning True")
            return True

        # If the diff_rows were all fetched very recently, and it or YF contains repair,
        # then means the repair depends on quantity of data. Clear cache.
        if correct:
            diffs_fetched_just_now = (h['FetchDate'][f_diff_all] > (dt_now-timedelta(minutes=5))).all()
            if diffs_fetched_just_now:
                msg = f"{self.ticker}: {self.istr}-prices problems"
                msg += f": dropping entire cached prices because recent minimal refetch didn't match YF"
                yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(f"{self.ticker}: " + msg)
                h = None
                h_modified = True

        if h is not None:
            # Update: adding "buffer rows", so that when fetching discarded data, there is
            # enough for YF to detect & repair 100x dividend errors.
            f_diff_all = f_diff_all | np.roll(f_diff_all, -1) | np.roll(f_diff_all, 1)

            n = h.shape[0]
            f = h['Final?'].to_numpy()
            n_diff = np.sum(f_diff_all.to_numpy())
            n_final = np.sum(f)
            if n_final == 0:
                error_pct_final = 0.0
            else:
                error_pct_final = n_diff / n_final
            if n > 10 and n_final > 0 and error_pct_final > 0.95:
                # If almost-all final data is marked as bad, then treat entire table as bad.
                f_diff_all.loc[~f_diff_all.to_numpy()] = True
            elif self.multiday and f_diff_all.name is not None and 'Dividends' in f_diff_all.name:
                # Correction needs a total refetch
                f_diff_all.loc[~f_diff_all.to_numpy()] = True

            h = h_new
            if correct in ['one', 'all']:
                drop_dts = f_diff_all.index[f_diff_all]
                drop_dts_not_recent = drop_dts
                msg = f"{self.ticker}: {self.istr}-prices problems"
                if f_diff_all.name == ";Dividends":
                    # Need to discard at least 1-year of data, so the refetch can repair reliably.
                    # And one week before, just-in-case div @ first drop dt.
                    drop_from_date = min(drop_dts_not_recent[0]-timedelta(days=7), dt_now-timedelta(days=375))
                    msg += f": dropping all rows from {drop_from_date.date()} for reliable div-repair"
                    yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(f"{self.ticker}: " + msg)
                    h = h[h.index < drop_from_date]
                    h_modified = True
                elif self.contiguous:
                    # Daily must always be contiguous, so drop everything from first diff
                    if len(drop_dts_not_recent) > 0:
                        if len(drop_dts_not_recent) == 1:
                            msg += f": dropping {drop_dts_not_recent[0].date()}"
                        else:
                            if self.interday:
                                msg += f": dropping all rows from {drop_dts_not_recent[0].date()}"
                            else:
                                msg += f": dropping all rows from {drop_dts_not_recent[0]}"
                        yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(f"{self.ticker}: " + msg)
                        h = h[h.index < drop_dts_not_recent[0]]
                        h_modified = True
                else:
                    n = self.h.shape[0]
                    n_drop = np.sum(f_diff_all)
                    if len(drop_dts_not_recent) > 0:
                        if len(drop_dts_not_recent) < 10:
                            if self.interday:
                                msg += f": dropping {drop_dts_not_recent.date.astype(str)}"
                            else:
                                msg += f": dropping {drop_dts_not_recent.tz_localize(None)}"
                        else:
                            msg += f": dropping {n_drop}/{n} rows"
                        yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(f"{self.ticker}: " + msg)
                        h = h.drop(drop_dts_not_recent)
                        h_modified = True
                if h is not None and h.empty:
                    h = None
                    h_modified = True

            else:
                if debug:
                    n = np.sum(f_diff_all)
                    if n < 5:
                        msg = "differences found but not correcting: "
                        if self.interday:
                            msg += f"{f_diff_all.index[f_diff_all].date.astype(str)}"
                        else:
                            msg += f"{f_diff_all.index[f_diff_all]}"
                    else:
                        msg = f"{np.sum(f_diff_all)} differences found but not correcting"
                    yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(f"{self.ticker}: " + msg)

        if correct in ['one', 'all'] and f_diff_all.name == ";Dividends" and self.interval != yfcd.Interval.Days1:
            # All differences caused by bad dividend data.
            # To fix, need to force a re-fetch of 1d data.
            hist1d = self.manager.GetHistory(yfcd.Interval.Days1)
            h1d = hist1d._getCachedPrices()
            # Update: force re-fetch anyway, as I improved repair of dividends. And discard more data.
            # Force a total re-fetch of 1d
            msg = f"hist-{self.istr} is discarding 1d data to force re-fetch of dividends"
            h1d = None

            yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(f"{self.ticker}: " + msg)
            hist1d._updatedCachedPrices(h1d)

        if h_modified:
            yfcm.StoreCacheDatum(self.ticker, self.cache_key, h)
            self.h = self._getCachedPrices()

        yfcl.TraceExit(f"PM::_verifyCachedPrices-{self.istr}() returning False")
        return False

    def _fetchYfHistory(self, start, end, prepost, debug, verify_intervals=True, disable_yfc_metadata=False, quiet=False):
        if start is None and end is None:
            raise ValueError("Must provide value for one of: 'start', 'end'")
        if start is not None:
            yfcu.TypeCheckIntervalDt(start, self.interval, "start")
        if end is not None:
            yfcu.TypeCheckIntervalDt(end, self.interval, "end")
        yfcu.TypeCheckBool(prepost, "prepost")
        yfcu.TypeCheckBool(debug, "debug")

        debug_yfc = False
        # debug_yfc = True

        log_msg = f"PM::_fetchYfHistory-{self.istr}({start}->{end}, prepost={prepost})"
        if yfcl.IsTracingEnabled():
            yfcl.TraceEnter(log_msg)
        elif debug_yfc:
            print("")
            print(log_msg)

        tz_exchange = self.tz
        td_1d = timedelta(days=1)
        dt_now = pd.Timestamp.utcnow().tz_convert(ZoneInfo("UTC"))

        if self.intraday:
            maxLookback = yfcd.yfMaxFetchLookback[self.interval] - timedelta(seconds=10)
            if maxLookback is not None:
                start = max(start, dt_now - maxLookback)
                if start >= end:
                    return None

        fetch_start = start
        fetch_end = end

        if end is not None:
            # If 'fetch_end' in future then cap to exchange midnight
            dtnow_exchange = dt_now.tz_convert(tz_exchange)
            if isinstance(end, datetime):
                end_dt = end
                # end_d = end.astimezone(tz_exchange).date()
                end_d = None
            else:
                end_d = end
                end_dt = datetime.combine(end, time(0), tz_exchange)
            if end_dt > dt_now:
                exchange_midnight_dt = datetime.combine(dtnow_exchange.date()+td_1d, time(0), tz_exchange)
                if isinstance(end, datetime):
                    fetch_end = exchange_midnight_dt
                else:
                    fetch_end = exchange_midnight_dt.date()
        if start is not None:
            if isinstance(start, datetime):
                start_dt = start
                # start_d = start.astimezone(tz_exchange).date()
                start_d = None
            else:
                start_d = start
                start_dt = datetime.combine(start, time(0), tz_exchange)

            if (fetch_start is not None) and (fetch_end <= fetch_start):
                return None

        if fetch_start is not None:
            if not isinstance(fetch_start, (datetime, pd.Timestamp)):
                fetch_start_dt = datetime.combine(fetch_start, time(0), self.tz)
            else:
                fetch_start_dt = fetch_start
        if fetch_end is not None:
            if not isinstance(fetch_end, (datetime, pd.Timestamp)):
                fetch_end_dt = datetime.combine(fetch_end, time(0), tz_exchange)
            else:
                fetch_end_dt = fetch_end

        if fetch_start is not None:
            if self.interval == yfcd.Interval.Week:
                # Ensure aligned to week start:
                fetch_start -= timedelta(days=fetch_start.weekday())

        td_1d = timedelta(days=1)
        td_14d = timedelta(days=14)
        if self.interval == yfcd.Interval.Days1:
            # Add padding days to ensure Yahoo returns correct Volume
            s = yfct.GetExchangeSchedule(self.exchange, fetch_start - td_14d, fetch_end + td_14d)
            fetch_start_pad = s.iloc[s.index.get_indexer([str(fetch_start)], method="ffill")[0]-1].name.date()

            first_fetch_failed = False
            try:
                df = self._fetchYfHistory_dateRange(fetch_start_pad, fetch_end, prepost, debug, quiet=quiet)
            except yfcd.NoPriceDataInRangeException as e:
                first_fetch_failed = True
                ex = e

            if first_fetch_failed and fetch_end is not None:
                # Try with wider date range, maybe entire range is just before listing date
                second_fetch_failed = False
                df_wider = None
                listing_date_check_tol = yfcd.listing_date_check_tols[self.interval]
                fetch_start -= 2*listing_date_check_tol
                fetch_end += 2*listing_date_check_tol
                if debug_yfc:
                    msg = "- first fetch failed, trying again with wider range: {} -> {}".format(fetch_start, fetch_end)
                    yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(msg)
                try:
                    df_wider = self._fetchYfHistory_dateRange(fetch_start, fetch_end, prepost, debug, quiet=quiet)
                    if debug_yfc:
                        msg = "- second fetch returned:"
                        yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(msg)
                        print(df_wider)
                except yfcd.NoPriceDataInRangeException:
                    second_fetch_failed = True

                if df_wider is not None:
                    if debug_yfc:
                        print("- detected listing date =", df_wider.index[0].date())
                    hist_md = yfcm.ReadCacheDatum(self.ticker, "history_metadata")
                    if hist_md is None:
                        hist_md = {}
                    hist_md = {'listingDate': df_wider.index[0].date()}
                    yfcm.StoreCacheDatum(self.ticker, "history_metadata", hist_md)

                    df = df_wider

            if first_fetch_failed:
                if second_fetch_failed:
                    # Hopefully code never comes here.
                    # raise ex
                    # Update: is possible for suspended tickers
                    self.manager.LogEvent("info", "PriceManager", str(ex))
                    return None
                else:
                    # Requested date range was just before stock listing date,
                    # but wider range crosses over so can continue
                    pass

        elif self.interday:
            # Add padding days to ensure Yahoo returns correct Volume
            s = yfct.GetExchangeSchedule(self.exchange, fetch_start - 2*self.itd, fetch_end + 2*self.itd)
            fetch_start_pad = s.iloc[s.index.get_indexer([str(fetch_start)], method="ffill")[0]-1].name.date()
            fetch_end_pad   = s.iloc[s.index.get_indexer([str(fetch_end)], method="bfill")[0]+1].name.date()

            df = self._fetchYfHistory_dateRange(fetch_start_pad, fetch_end_pad, prepost, debug, quiet=quiet)

        else:
            # Intraday
            fetch_ranges = [(fetch_start, fetch_end)]
            if self.intraday:
                # Add padding days to ensure Yahoo returns correct Volume
                maxRange = yfcd.yfMaxFetchRange[self.interval]
                if maxRange is not None:
                    s = yfct.GetExchangeSchedule(self.exchange, start_dt.date() - td_14d, end_dt.date() + td_14d)
                    s = s.iloc[s.index.get_indexer([str(start_dt.date())], method="ffill")[0]-1:]
                    s = s.iloc[:s.index.get_indexer([str(end_dt.date())], method="bfill")[0]+1+1]
                    lag = yfcd.exchangeToYfLag[self.exchange]
                    if start_dt > s["close"].iloc[1]+lag:
                        s = s.drop(s.index[0])
                    if end_dt < s["open"].iloc[-2]+lag:
                        s = s.drop(s.index[-1])
                    # fetch_ranges = yfcu.ChunkDatesIntoYfFetches(start_d, end_d, s, maxRange.days, overlapDays=2)
                    fetch_ranges = yfcu.ChunkDatesIntoYfFetches(s, maxRange.days, overlapDays=2)
                    if debug_yfc:
                        print("- fetch_ranges:")
                        pprint(fetch_ranges)
                    # Don't need to fetch all of padding days, just the end/start of session
                    # fetch_ranges[0][0] = s["close"].iloc[0] - timedelta(hours=2)
                    # fetch_ranges[-1][1] = s["open"].iloc[-1] + timedelta(hours=2)
                    # fetch_ranges[0]["fetch start"] = s["close"].iloc[0] - timedelta(hours=2)
                    # Update: need start further back for low-volume tickers
                    fetch_ranges[0]["fetch start"] = s["open"].iloc[0]
                    fetch_ranges[-1]["fetch end"] = s["open"].iloc[-1] + timedelta(hours=2)
                    maxLookback = yfcd.yfMaxFetchLookback[self.interval] - timedelta(seconds=10)
                    if maxLookback is not None:
                        maxLookback_dt = (dt_now - maxLookback).tz_convert(tz_exchange)
                        for i in range(len(fetch_ranges)-1, -1, -1):
                            if fetch_ranges[i]["fetch start"] < maxLookback_dt:
                                if debug_yfc:
                                    print("- capping start to maxLookback_dt")
                                # fetch_ranges[i]["fetch start"] = maxLookback_dt
                                fetch_ranges[i]["fetch start"] = maxLookback_dt.ceil("D")
                                fetch_ranges[i]["core start"] = fetch_ranges[i]["fetch start"] + td_1d
                                if fetch_ranges[i]["fetch start"] >= fetch_ranges[i]["fetch end"]:
                                    del fetch_ranges[i]

            df = None
            for r in fetch_ranges:
                if debug_yfc:
                    print("- fetching:")
                    print(r)
                fetch_start = r["fetch start"]
                fetch_end = r["fetch end"]
                dfr = self._fetchYfHistory_dateRange(fetch_start, fetch_end, prepost, debug, quiet=quiet)
                # Discard padding days:
                dfr = dfr.loc[r["core start"]: r["core end"] - timedelta(milliseconds=1)]
                if debug_yfc:
                    print("- dfr after discarding padding days:")
                    print(dfr[[c for c in ["Open", "Low", "High", "Close", "Dividends", "Volume"] if c in dfr.columns]])
                if df is None:
                    df = dfr
                else:
                    df = pd.concat([df, dfr[df.columns]])
                if df.index.duplicated().any():
                    raise Exception("df contains duplicated dates")

        fetch_dt_utc = pd.Timestamp.utcnow().tz_convert(ZoneInfo("UTC"))

        if (df is not None) and (df.index.tz is not None) and (not isinstance(df.index.tz, ZoneInfo)):
            # Convert to ZoneInfo
            df.index = df.index.tz_convert(tz_exchange)

        if debug_yfc:
            if df is None:
                msg = "- YF returned None"
                yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(msg)
            else:
                # pass
                msg = "- YF returned table:"
                yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(msg)
                df_pretty = df[[c for c in ["Open", "Low", "High", "Close", "Dividends", "Volume", 'Repaired?'] if c in df.columns]]
                print(df_pretty)

        # Detect listing day
        hist_md = yfcm.ReadCacheDatum(self.ticker, "history_metadata")
        if hist_md is None or 'listingDate' not in hist_md:
            listing_day = None
        else:
            listing_day = hist_md['listingDate']
        if listing_day is None:
            if self.interval == yfcd.Interval.Days1:
                found_listing_day = False
                listing_day = None
                if df is not None and not df.empty:
                    tol = yfcd.listing_date_check_tols[self.interval]
                    fetch_start_d = fetch_start.date() if isinstance(fetch_start, datetime) else fetch_start
                    if (df.index[0].date() - fetch_start_d) > tol:
                        # Yahoo returned data starting significantly after requested start date, indicates
                        # request is before stock listed on exchange
                        found_listing_day = True
                    if debug_yfc:
                        msg = "- found_listing_day = {}".format(found_listing_day)
                        yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(msg)
                    if found_listing_day:
                        listing_day = df.index[0].date()
                        if debug_yfc:
                            msg = "YFC: inferred listing_date = {}".format(listing_day)
                            yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(msg)
                        if hist_md is None:
                            hist_md = {}
                        hist_md['listingDate'] = listing_day
                        yfcm.StoreCacheDatum(self.ticker, "history_metadata", hist_md)

                    if (listing_day is not None) and first_fetch_failed:
                        if end <= listing_day:
                            # Aha! Requested date range was entirely before listing
                            if debug_yfc:
                                msg = "- requested date range was before listing date"
                                yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(msg)
                            return None
                if found_listing_day and start is not None:
                    # Apply to fetch start
                    if isinstance(start, datetime):
                        listing_date = datetime.combine(listing_day, time(0), self.tz)
                        start = max(start, listing_date)
                    else:
                        start = max(start, listing_day)
                        start_d = start

        if df is None:
            received_interval_starts = None
        else:
            if self.interday:
                received_interval_starts = df.index.date
            else:
                received_interval_starts = df.index.to_pydatetime()
        try:
            intervals_missing_df = yfct.IdentifyMissingIntervals(self.exchange, start, end, self.interval, received_interval_starts, ignore_breaks=True)
        except yfcd.NoIntervalsInRangeException:
            intervals_missing_df = None
        if (intervals_missing_df is not None) and (not intervals_missing_df.empty):
            # First, ignore any missing intervals today
            # For missing intervals during last 2 weeks, if few in number, then fill with NaNs
            # For missing intervals older than 2 weeks, fill all with NaNs

            if debug_yfc:
                n = intervals_missing_df.shape[0]
                if n <= 3:
                    msg = f"YF data missing {n} intervals: {intervals_missing_df['open'].to_numpy()}"
                else:
                    msg = f"YF data missing {n} intervals"
                yfcl.TracePrint('- ' + msg) if yfcl.IsTracingEnabled() else print('- ' + msg)

            cutoff_d = date.today() - timedelta(days=14)
            if self.interday:
                f_recent = intervals_missing_df["open"].to_numpy() > cutoff_d
            else:
                f_recent = intervals_missing_df["open"].dt.date > cutoff_d
            intervals_missing_df_recent = intervals_missing_df[f_recent]
            intervals_missing_df_old = intervals_missing_df[~f_recent]
            missing_intervals_to_add = None
            if not intervals_missing_df_old.empty:
                missing_intervals_to_add = intervals_missing_df_old["open"].to_numpy()

            if not intervals_missing_df_recent.empty:
                # If very few intervals and not today (so Yahoo should have data),
                # then assume no trading occurred and insert NaN rows.
                # Normally Yahoo has already filled with NaNs but sometimes they forget/are late
                nm = intervals_missing_df_recent.shape[0]
                if self.interday:
                    threshold = 1
                else:
                    if self.itd <= timedelta(minutes=2):
                        threshold = 10
                    elif self.itd <= timedelta(minutes=5):
                        threshold = 3
                    else:
                        threshold = 2
                if nm <= threshold:
                    if debug_yfc:
                        msg = "- found missing intervals, inserting nans:"
                        yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(msg)
                        print(intervals_missing_df_recent)
                    if missing_intervals_to_add is None:
                        missing_intervals_to_add = intervals_missing_df_recent["open"].to_numpy()
                    else:
                        missing_intervals_to_add = np.append(missing_intervals_to_add, intervals_missing_df_recent["open"].to_numpy())

            if missing_intervals_to_add is not None:
                n = missing_intervals_to_add.shape[0]
                if n <= 3:
                    msg = f"insertings NaNs for {n} missing intervals: {missing_intervals_to_add}"
                else:
                    msg = f"insertings NaNs for {n} missing intervals"
                if debug_yfc:
                    yfcl.TracePrint('- ' + msg) if yfcl.IsTracingEnabled() else print('- ' + msg)
                else:
                    self.manager.LogEvent("info", "PriceManager", msg)

                nm = missing_intervals_to_add.shape[0]
                df_missing = pd.DataFrame(data={k: [np.nan]*nm for k in yfcd.yf_data_cols}, index=missing_intervals_to_add)
                df_missing['Volume'] = 0 # Needs to be int type
                if "Repaired?" in df.columns:
                    df_missing["Repaired?"] = False
                df_missing.index = pd.to_datetime(df_missing.index)
                if self.interday:
                    df_missing.index = df_missing.index.tz_localize(tz_exchange)
                for c in ["Volume", "Dividends", "Stock Splits"]:
                    df_missing[c] = 0
                if df is not None and 'Capital Gains' in df.columns:
                    df_missing['Capital Gains'] = 0
                if df is None:
                    df = df_missing
                else:
                    df = pd.concat([df, df_missing[df.columns]])
                    df.index = pd.to_datetime(df.index, utc=True).tz_convert(tz_exchange)
                    df = df.sort_index()

        # Improve tolerance to calendar missing a recent new holiday:
        if df is None or df.empty:
            return None

        n = df.shape[0]

        fetch_dt = fetch_dt_utc.replace(tzinfo=ZoneInfo("UTC"))

        if self.interval == yfcd.Interval.Days1:
            # Update: move checking for new dividends to here, before discarding out-of-range data
            df_divs = df[df["Dividends"] != 0][["Dividends"]].copy()
            if not df_divs.empty:
                df_divs['Close before'] = df['Close'].ffill().shift(1).loc[df_divs.index]
                df_divs['Close repaired?'] = df['Repaired?'].ffill().shift(1).loc[df_divs.index]
                close_before_na = df_divs['Close before'].isna().to_numpy()
                if close_before_na[0]:
                    # Shouldn't be a problem because this row should already be in cache, 
                    # or is outside fetch range. So can be dropped.
                    if len(df_divs) == 1:
                        df_divs = pd.DataFrame()
                    else:
                        df_divs = df_divs.iloc[1:]
                if not df_divs.empty:
                    close_before_na = df_divs['Close before'].isna().to_numpy()
                    if close_before_na.any():
                        # actual close before was NaN, so look back a little more
                        for i in np.where(close_before_na)[0]:
                            dt = df_divs.index[i]
                            df_divs.loc[dt, 'Close before'] = df['Close'].dropna().shift(1).loc[dt-timedelta(days=4):dt].iloc[-1]
                        
                    df_divs['FetchDate'] = fetch_dt_utc
                    df_divs['Desplitted?'] = False
                    if debug_yfc:
                        print("- df_divs:")
                        print(df_divs)
                    cached_new_divs = yfcm.ReadCacheDatum(self.ticker, "new_divs")
                    if cached_new_divs is not None:
                        if 'Desplitted?' not in cached_new_divs.columns:
                            cached_new_divs['Desplitted?'] = False
                        if 'Close before' not in cached_new_divs.columns:
                            cached_new_divs['Close before'] = np.nan
                        if 'Close repaired?' not in cached_new_divs.columns:
                            cached_new_divs['Close repaired?'] = False
                        df_divs = df_divs[~df_divs.index.isin(cached_new_divs.index)]
                        if not df_divs.empty:
                            # Send these out so at least can de-adjust h_pre.
                            self.manager.GetHistory("Events").UpdateDividends(df_divs)

                            # append new divs in df_divs to new_divs
                            divs_pretty = df_divs['Dividends'].copy()
                            divs_pretty.index = divs_pretty.index.date
                            self.manager.LogEvent("info", "DividendManager", f"detected {divs_pretty.shape[0]} new dividends: {divs_pretty} (before reversing adjust)")
                            if yfcm.ReadCacheMetadata(self.ticker, "new_divs", "locked") is not None:
                                # locked
                                pass
                            else:
                                cached_new_divs = pd.concat([cached_new_divs, df_divs])
                                yfcm.StoreCacheDatum(self.ticker, "new_divs", cached_new_divs)
                    else:
                        yfcm.StoreCacheDatum(self.ticker, "new_divs", df_divs)
                        # Send these out so at least can de-adjust h_pre.
                        self.manager.GetHistory("Events").UpdateDividends(df_divs)

        # Remove any out-of-range data:
        if n > 0:
            if end is not None:
                df = df.loc[:fetch_end_dt-timedelta(milliseconds=1)]
                n = df.shape[0]
            #
            # And again for pre-start data:
            if start is not None:
                df = df.loc[fetch_start_dt:]
                n = df.shape[0]

        # Verify that all datetimes match up with actual intervals:
        if n == 0:
            raise yfcd.NoPriceDataInRangeException(self.ticker, self.istr, start, end)
        else:
            if self.interday:
                f = df.index.time != time(0)
                if f.any():
                    print(df[f])
                    raise Exception("Interday data contains times in index")
                yfIntervalStarts = df.index.date
            else:
                yfIntervalStarts = df.index.to_pydatetime()
            #
            if self.intraday and (self.exchange in yfcd.exchangesWithBreaks):
                # Discard any intervals fully within a break
                f_in_break = yfct.TimestampInBreak_batch(self.exchange, yfIntervalStarts, self.interval)
                if f_in_break.any():
                    # Discard these
                    if debug_yfc:
                        msg = "- dropping rows in break times"
                        yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(msg)
                    yfIntervalStarts = yfIntervalStarts[~f_in_break]
                    df = df[~f_in_break]
                    n = df.shape[0]
            #
            intervals = yfct.GetTimestampCurrentInterval_batch(self.exchange, yfIntervalStarts, self.interval, discardTimes=self.interday, ignore_breaks=True)

            f_na = intervals["interval_open"].isna().to_numpy()
            if verify_intervals and f_na.any():
                ts = intervals["interval_open"]
                if len(ts) != len(set(ts)):
                    dups = ts[ts.duplicated(keep=False)]
                    # Drop rows that map to duplicate intervals if no trading occurred.
                    f_no_trades = (df["Volume"] == 0) & ((df["Low"] == df["High"]) | df["Close"].isna())
                    drop_dts = None
                    for i in dups:
                        dts = intervals.index[intervals["interval_open"] == i]
                        dts_is_nan = np.array([f_no_trades[df.index.get_loc(dt)] for dt in dts])
                        if dts_is_nan.all():
                            # Keep first, drop others
                            drop_dts_sub = dts[1:]
                        else:
                            # Keep non-nan, drop nans
                            drop_dts_sub = dts[dts_is_nan]
                        drop_dts = drop_dts_sub if drop_dts is None else np.append(drop_dts, drop_dts_sub)
                    # print("dropping:", drop_dts)
                    yfIntervalStarts = np.delete(yfIntervalStarts, [df.index.get_loc(dt) for dt in drop_dts])
                    intervals = intervals.drop(drop_dts)
                    df = df.drop(drop_dts)
                    n = df.shape[0]
                    f_na = intervals["interval_open"].isna().to_numpy()

                if not self.interday:
                    # For some exchanges (e.g. JSE) Yahoo returns intraday timestamps right on market close.
                    # - remove if volume 0
                    # - else, merge with previous interval
                    df2 = df.copy() ; df2["_date"] = df2.index.date ; df2["_intervalStart"] = df2.index
                    sched = yfct.GetExchangeSchedule(self.exchange, df2["_date"].min(), df2["_date"].max()+td_1d)
                    rename_cols = {"open": "market_open", "close": "market_close"}
                    sched.columns = [rename_cols[c] if c in rename_cols else c for c in sched.columns]
                    sched_df = sched.copy()
                    sched_df["_date"] = sched_df.index.date
                    df2 = df2.merge(sched_df, on="_date", how="left")
                    df2.index = df.index
                    f_close = (df2["_intervalStart"] == df2["market_close"]).to_numpy()
                    f_close = f_close & f_na
                    f_vol0 = df2["Volume"] == 0
                    f_drop = f_vol0 & f_close
                    if f_drop.any():
                        if debug_yfc:
                            msg = "- dropping 0-volume rows starting at market close"
                            yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(msg)
                        yfIntervalStarts = yfIntervalStarts[~f_drop]
                        intervals = intervals[~f_drop]
                        df = df[~f_drop]
                        df2 = df2[~f_drop]
                        n = df.shape[0]
                        f_na = intervals["interval_open"].isna().to_numpy()
                    #
                    f_close = (df2["_intervalStart"] == df2["market_close"]).to_numpy()
                    f_close = f_close & f_na
                    if f_close.any():
                        # Must merge with previous interval. Tricky!
                        df3 = df2[f_close]
                        df3_index_rev = sorted(list(df3.index), reverse=True)
                        for dt in df3_index_rev:
                            i = df.index.get_loc(dt)
                            if i == 0:
                                # Can't fix
                                continue
                            dt_before = df.index[i-1]
                            if (dt-dt_before) <= self.itd:
                                # Merge
                                df_rows = df.iloc[i-1:i+1]
                                df.loc[dt_before, "Low"] = df_rows["Low"].dropna().min()
                                df.loc[dt_before, "High"] = df_rows["High"].dropna().max()
                                df.loc[dt_before, "Open"] = df_rows["Open"].dropna()[0]
                                df.loc[dt_before, "Close"] = df_rows["Close"].dropna()[-1]
                                df.loc[dt_before, "Adj Close"] = df_rows["Adj Close"].dropna()[-1]
                                df.loc[dt_before, "Volume"] = df_rows["Volume"].dropna().sum()

                                yfIntervalStarts = np.delete(yfIntervalStarts, i)
                                intervals = intervals.drop(dt)
                                df = df.drop(dt)
                                n = df.shape[0]
                                f_na = intervals["interval_open"].isna().to_numpy()
                            else:
                                # Previous interval too far, must insert a new interval
                                raise Exception("this code path not tested, review")
                                dt_correct = dt_before + self.itd
                                if dt_correct >= dt:
                                    raise Exception(f"dt_correct={dt_correct} >= dt={dt} , expected <")
                                if dt_correct.date() != dt.date():
                                    raise Exception(f"dt_correct={dt_correct} & dt={dt} , expected same day")
                                df.loc[dt_correct] = df.loc[dt] ; df = df.drop(dt).sort_index()
                                yfIntervalStarts[i] = dt_correct
                                intervals = intervals.drop(dt)
                                intervals.loc[dt_correct] = {"interval_open": dt_correct, "interval_close": df2.loc[dt, "market_close"]}
                                f_na = intervals["interval_open"].isna().to_numpy()

                if f_na.any():
                    f_no_divs_splits = (df['Dividends']==0).to_numpy() & (df['Stock Splits']==0).to_numpy()

                    # For some national holidays when exchange closed, Yahoo fills in row. Clue is 0 volume.
                    # Solution = drop:
                    f_na_zeroVol = f_na & (df["Volume"] == 0).to_numpy()
                    f_na_zeroVol = f_na_zeroVol & f_no_divs_splits
                    if f_na_zeroVol.any():
                        if debug_yfc:
                            msg = "- dropping {} 0-volume rows with no matching interval".format(sum(f_na_zeroVol))
                            yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(msg)
                        f_drop = f_na_zeroVol
                        yfIntervalStarts = yfIntervalStarts[~f_drop]
                        intervals = intervals[~f_drop]
                        df = df[~f_drop]
                        n = df.shape[0]
                        f_na = intervals["interval_open"].isna().to_numpy()
                        f_no_divs_splits = (df['Dividends']==0).to_numpy() & (df['Stock Splits']==0).to_numpy()

                    # ... another clue is row is identical to previous trading day
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
                                msg = "- dropping rows with no interval that are identical to previous row"
                                yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(msg)
                            yfIntervalStarts = yfIntervalStarts[~f_drop]
                            intervals = intervals[~f_drop]
                            df = df[~f_drop]
                            n = df.shape[0]
                            f_na = intervals["interval_open"].isna().to_numpy()
                        f_no_divs_splits = (df['Dividends']==0).to_numpy() & (df['Stock Splits']==0).to_numpy()

                    # ... and another clue is Open=High=Low=0.0
                    if f_na.any():
                        f_zero = (df['Open']==0).to_numpy() & (df['Low']==0).to_numpy() & (df['High']==0).to_numpy()
                        f_zero = f_zero & f_no_divs_splits
                        f_na_zero = f_na & f_zero
                        if f_na_zero.any():
                            if debug_yfc:
                                msg = "- dropping {} price=0 rows with no matching interval".format(sum(f_na_zero))
                                yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(msg)
                            f_drop = f_na_zero
                            yfIntervalStarts = yfIntervalStarts[~f_drop]
                            intervals = intervals[~f_drop]
                            df = df[~f_drop]
                            n = df.shape[0]
                            f_na = intervals["interval_open"].isna().to_numpy()

                if f_na.any() and self.interval == yfcd.Interval.Mins1:
                    # If 1-minute interval at market close, then merge with previous minute
                    indices = sorted(np.where(f_na)[0], reverse=True)
                    for idx in indices:
                        dt = df.index[idx]
                        sched = yfct.GetExchangeSchedule(self.exchange, dt.date(), dt.date()+td_1d)
                        if dt.time() == sched["close"].iloc[0].time():
                            if idx == 0:
                                # Discard
                                print("discarding")
                                pass
                            else:
                                print("merging")
                                # Merge with previous
                                dt1 = df.index[idx-1]
                                df.loc[dt1, "Close"] = df["Close"].iloc[idx]
                                df.loc[dt1, "High"] = df["High"].iloc[idx-1:idx+1].max()
                                df.loc[dt1, "Low"] = df["Low"].iloc[idx-1:idx+1].min()
                                df.loc[dt1, "Volume"] = df["Volume"].iloc[idx-1:idx+1].sum()
                            df = df.drop(dt)
                            intervals = intervals.drop(dt)
                            yfIntervalStarts = np.delete(yfIntervalStarts, idx)
                    f_na = intervals["interval_open"].isna().to_numpy()

                if f_na.any():
                    df_na = df[f_na][["Close", "Volume", "Dividends", "Stock Splits"]]
                    n = df_na.shape[0]
                    warning_msg = f"Failed to map these Yahoo intervals to xcal: (tkr={self.ticker}, exchange={self.exchange}, xcal={yfcd.exchangeToXcalExchange[self.exchange]})."
                    warning_msg += " Normally happens when 'exchange_calendars' is wrong so inform developers."
                    print("")
                    print(warning_msg)
                    print(df_na)
                    msg = "Accept into cache anyway?"
                    accept = yfcm._option_manager.calendar.accept_unexpected_Yahoo_intervals
                    if accept:
                        for idx in np.where(f_na)[0]:
                            dt = intervals.index[idx]
                            if self.interday:
                                intervals.loc[dt, "interval_open"] = df.index[idx].date()
                                intervals.loc[dt, "interval_close"] = df.index[idx].date() + self.itd
                            else:
                                intervals.loc[dt, "interval_open"] = df.index[idx]
                                intervals.loc[dt, "interval_close"] = df.index[idx] + self.itd
                    else:
                        raise Exception("Problem with dates returned by Yahoo, see above")

        if df is None or df.empty:
            return None

        df = df.copy()

        if not disable_yfc_metadata:
            lastDataDts = yfct.CalcIntervalLastDataDt_batch(self.exchange, intervals["interval_open"].to_numpy(), self.interval)
            if f_na.any():
                # Hacky solution to handle xcal having incorrect schedule, for valid Yahoo data
                lastDataDts[f_na] = intervals.index[f_na] + self.itd
                if self.intraday:
                    lastDataDts[f_na] += yfct.GetExchangeDataDelay(self.exchange)
                    # For some exchanges, Yahoo has trades that occurred soon afer official market close, e.g. Johannesburg:
                    if self.exchange in ["JNB"]:
                        lastDataDts[f_na] += timedelta(minutes=15)
                else:
                    # Add ~10 hours to ensure hit next market open
                    lastDataDts[f_na] += timedelta(hours=10)
            data_final = fetch_dt >= lastDataDts
            df["Final?"] = data_final

            # df["FetchDate"] = pd.Timestamp(fetch_dt_utc).tz_localize("UTC")
            df["FetchDate"] = fetch_dt_utc

            df["C-Check?"] = False

        log_msg = f"PM::_fetchYfHistory() returning DF {df.index[0]} -> {df.index[-1]}"
        if yfcl.IsTracingEnabled():
            yfcl.TraceExit(log_msg)
        elif debug_yfc:
            print(log_msg)

        return df

    def _fetchYfHistory_dateRange(self, start, end, prepost, debug, quiet=False):
        yfcu.TypeCheckIntervalDt(start, self.interval, "start")
        yfcu.TypeCheckIntervalDt(end, self.interval, "end")
        yfcu.TypeCheckBool(prepost, "prepost")
        yfcu.TypeCheckBool(debug, "debug")

        debug_yfc = False
        # debug_yfc = True

        log_msg = f"PM::_fetchYfHistory_dateRange-{self.istr}(start={start} , end={end} , prepost={prepost})"
        if yfcl.IsTracingEnabled():
            yfcl.TraceEnter(log_msg)
        elif debug_yfc:
            print("")
            print(log_msg)

        fetch_start = start
        fetch_end = end
        if not isinstance(fetch_start, (datetime, pd.Timestamp)):
            fetch_start_dt = datetime.combine(fetch_start, time(0), self.tz)
        else:
            fetch_start_dt = fetch_start

        history_args = {"period": None,
                        "interval": self.istr,
                        "start": fetch_start, "end": fetch_end,
                        "prepost": prepost,
                        "actions": True,  # Always fetch
                        "keepna": True,
                        "repair": True,
                        "auto_adjust": False,  # store raw data, adjust myself
                        "back_adjust": False,  # store raw data, adjust myself
                        "proxy": self.proxy,
                        "rounding": False,  # store raw data, round myself
                        "raise_errors": not quiet}
        if debug:
            yf_logger = logging.getLogger('yfinance')
            yf_logger.setLevel(logging.DEBUG)  # verbose: print errors & debug info

        if debug_yfc:
            if (not isinstance(fetch_start, datetime)) or fetch_start.time() == time(0):
                start_str = fetch_start.strftime("%Y-%m-%d")
            else:
                start_str = fetch_start.strftime("%Y-%m-%d %H:%M:%S")
            if (not isinstance(fetch_end, datetime)) or fetch_end.time() == time(0):
                end_str = fetch_end.strftime("%Y-%m-%d")
            else:
                end_str = fetch_end.strftime("%Y-%m-%d %H:%M:%S")
            msg = f"- {self.ticker}: fetching {self.istr} {start_str} -> {end_str}"
            yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(msg)

        df = None
        if debug_yfc:
            msg = f"- fetch_start={fetch_start} ; fetch_end={fetch_end}"
            yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(msg)
        try:
            df = self.dat.history(**history_args)
            currency = self.dat.history_metadata.get('currency', None)
            hist_md = yfcm.ReadCacheDatum(self.ticker, "history_metadata")
            if hist_md is None:
                hist_md = {}
            if currency is not None:
                hist_md['currency'] = currency
            yfcm.StoreCacheDatum(self.ticker, "history_metadata", hist_md)
        except yf.exceptions.YFPricesMissingError:
            raise yfcd.NoPriceDataInRangeException(self.ticker, self.istr, start, end)
        if df is None or df.empty:
            raise yfcd.NoPriceDataInRangeException(self.ticker, self.istr, start, end)
        df = df.sort_index()
        if "Repaired?" not in df.columns:
            df["Repaired?"] = False
        if debug_yfc:
            if df is None:
                msg = "- YF returned None"
                yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(msg)
            else:
                msg = "- YF returned table:"
                yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(msg)
                print(df[[c for c in ["Open", "Low", "High", "Close", "Dividends", "Volume"] if c in df.columns]])

        if fetch_start is not None:
            log_msg = f"requested from YF: {self.istr} {history_args['start']} -> {history_args['end']}"
            if self.interday:
                log_msg += f", received: {df.index[0].date()} -> {df.index[-1].date()}"
            else:
                log_msg += f", received: {df.index[0]} -> {df.index[-1]}"
            self.manager.LogEvent("info", "PriceManager", log_msg)

            df = df.loc[fetch_start_dt:]
            if df.empty:
                raise yfcd.NoPriceDataInRangeException(self.ticker, self.istr, start, end)

        # Check that weekly aligned to Monday. If not, shift start date back and re-fetch
        if self.interval == yfcd.Interval.Week and (not df.empty) and (df.index[0].weekday() != 0):
            # Despite fetch_start aligned to Monday, sometimes Yahoo returns weekly
            # data starting a different day. Shifting back a little fixes
            shift_backs = [2, 4]
            for d in shift_backs:
                fetch_start2 = fetch_start - timedelta(days=d)
                history_args["start"] = fetch_start2
                if debug_yfc:
                    msg = "- weekly data not aligned to Monday, re-fetching from {}".format(fetch_start2)
                    yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(msg)
                try:
                    df = self.dat.history(**history_args)
                    currency = self.dat.history_metadata['currency']
                    hist_md = yfcm.ReadCacheDatum(self.ticker, "history_metadata")
                    if hist_md is None:
                        hist_md = {}
                    hist_md['currency'] = currency
                    yfcm.StoreCacheDatum(self.ticker, "history_metadata", hist_md)
                except yf.exceptions.YFPricesMissingError:
                    pass
                if df is None or df.empty:
                    continue
                if "Repaired?" not in df.columns:
                    df["Repaired?"] = False
                if self.interval == yfcd.Interval.Week and (df.index[0].weekday() == 0):
                    log_msg = f"requested from YF: {self.istr} {history_args['start']} -> {history_args['end']}"
                    log_msg += f", received: {df.index[0]} -> {df.index[-1]}"
                    self.manager.LogEvent("info", "PriceManager", log_msg)

                    if isinstance(start, datetime):
                        start_dt = start
                    else:
                        start_dt = datetime.combine(start, time(0), self.tz)
                    df = df.loc[start_dt:]
                    break

            if self.interval == yfcd.Interval.Week and (df.index[0].weekday() != 0):
                # print("Date range requested: {} -> {}".format(fetch_start, fetch_end))
                print(df)
                raise Exception("Weekly data returned by YF doesn't begin Monday but {}".format(df.index[0].weekday()))

        if df is not None and df.empty:
            df = None

        if df is None:
            log_msg = "PM::_fetchYfHistory_dateRange() returning None"
        else:
            log_msg = f"PM::_fetchYfHistory_dateRange() returning DF {df.index[0]} -> {df.index[-1]}"
        if yfcl.IsTracingEnabled():
            yfcl.TraceExit(log_msg)
        elif debug_yfc:
            print(log_msg)

        # df = yfcu.CustomNanCheckingDataFrame(df)

        return df

    def _reconstruct_intervals_batch(self, df, tag=-1):
        if not isinstance(df, pd.DataFrame):
            raise Exception("'df' must be a Pandas DataFrame not", type(df))
        if self.interval == yfcd.Interval.Mins1:
            return df

        # Reconstruct values in df using finer-grained price data. Delimiter marks what to reconstruct

        debug = False
        # debug = True

        price_cols = [c for c in ["Open", "High", "Low", "Close", "Adj Close"] if c in df]
        data_cols = price_cols + ["Volume"]

        log_msg = f"PM::_reconstruct_intervals_batch-{self.istr}(dt0={df.index[0]})"
        yfcl.TraceEnter(log_msg)

        # If interval is weekly then can construct with daily. But if smaller intervals then
        # restricted to recent times:
        min_lookback = None
        # - daily = hourly restricted to last 730 days
        if self.interval == yfcd.Interval.Week:
            # Correct by fetching week of daily data
            sub_interval = yfcd.Interval.Days1
            td_range = timedelta(days=7)
        elif self.interval == yfcd.Interval.Days1:
            # Correct by fetching day of hourly data
            sub_interval = yfcd.Interval.Hours1
            td_range = timedelta(days=1)
            min_lookback = timedelta(days=730)
        elif self.interval == yfcd.Interval.Hours1:
            # Correct by fetching hour of 30m data
            sub_interval = yfcd.Interval.Mins30
            td_range = timedelta(hours=1)
            min_lookback = timedelta(days=60)
        elif self.interval == yfcd.Interval.Mins30:
            # Correct by fetching hour of 15m data
            sub_interval = yfcd.Interval.Mins15
            td_range = timedelta(minutes=30)
            min_lookback = timedelta(days=60)
        elif self.interval == yfcd.Interval.Mins15:
            # Correct by fetching hour of 5m data
            sub_interval = yfcd.Interval.Mins5
            td_range = timedelta(minutes=15)
            min_lookback = timedelta(days=60)
        elif self.interval == yfcd.Interval.Mins5:
            # Correct by fetching hour of 2m data
            sub_interval = yfcd.Interval.Mins2
            td_range = timedelta(minutes=5)
            min_lookback = timedelta(days=60)
        elif self.interval == yfcd.Interval.Mins2:
            # Correct by fetching hour of 1m data
            sub_interval = yfcd.Interval.Mins1
            td_range = timedelta(minutes=2)
            min_lookback = timedelta(days=30)
        else:
            msg = f"WARNING: Have not implemented repair for '{self.interval}' interval. Contact developers"
            yfcl.TracePrint(msg) if yfcl.IsTracingEnabled() else print(msg)
            log_msg = "PM::_reconstruct_intervals_batch() returning"
            yfcl.TraceExit(log_msg)
            return df
        sub_interday = sub_interval in [yfcd.Interval.Days1, yfcd.Interval.Week]#, yfcd.Interval.Months1, yfcd.Interval.Months3]
        sub_intraday = not sub_interday

        df = df.sort_index()

        f_repair = df[data_cols].to_numpy() == tag
        f_repair_rows = f_repair.any(axis=1)

        # Ignore old intervals for which Yahoo won't return finer data:
        if min_lookback is None:
            min_dt = None
        else:
            min_dt = pd.Timestamp.utcnow().tz_convert(ZoneInfo("UTC")) - min_lookback
        if debug:
            print(f"- min_dt={min_dt} interval={self.interval} sub_interval={sub_interval}")
        if min_dt is not None:
            f_recent = df.index > min_dt
            f_repair_rows = f_repair_rows & f_recent
            if not f_repair_rows.any():
                if debug:
                    print("- data too old to repair")
                return df

        dts_to_repair = df.index[f_repair_rows]
        # indices_to_repair = np.where(f_repair_rows)[0]

        if len(dts_to_repair) == 0:
            return df

        df_v2 = df.copy()
        if "Repaired?" not in df_v2.columns:
            df_v2["Repaired?"] = False
        df_good = df[~df[price_cols].isna().any(axis=1)]
        f_tag = df_v2[price_cols].to_numpy() == tag

        # Group nearby NaN-intervals together to reduce number of Yahoo fetches
        dts_groups = [[dts_to_repair[0]]]
        if self.interval == yfcd.Interval.Week:
            grp_max_size = dateutil.relativedelta.relativedelta(years=2)
        elif self.interval == yfcd.Interval.Days1:
            grp_max_size = dateutil.relativedelta.relativedelta(years=2)
        elif self.interval == yfcd.Interval.Hours1:
            grp_max_size = dateutil.relativedelta.relativedelta(years=1)
        else:
            grp_max_size = timedelta(days=30)
        if debug:
            print("- grp_max_size =", grp_max_size)
        for i in range(1, len(dts_to_repair)):
            # ind = indices_to_repair[i]
            dt = dts_to_repair[i]
            if dt.date() < dts_groups[-1][0].date()+grp_max_size:
                dts_groups[-1].append(dt)
            else:
                dts_groups.append([dt])
            # last_dt = dt
            # last_ind = ind

        if debug:
            print("Repair groups:")
            for g in dts_groups:
                print(f"- {g[0]} -> {g[-1]}")

        # Add some good data to each group, so can calibrate later:
        for i in range(len(dts_groups)):
            g = dts_groups[i]
            g0 = g[0]
            i0 = df_good.index.get_indexer([g0], method="nearest")[0]
            if i0 > 0:
                if (min_dt is None or df_good.index[i0-1] >= min_dt) \
                    and ((not self.intraday) or df_good.index[i0-1].date() == g0.date()):
                    i0 -= 1
            gl = g[-1]
            il = df_good.index.get_indexer([gl], method="nearest")[0]
            if il < len(df_good)-1:
                if (not self.intraday) or df_good.index[il+1].date() == gl.date():
                    il += 1
            good_dts = df_good.index[i0:il+1]
            dts_groups[i] += good_dts.to_list()
            dts_groups[i].sort()

        n_fixed = 0
        for g in dts_groups:
            df_block = df[df.index.isin(g)]

            if debug:
                print("df_block:") ; print(df_block)

            start_dt = g[0]
            start_d = start_dt.date()

            if sub_interval == yfcd.Interval.Hours1 and (date.today()-start_d) > timedelta(days=729):
                # Don't bother requesting more price data, Yahoo will reject
                continue
            elif sub_interval in [yfcd.Interval.Mins30, yfcd.Interval.Mins15] and (date.today()-start_d) > timedelta(days=59):
                # Don't bother requesting more price data, Yahoo will reject
                continue

            if self._record_stack_trace:
                # Log function calls to detect and manage infinite recursion
                fn_tuple = ("_reconstruct_intervals_batch()", f"dt0={df.index[0]}", f"interval={self.interval}")
                if fn_tuple in self._stack_trace:
                    # Detected a potential recursion loop
                    reconstruct_detected = False
                    for i in range(len(self._stack_trace)-1, -1, -1):
                        if "_reconstruct_intervals_batch" in str(self._stack_trace[i]):
                            reconstruct_detected = True
                            break
                    if reconstruct_detected:
                        self._infinite_recursion_detected = True
                self._stack_trace.append(fn_tuple)

            if self.interday:
                log_msg = f"repairing {self.istr} block {g[0].date()} -> {g[-1].date()+timedelta(days=1)}"
            else:
                log_msg = f"repairing {self.istr} block {g[0]} -> {g[-1]}"
            self.manager.LogEvent("info", "PriceManager", log_msg)

            # Infinite loop potential here via repair:
            # - request fine-grained data e.g. 1H
            # - 1H requires accurate dividend data
            # - triggers fetch of 1D data which must be kept contiguous
            # - triggers fetch of older 1D data which requires repair using 1H data -> recursion loop
            # Solution:
            # 1) add tuple to fn stack buy with YF=True
            # 2) if that tuple already in stack then raise Exception
            if self.interval == yfcd.Interval.Week:
                fetch_start = start_d - td_range  # need previous week too
                fetch_end = g[-1].date() + td_range
            elif self.interval == yfcd.Interval.Days1:
                fetch_start = start_d
                fetch_end = g[-1].date() + td_range
            else:
                fetch_start = g[0]
                fetch_end = g[-1] + td_range
            # print(f"fetch_start={fetch_start} fetch_end={fetch_end}")

            # prepost = self.interval == yfcd.Interval.Days1
            prepost = self.interday
            if debug:
                print(f"- fetch_start={fetch_start}, fetch_end={fetch_end} prepost={prepost}")
            if self._infinite_recursion_detected:
                for i in range(len(self._stack_trace)):
                    print("  "*i + str(self._stack_trace[i]))
                raise Exception("WARNING: Infinite recursion detected (see stack trace above). Switch to fetching prices direct from YF")
                print("WARNING: Infinite recursion detected (see stack trace above). Switch to fetching prices direct from YF")
                # df_fine = self.dat.history(start=fetch_start, end=fetch_end, interval=yfcd.intervalToString[sub_interval], auto_adjust=False, prepost=prepost, keepna=True)
            # elif self.interval in [yfcd.Interval.Days1]:  # or self._infinite_recursion_detected:
            #     # Assume infinite recursion will happen
            #     df_fine = self.dat.history(start=fetch_start, end=fetch_end, interval=yfcd.intervalToString[sub_interval], auto_adjust=False, repair=True)
            else:
                if prepost and sub_intraday:
                    # YFC cannot handle intraday pre- and post-market, so fetch via yfinance
                    if debug:
                        # print("- fetching df_fine direct from YF")
                        print(f"- - fetch_start={fetch_start} fetch_end={fetch_end}")
                    # Temp-disable YF logging.
                    yf_logger = logging.getLogger('yfinance')
                    if hasattr(yf_logger, 'level'):
                        yf_log_level = yf_logger.level
                        yf_logger.setLevel(logging.CRITICAL)
                    df_fine_old = self.dat.history(start=fetch_start, end=fetch_end, interval=yfcd.intervalToString[sub_interval], auto_adjust=True, prepost=prepost, raise_errors=False)
                    if hasattr(yf_logger, 'level'):
                        yf_logger.setLevel(yf_log_level)
                    hist_sub = self.manager.GetHistory(sub_interval)
                    if not isinstance(fetch_start, datetime):
                        fetch_start = datetime.combine(fetch_start, time(0), ZoneInfo(self.tzName))
                    if not isinstance(fetch_end, datetime):
                        fetch_end = datetime.combine(fetch_end, time(0), ZoneInfo(self.tzName))
                    if debug:
                        print("- fetching df_fine via _fetchYfHistory() wrapper")
                        print(f"- - fetch_start={fetch_start} fetch_end={fetch_end}")
                    try:
                        df_fine = hist_sub._fetchYfHistory(start=fetch_start, end=fetch_end, prepost=prepost, debug=False, verify_intervals=False, disable_yfc_metadata=True, quiet=True)
                    except yfcd.NoPriceDataInRangeException as e:
                        if debug:
                            print("- fetch of fine price data failed:" + str(e))
                        continue
                    if df_fine is not None:
                        adj = (df_fine["Adj Close"]/df_fine["Close"]).to_numpy()
                        for c in ["Open", "Low", "High", "Close"]:
                            df_fine[c] *= adj
                        df_fine = df_fine[["Open", "Low", "High", "Close", "Volume", "Dividends", "Stock Splits"]]
                    if debug:
                        print("df_fine_old:")
                        print(df_fine_old)
                        print("df_fine:")
                        print(df_fine)
                    # raise Exception("here")
                else:
                    if debug:
                        print("- fetching df_fine via YFC")
                    hist_sub = self.manager.GetHistory(sub_interval)
                    df_fine = hist_sub.get(fetch_start, fetch_end, adjust_splits=False, adjust_divs=False, repair=False, prepost=prepost, quiet=True)
                # df_fine["Adj Close"] = df_fine["Close"] * df_fine["CDF"]
            if debug:
                print("- df_fine:")
                print(df_fine)
            if df_fine is None or len(df_fine) == 0:
                yfcl.TracePrint("PM::_reconstruct_intervals_batch() Cannot reconstruct because Yahoo not returning data in interval")
                if self._record_stack_trace:
                    # Pop stack trace
                    if len(self._stack_trace) == 0:
                        raise Exception("Failing to correctly push/pop stack trace (is empty too early)")
                    if not self._stack_trace[-1] == fn_tuple:
                        for i in range(len(self._stack_trace)):
                            print("  "*i + str(self._stack_trace[i]))
                        raise Exception("Failing to correctly push/pop stack trace (see above)")
                    self._stack_trace.pop(len(self._stack_trace) - 1)
                continue

            df_fine["ctr"] = 0
            if self.interval == yfcd.Interval.Week:
                # df_fine["Week Start"] = df_fine.index.tz_localize(None).to_period("W-SUN").start_time
                weekdays = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]
                week_end_day = weekdays[(df_block.index[0].weekday() + 7 - 1) % 7]
                df_fine["Week Start"] = df_fine.index.tz_localize(None).to_period("W-"+week_end_day).start_time
                grp_col = "Week Start"
            elif self.interval == yfcd.Interval.Days1:
                df_fine["Day Start"] = pd.to_datetime(df_fine.index.date)
                grp_col = "Day Start"
            else:
                df_fine.loc[df_fine.index.isin(df_block.index), "ctr"] = 1
                df_fine["intervalID"] = df_fine["ctr"].cumsum()
                df_fine = df_fine.drop("ctr", axis=1)
                grp_col = "intervalID"
            # df_fine = df_fine[~df_fine[price_cols].isna().all(axis=1)]

            df_new = df_fine.groupby(grp_col).agg(
                Open=("Open", "first"),
                Close=("Close", "last"),
                # AdjClose=("Adj Close", "last"),
                Low=("Low", "min"),
                High=("High", "max"),
                Volume=("Volume", "sum"))
                # CSF=("CSF", "first"),
                # CDF=("CDF", "first"))#.rename(columns={"AdjClose": "Adj Close"})
            if grp_col in ["Week Start", "Day Start"]:
                df_new.index = df_new.index.tz_localize(df_fine.index.tz)
            else:
                df_fine["diff"] = df_fine["intervalID"].diff()
                new_index = np.append([df_fine.index[0]], df_fine.index[df_fine["intervalID"].diff() > 0])
                df_new.index = new_index

            # Calibrate! Check whether 'df_fine' has different split-adjustment.
            # If different, then adjust to match 'df'
            df_block_calib = df_block[price_cols]
            # df_new_calib = df_new[df_new.index.isin(df_block_calib.index)][price_cols]
            # df_block_calib = df_block_calib[df_block_calib.index.isin(df_new_calib)]
            common_index = np.intersect1d(df_block_calib.index, df_new.index)
            df_new_calib = df_new[price_cols].loc[common_index]
            df_block_calib = df_block_calib.loc[common_index]
            calib_filter = (df_block_calib != tag).to_numpy()
            calib_filter = calib_filter & (~np.isnan(df_new_calib.to_numpy()))
            if not calib_filter.any():
                # Can't calibrate so don't attempt repair
                if self._record_stack_trace:
                    # Pop stack trace
                    if len(self._stack_trace) == 0:
                        raise Exception("Failing to correctly push/pop stack trace (is empty too early)")
                    if not self._stack_trace[-1] == fn_tuple:
                        for i in range(len(self._stack_trace)):
                            print("  "*i + str(self._stack_trace[i]))
                        raise Exception("Failing to correctly push/pop stack trace (see above)")
                    self._stack_trace.pop(len(self._stack_trace) - 1)
                continue
            if debug:
                print("calib_filter:") ; print(calib_filter)
                print("df_new_calib:") ; print(df_new_calib)
                print("df_block_calib:") ; print(df_block_calib)
            # Avoid divide-by-zero warnings printing:
            df_new_calib = df_new_calib.to_numpy()
            df_block_calib = df_block_calib.to_numpy()
            for j in range(len(price_cols)):
                c = price_cols[j]
                f = ~calib_filter[:, j]
                if f.any():
                    df_block_calib[f, j] = 1
                    df_new_calib[f, j] = 1
            ratios = (df_block_calib / df_new_calib)[calib_filter]
            ratio = np.mean(ratios)
            #
            ratio_rcp = round(1.0 / ratio, 1)
            ratio = round(ratio, 1)
            if ratio == 1 and ratio_rcp == 1:
                # Good!
                pass
            else:
                if ratio > 1:
                    # data has different split-adjustment than fine-grained data
                    # Adjust fine-grained to match
                    df_new[price_cols] *= ratio
                    df_new["Volume"] /= ratio
                    df_new["Volume"] = df_new["Volume"].round(0).astype('int')
                elif ratio_rcp > 1:
                    # data has different split-adjustment than fine-grained data
                    # Adjust fine-grained to match
                    df_new[price_cols] *= 1.0 / ratio_rcp
                    df_new["Volume"] *= ratio_rcp
                    df_new["Volume"] = df_new["Volume"].round(0).astype('int')

            # Repair!
            bad_dts = df_block.index[(df_block[price_cols] == tag).any(axis=1)]

            for idx in bad_dts:
                if idx not in df_new.index:
                    # Yahoo didn't return finer-grain data for this interval,
                    # so probably no trading happened.
                    # print("no fine data")
                    continue
                df_new_row = df_new.loc[idx]

                if self.interval == yfcd.Interval.Week:
                    df_last_week = df_new.iloc[df_new.index.get_loc(idx)-1]
                    df_fine = df_fine.loc[idx:]

                df_bad_row = df.loc[idx]
                bad_fields = df_bad_row.index[df_bad_row == tag].to_numpy()
                if "High" in bad_fields:
                    df_v2.loc[idx, "High"] = df_new_row["High"]
                if "Low" in bad_fields:
                    df_v2.loc[idx, "Low"] = df_new_row["Low"]
                if "Open" in bad_fields:
                    if self.interval == yfcd.Interval.Week and idx != df_fine.index[0]:
                        # Exchange closed Monday. In this case, Yahoo sets Open to last week close
                        df_v2.loc[idx, "Open"] = df_last_week["Close"]
                        df_v2.loc[idx, "Low"] = min(df_v2.loc[idx, "Open"], df_v2.loc[idx, "Low"])
                    else:
                        df_v2.loc[idx, "Open"] = df_new_row["Open"]
                if "Close" in bad_fields:
                    df_v2.loc[idx, "Close"] = df_new_row["Close"]
                    # Should not need to copy over CDF & CSF, because
                    # correct values are already merged in from daily
                    # df_v2.loc[idx, "CDF"] = df_new_row["CDF"]
                    # df_v2.loc[idx, "CSF"] = df_new_row["CSF"]
                if "Volume" in bad_fields:
                    df_v2.loc[idx, "Volume"] = df_new_row["Volume"].round().astype('int')
                df_v2.loc[idx, "Repaired?"] = True
                n_fixed += 1

            # Restore un-repaired bad values
            f_nan = df_v2[price_cols].isna().to_numpy()
            f_failed = f_tag & f_nan
            for j in range(len(price_cols)):
                f = f_failed[:, j]
                if f.any():
                    c = price_cols[j]
                    df_v2.loc[f, c] = df.loc[f, c]

            if self._record_stack_trace:
                # Pop stack trace
                if len(self._stack_trace) == 0:
                    raise Exception("Failing to correctly push/pop stack trace (is empty too early)")
                if not self._stack_trace[-1] == fn_tuple:
                    for i in range(len(self._stack_trace)):
                        print("  "*i + str(self._stack_trace[i]))
                    raise Exception("Failing to correctly push/pop stack trace (see above)")
                self._stack_trace.pop(len(self._stack_trace) - 1)

        log_msg = "PM::_reconstruct_intervals_batch() returning"
        yfcl.TraceExit(log_msg)

        return df_v2

    def _repairUnitMixups(self, df, silent=False):
        df2 = self._fixUnitSwitch(df)
        df3 = self._repairSporadicUnitMixups(df2, silent)
        return df3

    def _repairSporadicUnitMixups(self, df, silent=False):
        yfcu.TypeCheckDataFrame(df, "df")

        # Sometimes Yahoo returns few prices in cents/pence instead of $/£
        # I.e. 100x bigger
        # Easy to detect and fix, just look for outliers = ~100x local median

        if df.empty:
            return df
        if df.shape[0] == 1:
            # Need multiple rows to confidently identify outliers
            return df

        log_msg_enter = f"PM::_repairSporadicUnitMixups-{self.istr}()"
        log_msg_exit = f"PM::_repairSporadicUnitMixups-{self.istr}() returning"
        yfcl.TraceEnter(log_msg_enter)

        df2 = df.copy()

        data_cols = ["High", "Open", "Low", "Close"]  # Order important, separate High from Low
        data_cols = [c for c in data_cols if c in df2.columns]
        f_zeroes = (df2[data_cols] == 0).any(axis=1)
        if f_zeroes.any():
            df2_zeroes = df2[f_zeroes]
            df2 = df2[~f_zeroes]
        else:
            df2_zeroes = None
        if df2.shape[0] <= 1:
            yfcl.TraceExit(log_msg_exit)
            return df
        df2_data = df2[data_cols].to_numpy()
        median = _ndimage.median_filter(df2_data, size=(3, 3), mode="wrap")
        ratio = df2_data / median
        ratio_rounded = (ratio / 20).round() * 20  # round ratio to nearest 20
        f = ratio_rounded == 100
        ratio_rcp = 1.0/ratio
        ratio_rcp_rounded = (ratio_rcp / 20).round() * 20  # round ratio to nearest 20
        f_rcp = (ratio_rounded == 100) | (ratio_rcp_rounded == 100)
        f_either = f | f_rcp
        if not f_either.any():
            yfcl.TraceExit(log_msg_exit)
            return df

        # Mark values to send for repair
        tag = -1.0
        for i in range(len(data_cols)):
            fi = f_either[:, i]
            c = data_cols[i]
            df2.loc[fi, c] = tag

        n_before = (df2_data == tag).sum()
        try:
            df2 = self._reconstruct_intervals_batch(df2, tag=tag)
        except Exception:
            if len(self._stack_trace) > 0:
                self._stack_trace.pop(len(self._stack_trace) - 1)
            raise
        n_after = (df2[data_cols].to_numpy() == tag).sum()

        if n_after > 0:
            # This second pass will *crudely* "fix" any remaining errors in High/Low
            # simply by ensuring they don't contradict e.g. Low = 100x High.
            f = (df2[data_cols].to_numpy() == tag) & f
            for i in range(f.shape[0]):
                fi = f[i, :]
                if not fi.any():
                    continue
                idx = df2.index[i]

                for c in ['Open', 'Close']:
                    j = data_cols.index(c)
                    if fi[j]:
                        df2.loc[idx, c] = df.loc[idx, c] * 0.01

                c = "High" ; j = data_cols.index(c)
                if fi[j]:
                    df2.loc[idx, c] = df2.loc[idx, ["Open", "Close"]].max()

                c = "Low" ; j = data_cols.index(c)
                if fi[j]:
                    df2.loc[idx, c] = df2.loc[idx, ["Open", "Close"]].min()

            f_rcp = (df2[data_cols].to_numpy() == tag) & f_rcp
            for i in range(f_rcp.shape[0]):
                fi = f_rcp[i, :]
                if not fi.any():
                    continue
                idx = df2.index[i]

                for c in ['Open', 'Close']:
                    j = data_cols.index(c)
                    if fi[j]:
                        df2.loc[idx, c] = df.loc[idx, c] * 100.0

                c = "High" ; j = data_cols.index(c)
                if fi[j]:
                    df2.loc[idx, c] = df2.loc[idx, ["Open", "Close"]].max()

                c = "Low" ; j = data_cols.index(c)
                if fi[j]:
                    df2.loc[idx, c] = df2.loc[idx, ["Open", "Close"]].min()

        n_after_crude = (df2[data_cols].to_numpy() == tag).sum()

        n_fixed = n_before - n_after_crude
        n_fixed_crudely = n_after - n_after_crude
        if not silent and n_fixed > 0:
            report_msg = f"{self.ticker}: fixed {n_fixed}/{n_before} currency unit mixups "
            if n_fixed_crudely > 0:
                report_msg += f"({n_fixed_crudely} crudely) "
            report_msg += f"in {self.interval} price data"
            print(report_msg)

        # Restore original values where repair failed
        f_either = df2[data_cols].to_numpy() == tag
        for j in range(len(data_cols)):
            fj = f_either[:, j]
            if fj.any():
                c = data_cols[j]
                df2.loc[fj, c] = df.loc[fj, c]
        if df2_zeroes is not None:
            df2 = pd.concat([df2, df2_zeroes]).sort_index()
            df2.index = pd.to_datetime()

        yfcl.TraceExit(log_msg_exit)

        return df2

    def _fixUnitSwitch(self, df):
        # Sometimes Yahoo returns few prices in cents/pence instead of $/£
        # I.e. 100x bigger
        # 2 ways this manifests:
        # - random 100x errors spread throughout table
        # - a sudden switch between $<->cents at some date
        # This function fixes the second.
        # Eventually Yahoo fixes but could take them 2 weeks.

        if self.exchange == 'KUW':
            # Kuwaiti Dinar divided into 1000 not 100
            n = 1000.0
        else:
            n = 100.0

        return self._fixPricesSuddenChange(df, n, correct_dividend=True)

    def _fixBadStockSplits(self, df):
        # Original logic only considered latest split adjustment could be missing, but 
        # actually **any** split adjustment can be missing. So check all splits in df.
        #
        # Improved logic looks for BIG daily price changes that closely match the
        # **nearest future** stock split ratio. This indicates Yahoo failed to apply a new
        # stock split to old price data.
        #
        # There is a slight complication, because Yahoo does another stupid thing.
        # Sometimes the old data is adjusted twice. So cannot simply assume
        # which direction to reverse adjustment - have to analyse prices and detect.
        # Not difficult.

        if df.empty:
            return df

        if not self.interday:
            return df

        df = df.sort_index()   # scan splits oldest -> newest
        split_f = df['Stock Splits'].to_numpy() != 0
        if not split_f.any():
            return df

        for split_idx in np.where(split_f)[0]:
            split_dt = df.index[split_idx]
            split = df.loc[split_dt, 'Stock Splits']
            if split_dt == df.index[0]:
                continue

            # logger.debug(f'price-repair-split: Checking split {split:.4f} @ {split_dt.date()} for possible repair')

            cutoff_idx = min(df.shape[0], split_idx+1)  # add one row after to detect big change
            df_pre_split = df.iloc[0:cutoff_idx+1]

            df_pre_split_repaired = self._fixPricesSuddenChange(df_pre_split, split, correct_volume=True, correct_dividend=True)
            # Merge back in:
            if cutoff_idx == df.shape[0]-1:
                df = df_pre_split_repaired
            else:
                df = pd.concat([df_pre_split_repaired.sort_index(), df.iloc[cutoff_idx+1:]])
        return df

    def _fixPricesSuddenChange(self, df, change, correct_volume=False, correct_dividend=False):
        log_func = f"PM::_fixPricesSuddenChange-{self.istr}(change={change:.2f})"
        yfcl.TraceEnter(log_func)

        df2 = df.sort_index(ascending=False)
        split = change
        split_rcp = 1.0 / split

        if change in [100.0, 0.01]:
            fix_type = '100x error'
            start_min = None
        else:
            fix_type = 'bad split'
            f = df2['Stock Splits'].to_numpy() != 0.0
            start_min = (df2.index[f].min() - dateutil.relativedelta.relativedelta(years=1)).date()

        OHLC = ['Open', 'High', 'Low', 'Close']

        # Do not attempt repair of the split is small, 
        # could be mistaken for normal price variance
        if 0.8 < split < 1.25:
            yfcl.TraceExit(log_func + ": aborting, split too near 1.0")
            return df

        n = df2.shape[0]

        df_debug = df2.copy()
        df_debug = df_debug.drop(['Volume', 'Dividends', 'Repaired?'], axis=1, errors='ignore')
        df_debug = df_debug.drop(['CDF', 'CSF', 'C-Check?', 'LastDivAdjustDt', 'LastSplitAdjustDt'], axis=1, errors='ignore')
        debug_cols = ['Open', 'Close']
        df_debug = df_debug.drop([c for c in OHLC if c not in debug_cols], axis=1, errors='ignore')

        # Calculate daily price % change. To reduce effect of price volatility, 
        # calculate change for each OHLC column.
        if self.interday and self.interval != yfcd.Interval.Days1 and split not in [100.0, 100, 0.001]:
            # Avoid using 'Low' and 'High'. For multiday intervals, these can be 
            # very volatile so reduce ability to detect genuine stock split errors
            _1d_change_x = np.full((n, 2), 1.0)
            price_data = df2[['Open','Close']].to_numpy()
            f_zero = price_data == 0.0
        else:
            _1d_change_x = np.full((n, 4), 1.0)
            price_data = df2[OHLC].to_numpy()
            f_zero = price_data == 0.0
        if f_zero.any():
            price_data[f_zero] = 1.0

        # Update: if a VERY large dividend is paid out, then can be mistaken for a 1:2 stock split.
        # Fix = use adjusted prices
        for j in range(price_data.shape[1]):
            price_data[:,j] *= df2['CDF']

        _1d_change_x[1:] = price_data[1:, ] / price_data[:-1, ]
        f_zero_num_denom = f_zero | np.roll(f_zero, 1, axis=0)
        if f_zero_num_denom.any():
            _1d_change_x[f_zero_num_denom] = 1.0
        if self.interday and self.interval != yfcd.Interval.Days1:
            # average change
            _1d_change_minx = np.average(_1d_change_x, axis=1)
        else:
            # # change nearest to 1.0
            # diff = np.abs(_1d_change_x - 1.0)
            # j_indices = np.argmin(diff, axis=1)
            # _1d_change_minx = _1d_change_x[np.arange(n), j_indices]
            # Still distorted by extreme-low high/low. So try median:
            _1d_change_minx = np.median(_1d_change_x, axis=1)
        f_na = np.isnan(_1d_change_minx)
        if f_na.any():
            # Possible if data was too old for reconstruction.
            _1d_change_minx[f_na] = 1.0
        df_debug['1D change X'] = _1d_change_minx
        df_debug['1D change X'] = df_debug['1D change X'].round(2).astype('str')

        # If all 1D changes are closer to 1.0 than split, exit
        split_max = max(split, split_rcp)
        if np.max(_1d_change_minx) < (split_max - 1) * 0.5 + 1 and np.min(_1d_change_minx) > 1.0 / ((split_max - 1) * 0.5 + 1):
            reason = "changes too near 1.0"
            reason += f" (_1d_change_minx = {np.min(_1d_change_minx):.2f} -> {np.max(_1d_change_minx):.2f})"
            yfcl.TracePrint(reason)
            yfcl.TraceExit(log_func + " aborting")
            return df

        # Calculate the true price variance, i.e. remove effect of bad split-adjustments.
        # Key = ignore 1D changes outside of interquartile range
        q1, q3 = np.percentile(_1d_change_minx, [25, 75])
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        f = (_1d_change_minx >= lower_bound) & (_1d_change_minx <= upper_bound)
        avg = np.mean(_1d_change_minx[f])
        sd = np.std(_1d_change_minx[f])
        # Now can calculate SD as % of mean
        sd_pct = sd / avg
        msg = f"Estimation of true 1D change stats: mean = {avg:.2f}, StdDev = {sd:.4f} ({sd_pct*100.0:.1f}% of mean)"
        self.manager.LogEvent('debug', 'price-repair-split-'+self.istr, msg)

        # Only proceed if split adjustment far exceeds normal 1D changes
        largest_change_pct = 5 * sd_pct
        if self.interday and self.interval != yfcd.Interval.Days1:
            largest_change_pct *= 3
            # if self.interval in [yfcd.Interval.Months1, yfcd.Interval.Months3]:
            #     largest_change_pct *= 2
        if max(split, split_rcp) < 1.0 + largest_change_pct:
            msg = "Split ratio too close to normal price volatility. Won't repair"
            self.manager.LogEvent('debug', 'price-repair-split-'+self.istr, msg)
            # msg = f"price-repair-split: my workings:" + '\n' + str(df_debug)
            # self.manager.LogEvent('debug', 'price-repair-split-'+self.istr, msg)
            yfcl.TraceExit(log_func + ": aborting, changes to near normal price volatility")
            return df

        # Now can detect bad split adjustments
        # Set threshold to halfway between split ratio and largest expected normal price change
        r = _1d_change_minx / split_rcp
        split_max = max(split, split_rcp)
        threshold = (split_max + 1.0 + largest_change_pct) * 0.5
        msg = f"split_max={split_max:.3f} largest_change_pct={largest_change_pct:.4f} threshold={threshold:.3f}"
        self.manager.LogEvent('debug', 'price-repair-split-'+self.istr, msg)

        if 'Repaired?' not in df2.columns:
            df2['Repaired?'] = False

        if self.interday and self.interval != yfcd.Interval.Days1:
            # Yahoo creates multi-day intervals using potentiall corrupt data, e.g.
            # the Close could be 100x Open. This means have to correct each OHLC column
            # individually
            correct_columns_individually = True
        else:
            correct_columns_individually = False

        if correct_columns_individually:
            _1d_change_x = np.full((n, 4), 1.0)
            price_data = df2[OHLC].replace(0.0, 1.0).to_numpy()
            _1d_change_x[1:] = price_data[1:, ] / price_data[:-1, ]
        else:
            _1d_change_x = _1d_change_minx

        r = _1d_change_x / split_rcp
        f_down = _1d_change_x < (1.0 / threshold)
        if f_down.any():
            # Discard where triggered by negative Adj Close after dividend
            f_neg = _1d_change_x < 0.0
            f_div = (df2['Dividends']>0).to_numpy()
            f_div_before = np.roll(f_div, 1)
            f_down = f_down & ~(f_neg + f_div_before)
        f_up = _1d_change_x > threshold
        f_up_ndims = len(f_up.shape)
        f_up_shifts = f_up if f_up_ndims==1 else f_up.any(axis=1)
        if f_up_shifts.any():
            for i in np.where(f_up_shifts)[0]:
                vol_change_pct = 0.0
                v = df2['Volume'].iloc[i]
                if v > 0:
                    vol_change_pct = df2['Volume'].iloc[i-1] / v
                if self.multiday:
                    v = df2['Volume'].iloc[i+1]
                    if v > 0:
                        vol_change_pct2 = df2['Volume'].iloc[i] / v
                        vol_change_pct = max(vol_change_pct, vol_change_pct2)
                if vol_change_pct > 5:
                    # big volume change = false positive
                    if f_up_ndims == 1:
                        f_up[i] = False
                    else:
                        f_up[i,:] = False
        f = f_down | f_up
        if not correct_columns_individually:
            df_debug['r'] = r
            df_debug['f_down'] = f_down
            df_debug['f_up'] = f_up
            df_debug['r'] = df_debug['r'].round(2).astype('str')
        else:
            for j in range(len(OHLC)):
                c = OHLC[j]
                if c in debug_cols:
                    df_debug[c + '_r'] = r[:, j]
                    df_debug[c + '_f_down'] = f_down[:, j]
                    df_debug[c + '_f_up'] = f_up[:, j]
                    df_debug[c + '_r'] = df_debug[c + '_r'].round(2).astype('str')

        if not f.any():
            yfcl.TraceExit(log_func + ": aborting, did not detect split errors")
            return df

        # If stock is currently suspended and not in USA, then usually Yahoo introduces
        # 100x errors into suspended intervals. Clue is no price change and 0 volume.
        # Better to use last active trading interval as baseline.
        f_no_activity = (df2['Low'] == df2['High']) & (df2['Volume']==0)
        f_no_activity = f_no_activity | df2[OHLC].isna().all(axis=1)
        appears_suspended = f_no_activity.any() and np.where(f_no_activity)[0][0]==0
        f_active = ~f_no_activity
        # First, ideally, look for 2 consecutive intervals of activity that are not
        # affected by change errors
        if f.ndim == 1:
            f_active = f_active & (~f)
        else:
            f_active = f_active & (~f.any(axis=1))
        f_active = f_active & np.roll(f_active, 1)
        if not f_active.any():
            # First plan failed, will have to settle for most recent active interval
            f_active = ~f_no_activity
            f_active = f_active & np.roll(f_active, 1)
        idx_latest_active = np.where(f_active)[0]
        if len(idx_latest_active) == 0:
            idx_latest_active = None
        else:
            idx_latest_active = int(idx_latest_active[0])
        msg = f'appears_suspended={appears_suspended} idx_latest_active={idx_latest_active}'
        if idx_latest_active is not None:
            msg += f' ({df2.index[idx_latest_active].date()})'
        self.manager.LogEvent('debug', 'price-repair-split-'+self.istr, msg)

        # Update: if any 100x changes are soon after a stock split, so could be confused with split error, then abort
        threshold_days = 30
        f_splits = df2['Stock Splits'].to_numpy() != 0.0
        if change in [100.0, 0.01] and f_splits.any():
            indices_A = np.where(f_splits)[0]
            indices_B = np.where(f)[0]
            if not len(indices_A) or not len(indices_B):
                yfcl.TraceExit(log_func)
                return None
            gaps = indices_B[:, None] - indices_A
            # Because data is sorted in DEscending order, need to flip gaps
            gaps *= -1
            f_pos = gaps > 0
            if f_pos.any():
                gap_min = gaps[f_pos].min()
                gap_td = self.itd * gap_min
                if isinstance(gap_td, dateutil.relativedelta.relativedelta):
                    threshold = dateutil.relativedelta.relativedelta(days=threshold_days)
                else:
                    threshold = timedelta(days=threshold_days)
                if gap_td < threshold:
                    msg = 'price-repair-split: 100x changes are too soon after stock split events, aborting'
                    self.manager.LogEvent('info', 'price-repair-split-'+self.istr, msg)
                    yfcl.TraceExit(log_func)
                    return df

        # if self.interday:
        #     df_debug.index = df_debug.index.date
        # for c in ['FetchDate']:
        #     df_debug[c] = df_debug[c].dt.strftime('%Y-%m-%d %H:%M:%S%z')
        # # f_change_happened = df_debug['f_down'] | df_debug['f_up']
        # f_change_happened = df_debug['High_f_down'] | df_debug['High_f_up'] | df_debug['Low_f_down'] | df_debug['Low_f_up']
        # f_change_happened = f_change_happened | np.roll(f_change_happened, 1) | np.roll(f_change_happened, -1) | np.roll(f_change_happened, 2) | np.roll(f_change_happened, -2)
        # f_change_happened[0] = True ; f_change_happened[-1] = True
        # df_debug = df_debug[f_change_happened]
        # # # df_debug = df_debug.loc[df.index.date <= date(2023, 2, 13)]['Close'].to_numpy()
        # # # df_debug = df_debug.iloc[42*5 : 46*5]
        # # # df_debug = df.sort_index().loc['2023-06-29':'2023-07-04'][OHLC].sort_index(ascending=False)
        # msg = f"price-repair-split: my workings:" + '\n' + str(df_debug)
        # self.manager.LogEvent('debug', 'price-repair-split-'+self.istr, msg)
        # quit()

        def map_signals_to_ranges(f, f_up, f_down):
            # Ensure 0th element is False, because True is nonsense
            if f[0]:
                f = np.copy(f) ; f[0] = False
                f_up = np.copy(f_up) ; f_up[0] = False
                f_down = np.copy(f_down) ; f_down[0] = False

            if not f.any():
                return []

            true_indices = np.where(f)[0]
            ranges = []

            for i in range(len(true_indices) - 1):
                if i % 2 == 0:
                    if split > 1.0:
                        adj = 'split' if f_down[true_indices[i]] else '1.0/split'
                    else:
                        adj = '1.0/split' if f_down[true_indices[i]] else 'split'
                    ranges.append((true_indices[i], true_indices[i + 1], adj))

            if len(true_indices) % 2 != 0:
                if split > 1.0:
                    adj = 'split' if f_down[true_indices[-1]] else '1.0/split'
                else:
                    adj = '1.0/split' if f_down[true_indices[-1]] else 'split'
                ranges.append((true_indices[-1], len(f), adj))

            return ranges

        if idx_latest_active is not None:
            idx_rev_latest_active = df.shape[0] - 1 - idx_latest_active
            msg = f'idx_latest_active={idx_latest_active}, idx_rev_latest_active={idx_rev_latest_active}'
            self.manager.LogEvent('debug', 'price-repair-split-'+self.istr, msg)
        if correct_columns_individually:
            f_corrected = np.full(n, False)
            if correct_volume:
                # If Open or Close is repaired but not both, 
                # then this means the interval has a mix of correct
                # and errors. A problem for correcting Volume, 
                # so use a heuristic:
                # - if both Open & Close were Nx bad => Volume is Nx bad
                # - if only one of Open & Close are Nx bad => Volume is 0.5*Nx bad
                f_open_fixed = np.full(n, False)
                f_close_fixed = np.full(n, False)

            OHLC_correct_ranges = [None, None, None, None]
            for j in range(len(OHLC)):
                c = OHLC[j]
                idx_first_f = np.where(f)[0][0]
                if appears_suspended and (idx_latest_active is not None and idx_latest_active >= idx_first_f):
                    # Suspended midway during data date range.
                    # 1: process data before suspension in index-ascending (date-descending) order.
                    # 2: process data after suspension in index-descending order. Requires signals to be reversed, 
                    #    then returned ranges to also be reversed, because this logic was originally written for
                    #    index-ascending (date-descending) order.
                    fj = f[:, j]
                    f_upj = f_up[:, j]
                    f_downj = f_down[:, j]
                    ranges_before = map_signals_to_ranges(fj[idx_latest_active:], f_upj[idx_latest_active:], f_downj[idx_latest_active:])
                    if len(ranges_before) > 0:
                        # Shift each range back to global indexing
                        for i in range(len(ranges_before)):
                            r = ranges_before[i]
                            ranges_before[i] = (r[0] + idx_latest_active, r[1] + idx_latest_active, r[2])
                    f_rev_downj = np.flip(np.roll(f_upj, -1))  # correct
                    f_rev_upj = np.flip(np.roll(f_downj, -1))  # correct
                    f_revj = f_rev_upj | f_rev_downj
                    ranges_after = map_signals_to_ranges(f_revj[idx_rev_latest_active:], f_rev_upj[idx_rev_latest_active:], f_rev_downj[idx_rev_latest_active:])
                    if len(ranges_after) > 0:
                        # Shift each range back to global indexing:
                        for i in range(len(ranges_after)):
                            r = ranges_after[i]
                            ranges_after[i] = (r[0] + idx_rev_latest_active, r[1] + idx_rev_latest_active, r[2])
                        # Flip range to normal ordering
                        for i in range(len(ranges_after)):
                            r = ranges_after[i]
                            ranges_after[i] = (n-r[1], n-r[0], r[2])
                    ranges = ranges_before ; ranges.extend(ranges_after)
                else:
                    ranges = map_signals_to_ranges(f[:, j], f_up[:, j], f_down[:, j])

                if start_min is not None:
                    # Prune ranges that are older than start_min
                    for i in range(len(ranges)-1, -1, -1):
                        r = ranges[i]
                        if df2.index[r[0]].date() < start_min:
                            msg = f'price-repair-split: Pruning {c} range {df2.index[r[0]]}->{df2.index[r[1]-1]} because too old.'
                            self.manager.LogEvent('info', 'price-repair-split-'+self.istr, msg)
                            del ranges[i]

                if len(ranges) > 0:
                    OHLC_correct_ranges[j] = ranges

            count = sum([1 if x is not None else 0 for x in OHLC_correct_ranges])
            if count == 0:
                pass
            elif count == 1:
                # If only 1 column then assume false positive
                idxs = [i if OHLC_correct_ranges[i] else -1 for i in range(len(OHLC))]
                idx = np.where(np.array(idxs) != -1)[0][0]
                col = OHLC[idx]
                msg = f'price-repair-split: Potential {fix_type} detected only in column {col}, so treating as false positive (ignore)'
                self.manager.LogEvent('info', 'price-repair-split-'+self.istr, msg)
            else:
                # Only correct if at least 2 columns require correction.
                for j in range(len(OHLC)):
                    c = OHLC[j]
                    ranges = OHLC_correct_ranges[j]
                    if ranges is None:
                        ranges = []
                    for r in ranges:
                        if r[2] == 'split':
                            m = split ; m_rcp = split_rcp
                        else:
                            m = split_rcp ; m_rcp = split
                        if self.interday:
                            msg = f"Corrected bad split adjustment on col={c} range=[{df2.index[r[1]-1].date()}:{df2.index[r[0]].date()}] m={m:.4f}"
                        else:
                            msg = f"Corrected bad split adjustment on col={c} range=[{df2.index[r[1]-1]}:{df2.index[r[0]]}] m={m:.4f}"
                        self.manager.LogEvent('info', 'price-repair-split-'+self.istr, msg)
                        df2.iloc[r[0]:r[1], df2.columns.get_loc(c)] *= m
                        if correct_volume:
                            if c == 'Open':
                                f_open_fixed[r[0]:r[1]] = True
                            elif c == 'Close':
                                f_close_fixed[r[0]:r[1]] = True
                        f_corrected[r[0]:r[1]] = True

                if correct_volume:
                    f_open_and_closed_fixed = f_open_fixed & f_close_fixed
                    f_open_xor_closed_fixed = np.logical_xor(f_open_fixed, f_close_fixed)
                    if f_open_and_closed_fixed.any():
                        df2.loc[f_open_and_closed_fixed, "Volume"] = (df2.loc[f_open_and_closed_fixed, "Volume"]*m_rcp).round().astype('int')
                    if f_open_xor_closed_fixed.any():
                        df2.loc[f_open_and_closed_fixed, "Volume"] = (df2.loc[f_open_and_closed_fixed, "Volume"]*0.5*m_rcp).round().astype('int')

                df2.loc[f_corrected, 'Repaired?'] = True

        else:
            idx_first_f = np.where(f)[0][0]
            if appears_suspended and (idx_latest_active is not None and idx_latest_active >= idx_first_f):
                # Suspended midway during data date range.
                # 1: process data before suspension in index-ascending (date-descending) order.
                # 2: process data after suspension in index-descending order. Requires signals to be reversed, 
                #    then returned ranges to also be reversed, because this logic was originally written for
                #    index-ascending (date-descending) order.
                ranges_before = map_signals_to_ranges(f[idx_latest_active:], f_up[idx_latest_active:], f_down[idx_latest_active:])
                if len(ranges_before) > 0:
                    # Shift each range back to global indexing
                    for i in range(len(ranges_before)):
                        r = ranges_before[i]
                        ranges_before[i] = (r[0] + idx_latest_active, r[1] + idx_latest_active, r[2])
                f_rev_down = np.flip(np.roll(f_up, -1))
                f_rev_up = np.flip(np.roll(f_down, -1))
                f_rev = f_rev_up | f_rev_down
                ranges_after = map_signals_to_ranges(f_rev[idx_rev_latest_active:], f_rev_up[idx_rev_latest_active:], f_rev_down[idx_rev_latest_active:])
                if len(ranges_after) > 0:
                    # Shift each range back to global indexing:
                    for i in range(len(ranges_after)):
                        r = ranges_after[i]
                        ranges_after[i] = (r[0] + idx_rev_latest_active, r[1] + idx_rev_latest_active, r[2])
                    # Flip range to normal ordering
                    for i in range(len(ranges_after)):
                        r = ranges_after[i]
                        ranges_after[i] = (n-r[1], n-r[0], r[2])
                ranges = ranges_before ; ranges.extend(ranges_after)
            else:
                ranges = map_signals_to_ranges(f, f_up, f_down)
            if start_min is not None:
                # Prune ranges that are older than start_min
                for i in range(len(ranges)-1, -1, -1):
                    r = ranges[i]
                    if df2.index[r[0]].date() < start_min:
                        msg = f'price-repair-split: Pruning range {df2.index[r[0]]}->{df2.index[r[1]-1]} because too old.'
                        self.manager.LogEvent('debug', 'price-repair-split-'+self.istr, msg)
                        del ranges[i]
            for r in ranges:
                if r[2] == 'split':
                    m = split ; m_rcp = split_rcp
                else:
                    m = split_rcp ; m_rcp = split
                msg = f"price-repair-split: range={r} m={m}"
                self.manager.LogEvent('debug', 'price-repair-split-'+self.istr, msg)
                for c in ['Open', 'High', 'Low', 'Close']:
                    df2.iloc[r[0]:r[1], df2.columns.get_loc(c)] *= m
                if correct_dividend:
                    df2.iloc[r[0]:r[1], df2.columns.get_loc('Dividends')] *= m
                if correct_volume:
                    df2.iloc[r[0]:r[1], df2.columns.get_loc("Volume")] = (df2['Volume'].iloc[r[0]:r[1]]*m_rcp).round().astype('int')
                df2.iloc[r[0]:r[1], df2.columns.get_loc('Repaired?')] = True
                if r[0] == r[1] - 1:
                    if self.interday:
                        msg = f"Corrected bad split adjustment on interval {df2.index[r[0]].date()} m={m:.4f}"
                    else:
                        msg = f"Corrected bad split adjustment on interval {df2.index[r[0]]} m={m:.4f}"
                else:
                    # Note: df2 sorted with index descending
                    start = df2.index[r[1] - 1]
                    end = df2.index[r[0]]
                    if self.interday:
                        msg = f"Corrected bad split adjustment across intervals {start.date()} -> {end.date()} (inclusive) m={m:.4f}"
                    else:
                        msg = f"Corrected bad split adjustment across intervals {start} -> {end} (inclusive) m={m:.4f}"
                self.manager.LogEvent('info', 'price-repair-split-'+self.istr, msg)

        if correct_volume:
            df2['Volume'] = df2['Volume'].round(0).astype('int')

        yfcl.TraceExit(log_func + " returning")
        return df2.sort_index()

    def _repairZeroPrices(self, df, silent=False):
        yfcu.TypeCheckDataFrame(df, "df")

        # Sometimes Yahoo returns prices=0 when obviously wrong e.g. Volume > 0 and Close > 0.
        # Easy to detect and fix

        if df.empty:
            return df
        if df.shape[0] == 1:
            # Need multiple rows to confidently identify outliers
            return df

        log_msg_enter = f"PM::_repairZeroPrices-{self.istr}(date_range={df.index[0]}->{df.index[-1]+self.itd})"
        log_msg_exit = f"PM::_repairZeroPrices-{self.istr}() returning"
        yfcl.TraceEnter(log_msg_enter)

        df2 = df.copy()

        price_cols = [c for c in ["Open", "High", "Low", "Close", "Adj Close"] if c in df2.columns]
        f_zero_or_nan = (df2[price_cols] == 0.0).to_numpy() | df2[price_cols].isna().to_numpy()
        # Check whether worth attempting repair
        if f_zero_or_nan.any(axis=1).sum() == 0:
            yfcl.TraceExit(log_msg_exit + " (no bad data)")
            return df
        if f_zero_or_nan.sum() == len(price_cols)*len(df2):
            # Need some good data to calibrate
            yfcl.TraceExit(log_msg_exit + " (insufficient calibration data)")
            return df
        # - avoid repair if many zeroes/NaNs
        pct_zero_or_nan = f_zero_or_nan.sum() / (len(price_cols)*len(df2))
        if f_zero_or_nan.any(axis=1).sum() > 2 and pct_zero_or_nan > 0.05:
            yfcl.TraceExit(log_msg_exit + " (too much bad data)")
            return df

        data_cols = price_cols + ["Volume"]

        # Mark values to send for repair
        tag = -1.0
        for i in range(len(price_cols)):
            c = price_cols[i]
            df2.loc[f_zero_or_nan[:, i], c] = tag
        # If volume=0 or NaN for bad prices, then tag volume for repair
        if self.ticker.endswith("=X"):
            # FX, volume always 0
            pass
        else:
            df2.loc[f_zero_or_nan.any(axis=1) & (df2["Volume"] == 0), "Volume"] = tag
            df2.loc[f_zero_or_nan.any(axis=1) & (df2["Volume"].isna()), "Volume"] = tag

        # print(df2[f_zero_or_nan.any(axis=1)])
        n_before = (df2[data_cols].to_numpy() == tag).sum()
        # print("n_before =", n_before)
        try:
            df2 = self._reconstruct_intervals_batch(df2, tag=tag)
        except Exception:
            if len(self._stack_trace) > 0:
                self._stack_trace.pop(len(self._stack_trace) - 1)
            raise
        n_after = (df2[data_cols].to_numpy() == tag).sum()
        n_fixed = n_before - n_after
        msg = f"Fixed {n_fixed}/{n_before} price=0.0 errors in {self.istr} price data"
        if not silent and n_fixed > 0:
            print(f"{self.ticker}: " + msg)
        else:
            self.manager.LogEvent("info", "PriceManager", msg)

        # Restore original values where repair failed (i.e. remove tag values)
        f = df2[data_cols].to_numpy() == tag
        for j in range(len(data_cols)):
            fj = f[:, j]
            if fj.any():
                c = data_cols[j]
                df2.loc[fj, c] = df.loc[fj, c]

        yfcl.TraceExit(log_msg_exit)

        return df2

    def _reverseYahooAdjust(self, df):
        yfcu.TypeCheckDataFrame(df, "df")

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

        debug = False
        # debug = True

        if self.interday:
            log_msg = f"PM::_reverseYahooAdjust-{self.istr}, {df.index[0].date()}->{df.index[-1].date()}"
        else:
            log_msg = f"PM::_reverseYahooAdjust-{self.istr}, {df.index[0]}->{df.index[-1]}"
        if yfcl.IsTracingEnabled():
            yfcl.TraceEnter(log_msg)
        elif debug:
            print("")
            print(log_msg)

        if debug:
            print(df[["Open", "Low", "High", "Close", "Volume"]])

        cdf = None
        csf = None

        if self.interval != yfcd.Interval.Days1:
            # Trigger update of daily price data, to get all events
            histDaily = self.manager.GetHistory(yfcd.Interval.Days1)
            df_daily_raw = histDaily.get(start=df.index[0].date(), repair=False)

        # Step 1: ensure intraday price data is always split-adjusted
        td_7d = timedelta(days=7)
        if self.intraday:
            # Get daily price data during and after 'df'

            df_daily = df_daily_raw.copy()
            for c in ["Open", "Close", "Low", "High"]:
                df_daily[c] *= df_daily["CSF"]
            df_daily["Volume"] /= df_daily["CSF"]
            df_daily = df_daily.drop("CSF", axis=1)

            if df_daily is None or df_daily.empty:
                # df = df.drop("Adj Close", axis=1)
                df["CSF"] = 1.0
                df["CDF"] = 1.0
                return df

            f_post = df_daily.index.date > df.index[-1].date()
            df_daily_during = df_daily[~f_post].copy()
            df_daily_post = df_daily[f_post].copy()
            df_daily_during.index = df_daily_during.index.date ; df_daily_during.index.name = "_date"

            # Also get raw daily data from cache
            df_daily_raw = df_daily_raw[df_daily_raw.index.date >= df.index[0].date()]
            #
            f_post = df_daily_raw.index.date > df.index[-1].date()
            df_daily_raw_during = df_daily_raw[~f_post].copy()
            df_daily_raw_during_d = df_daily_raw_during.copy()
            df_daily_raw_during_d.index = df_daily_raw_during_d.index.date ; df_daily_raw_during_d.index.name = "_date"

            if df_daily_post.empty:
                csf_post = 1.0
            else:
                csf_post = yfcu.GetCSF0(df_daily_post)
            expectedRatio = 1.0 / csf_post

            ss_ratio = expectedRatio
            ss_ratioRcp = 1.0 / ss_ratio
            #
            price_data_cols = ["Open", "Close", "Adj Close", "Low", "High"]
            if ss_ratio > 1.01:
                for c in price_data_cols:
                    df[c] *= ss_ratioRcp
                if debug:
                    print("Applying 1:{} stock-split".format(round(ss_ratio, 2)))
            elif ss_ratioRcp > 1.01:
                for c in price_data_cols:
                    df[c] *= ss_ratio
                if debug:
                    print("Applying {.2f}:1 reverse-split-split".format(ss_ratioRcp))
            # Note: volume always returned unadjusted

            # Yahoo messes up dividend adjustment too so copy correct dividend from daily,
            # but only to first time periods of each day:
            df["_date"] = df.index.date
            df["_indexBackup"] = df.index
            # Copy over CSF and CDF too from daily
            df = pd.merge(df, df_daily_raw_during_d[["CDF", "CSF"]], how="left", on="_date", validate="many_to_one")
            df.index = df["_indexBackup"] ; df.index.name = None ; df = df.drop(["_indexBackup", "_date"], axis=1)
            cdf = df["CDF"].to_numpy()
            df["Adj Close"] = df["Close"] * cdf
            csf = df["CSF"]

            if not df_daily_post.empty:
                post_csf = yfcu.GetCSF0(df_daily_post)

        elif self.interval == yfcd.Interval.Week:
            df_daily = histDaily.get(start=df.index[-1].date()+td_7d, repair=False)
            if (df_daily is not None) and (not df_daily.empty):
                post_csf = yfcu.GetCSF0(df_daily)
                if debug:
                    print("- post_csf of daily date range {}->{} = {}".format(df_daily.index[0], df_daily.index[-1], post_csf))

        # elif self.interval in [yfcd.Interval.Months1, yfcd.Interval.Months3]:
        #     raise Exception("not implemented")

        # If 'df' does not contain all stock splits until present, then
        # set 'post_csf' to cumulative stock split factor just after last 'df' date
        splits_post = self.manager.GetHistory("Events").GetSplits(start=df.index[-1].date()+timedelta(days=1))
        if splits_post is not None:
            post_csf = 1.0/splits_post["Stock Splits"].prod()
        else:
            post_csf = None

        # Cumulative dividend factor
        if cdf is None:
            f_nna = ~df["Close"].isna()
            if not f_nna.any():
                cdf = 1.0
            else:
                cdf = np.full(df.shape[0], np.nan)
                cdf[f_nna] = df.loc[f_nna, "Adj Close"] / df.loc[f_nna, "Close"]
                cdf = pd.Series(cdf).bfill().ffill().to_numpy()

        # In rare cases, Yahoo is not calculating 'Adj Close' correctly
        search_start_dt = df.index[-1].date() + max(timedelta(days=1), self.itd)
        divs_since = self.manager.GetHistory("Events").GetDivs(start=search_start_dt)
        if divs_since is not None and not divs_since.empty:
            # Check that 'Adj Close' reflects all future dividends
            expected_adj = divs_since['Back Adj.'].prod()
            if self.interday and self.interval != yfcd.Interval.Days1:
                if df['Dividends'].iloc[-1] != 0.0:
                    dt = df.index[-1]
                    # Note: df hasn't been de-splitted yet
                    hist_before = self.manager.GetHistory(yfcd.Interval.Days1).get(start=dt.date()-timedelta(days=7), end=dt.date(), adjust_splits=True, adjust_divs=False)
                    close = hist_before['Close'].iloc[-1]
                    adj_adj = 1 - df['Dividends'].iloc[-1] / close
                    if adj_adj < 1.0:
                        msg = f"Adjusting expected_adj={expected_adj:.3f} by last-row div={df['Dividends'].iloc[-1]} adj={adj_adj:.3f} @ {dt.date()}"
                        self.manager.LogEvent("info", '_reverseYahooAdjust', msg)
                        if yfcl.IsTracingEnabled():
                            yfcl.TracePrint(msg)
                        expected_adj *= adj_adj
            actual_adj = cdf[-1]
            if not np.isnan(actual_adj):
                if actual_adj == 1.0:
                    if expected_adj == actual_adj:
                        ratio = 1.0
                    else:
                        ratio = np.inf
                else:
                    ratio = (1-expected_adj)/(1-actual_adj)
                diff_pct = abs(ratio -1.0)
                if diff_pct > 0.005:
                    msg = f'expected_adj={expected_adj:.4f} != actual_adj={actual_adj:.4f} @ {df.index[-1].date()}, correcting'
                    self.manager.LogEvent("info", '_reverseYahooAdjust', msg)
                    divs_since.index = divs_since.index.date
                    # Bad. Dividends have occurred after this price data, but 'Adj Close' is missing adjustment(s).
                    # Fix
                    cdf *= expected_adj / actual_adj

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
        if 'Capital Gains' in df.columns:
            df['Capital Gains'] = df['Capital Gains'] * csf_rcp
        if not self.interday:
            # Don't need to de-split volume data because Yahoo always returns interday volume unadjusted
            pass
        else:
            df["Volume"] *= csf

        if df["Volume"].dtype != 'int64':
            df["Volume"] = df["Volume"].round(0).astype('int')

        # Drop 'Adj Close', replace with scaling factors:
        df = df.drop("Adj Close", axis=1)
        df["CSF"] = csf
        df["CDF"] = cdf

        h_lastDivAdjustDt = pd.Timestamp.utcnow().tz_convert(ZoneInfo("UTC"))
        h_lastSplitAdjustDt = h_lastDivAdjustDt
        df["LastDivAdjustDt"] = h_lastDivAdjustDt
        df["LastSplitAdjustDt"] = h_lastSplitAdjustDt

        if debug:
            print("- unadjusted:")
            print(df[["Close", "Dividends", "Volume", "CSF", "CDF"]])
            f = df["Dividends"] != 0.0
            if f.any():
                print("- dividends:")
                print(df.loc[f, ["Open", "Low", "High", "Close", "Dividends", "Volume", "CSF", "CDF"]])
            print("")

        log_msg = f"PM::_reverseYahooAdjust-{self.istr}() returning"
        if yfcl.IsTracingEnabled():
            yfcl.TraceExit(log_msg)
        elif debug:
            print(log_msg)

        if debug:
            print(df[["Open", "Low", "High", "Close", "Dividends", "Volume", "CSF"]])

        return df

    def _applyNewEvents(self):
        if self.h is None or self.h.empty:
            return

        h_modified = False

        log_msg = f"PM::_applyNewEvents()-{self.istr}"
        if yfcl.IsTracingEnabled():
            yfcl.TraceEnter(log_msg)

        # Backport new splits across entire h table
        lastSplitAdjustDt_min = self.h["LastSplitAdjustDt"].min()
        splits_since = self.manager.GetHistory("Events").GetSplitsFetchedSince(lastSplitAdjustDt_min)
        if splits_since is not None and not splits_since.empty:
            LastSplitAdjustDt_new = self.h["LastSplitAdjustDt"].copy()

            f_sup = splits_since["Superseded split"] != 0.0
            if f_sup.any():
                for dt in splits_since.index[f_sup]:
                    split = splits_since.loc[dt]
                    f1 = self.h.index < dt
                    diff1 = (self.h["LastSplitAdjustDt"] - split["Superseded split FetchDate"]).abs()
                    f2 = (diff1 < pd.Timedelta("15s")).to_numpy()
                    diff2 = (self.h["FetchDate"] - split["Superseded split FetchDate"]).abs()
                    f3 = (diff2 < pd.Timedelta("15s")).to_numpy()
                    f = f1 & (f2 | f3)
                    if not f.any():
                        if self.interval != yfcd.Interval.Days1:
                            # Probably ok, assuming superseded split was never applied to this price data
                            continue
                        print(split)
                        raise Exception("For superseded split above, failed to identify rows to undo. Problem?")
                    else:
                        # Next check: expect cached CSF != 1.0

                        log_msg = f"{self.istr}: Reversing split [dt={dt.date()} {split['Superseded split']} fetch={split['Superseded split FetchDate'].strftime('%Y-%m-%d %H:%M:%S%z')}]"
                        indices = np.where(f)[0]
                        log_msg += " from intervals "
                        if self.interday:
                            log_msg += f"{self.h.index[indices[0]].date()} -> {self.h.index[indices[-1]].date()} (inc)"
                        else:
                            log_msg += f"{self.h.index[indices[0]]} -> {self.h.index[indices[-1]]}"
                        h_lastRow = self.h.loc[self.h.index[indices[-1]]]
                        log_msg += f". Last CSF = {h_lastRow['CSF']:.5f} @ {h_lastRow['LastSplitAdjustDt'].strftime('%Y-%m-%d %H:%M:%S%z')}"
                        self.manager.LogEvent("info", "PriceManager", log_msg)
                        if yfcl.IsTracingEnabled():
                            yfcl.TracePrint(log_msg)

                        self.h.loc[f, "CSF"] *= split["Stock Splits"]
                        LastSplitAdjustDt_new[f] = np.maximum(split['FetchDate'], LastSplitAdjustDt_new[f])

            for dt in splits_since.index:
                split = splits_since.loc[dt]
                if split["Stock Splits"] == 1.0:
                    continue
                f1 = self.h.index < dt
                f2 = self.h["LastSplitAdjustDt"] < (split["FetchDate"] - timedelta(minutes=2))
                f = f1 & f2
                # # Update: handle scenario where prices fetched day before split - usually already adjusted
                # if split['Superseded split'] == 0.0:
                #     window = timedelta(hours=12)
                #     f3 = (self.h['FetchDate'] + window) < dt
                #     f = f & f3
                # Update 2: disable above, it was causing new splits to not be applied
                if f.any():
                    log_msg = f"{self.istr}: Applying split [dt={dt.date()} {split['Stock Splits']} fetch={split['FetchDate'].strftime('%Y-%m-%d %H:%M:%S%z')}]"
                    indices = np.where(f)[0]
                    log_msg += " across intervals "
                    if self.interday:
                        log_msg += f"{self.h.index[indices[0]].date()} -> {self.h.index[indices[-1]].date()} (inc)"
                    else:
                        log_msg += f"{self.h.index[indices[0]]} -> {self.h.index[indices[-1]]}"
                    h_lastRow = self.h.loc[self.h.index[indices[-1]]]
                    log_msg += f". Last CSF = {h_lastRow['CSF']:.5f} @ {h_lastRow['LastSplitAdjustDt'].strftime('%Y-%m-%d %H:%M:%S%z')}"
                    self.manager.LogEvent("info", "PriceManager", log_msg)
                    if yfcl.IsTracingEnabled():
                        yfcl.TracePrint(log_msg)

                    if isinstance(self.h["CSF"].iloc[0], (int, np.int64)):
                        self.h["CSF"] = self.h["CSF"].astype(float)
                    self.h.loc[f, "CSF"] /= split["Stock Splits"]
                    LastSplitAdjustDt_new.loc[f] = np.maximum(LastSplitAdjustDt_new[f], split["FetchDate"])
            self.h["LastSplitAdjustDt"] = LastSplitAdjustDt_new

            h_modified = True

        # Backport new divs across entire h table
        lastDivAdjustDt_min = self.h["LastDivAdjustDt"].min()
        if isinstance(lastDivAdjustDt_min.tzinfo, pytz.BaseTzInfo):
            self.h["LastDivAdjustDt"] = self.h["LastDivAdjustDt"].dt.tz_convert(self.h.index.tz)
            h_modified = True
            lastDivAdjustDt_min = self.h["LastDivAdjustDt"].min()
        divs_since = self.manager.GetHistory("Events").GetDivsFetchedSince(lastDivAdjustDt_min)
        if divs_since is not None and not divs_since.empty:
            LastDivAdjustDt_new = self.h["LastDivAdjustDt"].copy()

            f_sup = divs_since["Superseded back adj."] != 0.0
            if f_sup.any():
                for dt in divs_since.index[f_sup]:
                    div = divs_since.loc[dt]
                    f1 = self.h.index < dt
                    # Update: new strategy
                    # Instead of last adjust being the superseded dividend,
                    # set condition as last adjust being before this new dividend
                    f2 = ((div["FetchDate"] - self.h["LastDivAdjustDt"]) > pd.Timedelta('1m')).to_numpy()
                    f3 = ((div["FetchDate"] - self.h["FetchDate"]) > pd.Timedelta('1m')).to_numpy()
                    f = f1 & (f2 & f3)
                    if not f.any():
                        if self.interval != yfcd.Interval.Days1:
                            # Probably ok, assuming superseded div was never applied to this price data
                            continue
                        else:
                            diff = (self.h["FetchDate"] - div["FetchDate"]).abs()
                            f_recent = (diff < pd.Timedelta("15s")).to_numpy()
                            if f_recent[f1].all():
                                # All price data that could be affected by new dividend, was just
                                # fetched with that dividend. So can safely ignore.
                                continue
                    else:
                        # Next check: expect cached CDF < 1.0:
                        f1 = self.h.loc[f, "CDF"] >= 1.0
                        if f1.any():
                            # This can happen with recent multiday intervals and that's ok, and will correct manually.
                            f1_oldest_idx = np.where(f1)[0][-1]
                            f1_oldest_dt = self.h.index[f1_oldest_idx]
                            is_f1_oldest_dt_recent = (pd.Timestamp.utcnow() - f1_oldest_dt) < (1.5*self.itd)
                            if self.interday and self.interval != yfcd.Interval.Days1 and is_f1_oldest_dt_recent:
                                # Yup, that's what happened
                                f[f1_oldest_idx:] = False
                                f1 = self.h.loc[f, "CDF"] >= 1.0

                        log_msg = f"{self.istr}: Reversing div [dt={dt.date()} {div['Superseded div']} adj={div['Superseded back adj.']:.5f} fetch={div['Superseded div FetchDate'].strftime('%Y-%m-%d %H:%M:%S%z')}]"
                        indices = np.where(f)[0]
                        log_msg += " from intervals "
                        if self.interday:
                            log_msg += f"{self.h.index[indices[0]].date()} -> {self.h.index[indices[-1]].date()} (inc)"
                        else:
                            log_msg += f"{self.h.index[indices[0]]} -> {self.h.index[indices[-1]]}"
                        h_lastRow = self.h.loc[self.h.index[indices[-1]]]
                        log_msg += f". Last CDF = {h_lastRow['CDF']:.5f} @ {h_lastRow['LastDivAdjustDt'].strftime('%Y-%m-%d %H:%M:%S%z')}"
                        self.manager.LogEvent("info", "PriceManager", log_msg)
                        if yfcl.IsTracingEnabled():
                            yfcl.TracePrint(log_msg)

                        self.h.loc[f, "CDF"] /= div["Superseded back adj."]
                        LastDivAdjustDt_new[f] = np.maximum(div['FetchDate'], LastDivAdjustDt_new[f])

                self.h['CDF'] = self.h['CDF'].clip(lower=None, upper=1)

            for dt in divs_since.index:
                div = divs_since.loc[dt]
                if div['Dividends'] == 0.0:
                    continue
                f1 = self.h.index < dt
                # Update: add a small 2min buffer, in case we are adjusting data fetched just before 
                # this div, so obviously the data already has div applied:
                f2 = self.h["LastDivAdjustDt"] < (div["FetchDate"] - timedelta(minutes=2))
                f = f1 & f2
                # Update: handle scenario where prices fetched day before div - usually already adjusted.
                # But only if this div does not supersede. If it does supersede, then must apply this div.
                # if div['Superseded div'] == 0.0:
                #     window = timedelta(hours=12)
                #     f_inWindow = ((self.h['FetchDate'] + window) >= dt).to_numpy()
                #     if f_inWindow.any():
                #         # Only reverse div if CDF < 1.0
                #         f_hasCdf = (self.h['CDF'] < 1.0).to_numpy()
                #         f_inWindowAndAdjusted = f_inWindow & f_hasCdf
                #         if f_inWindowAndAdjusted.any():
                #             # Exclude these
                #             f = f & (~f_inWindowAndAdjusted)

                if f.any():
                    log_msg = f"{self.istr}: Applying div [dt={dt.date()} {div['Dividends']} adj={div['Back Adj.']:.5f} fetch={div['FetchDate'].strftime('%Y-%m-%d %H:%M:%S%z')}]"
                    indices = np.where(f)[0]
                    log_msg += " across intervals "
                    if self.interday:
                        log_msg += f"{self.h.index[indices[0]].date()} -> {self.h.index[indices[-1]].date()} (inc)"
                    else:
                        log_msg += f"{self.h.index[indices[0]]} -> {self.h.index[indices[-1]]}"
                    h_lastRow = self.h.loc[self.h.index[indices[-1]]]
                    log_msg += f". Last CDF = {h_lastRow['CDF']:.5f} @ {h_lastRow['LastDivAdjustDt'].strftime('%Y-%m-%d %H:%M:%S%z')}"
                    self.manager.LogEvent("info", "PriceManager", log_msg)
                    if yfcl.IsTracingEnabled():
                        yfcl.TracePrint(log_msg)

                    self.h.loc[f, "CDF"] *= div["Back Adj."]
                    if div["Back Adj."] == 1.0:
                        idx = self.h.index.searchsorted(dt, side='right') -1
                        if div['Superseded div'] == 0.0:
                            # Problem. Only option is zeroing the div
                            self.h.loc[self.h.index[idx], 'Dividends'] = 0
                        else:
                            self.h.loc[self.h.index[idx], 'Dividends'] -= div['Superseded div']
                    LastDivAdjustDt_new[f] = np.maximum(LastDivAdjustDt_new[f], div["FetchDate"])
            self.h["LastDivAdjustDt"] = LastDivAdjustDt_new

            h_modified = True

        if h_modified:
            self.h['CDF'] = self.h['CDF'].clip(lower=None, upper=1)

            self._updatedCachedPrices(self.h)

        log_msg = "PM::_applyNewEvents() returning"
        if yfcl.IsTracingEnabled():
            yfcl.TraceExit(log_msg)
