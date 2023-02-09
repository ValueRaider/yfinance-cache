import yfinance as yf

from . import yfc_cache_manager as yfcm
from . import yfc_dat as yfcd
from . import yfc_time as yfct
from . import yfc_utils as yfcu

import numpy as np
import pandas as pd
# import itertools
from scipy import ndimage as _ndimage
from datetime import datetime, date, time, timedelta
import dateutil
from zoneinfo import ZoneInfo
from pprint import pprint
import click


# TODOs:
# - when filling a missing interval with NaNs, try to reconstruct first


class TraceLogger:
    def __init__(self):
        self._trace_depth = 0

    def Print(self, log_msg):
        print(" "*self._trace_depth + log_msg)

    def Enter(self, log_msg):
        self.Print(log_msg)
        self._trace_depth += 2

    def Exit(self, log_msg):
        self._trace_depth -= 2
        self.Print(log_msg)

tc = None
# tc = TraceLogger()

class HistoriesManager:
    # Intended as single to class to ensure:
    # - only one History() object exists for each timescale/data type
    # - different History() objects and communicate

    def __init__(self, ticker, exchange, tzName, session, proxy):
        yfcu.TypeCheckStr(ticker, "ticker")
        yfcu.TypeCheckStr(exchange, "exchange")
        yfcu.TypeCheckStr(tzName, "tzName")

        self.ticker = ticker
        self.exchange = exchange
        self.tzName = tzName
        self.histories = {}
        self.session = session
        self.proxy = proxy

    def GetHistory(self, key):
        permitted_keys = set(yfcd.intervalToString.keys()) | {"Events"}
        if key not in permitted_keys:
            raise ValueError(f"key='{key}' is invalid, must be one of: {permitted_keys}")

        if key not in self.histories:
            if key in yfcd.intervalToString.keys():
                if key == yfcd.Interval.Days1:
                    self.histories[key] = PriceHistory(self, self.ticker, self.exchange, self.tzName, key, self.session, self.proxy, repair=True, contiguous=True)
                else:
                    self.histories[key] = PriceHistory(self, self.ticker, self.exchange, self.tzName, key, self.session, self.proxy, repair=True, contiguous=False)
            elif key == "Events":
                self.histories[key] = EventsHistory(self, self.ticker, self.exchange, self.tzName, self.session, self.proxy)
            else:
                raise Exception(f"Not implemented code path for key='{key}'")

        return self.histories[key]


class EventsHistory:
    def __init__(self, manager, ticker, exchange, tzName, session, proxy):
        if not isinstance(manager, HistoriesManager):
            raise TypeError(f"'manager' must be HistoriesManager not {type(manager)}")
        yfcu.TypeCheckStr(ticker, "ticker")
        yfcu.TypeCheckStr(exchange, "exchange")
        yfcu.TypeCheckStr(tzName, "tzName")

        self.manager = manager
        self.ticker = ticker
        self.exchange = exchange
        self.tzName = tzName
        self.session = session
        self.proxy = proxy

        self.tz = ZoneInfo(self.tzName)

        if yfcm.IsDatumCached(self.ticker, "dividends"):
            self.divs = yfcm.ReadCacheDatum(self.ticker, "dividends")
        else:
            self.divs = None

        if yfcm.IsDatumCached(self.ticker, "splits"):
            self.splits = yfcm.ReadCacheDatum(self.ticker, "splits")
        else:
            self.splits = None


    def GetDivs(self, start, end=None):
        yfcu.TypeCheckDateStrict(start, "start")
        if end is not None:
            yfcu.TypeCheckDateStrict(end, "end")

        if self.divs is None or self.divs.empty:
            return None

        tz = self.divs.index[0].tz
        # tz = self.divs["Date"].iloc[0].tz
        start = pd.Timestamp(start).tz_localize(tz)
        if end is not None:
            end = pd.Timestamp(end).tz_localize(tz)

        td_1d = timedelta(days=1)
        if end is None:
            slc = self.divs.loc[start:]
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

        log_msg = "GetDivsFetchedSince() returning"
        if result is None:
            log_msg += " None"
        else:
            log_msg += f" {len(result)}"

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
        if tc is not None:
            n = splits_df.shape[0]
            if n <= 2:
                tc.Enter(f"UpdateSplits({splits_df.index.date})")
            else:
                tc.Enter(f"UpdateSplits(n={n})")

        debug = False
        debug = True

        yfcu.TypeCheckDataFrame(splits_df, "splits_df")
        splits_df = splits_df.copy()
        if not splits_df.empty:
            expected_cols = ["Stock Splits", "FetchDate"]
            for c in expected_cols:
                if c not in splits_df.columns:
                    raise ValueError("UpdateSplits() 'splits_df' columns must contain: '{expected_cols}'")

            # Prepare 'splits_df' for append
            splits_df["Supersede?"] = False
            for dt in splits_df.index:
                new_split = splits_df.loc[dt, "Stock Splits"]
                if self.splits is not None and dt in self.splits.index:
                    cached_split = self.splits.loc[dt, "Stock Splits"]
                    if debug and tc is not None:
                        tc.Print(f"pre-existing stock-split @ {dt}: {cached_split} vs {new_split}")
                    diff_pct = 100*abs(cached_split-new_split)/cached_split
                    if diff_pct < 0.01:
                        # tiny difference, easier to just keep old value
                        splits_df = splits_df.drop(dt)
                        if debug and tc is not None:
                            tc.Print("ignoring")
                    else:
                        self.splits = self.splits.drop(dt)
                        splits_df.loc[dt, "Supersede?"] = True
                        if debug and tc is not None:
                            tc.Print("supersede")

            cols = ["Stock Splits", "FetchDate"]
            if not splits_df.empty:
                if self.splits is None:
                    self.splits = splits_df[cols].copy()
                else:
                    self.splits = pd.concat([self.splits, splits_df], sort=True)[cols]
                yfcm.StoreCacheDatum(self.ticker, "splits", self.splits)

        if tc is not None:
            tc.Exit("UpdateSplits() returning")


    def UpdateDividends(self, divs_df):
        if tc is not None:
            n = divs_df.shape[0]
            if n <= 2:
                tc.Enter(f"UpdateDividends({divs_df.index.date})")
            else:
                tc.Enter(f"UpdateDividends(n={n})")

        debug = False
        debug = True

        yfcu.TypeCheckDataFrame(divs_df, "divs_df")
        divs_df = divs_df.copy()
        if not divs_df.empty:
            expected_cols = ["Dividends", "FetchDate", "Close day before"]
            # expected_cols = ["Dividends", "FetchDate", "Close today"]
            for c in expected_cols:
                if c not in divs_df.columns:
                    raise ValueError(f"AddDividends() 'divs_df' is missing column: '{c}'")

            # Prepare 'divs_df' for append
            divs_df["Back Adj."] = np.nan
            divs_df["Supersede?"] = False
            for dt in divs_df.index:
                new_div = divs_df.loc[dt, "Dividends"]
                if self.divs is not None and dt in self.divs.index:
                    cached_div = self.divs.loc[dt, "Dividends"]
                    if debug and tc is not None:
                        tc.Print(f"pre-existing dividend @ {dt}: {cached_div} vs {new_div}")
                    diff_pct = 100*abs(cached_div-new_div)/cached_div
                    if diff_pct < 0.01:
                        # tiny difference, easier to just keep old value
                        divs_df = divs_df.drop(dt)
                        if debug and tc is not None:
                            tc.Print("ignoring")
                    else:
                        self.divs = self.divs.drop(dt)
                        divs_df.loc[dt, "Supersede?"] = True
                        if debug and tc is not None:
                            tc.Print("supersede")
                else:
                    close_before = divs_df.loc[dt, "Close day before"]
                    # adj = (close_before - new_div) / close_before
                    adj = 1.0 - new_div / close_before
                    # # F = P2/(P2+D)
                    # # http://marubozu.blogspot.com/2006/09/how-yahoo-calculates-adjusted-closing.html#c8038064975185708856
                    # close_today = divs_df.loc[dt, "Close today"]
                    # adj = close_today / (close_today + new_div)
                    if debug:
                        if tc is not None:
                            tc.Print(f"new dividend: {new_div} @ {dt}, close_before={close_before} , adj={adj}")
                    divs_df.loc[dt, "Back Adj."] = adj
            divs_df = divs_df.drop("Close day before", axis=1)
            # divs_df = divs_df.drop("Close today", axis=1)

            # cols = ["Dividends", "Back Adj.", "FetchDate"]
            cols = ["Dividends", "Back Adj.", "FetchDate", "Supersede?"]
            if not divs_df.empty:
                if self.divs is None:
                    self.divs = divs_df[cols].copy()
                else:
                    self.divs = pd.concat([self.divs, divs_df], sort=True)[cols]
                yfcm.StoreCacheDatum(self.ticker, "dividends", self.divs)

        if tc is not None:
            tc.Exit("UpdateDividends() returning")


class PriceHistory:
    def __init__(self, manager, ticker, exchange, tzName, interval, session, proxy, repair=True, contiguous=False):
        if isinstance(interval, str):
            if interval not in yfcd.intervalStrToEnum.keys():
                raise Exception("'interval' if str must be one of: {}".format(yfcd.intervalStrToEnum.keys()))
            interval = yfcd.intervalStrToEnum[interval]
        yfcu.TypeCheckStr(ticker, "ticker")
        yfcu.TypeCheckStr(exchange, "exchange")
        yfcu.TypeCheckStr(tzName, "tzName")
        yfcu.TypeCheckBool(repair, "repair")
        yfcu.TypeCheckBool(contiguous, "contiguous")

        self.manager = manager
        self.ticker = ticker
        self.exchange = exchange
        self.tzName = tzName
        self.interval = interval
        self.session = session
        self.proxy = proxy
        self.repair = repair
        self.contiguous = contiguous

        self.dat = yf.Ticker(self.ticker, session=self.session)
        self.tz = ZoneInfo(self.tzName)

        self.itd = yfcd.intervalToTimedelta[self.interval]
        self.istr = yfcd.intervalToString[self.interval]
        self.interday = self.interval in [yfcd.Interval.Days1, yfcd.Interval.Week, yfcd.Interval.Months1, yfcd.Interval.Months3]
        self.intraday = not self.interday

        # Load from cache
        self.cache_key = "history-"+self.istr
        self.h = self._getCachedPrices()

        # A place to temporarily store new dividends, until prices have 
        # been repaired, then they can be sent to EventsHistory
        # TODO: store these in file, in case script killed before 
        # divs can be stored properly. Delete file after sent.
        self._newDivs = []

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
        elif h is not None:
            h_modified = False

            if "Adj Close" in h.columns:
                raise Exception("Adj Close in cached h")

            f_dups = h.index.duplicated()
            if f_dups.any():
                raise Exception("{}: These timepoints have been duplicated: {}".format(self.ticker, h.index[f_dups]))

            f_na = np.isnan(h["CDF"].to_numpy())
            if f_na.any():
                h["CDF"] = h["CDF"].fillna(method="bfill").fillna(method="ffill")
                f_na = h["CDF"].isna()
                if f_na.any():
                    raise Exception("CDF NaN repair failed")
                h_modified = True

            if h_modified:
                yfcm.StoreCacheDatum(self.ticker, self.cache_key, h)

        return h


    def _updatedCachedPrices(self, df):
        yfcu.TypeCheckDataFrame(df, "df")

        expected_cols = ["Open", "High", "Low", "Close", "Volume", "Dividends", "Stock Splits"]
        expected_cols += ["Final?", "C-Check?", "FetchDate", "CSF", "CDF"]
        expected_cols += ["LastDivAdjustDt", "LastSplitAdjustDt"]

        missing_cols = [c for c in expected_cols if c not in df.columns]
        if len(missing_cols) > 0:
            raise Exception(f"DF missing these columns: {missing_cols}")

        yfcm.StoreCacheDatum(self.ticker, self.cache_key, df)

        self.h = df


    def get(self, start=None, end=None, period=None, max_age=None, repair=True, prepost=False, adjust_splits=False, adjust_divs=False, quiet=False):
        if start is None and end is None and period is None:
            raise ValueError("Must provide value for one of: 'start', 'end', 'period'")
        if start is not None:
            yfcu.TypeCheckIntervalDt(start, self.interval, "start", strict=False)
        if end is not None:
            yfcu.TypeCheckIntervalDt(end, self.interval, "end", strict=False)
        if period is not None:
            yfcu.TypeCheckPeriod(period, "period")
        yfcu.TypeCheckBool(repair, "repair")
        yfcu.TypeCheckBool(adjust_splits, "adjust_splits")
        yfcu.TypeCheckBool(adjust_divs, "adjust_divs")

        # TODO: enforce 'max_age' value provided. Only 'None' while I dev
        if max_age is None:
            if self.interval == yfcd.Interval.Days1:
                max_age = timedelta(hours=4)
            elif self.interval == yfcd.Interval.Week:
                max_age = timedelta(hours=60)
            elif self.interval == yfcd.Interval.Months1:
                max_age = timedelta(days=15)
            elif self.interval == yfcd.Interval.Months3:
                max_age = timedelta(days=45)
            else:
                max_age = 0.5*yfcd.intervalToTimedelta[self.interval]

        # YFC cannot handle pre- and post-market intraday
        prepost = self.interday

        yfct.SetExchangeTzName(self.exchange, self.tzName)
        td_1d = timedelta(days=1)
        tz_exchange = ZoneInfo(self.tzName)
        dt_now = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
        d_now_exchange = dt_now.astimezone(tz_exchange).date()
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

        debug = False
        debug_yfc = self._debug
        # debug_yfc = True

        log_msg = f"PriceHistory-{self.istr}.get(tkr={self.ticker}, start={start}, end={end}, period={period}, prepost={prepost}, repair={repair})"
        if tc is not None:
            tc.Enter(log_msg)
        elif debug_yfc:
            print(log_msg)

        yf_lag = yfcd.exchangeToYfLag[self.exchange]

        # h_modified = False
        # h_lastAdjustD = None

        pstr = None
        end_d = None ; end_dt = None
        if period is not None:
            if self.h is None:
                pstr = yfcd.periodToString[period]
            else:
                start_d, end_d = yfct.MapPeriodToDates(self.exchange, period)
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

        if period is None:
            if self.interday:
                if isinstance(start, datetime) or isinstance(end, datetime):
                    raise TypeError(f"'start' and 'end' must be date type not {type(start)}, {type(end)}")
            else:
                if (not isinstance(start, datetime)) and (not isinstance(end, datetime)):
                    raise TypeError(f"'start' and 'end' must be datetime type not {type(start)}, {type(end)}")

        listing_date = yfcm.ReadCacheDatum(self.ticker, "listing_date")
        if listing_date is not None and start is not None:
            listing_date_dt = datetime.combine(listing_date, time(0), tz_exchange)
            if isinstance(start, datetime):
                start = max(start, listing_date_dt)
            else:
                start = max(start, listing_date)

        if self.h is not None:
            if self.h.empty:
                self.h = None
                
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
                last_expired = yfct.IsPriceDatapointExpired(h_interval_dt, fetch_dt, max_age, self.exchange, self.interval, yf_lag=yf_lag)
                if last_expired:
                    # Drop last row because expired
                    self.h = self.h.drop(self.h.index[-1])
                    h_interval_dts = h_interval_dts[0: n-1]
                    n -= 1
            else:
                expired = np.array([False]*n)
                f_final = self.h["Final?"].to_numpy()
                for idx in np.where(~f_final)[0]:
                    h_interval_dt = h_interval_dts[idx]
                    fetch_dt = yfct.ConvertToDatetime(self.h["FetchDate"][idx], tz=tz_exchange)
                    expired[idx] = yfct.IsPriceDatapointExpired(h_interval_dt, fetch_dt, max_age, self.exchange, self.interval, yf_lag=yf_lag)
                if expired.any():
                    self.h = self.h.drop(self.h.index[expired])
                    h_interval_dts = h_interval_dts[~expired]
            if self.h.empty:
                self.h = None

        # h_lastDivAdjustDt = None
        # h_lastSplitAdjustDt = None
        if self.h is None:
            # Simple, just fetch the requested data

            if period is not None:
                h = self._fetchYfHistory(pstr, None, None, prepost, debug)
                if h is None:
                    raise Exception(f"{self.ticker}: Failed to fetch period={period}")
            else:
                # if self.interval == yfcd.Interval.Days1:
                if self.contiguous:
                    # Ensure daily always up-to-now
                    h = self._fetchYfHistory(pstr, start, tomorrow, prepost, debug)
                else:
                    h = self._fetchYfHistory(pstr, start, end, prepost, debug)
                if h is None:
                    raise Exception(f"{self.ticker}: Failed to fetch date range {start}->{end}")
            # h_modified = True

            # Adjust
            # if not self.interval == yfcd.Interval.Days1:
            #     self.manager.GetHistory(yfcd.Interval.Days1).get(start=self.h.index[-1].date()+td_1d)  # doubt needed
            h = self._reverseYahooAdjust(h)

            if self.interval == yfcd.Interval.Days1:
                h_splits = h[h["Stock Splits"] != 0]
                if len(h_splits) > 0:
                    self.manager.GetHistory("Events").UpdateSplits(h_splits)
                h_divs = h[h["Dividends"] != 0]
                if len(h_divs) > 0:
                    # self.manager.GetHistory("Events").UpdateDividends(h_divs)
                    self._newDivs.append(h_divs.copy())

            # if self.repair:
            #     h = self._repairZeroPrices(h)
            #     h = self._repairUnitMixups(h)

            # Cache
            # h_lastDivAdjustDt = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
            # h_lastSplitAdjustDt = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
            # yfcm.WriteCacheMetadata(self.ticker, self.cache_key, "LastDivAdjustDt", h_lastDivAdjustDt)
            # yfcm.WriteCacheMetadata(self.ticker, self.cache_key, "LastSplitAdjustDt", h_lastSplitAdjustDt)
            self._updatedCachedPrices(h)

        else:
            # Compare request against cached data, only fetch missing/expired data

            # n = self.h.shape[0]
            # if self.interday:
            #     h_interval_dts = self.h.index.date if isinstance(self.h.index[0], pd.Timestamp) else self.h.index
            # else:
            #     h_interval_dts = np.array([yfct.ConvertToDatetime(dt, tz=tz_exchange) for dt in self.h.index])
            # h_interval_dts = np.array(h_interval_dts)
            # # if self.interval == yfcd.Interval.Days1:
            # if self.contiguous:
            #     # Daily data is always contiguous so only need to check last row
            #     h_interval_dt = h_interval_dts[-1]
            #     fetch_dt = yfct.ConvertToDatetime(self.h["FetchDate"].iloc[-1], tz=tz_exchange)
            #     last_expired = yfct.IsPriceDatapointExpired(h_interval_dt, fetch_dt, max_age, self.exchange, self.interval, yf_lag=yf_lag)
            #     if last_expired:
            #         # Drop last row because expired
            #         self.h = self.h.drop(self.h.index[-1])
            #         h_interval_dts = h_interval_dts[0: n-1]
            #         n -= 1
            # else:
            #     expired = np.array([False]*n)
            #     f_final = self.h["Final?"].to_numpy()
            #     for idx in np.where(~f_final)[0]:
            #         h_interval_dt = h_interval_dts[idx]
            #         fetch_dt = yfct.ConvertToDatetime(self.h["FetchDate"][idx], tz=tz_exchange)
            #         expired[idx] = yfct.IsPriceDatapointExpired(h_interval_dt, fetch_dt, max_age, self.exchange, self.interval, yf_lag=yf_lag)
            #     if expired.any():
            #         self.h = self.h.drop(self.h.index[expired])
            #         h_interval_dts = h_interval_dts[~expired]
            # if self.h.empty:
            #     self.h = None

            # Performance TODO: tag rows as fully contiguous to avoid searching for gaps

            # Calculate ranges_to_fetch
            # if self.interval == yfcd.Interval.Days1:
            if self.contiguous:
                if self.h is None or self.h.empty:
                    if self.interday:
                        ranges_to_fetch = yfct.IdentifyMissingIntervalRanges(self.exchange, start_d, end_d, self.interval, [], weeklyUseYahooDef=True, minDistanceThreshold=5)
                    else:
                        ranges_to_fetch = yfct.IdentifyMissingIntervalRanges(self.exchange, start, end, self.interval, [], weeklyUseYahooDef=True, minDistanceThreshold=5)
                else:
                    # Ensure that daily data always up-to-date to now
                    dt_start = yfct.ConvertToDatetime(self.h.index[0], tz=tz_exchange)
                    dt_end = yfct.ConvertToDatetime(self.h.index[-1], tz=tz_exchange)
                    h_start = yfct.GetTimestampCurrentInterval(self.exchange, dt_start, self.interval, ignore_breaks=True, weeklyUseYahooDef=True)["interval_open"]
                    h_end = yfct.GetTimestampCurrentInterval(self.exchange, dt_end, self.interval, ignore_breaks=True, weeklyUseYahooDef=True)["interval_close"]

                    rangePre_to_fetch = None
                    if start < h_start:
                        try:
                            rangePre_to_fetch = yfct.IdentifyMissingIntervalRanges(self.exchange, start, h_start, self.interval, None, ignore_breaks=True, weeklyUseYahooDef=True, minDistanceThreshold=5)
                        except yfcd.NoIntervalsInRangeException:
                            rangePre_to_fetch = None
                    if rangePre_to_fetch is not None:
                        if len(rangePre_to_fetch) > 1:
                            raise Exception("Expected only one element in rangePre_to_fetch[], but = {}".format(rangePre_to_fetch))
                        rangePre_to_fetch = rangePre_to_fetch[0]
                    #
                    target_end_d = tomorrow_d
                    rangePost_to_fetch = None
                    if self.interday:
                        if h_end < target_end_d:
                            try:
                                rangePost_to_fetch = yfct.IdentifyMissingIntervalRanges(self.exchange, h_end, target_end_d, self.interval, None, weeklyUseYahooDef=True, minDistanceThreshold=5)
                            except yfcd.NoIntervalsInRangeException:
                                rangePost_to_fetch = None
                    else:
                        target_end_dt = dt_now
                        d = target_end_dt.astimezone(tz_exchange).date()
                        sched = yfct.GetExchangeSchedule(self.exchange, d, d + td_1d)
                        if (sched is not None) and (not sched.empty) and (dt_now > sched["open"].iloc[0]):
                            target_end_dt = sched["close"].iloc[0]+timedelta(hours=2)
                        if h_end < target_end_dt:
                            try:
                                rangePost_to_fetch = yfct.IdentifyMissingIntervalRanges(self.exchange, h_end, target_end_dt, self.interval, None, weeklyUseYahooDef=True, minDistanceThreshold=5)
                            except yfcd.NoIntervalsInRangeException:
                                rangePost_to_fetch = None
                    # if not yfct.IsTimestampInActiveSession(self.exchange, dt_now):
                    # if rangePost_to_fetch is not None:
                    #     print("rangePost_to_fetch:")
                    #     for r in rangePost_to_fetch:
                    #         print("- ", r)
                    #     day_sched = yfct.GetExchangeSchedule(self.exchange, d_now_exchange, tomorrow_d)
                    #     print(f"d_now_exchange={d_now_exchange}, tomorrow_d={tomorrow_d}")
                    #     print("day_sched:")
                    #     print(day_sched)
                    #     if dt_now < day_sched["open"].iloc[0]:
                    #         for i in range(len(rangePost_to_fetch)-1, -1, -1):
                    #             r = rangePost_to_fetch[i]
                    #             if self.interday:
                    #                 # if r[1] == (dt_now.astimezone(tz_exchange).date()+td_1d):
                    #                 if r[1] == tomorrow_d:
                    #                     # r[1] -= td_1d
                    #                     rangePost_to_fetch[i] = (r[0], r[1] - td_1d)
                    #                     if r[1] == r[0]:
                    #                         del rangePost_to_fetch[i]
                    #         if len(rangePost_to_fetch) == 0:
                    #             rangePost_to_fetch = None
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
                h_intervals = yfct.GetTimestampCurrentInterval_batch(self.exchange, h_interval_dts, self.interval, ignore_breaks=True, weeklyUseYahooDef=True)
                if h_intervals is None:
                    h_intervals = pd.DataFrame(data={"interval_open": [], "interval_close": []})
                else:
                    f_na = h_intervals["interval_open"].isna().to_numpy()
                    if f_na.any():
                        # Mapping Yahoo intervals -> xcal can fail now, because sometime xcal is wrong.
                        # Need to tolerate
                        h_intervals.loc[f_na, "interval_open"] = h_interval_dts[f_na]
                        h_intervals.loc[f_na, "interval_close"] = h_interval_dts[f_na]+self.itd
                # f_na = h_intervals["interval_open"].isna().to_numpy()
                # if f_na.any():
                #     print(self.h[f_na])
                #     raise Exception("Bad rows found in prices table")
                #     if debug_yfc:
                #         print("- found bad rows, deleting:")
                #         print(self.h[f_na])
                #     self.h = self.h[~f_na].copy()
                #     h_intervals = h_intervals[~f_na]
                if (not h_intervals.empty) and isinstance(h_intervals["interval_open"][0], datetime):
                    h_interval_opens = [x.to_pydatetime().astimezone(tz_exchange) for x in h_intervals["interval_open"]]
                else:
                    h_interval_opens = h_intervals["interval_open"].to_numpy()

                try:
                    ranges_to_fetch = yfct.IdentifyMissingIntervalRanges(self.exchange, start, end, self.interval, h_interval_opens, ignore_breaks=True, weeklyUseYahooDef=True, minDistanceThreshold=5)
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
                        sched = yfct.GetExchangeSchedule(self.exchange, x.date(), x.date() + 3*td_1d)
                        delete_range = dt_now < (sched["open"].iloc[0] + yf_lag)
                else:
                    if datetime.combine(x, time(0), tzinfo=tz_exchange) > dt_now:
                        delete_range = True
                    else:
                        sched = yfct.GetExchangeSchedule(self.exchange, x, x + 3*td_1d)
                        delete_range = dt_now < (sched["open"].iloc[0] + yf_lag)
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
                    self._fetchAndAddRanges_contiguous(pstr, ranges_to_fetch, prepost, debug, quiet=quiet)
                else:
                    self._fetchAndAddRanges_sparse(pstr, ranges_to_fetch, prepost, debug, quiet=quiet)

        if "Adj Close" in self.h.columns:
            raise Exception("Adj Close in self.h")

        # repair after all fetches complete
        if self.repair and repair:
            f_not_checked = ~(self.h["C-Check?"].to_numpy())
            if f_not_checked.any():
                ha = self.h[f_not_checked].copy()
                hb = self.h[~f_not_checked]
                ha = self._repairZeroPrices(ha, silent=True)
                ha = self._repairUnitMixups(ha, silent=True)
                ha["C-Check?"] = True
                if not hb.empty:
                    self.h = pd.concat([ha, hb], sort=True)
                    self.h.index = pd.to_datetime(self.h.index, utc=True).tz_convert(tz_exchange)
                else:
                    self.h = ha
                self.h = self.h.sort_index()
                self._updatedCachedPrices(self.h)

        # Now prices have been repaired, can send out dividends
        if len(self._newDivs) > 0:
            if tc is not None:
                tc.Print("sending out new dividends ...")
            # TODO: remove duplicates from _newDivs (possible when restoring file file)
            for divs_df in self._newDivs:
                divs_df["Close day before"] = np.nan
                for dt in divs_df.index:
                    if dt >= self.h.index[1]:
                        idx = self.h.index.get_loc(dt)
                        close_day_before = self.h["Close"].iloc[idx-1]
                        # close_day_before = self.h["Close"].iloc[idx-1] * self.h["CSF"].iloc[idx-1]
                    else:
                        # hist_before = self.manager.GetHistory(yfcd.Interval.Days1).get(start=dt.date()-timedelta(days=7), end=dt.date())
                        # hist_before = self.manager.GetHistory(yfcd.Interval.Days1).get(start=dt.date()-timedelta(days=7), end=dt.date(), adjust_splits=True)
                        hist_before = self.manager.GetHistory(yfcd.Interval.Days1).get(start=dt.date()-timedelta(days=7), end=dt.date(), adjust_splits=False)
                        close_day_before = hist_before["Close"].iloc[-1]
                    divs_df.loc[dt, "Close day before"] = close_day_before
                    # close_today = self.h.loc[dt, "Close"]
                    # divs_df.loc[dt, "Close today"] = close_today
                self.manager.GetHistory("Events").UpdateDividends(divs_df)
            self._newDivs = []
            self._applyNewEvents()

        if "Adj Close" in self.h.columns:
            raise Exception("Adj Close in self.h")

        if (start is not None) and (end is not None):
            h_copy = self.h.loc[start_dt:end_dt-timedelta(milliseconds=1)].copy()
        else:
            h_copy = self.h.copy()

        if adjust_splits:
            for c in ["Open", "High", "Low", "Close", "Dividends"]:
                h_copy[c] *= h_copy["CSF"]
            h_copy["Volume"] /= h_copy["CSF"]
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
        if tc is not None:
            tc.Exit(log_msg)
        elif debug_yfc:
            print(log_msg)

        return h_copy


    def _fetchAndAddRanges_contiguous(self, pstr, ranges_to_fetch, prepost, debug, quiet=False):
        if pstr is not None:
            yfcu.TypeCheckStr(pstr, "pstr")
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
        if tc is not None:
            tc.Enter(log_msg)
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
        if range_pre is not None:
            r = range_pre
            check_for_listing = False
            try:
                h2_pre = self._fetchYfHistory(pstr, r[0], r[1], prepost, debug)
            except yfcd.NoPriceDataInRangeException:
                if self.interval == yfcd.Interval.Days1 and r[1] - r[0] == td_1d:
                    # If only trying to fetch 1 day of 1d data, then print warning instead of exception.
                    # Could add additional condition of dividend previous day (seems to mess up table).
                    if not quiet:
                        # print("WARNING: No {}-price data fetched for ticker {} between dates {} -> {}".format(yfcd.intervalToString[self.interval], self.ticker, r[0], r[1]))
                        print(f"WARNING: {self.ticker}: No {yfcd.intervalToString[self.interval]}-price data fetched for {r[0]} -> {r[1]}")
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
                    df = self._fetchYfHistory(pstr, r[0], r[1]+td_1d*7, prepost, debug)
                except Exception:
                    # Discard
                    pass
                if df is not None:
                    # Then the exception above occurred because trying to fetch before listing dated!
                    yfcm.StoreCacheDatum(self.ticker, "listing_date", self.h.index[0].date())
                else:
                    # Then the exception above was genuine and needs to be propagated
                    raise yfcd.NoPriceDataInRangeException(self.ticker, self.istr, r[0], r[1])

        if range_post is not None:
            r = range_post
            try:
                h2_post = self._fetchYfHistory(pstr, r[0], r[1], prepost, debug)
            except yfcd.NoPriceDataInRangeException:
                # If only trying to fetch 1 day of 1d data, then print warning instead of exception.
                # Could add additional condition of dividend previous day (seems to mess up table).
                if self.interval == yfcd.Interval.Days1 and r[1] - r[0] == td_1d:
                    if not quiet:
                        # print("WARNING: No {}-price data fetched for ticker {} between dates {} -> {}".format(yfcd.intervalToString[self.interval], self.ticker, r[0], r[1]))
                        print(f"WARNING: {self.ticker}: No {yfcd.intervalToString[self.interval]}-price data fetched for {r[0]} -> {r[1]}")
                    h2_post = None
                else:
                    raise

        if h2_post is not None:
            # De-adjust the new data, and backport any new events in cached data
            # Note: Yahoo always returns split-adjusted price, so reverse it

            # Simple append to bottom of table
            # 1) adjust h2_post
            h2_post = self._reverseYahooAdjust(h2_post)
            if debug_yfc:
                print("- h2_post:")
                print(h2_post)

            # Update: repair AFTER fetches
            # if self.repair:
            #     h2_post = self._repairZeroPrices(h2_post)
            #     h2_post = self._repairUnitMixups(h2_post)

            # TODO: Instead of updating events from here (daily prices fetch), 
            #       add a fetch method to EventsHistory class that can update itself.
            #       Maybe also allow EventsHistory to send new prices to PriceHistory
            # TODO: Problem: dividends need correct close
            if self.interval == yfcd.Interval.Days1:
                # Update: moving UpdateDividends() to after repair
                h2_post_divs = h2_post[h2_post["Dividends"] != 0][["Dividends", "FetchDate"]].copy()
                if not h2_post_divs.empty:
                    if debug_yfc:
                        print("- h2_post_divs:")
                        print(h2_post_divs)
                    self._newDivs.append(h2_post_divs.copy())
                h2_post_splits = h2_post[h2_post["Stock Splits"] != 0][["Stock Splits", "FetchDate"]].copy()
                if not h2_post_splits.empty:
                    self.manager.GetHistory("Events").UpdateSplits(h2_post_splits)


            # Backport new events across entire h table
            self._applyNewEvents()


            if h2_post is not None and not isinstance(h2_post.index, pd.DatetimeIndex):
                raise Exception("h2_post.index not DatetimeIndex")

            if "Adj Close" in h2_post.columns:
                raise Exception("Adj Close in h2_post")
            self.h = pd.concat([self.h, h2_post], sort=True)
            self.h.index = pd.to_datetime(self.h.index, utc=True).tz_convert(tz_exchange)


        if h2_pre is not None:
            if debug_yfc:
                print("- prepending new data")
            # Simple prepend to top of table

            h2_pre = self._reverseYahooAdjust(h2_pre)

            # Update: repair AFTER fetches
            # if self.repair:
            #     h2_pre = self._repairZeroPrices(h2_pre)
            #     h2_pre = self._repairUnitMixups(h2_pre)

            # h2_pre = h2_pre.drop(["Dividends", "Stock Splits"], axis=1)
            if "Adj Close" in h2_pre.columns:
                raise Exception("Adj Close in h2_pre")
            self.h = pd.concat([self.h, h2_pre], sort=True)
            self.h.index = pd.to_datetime(self.h.index, utc=True).tz_convert(tz_exchange)


        self.h = self.h.sort_index()
        self._updatedCachedPrices(self.h)

        log_msg = "_fetchAndAddRanges_contiguous() returning"
        if tc is not None:
            tc.Exit(log_msg)
        elif debug_yfc:
            print("- h:")
            print(self.h)
            print(log_msg)


    def _fetchAndAddRanges_sparse(self, pstr, ranges_to_fetch, prepost, debug, quiet=False):
        if pstr is not None:
            yfcu.TypeCheckStr(pstr, "pstr")
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
        if tc is not None:
            tc.Enter(log_msg)
        if debug_yfc:
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
                h2 = self._fetchYfHistory(pstr, fetch_start, fetch_end, prepost, debug)
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

            # Update: repair AFTER fetches
            # if self.repair:
            #     h2 = self._repairZeroPrices(h2)
            #     h2 = self._repairUnitMixups(h2)

            if "Adj Close" in h2.columns:
                raise Exception("Adj Close in h2")
            try:
                self.h = self.h[yfcu.np_isin_optimised(self.h.index, h2.index, invert=True)]
            except Exception:
                print("self.h.shape:", self.h.shape)
                print("h2.shape:", h2.shape)
                raise
            self.h = pd.concat([self.h, h2], sort=True)
            self.h.index = pd.to_datetime(self.h.index, utc=True).tz_convert(tz_exchange)

            f_dups = self.h.index.duplicated()
            if f_dups.any():
                raise Exception(f"{self.ticker}: Adding range {rstart}->{rend} has added duplicate timepoints have been duplicated: {self.h.index[f_dups]}")

        self.h = self.h.sort_index()
        self._updatedCachedPrices(self.h)

        log_msg = "_fetchAndAddRanges_sparse() returning"
        if tc is not None:
            tc.Exit(log_msg)
        elif debug_yfc:
            print(log_msg)


    def _verifyCachedPrices(self, correct=False, discard_old=False, quiet=True, debug=False):
        yfcu.TypeCheckBool(correct, "correct")

        if self.h is None or self.h.empty:
            return True

        log_msg = f"_verifyCachedPrices-{self.istr}(correct={correct}, debug={debug})"
        if tc is not None:
            tc.Enter(log_msg)
        # elif debug:
        #     print(log_msg)

        def _verifyCachedPrices_exitClean():
            log_msg = f"_verifyCachedPrices-{self.istr}() returning"
            if tc is not None:
                tc.Exit(log_msg)
            # elif debug:
            #     print(log_msg)

        # New code: hopefully this will correct bad CDF in 1wk etc
        self._applyNewEvents()

        # h = self._getCachedPrices()
        h = self.h.copy()  # working copy for comparison with YF
        h_modified = False
        h_new = self.h.copy()  # copy for storing changes
        # Keep track of problems:
        f_diff_all = pd.Series(np.full(h.shape[0], False), h.index)
        n = h.shape[0]


        # Ignore non-final data because will differ to Yahoo
        h = h[h["Final?"].to_numpy()]


        # Apply stock-split adjustment to match YF
        for c in ["Open", "Close", "Low", "High", "Dividends"]:
            h[c] = h[c].to_numpy() * h["CSF"].to_numpy()
        h["Volume"] = h["Volume"].to_numpy() / h["CSF"].to_numpy()


        td_1d = pd.Timedelta("1D")
        dt_now = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))


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
            dt_now_local = pd.Timestamp(dt_now).tz_convert(self.tzName)
            if self.interval == yfcd.Interval.Hours1:
                max_lookback_days = 365*2
            elif self.istr in ["90m", "30m", "15m", "5m", "2m"]:
                max_lookback_days = 60
            elif self.interval == yfcd.Interval.Mins1:
                # max_lookback_days = 7
                max_lookback_days = 30
            max_lookback = timedelta(days=max_lookback_days)
            max_lookback -= timedelta(minutes=5)  # allow time for server processing
            # fetch_start_min = dt_now_local.ceil("1D") - max_lookback
            # fetch_start_min = dt_now_local.floor("1D") - max_lookback
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

                h = h.loc[fetch_start_min:]

        if not h.empty:
            # Fetch YF data
            start_dt = h.index[0]
            last_dt = h.index[-1]
            end_dt = last_dt + self.itd
            # if debug:
            #     msg = f"requesting YF fetch"
            #     if tc is not None:
            #         tc.Print(msg)
            #     else:
            #         print(msg)
            fetch_start = start_dt.date()
            if self.itd > timedelta(days=1):
                fetch_end = last_dt.date()+yfcd.intervalToTimedelta[self.interval]
            else:
                fetch_end = last_dt.date()+td_1d
            # Sometimes Yahoo doesn't return full trading data for last day if end = day after.
            # Add some more days to avoid problem.
            fetch_end += 3*td_1d
            fetch_end = min(fetch_end, pd.Timestamp.utcnow().tz_convert(self.tzName).ceil("D").date())
            repair = True if debug else "silent"
            if self.intraday:
                if self.interval == yfcd.Interval.Mins1:
                    # Fetch in 7-day batches
                    df_yf = None
                    td_7d = timedelta(days=7)
                    fetch_end_batch = fetch_end
                    fetch_start_batch = fetch_end - td_7d
                    while fetch_end_batch > fetch_start:
                        if debug:
                            msg = f"requesting YF fetch: {self.istr} {fetch_start_batch} -> {fetch_end_batch}"
                            if tc is not None:
                                tc.Print(msg)
                            else:
                                print(msg)
                        df_yf_batch = self.dat.history(interval=self.istr, start=fetch_start_batch, end=fetch_end_batch, auto_adjust=False, repair=repair, keepna=True)
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
                    if debug:
                        msg = f"requesting YF fetch: {self.istr} {fetch_start} -> {fetch_end}"
                        if tc is not None:
                            tc.Print(msg)
                        else:
                            print(msg)
                    # df_yf = self.dat.history(interval=self.istr, start=fetch_start, end=fetch_end, auto_adjust=False, repair=repair)
                    df_yf = self.dat.history(interval=self.istr, start=fetch_start, end=fetch_end, auto_adjust=False, repair=repair, keepna=True)
                    df_yf = df_yf.loc[start_dt:]
                    df_yf = df_yf[df_yf.index < end_dt]

                # Yahoo doesn't div-adjust intraday
                df_yf_1d = self.dat.history(interval="1d", start=df_yf.index[0].date(), end=df_yf.index[-1].date()+td_1d, auto_adjust=False)
                df_yf["_indexBackup"] = df_yf.index
                df_yf["_date"] = df_yf.index.date
                df_yf_1d["_date"] = df_yf_1d.index.date
                #
                # df_yf = df_yf.drop("Adj Close", axis=1).merge(df_yf_1d[["Adj Close", "_date"]], how="left", on="_date")
                df_yf_1d["Adj"] = df_yf_1d["Adj Close"].to_numpy() / df_yf_1d["Close"].to_numpy()
                df_yf = df_yf.merge(df_yf_1d[["Adj", "_date"]], how="left", on="_date")
                df_yf["Adj Close"] = df_yf["Close"].to_numpy() * df_yf["Adj"].to_numpy()
                df_yf = df_yf.drop("Adj", axis=1)
                #
                df_yf.index = df_yf["_indexBackup"]
                df_yf = df_yf.drop(["_indexBackup", "_date"], axis=1)
            else:
                if debug:
                    msg = f"requesting YF fetch: {self.istr} {fetch_start} -> {fetch_end}"
                    if tc is not None:
                        tc.Print(msg)
                    else:
                        print(msg)
                # df_yf = self.dat.history(interval=self.istr, start=fetch_start, end=fetch_end, auto_adjust=False, repair=repair)
                df_yf = self.dat.history(interval=self.istr, start=fetch_start, end=fetch_end, auto_adjust=False, repair=repair, keepna=True)
            if df_yf is None or df_yf.empty:
                raise Exception(f"Fetching reference yfinance data failed (interval={self.istr}, start_dt={start_dt}, last_dt={last_dt})")
            if self.interval == yfcd.Interval.Week:
                # Ensure data aligned to Monday:
                if not df_yf.index[0].weekday() == 0:
                    n = 0
                    while n < 3:
                        fetch_start -= timedelta(days=2)
                        df_yf = self.dat.history(interval=self.istr, start=fetch_start, end=fetch_end, auto_adjust=False, repair=repair)
                        n += 1
                        if df_yf.index[0].weekday() == 0:
                            break
                    if not df_yf.index[0].weekday() == 0:
                        raise Exception("Failed to get Monday-aligned weekly data from YF")
                    df_yf = df_yf.loc[h.index[0]:]
            # if debug:
            #     msg = f"requesting YF fetch - complete"
            #     if tc is not None:
            #         tc.Print(msg)
            #     else:
            #         print(msg)

            # if self.exchange == "ASX" and self.istr in ["1m", "2m", "5m"]:
            #     # ASX market closes at 4pm with auction around 4:11pm,
            #     # but sometimes Yahoo thinks tiny volume of trades occurred 
            #     # inbetween. Wrong!
            #     # Fix = merge with previous interval
            #     f = df_yf.index.time == time(16)
            #     if f.any():
            #         indices = sorted(list(np.where(f)[0]))
            #         for i in indices:
            #             df_yf.loc[df_yf.index[i-1], "Volume"] += df_yf["Volume"].iloc[i]
            #             df_yf.loc[df_yf.index[i-1], "Close"] = df_yf["Close"].iloc[i]
            #         df_yf = df_yf.drop(df_yf.index[indices])
            # Update: I think Yahoo right because TradingView confirms

            if not self.interday:
                # Volume not split-adjusted
                ss = df_yf["Stock Splits"].copy()
                ss[(ss == 0.0) | ss.isna()] = 1.0
                ss_rcp = 1.0 / ss
                csf = ss_rcp.sort_index(ascending=False).cumprod().sort_index(ascending=True).shift(-1, fill_value=1.0)
                df_yf["Volume"] = df_yf["Volume"].to_numpy() / csf


            if self.interval != yfcd.Interval.Days1 and correct:
                # Copy over any missing dividends
                c = "Dividends"
                h_divs = h.loc[h[c] != 0.0, c].copy().dropna()
                yf_divs = df_yf.loc[df_yf[c] != 0.0, c]
                dts_missing_from_cache = yf_divs.index[~yf_divs.index.isin(h_divs.index)]
                dts_missing_from_cache = [dt for dt in dts_missing_from_cache if dt in h.index]
                if len(dts_missing_from_cache) > 0:
                    if debug:
                        msg = f"CORRECTING: Cache missing these dividends: {dts_missing_from_cache}"
                        if tc is not None:
                            tc.Print(msg)
                        else:
                            print(msg)
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
                        if tc is not None:
                            tc.Print(msg)
                        else:
                            print(msg)
                    for dt in dts_missing_from_cache:
                        # Correct here
                        h.loc[dt, c] = yf_ss.loc[dt]
                        h_new.loc[dt, c] = yf_ss.loc[dt]
                        h_modified = True

            f_diff_all = f_diff_all | yfcu.VerifyPricesDf(h, df_yf, self.interval, quiet=quiet, debug=debug)

        if not f_diff_all.any():
            if debug:
                msg = "No differences"
                if tc is not None:
                    tc.Print(msg)
                else:
                    print(msg)

            if h_modified:
                if debug:
                    msg = "Writing DF to cache"
                    if tc is not None:
                        tc.Print(msg)
                    else:
                        print(msg)
                    # print(h_new.loc["2022-12-26"]) ; raise Exception("here")
                # yfcm.StoreCacheDatum(self.ticker, self.cache_key, h)
                yfcm.StoreCacheDatum(self.ticker, self.cache_key, h_new)
                self.h = self._getCachedPrices()

            _verifyCachedPrices_exitClean()
            return True

        # h = self.h
        h = h_new
        if correct:
            drop_dts = f_diff_all.index[f_diff_all]
            # dtnow = pd.Timestamp.utcnow()
            dtnow = pd.Timestamp.utcnow().tz_convert(self.tz).floor("D")
            drop_dts_ages = dtnow - drop_dts
            if self.interval == yfcd.Interval.Week:
                f = drop_dts_ages > timedelta(days=8)
            else:
                f = drop_dts_ages > timedelta(days=4)  # allow for weekend
            drop_dts_not_recent = drop_dts[f]
            drop_dts_ages = drop_dts_ages[f]
            msg = f"{self.ticker}: {self.istr}-prices problems"
            if self.contiguous:
                # Daily must always be contiguous, so drop everything from first diff
                if len(drop_dts_not_recent) > 0:
                    if self.interday:
                        msg += f": dropping all rows from {drop_dts_not_recent[0].date()}"
                    else:
                        msg += f": dropping all rows from {drop_dts_not_recent[0]}"
                    if tc is not None:
                        tc.Print(msg)
                    else:
                        print(msg)
                h = h[h.index < drop_dts[0]]
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
                    if tc is not None:
                        tc.Print(msg)
                    else:
                        print(msg)
                h2 = h.drop(drop_dts)
                if h.shape[0]-h2.shape[0] != n_drop:
                    raise Exception("here")
                h = h2
                h_modified = True
            if h.empty:
                h = None
                h_modified = True

        else:
            if debug:
                n = np.sum(f_diff_all)
                if n < 5:
                    msg = f"differences found but not correcting: {f_diff_all.index[f_diff_all]}"
                else:
                    msg = f"{np.sum(f_diff_all)} differences found but not correcting"
                if tc is not None:
                    tc.Print(msg)
                else:
                    print(msg)

        if h_modified:
            if debug:
                msg = "Writing DF to cache"
                if tc is not None:
                    tc.Print(msg)
                else:
                    print(msg)
            yfcm.StoreCacheDatum(self.ticker, self.cache_key, h)
            self.h = self._getCachedPrices()

        _verifyCachedPrices_exitClean()
        return False


    def _fetchYfHistory(self, pstr, start, end, prepost, debug, verify_intervals=True, disable_yfc_metadata=False):
        if start is None and end is None and pstr is None:
            raise ValueError("Must provide value for one of: 'start', 'end', 'pstr'")
        if pstr is not None:
            yfcu.TypeCheckStr(pstr, "pstr")
        if start is not None:
            yfcu.TypeCheckIntervalDt(start, self.interval, "start")
        if end is not None:
            yfcu.TypeCheckIntervalDt(end, self.interval, "end")
        yfcu.TypeCheckBool(prepost, "prepost")
        yfcu.TypeCheckBool(debug, "debug")

        _debug = False
        # _debug = True

        log_msg = f"_fetchYfHistory-{self.istr}(pstr={pstr} , {start}->{end}, prepost={prepost})"
        if tc is not None:
            tc.Enter(log_msg)
        elif _debug:
            print("")
            print(log_msg)

        if pstr is not None:
            if (start is not None) and (end is not None):
                # start/end take precedence over pstr
                pstr = None

        tz_exchange = self.tz
        td_1d = timedelta(days=1)
        dt_now = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))

        if pstr is not None:
            df = self._fetchYfHistory_period(pstr, prepost, debug)
            first_fetch_failed = df is None or df.empty

        else:
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
                dtnow = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
                dtnow_exchange = dtnow.astimezone(tz_exchange)
                if isinstance(end, datetime):
                    end_dt = end
                    # end_d = end.astimezone(tz_exchange).date()
                    end_d = None
                else:
                    end_d = end
                    end_dt = datetime.combine(end, time(0), tz_exchange)
                if end_dt > dtnow:
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

            if self.interval == yfcd.Interval.Days1:
                first_fetch_failed = False
                try:
                    df = self._fetchYfHistory_dateRange(fetch_start, fetch_end, prepost, debug)
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
                    if _debug:
                        msg = "- first fetch failed, trying again with wider range: {} -> {}".format(fetch_start, fetch_end)
                        tc.Print(msg) if tc is not None else print(msg)
                    try:
                        df_wider = self._fetchYfHistory_dateRange(fetch_start, fetch_end, prepost, debug)
                        if _debug:
                            msg = "- second fetch returned:"
                            tc.Print(msg) if tc is not None else print(msg)
                            print(df_wider)
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
                            df = df.loc[:fetch_end_dt-timedelta(milliseconds=1)]

                if first_fetch_failed:
                    if second_fetch_failed:
                        # Hopefully code never comes here
                        raise ex
                    else:
                        # Requested date range was just before stock listing date,
                        # but wider range crosses over so can continue
                        pass

            elif self.interday:
                df = self._fetchYfHistory_dateRange(fetch_start, fetch_end, prepost, debug)

            else:
                # Intraday
                fetch_ranges = [(fetch_start, fetch_end)]
                if self.intraday:
                    maxRange = yfcd.yfMaxFetchRange[self.interval]
                    if maxRange is not None:
                        td_1d = timedelta(days=1)
                        td_7d = timedelta(days=7)
                        s = yfct.GetExchangeSchedule(self.exchange, start_dt.date() - td_7d, end_dt.date() + td_7d)
                        s = s.iloc[s.index.get_indexer([str(start_dt.date())], method="ffill")[0]-1:]
                        s = s.iloc[:s.index.get_indexer([str(end_dt.date())], method="bfill")[0]+1+1]
                        lag = yfcd.exchangeToYfLag[self.exchange]
                        if start_dt > s["close"].iloc[1]+lag:
                            s = s.drop(s.index[0])
                        if end_dt < s["open"].iloc[-2]+lag:
                            s = s.drop(s.index[-1])
                        # fetch_ranges = yfcu.ChunkDatesIntoYfFetches(start_d, end_d, s, maxRange.days, overlapDays=2)
                        fetch_ranges = yfcu.ChunkDatesIntoYfFetches(s, maxRange.days, overlapDays=2)
                        if _debug:
                            print("- fetch_ranges:")
                            pprint(fetch_ranges)
                        # Don't need to fetch all of padding days, just the end/start of session
                        # fetch_ranges[0][0] = s["close"].iloc[0] - timedelta(hours=2)
                        # fetch_ranges[-1][1] = s["open"].iloc[-1] + timedelta(hours=2)
                        fetch_ranges[0]["fetch start"] = s["close"].iloc[0] - timedelta(hours=2)
                        fetch_ranges[-1]["fetch end"] = s["open"].iloc[-1] + timedelta(hours=2)
                        # print("- fetch_ranges:")
                        # pprint(fetch_ranges)
                        maxLookback = yfcd.yfMaxFetchLookback[self.interval] - timedelta(seconds=10)
                        if maxLookback is not None:
                            maxLookback_dt = (pd.Timestamp.utcnow() - maxLookback).tz_convert(tz_exchange)
                            for i in range(len(fetch_ranges)-1, -1, -1):
                                if fetch_ranges[i]["fetch start"] < maxLookback_dt:
                                    if _debug:
                                        print("- capping start to maxLookback_dt")
                                    # fetch_ranges[i]["fetch start"] = maxLookback_dt
                                    fetch_ranges[i]["fetch start"] = maxLookback_dt.ceil("D")
                                    fetch_ranges[i]["core start"] = fetch_ranges[i]["fetch start"] + td_1d
                                    if fetch_ranges[i]["fetch start"] >= fetch_ranges[i]["fetch end"]:
                                        del fetch_ranges[i]

                df = None
                for r in fetch_ranges:
                    if _debug:
                        print("- fetching:")
                        print(r)
                    fetch_start = r["fetch start"]
                    fetch_end = r["fetch end"]
                    dfr = self._fetchYfHistory_dateRange(fetch_start, fetch_end, prepost, debug)
                    # Discard padding days:
                    dfr = dfr.loc[r["core start"] : r["core end"] - timedelta(milliseconds=1)]
                    if _debug:
                        print("- dfr after discarding padding days:")
                        print(dfr[[c for c in ["Open", "Low", "High", "Close", "Dividends", "Volume"] if c in dfr.columns]])
                    if df is None:
                        df = dfr
                    else:
                        df = pd.concat([df, dfr], sort=True)
                    if df.index.duplicated().any():
                        raise Exception("df contains duplicated dates")


        fetch_dt_utc = datetime.utcnow()

        if (df is not None) and (df.index.tz is not None) and (not isinstance(df.index.tz, ZoneInfo)):
            # Convert to ZoneInfo
            df.index = df.index.tz_convert(tz_exchange)

        if _debug:
            if df is None:
                msg = "- YF returned None"
                tc.Print(msg) if tc is not None else print(msg)
            else:
                # pass
                msg = "- YF returned table:"
                tc.Print(msg) if tc is not None else print(msg)
                print(df[[c for c in ["Open", "Low", "High", "Close", "Dividends", "Volume"] if c in df.columns]])

        # Detect listing day
        listing_day = yfcm.ReadCacheDatum(self.ticker, "listing_date")
        if listing_day is None:
            if self.interval == yfcd.Interval.Days1:
                found_listing_day = False
                listing_day = None
                if df is not None and not df.empty:
                    if pstr == "max":
                        found_listing_day = True
                    else:
                        if pstr is not None:
                            fetch_start, fetch_end_d = yfct.MapPeriodToDates(self.exchange, yfcd.periodStrToEnum[pstr])
                        tol = yfcd.listing_date_check_tols[self.interval]
                        if fetch_start is not None:
                            fetch_start_d = fetch_start.date() if isinstance(fetch_start, datetime) else fetch_start
                            if (df.index[0].date() - fetch_start_d) > tol:
                                # Yahoo returned data starting significantly after requested start date, indicates
                                # request is before stock listed on exchange
                                found_listing_day = True
                        else:
                            start_expected = yfct.DtSubtractPeriod(fetch_dt_utc.date()+td_1d, yfcd.periodStrToEnum[pstr])
                            # if self.interval == yfcd.Interval.Week:
                            #     start_expected -= timedelta(days=start_expected.weekday())
                            if (df.index[0].date() - start_expected) > tol:
                                found_listing_day = True
                    if _debug:
                        msg = "- found_listing_day = {}".format(found_listing_day)
                        tc.Print(msg) if tc is not None else print(msg)
                    if found_listing_day:
                        listing_day = df.index[0].date()
                        if _debug:
                            msg = "YFC: inferred listing_date = {}".format(listing_day)
                            tc.Print(msg) if tc is not None else print(msg)
                        yfcm.StoreCacheDatum(self.ticker, "listing_date", listing_day)

                    if (listing_day is not None) and first_fetch_failed:
                        if end <= listing_day:
                            # Aha! Requested date range was entirely before listing
                            if _debug:
                                msg = "- requested date range was before listing date"
                                tc.Print(msg) if tc is not None else print(msg)
                            return None
                if found_listing_day:
                    # Apply to fetch start
                    if isinstance(start, datetime):
                        listing_date = datetime.combine(listing_day, time(0), self.tz)
                        start = max(start, listing_date)
                    else:
                        start = max(start, listing_day)
                        start_d = start

        if pstr is None:
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

                if _debug:
                    n = intervals_missing_df.shape[0]
                    if n <= 3:
                        msg = f"- YF data missing {n} intervals: {intervals_missing_df['open'].to_numpy()}"
                    else:
                        msg = f"- YF data missing {n} intervals"
                    tc.Print(msg) if tc is not None else print(msg)

                cutoff_d = date.today() - timedelta(days=14)
                dt_now_local = dt_now.astimezone(self.tz)
                if self.interday:
                    f_recent = intervals_missing_df["open"].to_numpy() > cutoff_d
                    f_today = intervals_missing_df["open"].to_numpy() == dt_now_local.date()
                else:
                    f_recent = intervals_missing_df["open"].dt.date > cutoff_d
                    f_today = intervals_missing_df["open"].dt.date == dt_now_local.date()
                intervals_missing_df_recent = intervals_missing_df[f_recent & ~f_today]
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
                        if _debug:
                            msg = "- found missing intervals, inserting nans:"
                            tc.Print(msg) if tc is not None else print(msg)
                            print(intervals_missing_df_recent)
                        if missing_intervals_to_add is None:
                            missing_intervals_to_add = intervals_missing_df_recent["open"].to_numpy()
                        else:
                            missing_intervals_to_add = np.append(missing_intervals_to_add, intervals_missing_df_recent["open"].to_numpy())

                if missing_intervals_to_add is not None:
                    if _debug:
                        n = missing_intervals_to_add.shape[0]
                        if n <= 3:
                            msg = f"- insertings NaNs for {n} missing intervals: {missing_intervals_to_add}"
                        else:
                            msg = f"- insertings NaNs for {n} missing intervals"
                        tc.Print(msg) if tc is not None else print(msg)

                    nm = missing_intervals_to_add.shape[0]
                    df_missing = pd.DataFrame(data={k: [np.nan]*nm for k in yfcd.yf_data_cols}, index=missing_intervals_to_add)
                    df_missing.index = pd.to_datetime(df_missing.index)
                    if self.interday:
                        df_missing.index = df_missing.index.tz_localize(tz_exchange)
                    for c in ["Volume", "Dividends", "Stock Splits"]:
                        df_missing[c] = 0
                    if df is None:
                        df = df_missing
                    else:
                        df = pd.concat([df, df_missing], sort=True)
                        df.index = pd.to_datetime(df.index, utc=True).tz_convert(tz_exchange)
                        df = df.sort_index()

        # Improve tolerance to calendar missing a recent new holiday:
        if (df is None) or df.empty:
            return None

        n = df.shape[0]

        fetch_dt = fetch_dt_utc.replace(tzinfo=ZoneInfo("UTC"))

        if (n > 0) and (pstr is None):
            # Remove any out-of-range data:
            # NOTE: YF has a bug-fix pending merge: https://github.com/ranaroussi/yfinance/pull/1012
            if end is not None:
                if self.interday:
                    df = df[df.index.date < end_d]
                else:
                    df = df[df.index < end_dt]
                n = df.shape[0]
            #
            # And again for pre-start data:
            if start is not None:
                if self.interday:
                    df = df[df.index.date >= start_d]
                else:
                    df = df[df.index >= start_dt]
                n = df.shape[0]

        if n == 0:
            raise yfcd.NoPriceDataInRangeException(self.ticker, self.istr, start, end)
        else:
            # Verify that all datetimes match up with actual intervals:
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
                    if _debug:
                        msg = "- dropping rows in break times"
                        tc.Print(msg) if tc is not None else print(msg)
                    yfIntervalStarts = yfIntervalStarts[~f_in_break]
                    df = df[~f_in_break]
                    n = df.shape[0]
            #
            intervals = yfct.GetTimestampCurrentInterval_batch(self.exchange, yfIntervalStarts, self.interval, ignore_breaks=True, weeklyUseYahooDef=True)

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
                        if _debug:
                            msg = "- dropping 0-volume rows starting at market close"
                            tc.Print(msg) if tc is not None else print(msg)
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
                    # For some national holidays when exchange closed, Yahoo fills in row. Clue is 0 volume.
                    # Solution = drop:
                    f_na_zeroVol = f_na & (df["Volume"] == 0).to_numpy()
                    if f_na_zeroVol.any():
                        if _debug:
                            msg = "- dropping {} 0-volume rows with no matching interval".format(sum(f_na_zeroVol))
                            tc.Print(msg) if tc is not None else print(msg)
                        f_drop = f_na_zeroVol
                        yfIntervalStarts = yfIntervalStarts[~f_drop]
                        intervals = intervals[~f_drop]
                        df = df[~f_drop]
                        n = df.shape[0]
                        f_na = intervals["interval_open"].isna().to_numpy()
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
                            if _debug:
                                msg = "- dropping rows with no interval that are identical to previous row"
                                tc.Print(msg) if tc is not None else print(msg)
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
                    # ctr = 0
                    # for idx in np.where(f_na)[0]:
                    #     dt = df.index[idx]
                    #     ctr += 1
                    #     if ctr < 10:
                    #         print("Failed to map: {} (exchange={}, xcal={})".format(dt, self.exchange, yfcd.exchangeToXcalExchange[self.exchange]))
                    #         print(df.loc[dt])
                    #     elif ctr == 10:
                    #         print("- stopped printing at 10 failures")
                    #
                    df_na = df[f_na][["Close", "Volume", "Dividends", "Stock Splits"]]
                    n = df_na.shape[0]
                    warning_msg = f"Failed to map these Yahoo intervals to xcal: (exchange={self.exchange}, xcal={yfcd.exchangeToXcalExchange[self.exchange]})."
                    warning_msg += " Normally happens when 'exchange_calendars' is wrong so inform developers."
                    print("")
                    print(warning_msg)
                    print(df_na)
                    msg = "Accept into cache anyway?"
                    # if self.exchange in ["ASX"]:
                    if False:
                        accept = True
                    else:
                        accept = click.confirm(msg, default=False)
                        # accept = True
                    if accept:
                        for idx in np.where(f_na)[0]:
                            dt = intervals.index[idx]
                            # print(f"- idx={idx} dt={dt}")
                            intervals.loc[dt, "interval_open"] = df.index[idx]
                            intervals.loc[dt, "interval_close"] = df.index[idx] + self.itd
                            # print(intervals.loc[dt])
                        # print(intervals)
                    else:
                        raise Exception("Problem with dates returned by Yahoo, see above")

        df = df.copy()

        if not disable_yfc_metadata:
            # lastDataDts = yfct.CalcIntervalLastDataDt_batch(self.exchange, yfIntervalStarts, self.interval)
            lastDataDts = yfct.CalcIntervalLastDataDt_batch(self.exchange, intervals["interval_open"].to_numpy(), self.interval)
            # lastDataDts = yfct.CalcIntervalLastDataDt_batch(self.exchange, intervals["interval_open"].to_numpy(), self.interval, ignore_breaks=True)
            if f_na.any():
                # Hacky solution to handle xcal having incorrect schedule, for valid Yahoo data
                lastDataDts[f_na] = intervals.index[f_na] + self.itd
                if self.intraday:
                    lastDataDts[f_na] += yfct.GetExchangeDataDelay(self.exchange)
                    # For some exchanges, Yahoo has trades that occurred soon afer official market close, e.g. Johannesburg:
                    if self.exchange in ["JNB"]:
                        lastDataDts[f_na] += timedelta(minutes=15)
            data_final = fetch_dt >= lastDataDts
            df["Final?"] = data_final

            # df["FetchDate"] = pd.Timestamp(fetch_dt_utc).tz_localize("UTC")
            df["FetchDate"] = pd.Timestamp(fetch_dt_utc).tz_localize(ZoneInfo("UTC"))

            df["C-Check?"] = False

        # log_msg = "_fetchYfHistory() returning"
        log_msg = f"_fetchYfHistory() returning DF {df.index[0]} -> {df.index[-1]}"
        if tc is not None:
            tc.Exit(log_msg)
        elif _debug:
            print(log_msg)

        return df

    def _fetchYfHistory_period(self, pstr, prepost, debug):
        yfcu.TypeCheckStr(pstr, "pstr")
        yfcu.TypeCheckBool(prepost, "prepost")
        yfcu.TypeCheckBool(debug, "debug")

        debug_yfc = False
        # debug_yfc = True

        log_msg = f"_fetchYfHistory_period-{self.istr}(pstr={pstr} , prepost={prepost})"
        if tc is not None:
            tc.Enter(log_msg)
        elif debug_yfc:
            print("")
            print(log_msg)

        history_args = {"period": pstr,
                        "interval": self.istr,
                        "prepost": prepost,
                        "actions": True,  # Always fetch
                        "keepna": True,
                        "repair": "silent",
                        "auto_adjust": False,  # store raw data, adjust myself
                        "back_adjust": False,  # store raw data, adjust myself
                        "proxy": self.proxy,
                        "rounding": False,  # store raw data, round myself
                        "raise_errors": True,
                        "debug": debug}

        if debug_yfc:
            msg = f"- {self.ticker}: fetching {pstr} period"
            tc.Print(msg) if tc is not None else print(msg)

        df = self.dat.history(**history_args)

        log_msg = f"_fetchYfHistory_period() returning DF {df.index[0]} -> {df.index[-1]}"
        if tc is not None:
            tc.Exit(log_msg)
        elif debug_yfc:
            print(log_msg)

        return df

    def _fetchYfHistory_dateRange(self, start, end, prepost, debug):
        yfcu.TypeCheckIntervalDt(start, self.interval, "start")
        yfcu.TypeCheckIntervalDt(end, self.interval, "end")
        yfcu.TypeCheckBool(prepost, "prepost")
        yfcu.TypeCheckBool(debug, "debug")

        debug_yfc = False
        # debug_yfc = True

        log_msg = f"_fetchYfHistory_dateRange-{self.istr}(start={start} , end={end} , prepost={prepost})"
        if tc is not None:
            tc.Enter(log_msg)
        elif debug_yfc:
            print("")
            print(log_msg)

        fetch_start = start
        fetch_end = end
        if not isinstance(fetch_start, (datetime, pd.Timestamp)):
            fetch_start_dt = datetime.combine(fetch_start, time(0), self.tz)
        else:
            fetch_start_dt = fetch_start
        # if not isinstance(fetch_end, (datetime, pd.Timestamp)):
        #     fetch_end_dt = datetime.combine(fetch_end, time(0), self.tz)
        # else:
        #     fetch_end_dt = fetch_end

        history_args = {"period": None,
                        "interval": self.istr,
                        "start": fetch_start, "end": fetch_end,
                        "prepost": prepost,
                        "actions": True,  # Always fetch
                        "keepna": True,
                        "repair": "silent",
                        "auto_adjust": False,  # store raw data, adjust myself
                        "back_adjust": False,  # store raw data, adjust myself
                        "proxy": self.proxy,
                        "rounding": False,  # store raw data, round myself
                        "raise_errors": True,
                        "debug": debug}

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
            tc.Print(msg) if tc is not None else print(msg)

        first_fetch_failed = False
        try:
            if debug_yfc:
                msg = f"- fetch_start={fetch_start} ; fetch_end={fetch_end}"
                tc.Print(msg) if tc is not None else print(msg)
            df = self.dat.history(**history_args)
            if debug_yfc:
                if df is None:
                    msg = "- YF returned None"
                    tc.Print(msg) if tc is not None else print(msg)
                else:
                    msg = "- YF returned table:"
                    tc.Print(msg) if tc is not None else print(msg)
                    print(df[[c for c in ["Open", "Low", "High", "Close", "Dividends", "Volume"] if c in df.columns]])
            if df is None or df.empty:
                raise Exception("No data found for this date range")
        except Exception as e:
            first_fetch_failed = True
            if "Data doesn't exist for startDate" in str(e):
                ex = yfcd.NoPriceDataInRangeException(self.ticker, self.istr, start, end)
                raise ex
            elif "No data found for this date range" in str(e):
                ex = yfcd.NoPriceDataInRangeException(self.ticker, self.istr, start, end)
                raise ex
            else:
                print("df:")
                print(df)
                raise e

        if not first_fetch_failed and fetch_start is not None:
            df = df.loc[fetch_start_dt:]
            if df.empty:
                first_fetch_failed = True
                ex = yfcd.NoPriceDataInRangeException(self.ticker, self.istr, start, end)

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
                    tc.Print(msg) if tc is not None else print(msg)
                df = self.dat.history(**history_args)
                if self.interval == yfcd.Interval.Week and (df.index[0].weekday() == 0):
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
            log_msg = "_fetchYfHistory_dateRange() returning None"
        else:
            log_msg = f"_fetchYfHistory_dateRange() returning DF {df.index[0]} -> {df.index[-1]}"
        if tc is not None:
            tc.Exit(log_msg)
        elif debug_yfc:
            print(log_msg)

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

        log_msg = f"_reconstruct_intervals_batch-{self.istr}(dt0={df.index[0]})"
        if tc is not None:
            tc.Enter(log_msg)

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
            # print(df_row)
            msg = f"WARNING: Have not implemented repair for '{self.interval}' interval. Contact developers"
            # raise Exception(msg)
            tc.Print(msg) if tc is not None else print(msg)
            log_msg = "_reconstruct_intervals_batch() returning"
            if tc is not None:
                tc.Exit(log_msg)
            return df
        sub_interday = sub_interval in [yfcd.Interval.Days1, yfcd.Interval.Week, yfcd.Interval.Months1, yfcd.Interval.Months3]
        sub_intraday = not sub_interday

        df = df.sort_index()

        f_repair = df[data_cols].to_numpy() == tag
        f_repair_rows = f_repair.any(axis=1)

        # Ignore old intervals for which Yahoo won't return finer data:
        # if sub_interval == yfcd.Interval.Hours1:
        #     f_recent = date.today() - df.index.date < timedelta(days=730)
        #     f_repair_rows = f_repair_rows & f_recent
        # elif sub_interval in [yfcd.Interval.Mins30, yfcd.Interval.Mins15]:
        #     f_recent = date.today() - df.index.date < timedelta(days=60)
        #     f_repair_rows = f_repair_rows & f_recent
        if min_lookback is None:
            min_dt = None
        else:
            min_dt = pd.Timestamp.utcnow() - min_lookback
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
        df_good = df[~df[price_cols].isna().any(axis=1)]
        f_tag = df_v2[price_cols].to_numpy() == tag

        # Group nearby NaN-intervals together to reduce number of Yahoo fetches
        dts_groups = [[dts_to_repair[0]]]
        # last_dt = dts_to_repair[0]
        # last_ind = indices_to_repair[0]
        # td = yfcd.intervalToTimedelta[self.interval]
        # if self.interval == yfcd.Interval.Months1:
        #     grp_td_threshold = timedelta(days=28)
        # elif self.interval == yfcd.Interval.Week:
        #     grp_td_threshold = timedelta(days=28)
        # elif self.interval == yfcd.Interval.Days1:
        #     grp_td_threshold = timedelta(days=14)
        # elif self.interval == yfcd.Interval.Hours1:
        #     grp_td_threshold = timedelta(days=7)
        # else:
        #     grp_td_threshold = timedelta(days=2)
        #     # grp_td_threshold = timedelta(days=7)
        # for i in range(1, len(dts_to_repair)):
        #     ind = indices_to_repair[i]
        #     dt = dts_to_repair[i]
        #     if (dt-dts_groups[-1][-1]) < grp_td_threshold:
        #         dts_groups[-1].append(dt)
        #     elif ind - last_ind <= 3:
        #         dts_groups[-1].append(dt)
        #     else:
        #         dts_groups.append([dt])
        #     last_dt = dt
        #     last_ind = ind
        # for i in range(1, len(dts_to_repair)):
        #     ind = indices_to_repair[i]
        #     dt = dts_to_repair[i]
        #     if (dt-dts_groups[-1][-1]) < grp_td_threshold:
        #         dts_groups[-1].append(dt)
        #     elif ind - last_ind <= 3:
        #         dts_groups[-1].append(dt)
        #     else:
        #         dts_groups.append([dt])
        #     last_dt = dt
        #     last_ind = ind
        if self.interval == yfcd.Interval.Months1:
            grp_max_size = dateutil.relativedelta.relativedelta(years=2)
        elif self.interval == yfcd.Interval.Week:
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
        # for i in range(len(dts_groups)):
        #     g = dts_groups[i]
        #     g0 = g[0]
        #     i0 = df_good.index.get_loc(g0)
        #     if i0 > 0:
        #         dts_groups[i].insert(0, df_good.index[i0-1])
        #     gl = g[-1]
        #     il = df_good.index.get_loc(gl)
        #     if il < len(df_good)-1:
        #         dts_groups[i].append(df_good.index[il+1])
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
            #     df_fine = self.dat.history(start=fetch_start, end=fetch_end, interval=yfcd.intervalToString[sub_interval], auto_adjust=False, repair="silent")
            else:
                if prepost and sub_intraday:
                    # YFC cannot handle intraday pre- and post-market, so fetch via yfinance
                    if debug:
                        # print("- fetching df_fine direct from YF")
                        print(f"- - fetch_start={fetch_start} fetch_end={fetch_end}")
                    df_fine_old = self.dat.history(start=fetch_start, end=fetch_end, interval=yfcd.intervalToString[sub_interval], auto_adjust=True, prepost=prepost)
                    hist_sub = self.manager.GetHistory(sub_interval)
                    if not isinstance(fetch_start, datetime):
                        fetch_start = datetime.combine(fetch_start, time(0), ZoneInfo(self.tzName))
                    if not isinstance(fetch_end, datetime):
                        fetch_end = datetime.combine(fetch_end, time(0), ZoneInfo(self.tzName))
                    if debug:
                        print("- fetching df_fine via _fetchYfHistory() wrapper")
                        print(f"- - fetch_start={fetch_start} fetch_end={fetch_end}")
                    df_fine = hist_sub._fetchYfHistory(pstr=None, start=fetch_start, end=fetch_end, prepost=prepost, debug=False, verify_intervals=False, disable_yfc_metadata=True)
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
                    df_fine = hist_sub.get(fetch_start, fetch_end, adjust_splits=False, adjust_divs=False, repair=False, prepost=prepost)
                # df_fine["Adj Close"] = df_fine["Close"] * df_fine["CDF"]
            if debug:
                print("- df_fine:")
                print(df_fine)
            if df_fine is None or len(df_fine) == 0:
                print("YF: WARNING: Cannot reconstruct because Yahoo not returning data in interval")
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
                Volume=("Volume", "sum"))  #,
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
                elif ratio_rcp > 1:
                    # data has different split-adjustment than fine-grained data
                    # Adjust fine-grained to match
                    df_new[price_cols] *= 1.0 / ratio_rcp
                    df_new["Volume"] *= ratio_rcp

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
                    df_v2.loc[idx, "Volume"] = df_new_row["Volume"]
                n_fixed += 1

            # Restore un-repaired bad values
            f_nan = df_v2[price_cols].isna().to_numpy()
            f_failed = f_tag & f_nan
            for j in range(len(price_cols)):
                f = f_failed[:, j]
                if f.any():
                    c = price_cols[j]
                    # for i in np.where(f)[0]:
                    #     idx = df_block.index[i]
                    #     df_v2.loc[idx, c] = df.loc[idx, c]
                    df_v2.loc[f, c] = df.loc[f, c]
            # if f_failed.any():
            #     raise Exception("here")

            if self._record_stack_trace:
                # Pop stack trace
                if len(self._stack_trace) == 0:
                    raise Exception("Failing to correctly push/pop stack trace (is empty too early)")
                if not self._stack_trace[-1] == fn_tuple:
                    for i in range(len(self._stack_trace)):
                        print("  "*i + str(self._stack_trace[i]))
                    raise Exception("Failing to correctly push/pop stack trace (see above)")
                self._stack_trace.pop(len(self._stack_trace) - 1)

        # if debug:
        #     print("- df_v2:")
        #     print(df_v2)

        log_msg = "_reconstruct_intervals_batch() returning"
        if tc is not None:
            tc.Exit(log_msg)

        return df_v2


    def _repairUnitMixups(self, df, silent=False):
        yfcu.TypeCheckDataFrame(df, "df")

        # Sometimes Yahoo returns few prices in cents/pence instead of $/£
        # I.e. 100x bigger
        # Easy to detect and fix, just look for outliers = ~100x local median

        if df.empty:
            return df
        if df.shape[0] == 1:
            # Need multiple rows to confidently identify outliers
            return df

        log_msg_enter = f"_repairUnitMixups-{self.istr}()"
        log_msg_exit = f"_repairUnitMixups-{self.istr}() returning"
        if tc is not None:
            tc.Enter(log_msg_enter)

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
            if tc is not None:
                tc.Exit(log_msg_exit)
            return df
        median = _ndimage.median_filter(df2[data_cols].to_numpy(), size=(3, 3), mode="wrap")

        if (median == 0).any():
            print("Ticker =", self.ticker)
            print("yf =", yf)
            print("df:")
            print(df)
            raise Exception("median contains zeroes, why?")
        ratio = df2[data_cols].to_numpy() / median
        ratio_rounded = (ratio / 20).round() * 20  # round ratio to nearest 20
        f = ratio_rounded == 100
        if not f.any():
            if tc is not None:
                tc.Exit(log_msg_exit)
            return df

        # Mark values to send for repair
        tag = -1.0
        for i in range(len(data_cols)):
            fi = f[:, i]
            c = data_cols[i]
            df2.loc[fi, c] = tag

        n_before = (df2[data_cols].to_numpy() == tag).sum()
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
            f = df2[data_cols].to_numpy() == tag
            for i in range(f.shape[0]):
                fi = f[i, :]
                if not fi.any():
                    continue
                idx = df2.index[i]

                c = "Open"
                j = data_cols.index(c)
                if fi[j]:
                    df2.loc[idx, c] = df.loc[idx, c] * 0.01
                #
                c = "Close"
                j = data_cols.index(c)
                if fi[j]:
                    df2.loc[idx, c] = df.loc[idx, c] * 0.01
                #
                c = "High"
                j = data_cols.index(c)
                if fi[j]:
                    df2.loc[idx, c] = df2.loc[idx, ["Open", "Close"]].max()
                #
                c = "Low"
                j = data_cols.index(c)
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
        f = df2[data_cols].to_numpy() == tag
        for j in range(len(data_cols)):
            fj = f[:, j]
            if fj.any():
                c = data_cols[j]
                df2.loc[fj, c] = df.loc[fj, c]
        if df2_zeroes is not None:
            df2 = pd.concat([df2, df2_zeroes]).sort_index()
            df2.index = pd.to_datetime()

        if tc is not None:
            tc.Exit(log_msg_exit)

        return df2


    def _repairZeroPrices(self, df, silent=False):
        yfcu.TypeCheckDataFrame(df, "df")

        # Sometimes Yahoo returns prices=0 when obviously wrong e.g. Volume > 0 and Close > 0.
        # Easy to detect and fix

        if df.empty:
            return df
        if df.shape[0] == 1:
            # Need multiple rows to confidently identify outliers
            return df

        log_msg_enter = f"_repairZeroPrices-{self.istr}(date_range={df.index[0]}->{df.index[-1]+self.itd})"
        log_msg_exit = f"_repairZeroPrices-{self.istr}() returning"
        if tc is not None:
            tc.Enter(log_msg_enter)

        df2 = df.copy()

        price_cols = [c for c in ["Open", "High", "Low", "Close", "Adj Close"] if c in df2.columns]
        f_zero_or_nan = (df2[price_cols] == 0.0).to_numpy() | df2[price_cols].isna().to_numpy()
        # Check whether worth attempting repair
        if f_zero_or_nan.any(axis=1).sum() == 0:
            if tc is not None:
                tc.Exit(log_msg_exit)
            return df
        if f_zero_or_nan.sum() == len(price_cols)*len(df2):
            # Need some good data to calibrate
            if tc is not None:
                tc.Exit(log_msg_exit)
            return df
        # - avoid repair if many zeroes/NaNs
        pct_zero_or_nan = f_zero_or_nan.sum() / (len(price_cols)*len(df2))
        if f_zero_or_nan.any(axis=1).sum() > 2 and pct_zero_or_nan > 0.05:
            if tc is not None:
                tc.Exit(log_msg_exit)
            return df

        data_cols = price_cols + ["Volume"]

        # Mark values to send for repair
        tag = -1.0
        for i in range(len(price_cols)):
            c = price_cols[i]
            df2.loc[f_zero_or_nan[:, i], c] = tag
        # If volume=0 or NaN for bad prices, then tag volume for repair
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
        if not silent and n_fixed > 0:
            print(f"{self.ticker}: fixed {n_fixed}/{n_before} price=0.0 errors in {self.istr} price data")

        # Restore original values where repair failed (i.e. remove tag values)
        f = df2[data_cols].to_numpy() == tag
        for j in range(len(data_cols)):
            fj = f[:, j]
            if fj.any():
                c = data_cols[j]
                df2.loc[fj, c] = df.loc[fj, c]

        if tc is not None:
            tc.Exit(log_msg_exit)

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

        log_msg = f"_reverseYahooAdjust-{self.istr}, {df.index[0]}->{df.index[-1]}"
        if tc is not None:
            tc.Enter(log_msg)
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
        if not self.interday:
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

            # Merge 'df' with daily data to compare and infer adjustment
            # df_aggByDay = df.copy()
            # df_aggByDay["_date"] = df_aggByDay.index.date
            # df_aggByDay = df_aggByDay.groupby("_date").agg(
            #     Low=("Low", "min"),
            #     High=("High", "max"),
            #     Open=("Open", "first"),
            #     Close=("Close", "last"))
            df_grp = df.copy()
            df_grp["_date"] = df_grp.index.date
            df_grp = df_grp.groupby("_date")
            df_aggByDay = df_grp.agg(
                Low=("Low", "min"),
                High=("High", "max"),
                Open=("Open", "first"),
                Close=("Close", "last"))

            # Cross-check inferred split-adjustment vs expected
            # - update: disable, because only fails due to insufficient data
            # data_cols = ["Open", "Close", "Low", "High"]
            # df2 = pd.merge(df_aggByDay, df_daily_during, how="left", on="_date", validate="one_to_one", suffixes=("", "_day"))
            # # If 'df' has not been split-adjusted by Yahoo, but it should have been,
            # # then the inferred split-adjust ratio should be close to 1.0/post_csf.
            # # Apply a few sanity tests against inferred ratio - not NaN, low variance
            # if df2[~df2["Close"].isna()].empty:
            #     ss_ratio = expectedRatio
            #     stdev_pct = 0.0
            # elif df.shape[0] == 1:
            #     ss_ratio = df2["Close"].iloc[0] / df2["Close_day"].iloc[0]
            #     stdev_pct = 0.0
            # else:
            #     ratios = df2[data_cols].to_numpy() / df2[[dc + "_day" for dc in data_cols]].to_numpy()
            #     ratios[df2[data_cols].isna()] = 1.0
            #     # ss_ratio = np.mean(ratios)
            #     # stdev_pct = np.std(ratios) / ss_ratio
            #     # Weight by number of rows per group, because groups with 1 row is screwing variance.
            #     stdev_pct = 0.0
            #     weights = df_grp.size().to_numpy().astype(float)
            #     weights_2d = np.tile(weights[:,None], ratios.shape[1])
            #     ss_ratio, std = yfcu.np_weighted_mean_and_std(ratios, weights_2d)
            #     stdev_pct = std / ss_ratio
            # #
            # # if stdev_pct > 0.05:
            # if stdev_pct > 0.03:
            #     cols_to_print = []
            #     for dc in data_cols:
            #         df2[dc + "_r"] = df2[dc] / df2[dc + "_day"]
            #         cols_to_print.append(dc)
            #         cols_to_print.append(dc + "_day")
            #         cols_to_print.append(dc + "_r")
            #     print(df2[cols_to_print])
            #     raise Exception("STDEV % of estimated stock-split ratio is {}%, should be near zero".format(round(stdev_pct * 100, 1)))

            # # if abs(1.0 - ss_ratio / expectedRatio) > 0.05:
            # if abs(1.0 - ss_ratio / expectedRatio) > 0.03:
            #     cols_to_print = []
            #     for dc in data_cols:
            #         df2[dc + "_r"] = df2[dc] / df2[dc + "_day"]
            #         cols_to_print.append(dc)
            #         cols_to_print.append(dc + "_day")
            #         cols_to_print.append(dc + "_r")
            #     print(df2[cols_to_print])
            #     print(df2[cols_to_print].iloc[-1])
            #     print("df:") ; print(df)
            #     raise Exception("ss_ratio={} != expected_ratio={}".format(ss_ratio, expectedRatio))

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
            # df = df.drop("Dividends", axis=1)
            # # - get first times
            # df["_time"] = df.index.time
            # df_openTimes = df[["_date", "_time"]].groupby("_date", as_index=False, group_keys=False).min().rename(columns={"_time": "_open_time"})
            # df = df.drop("_time", axis=1)
            # # - merge
            # df = pd.merge(df, df_daily_during[["Dividends"]], how="left", on="_date", validate="many_to_one")
            # df = pd.merge(df, df_openTimes, how="left", on="_date")
            # df.index = df["_indexBackup"] ; df.index.name = None
            # # - correct dividends
            # df.loc[df.index.time != df["_open_time"], "Dividends"] = 0.0
            # df = df.drop("_open_time", axis=1)
            # UPDATE: keep original dividends, only transfer CDF & CSF
            # Copy over CSF and CDF too from daily
            df = pd.merge(df, df_daily_raw_during_d[["CDF", "CSF"]], how="left", on="_date", validate="many_to_one")
            df.index = df["_indexBackup"] ; df.index.name = None ; df = df.drop(["_indexBackup", "_date"], axis=1)
            cdf = df["CDF"]
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

        elif self.interval in [yfcd.Interval.Months1, yfcd.Interval.Months3]:
            raise Exception("not implemented")

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
                cdf = pd.Series(cdf).fillna(method="bfill").fillna(method="ffill").to_numpy()

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
        if not self.interday:
            # Don't need to de-split volume data because Yahoo always returns interday volume unadjusted
            pass
        else:
            df["Volume"] *= csf

        # Drop 'Adj Close', replace with scaling factors:
        df = df.drop("Adj Close", axis=1)
        df["CSF"] = csf
        df["CDF"] = cdf

        h_lastDivAdjustDt = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
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

        log_msg = f"_reverseYahooAdjust-{self.istr}() returning"
        if tc is not None:
            tc.Exit(log_msg)
        elif debug:
            print(log_msg)

        if debug:
            print(df[["Open", "Low", "High", "Close", "Dividends", "Volume", "CSF"]])

        return df


    def _applyNewEvents(self):
        if self.h is None or self.h.empty:
            return

        debug = False
        # debug = True

        if debug and tc is not None:
            tc.Enter(f"_applyNewEvents()-{self.istr}")

        h_modified = False

        # Backport new splits across entire h table
        lastSplitAdjustDts = self.h["LastSplitAdjustDt"].unique()
        try:
            lastSplitAdjustDt_min = min(lastSplitAdjustDts)
        except Exception:
            print("self.h.shape:", self.h.shape)
            raise
        splits_since = self.manager.GetHistory("Events").GetSplitsFetchedSince(lastSplitAdjustDt_min)
        if splits_since is not None:
            if debug:
                print("splits_since:")
                print(splits_since)
            for dt in splits_since.index:
                f1 = self.h.index < dt
                f2 = self.h["LastSplitAdjustDt"] < splits_since.loc[dt, "FetchDate"]
                f = f1 & f2
                if f.any():
                    if debug and tc is not None:
                        msg = f"{self.ticker}: {self.istr}: Applying split {splits_since.loc[dt, 'Stock Splits']} @ {dt}"
                        indices = np.where(f)[0]
                        msg += f" across intervals {self.h.index[indices[0]]} -> {self.h.index[indices[-1]]}"
                        tc.Print(msg)
                    if isinstance(self.h["CSF"].iloc[0], (int, np.int64)):
                        self.h["CSF"] = self.h["CSF"].astype(float)
                    self.h.loc[f, "CSF"] /= splits_since.loc[dt, "Stock Splits"]
            self.h["LastSplitAdjustDt"] = splits_since["FetchDate"].max()
            h_modified = True
        #
        # Backport new divs across entire h table
        lastDivAdjustDts = self.h["LastDivAdjustDt"].unique()
        lastDivAdjustDt_min = min(lastDivAdjustDts)
        divs_since = self.manager.GetHistory("Events").GetDivsFetchedSince(lastDivAdjustDt_min)
        if divs_since is not None:
            if divs_since["Supersede?"].any():
                # CDF is corrupt, recalculate from scratch
                if debug:
                    if tc is not None:
                        tc.Print(f"{self.ticker}: WARNING: divs_since contains Supersede, recalculating CDF from scratch")
                divs_since = self.manager.GetHistory("Events").GetDivsFetchedSince(self.h.index[0].floor("1D"))
                self.h["CDF"] = 1.0
            for dt in divs_since.index:
                f1 = self.h.index < dt
                f2 = self.h["LastDivAdjustDt"] < divs_since.loc[dt, "FetchDate"]
                f = f1 & f2
                if f.any():
                    if debug and tc is not None:
                        msg = f"{self.ticker}: {self.istr}: Applying div {divs_since.loc[dt, 'Dividends']} @ {dt}"
                        indices = np.where(f)[0]
                        msg += f" across intervals {self.h.index[indices[0]]} -> {self.h.index[indices[-1]]}"
                        tc.Print(msg)
                    self.h.loc[f, "CDF"] *= divs_since.loc[dt, "Back Adj."]
            self.h["LastDivAdjustDt"] = divs_since["FetchDate"].max()
            h_modified = True

        if h_modified:
            self._updatedCachedPrices(self.h)

        if debug and tc is not None:
            tc.Exit("_applyNewEvents() returning")
