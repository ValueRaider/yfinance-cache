from pprint import pprint
import sqlite3 as sql
from copy import deepcopy
from functools import lru_cache

from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import numpy as np
import exchange_calendars as xcal

from yfinance_cache import yfc_logging as yfcl

from . import yfc_dat as yfcd
from . import yfc_cache_manager as yfcm
from . import yfc_utils as yfcu


exchangeTzCache = {}
def GetExchangeTzName(exchange):
    yfcu.TypeCheckStr(exchange, "exchange")

    if exchange not in exchangeTzCache:
        tz = yfcm.ReadCacheDatum("exchange-"+exchange, "tz")
        if tz is None:
            raise Exception("Do not know timezone for exchange '{}'".format(exchange))
        exchangeTzCache[exchange] = tz
    else:
        tz = exchangeTzCache[exchange]
    return tz
def SetExchangeTzName(exchange, tz):
    yfcu.TypeCheckStr(exchange, "exchange")
    yfcu.TypeCheckStr(tz, "tz")

    if exchange not in yfcd.exchange_locks:
        raise Exception(f"Need to add mapping of exchange {exchange} to xcal")
    exchange_lock = yfcd.exchange_locks[exchange]
    with exchange_lock:
        tzc = yfcm.ReadCacheDatum("exchange-"+exchange, "tz")
        if tzc is not None:
            if tzc != tz:
                # Different names but maybe same tz
                tzc_zi = ZoneInfo(tzc)
                tz_zi = ZoneInfo(tz)
                dt = datetime.now()
                if tz_zi.utcoffset(dt) != tzc_zi.utcoffset(dt):
                    print("tz_zi = {} ({})".format(tz_zi, type(tz_zi)))
                    print("tzc_zi = {} ({})".format(tzc_zi, type(tzc_zi)))
                    raise Exception("For exchange '{}', new tz {} != cached tz {}".format(exchange, tz, tzc))
        else:
            exchangeTzCache[exchange] = tz
            yfcm.StoreCacheDatum("exchange-"+exchange, "tz", tz)

calCache = {}
schedCache = {}
schedDbMetadata = {}
db_mem = sql.connect(":memory:")
schedIntervalsCache = {}


# TODO: Ensure all methods support Monthly intervals, e.g. GetTimestampCurrentInterval


def GetExchangeDataDelay(exchange):
    yfcu.TypeCheckStr(exchange, "exchange")

    d = yfcm.ReadCacheDatum("exchange-"+exchange, "yf_lag")
    if d is None:
        d = yfcd.exchangeToYfLag[exchange]
    return d


def Simple247xcal(opens, closes):
    yfcu.TypeCheckDatetimeIndex(opens, 'opens')
    yfcu.TypeCheckDatetimeIndex(closes, 'closes')

    # Use any xcal calendar as base
    cal = xcal.get_calendar('NYSE')

    cal._opens = opens
    cal._closes = closes

    cal.schedule = pd.DataFrame(data={"open":opens, "close":closes}, index=opens)
    cal.schedule.index = cal.schedule.index.tz_localize(None)
    cal.schedule['break_start'] = pd.NaT
    cal.schedule['break_end'] = pd.NaT

    cal.opens_nanos = np.array([x.value for x in opens])
    cal.closes_nanos = np.array([x.value for x in closes])

    cal._late_opens = []
    cal._early_closes = []

    cal._break_starts = None
    cal._break_ends = None
    cal.break_starts_nanos = cal.schedule['break_start'].values.astype(np.int64)
    cal.break_ends_nanos = cal.schedule['break_end'].values.astype(np.int64)

    return cal


def JoinTwoXcals(cal1, cal2):
    # TODO: Check no overlap, and also they are close together implying no business days between

    if cal1.schedule.index[0] > cal2.schedule.index[0]:
        # Flip
        tmp = cal1
        cal1 = cal2
        cal2 = tmp

    cal12 = deepcopy(cal1)

    def _safeAppend(a1, a2):
        if a1 is not None and a2 is not None:
            return np.append(a1, a2)
        elif a1 is not None:
            return a1
        else:
            return a2
    #
    cal12._opens = _safeAppend(cal1._opens, cal2._opens)
    cal12._break_starts = _safeAppend(cal1._break_starts, cal2._break_starts)
    cal12._break_ends = _safeAppend(cal1._break_ends, cal2._break_ends)
    cal12._closes = _safeAppend(cal1._closes, cal2._closes)
    #
    cal12.opens_nanos = _safeAppend(cal1.opens_nanos, cal2.opens_nanos)
    cal12.break_starts_nanos = _safeAppend(cal1.break_starts_nanos, cal2.break_starts_nanos)
    cal12.break_ends_nanos = _safeAppend(cal1.break_ends_nanos, cal2.break_ends_nanos)
    cal12.closes_nanos = _safeAppend(cal1.closes_nanos, cal2.closes_nanos)
    #
    cal12._late_opens = _safeAppend(cal1._late_opens, cal2._late_opens)
    cal12._early_closes = _safeAppend(cal1._early_closes, cal2._early_closes)
    #
    cal12.schedule = pd.concat([cal1.schedule, cal2.schedule])

    return cal12


@lru_cache(maxsize=1000)
def GetCalendarViaCache(exchange, start, end=None):
    global calCache

    if isinstance(start, date):
        if start.month == 1 and start.day < 5:
            if end is None:
                end = start
            start = start.year - 1
        else:
            start = start.year
    if end is not None and isinstance(end, date):
        if end.month == 12 and end.day > 25:
            end = end.year + 1
        else:
            end = end.year

    yfcu.TypeCheckStr(exchange, "exchange")
    yfcu.TypeCheckYear(start, "start")
    if end is not None:
        yfcu.TypeCheckYear(end, "end")
    if end is None:
        end = date.today().year

    cache_key = "exchange-"+exchange
    if exchange == 'CCC':
        # Binance, 24/7
        cal_name = exchange
    else:
        if exchange not in yfcd.exchangeToXcalExchange:
            raise Exception("Need to add mapping of exchange {} to xcal".format(exchange))
        cal_name = yfcd.exchangeToXcalExchange[exchange]

    cal = None

    exchange_lock = yfcd.exchange_locks[exchange]

    def _customModSchedule(cal):
        tz = ZoneInfo(GetExchangeTzName(exchange))
        df = cal.schedule
        df["open"] = df["open"].dt.tz_convert(tz)
        df["close"] = df["close"].dt.tz_convert(tz)
        if (exchange in yfcd.exchangesWithAuction) and ("auction" not in df.columns):
            df["auction"] = df["close"] + yfcd.exchangeAuctionDelay[exchange]
        df["idx_nanos"] = df.index.values.astype("int64")
        if exchange == "ASX":
            # Yahoo sometimes returns trading data occurring
            # between 4pm and 4:01pm. TradingView agress.
            # Have to assume this is real data.
            f = df["close"].dt.time == time(16)
            if f.any():
                # df.loc[f, "close"] += timedelta(minutes=1)
                closes = df["close"].to_numpy()
                closes[f] += timedelta(minutes=1)
                df["close"] = closes
                cal.closes_nanos = df["close"].values.astype("int64")
        return cal

    with exchange_lock:
        # Load from cache
        if cal_name in calCache:
            cal = calCache[cal_name]
        elif yfcm.IsDatumCached(cache_key, "cal"):
            cal, md = yfcm.ReadCacheDatum(cache_key, "cal", True)
            if xcal.__version__ != md["version"]:
                cal = None
            elif 'np version' not in md or md['np version'] != np.__version__:
                cal = None

        # Calculate missing data
        pre_range = None ; post_range = None
        if cal is not None:
            cached_range = (cal.schedule.index[0].year, cal.schedule.index[-1].year)
            if start < cached_range[0]:
                pre_range = (start, cached_range[0]-1)
            if end > cached_range[1]:
                post_range = (cached_range[1]+1, end)
        else:
            pre_range = (start, end)

        # Fetch missing data
        if pre_range is not None:
            start = date(pre_range[0], 1, 1)
            end = date(pre_range[1], 12, 31)
            if exchange == 'CCC':
                opens = pd.date_range(start=start, end=end, freq='1d').tz_localize(ZoneInfo(GetExchangeTzName(exchange)))
                ends = opens + pd.Timedelta('1d')
                pre_cal = Simple247xcal(opens, ends)
            else:
                pre_cal = xcal.get_calendar(cal_name, start=start, end=end)
            pre_cal = _customModSchedule(pre_cal)
            if cal is None:
                cal = pre_cal
            else:
                cal = JoinTwoXcals(pre_cal, cal)
        if post_range is not None:
            start = date(post_range[0], 1, 1)
            end = date(post_range[1], 12, 31)
            if exchange == 'CCC':
                opens = pd.date_range(start=start, end=end, freq='1d').tz_localize(ZoneInfo(GetExchangeTzName(exchange)))
                ends = opens + pd.Timedelta('1d')
                post_cal = Simple247xcal(opens, ends)
            else:
                post_cal = xcal.get_calendar(cal_name, start=start, end=end)
            post_cal = _customModSchedule(post_cal)
            if cal is None:
                cal = post_cal
            else:
                cal = JoinTwoXcals(cal, post_cal)

        # Write to cache
        calCache[cal_name] = cal
        if pre_range is not None or post_range is not None:
            yfcm.StoreCacheDatum(cache_key, "cal", cal, metadata={"version": xcal.__version__, 'np version': np.__version__})

    return cal


def ExchangeOpenOnDay(exchange, d):
    yfcu.TypeCheckStr(exchange, "exchange")
    yfcu.TypeCheckDateStrict(d, "d")

    cal = GetCalendarViaCache(exchange, d)

    return d.isoformat() in cal.schedule.index


# @lru_cache(maxsize=1000)  # changes index of returned dataframe from datetimeindex to index
def GetExchangeSchedule(exchange, start_d, end_d):
    yfcu.TypeCheckStr(exchange, "exchange")
    yfcu.TypeCheckDateStrict(start_d, "start_d")
    yfcu.TypeCheckDateStrict(end_d, "end_d")
    if start_d >= end_d:
        raise Exception("start_d={} must < end_d={}".format(start_d, end_d))

    debug = False
    # debug = True

    if debug:
        print("GetExchangeSchedule(exchange={}, start_d={}, end_d={}".format(exchange, start_d, end_d))

    end_d_sub1 = end_d - timedelta(days=1)

    # num_years = end_d.year - start_d.year + 1
    num_years = end_d_sub1.year - start_d.year + 1
    if num_years <= 2:
        # Cache
        cache_key = (exchange, start_d.year, num_years)
        if cache_key in schedCache:
            s = schedCache[cache_key]
        else:
            cal = GetCalendarViaCache(exchange, start_d, end_d)
            s = cal.schedule.loc[str(start_d.year):str(end_d_sub1.year)].copy()
            schedCache[cache_key] = s
    else:
        cal = GetCalendarViaCache(exchange, start_d, end_d)
        s = cal.schedule

    if s is not None:
        start_ts = pd.Timestamp(start_d)
        end_ts = pd.Timestamp(end_d_sub1)
        slice_start = s["idx_nanos"].values.searchsorted(start_ts.value, side="left")
        slice_end = s["idx_nanos"].values.searchsorted(end_ts.value, side="right")
        sched = s[slice_start:slice_end].copy()
    else:
        sched = None

    if sched is None or sched.empty:
        df = None
    else:
        cols = ["open", "close"]
        if "auction" in sched.columns:
            cols.append("auction")
        df = sched[cols]

    if debug:
        print("GetExchangeSchedule() returning")

    return df

    # Experiment with sqlite3. But pd.read_sql() -> _parse_date_columns() kills performance
    global calCache
    global schedDbMetadata
    global db_mem
    if exchange not in schedDbMetadata:
        cal = GetCalendarViaCache(exchange, start_d, end_d)
        cal.schedule["open"] = cal.schedule["open"].dt.tz_convert("UTC")
        cal.schedule["close"] = cal.schedule["close"].dt.tz_convert("UTC")
        cal.schedule.to_sql(exchange, db_mem, index_label="indexpd")
        schedDbMetadata[exchange] = {}
        schedDbMetadata[exchange]["columns"] = cal.schedule.columns.values
    #
    # query = "PRAGMA table_info('{}')".format(exchange)
    # print("query:")
    # print(query)
    # c = db_mem.execute(query)
    # table_schema = c.fetchall()
    # print(table_schema)
    # print(schedDbMetadata[exchange])
    # quit()
    #
    tz_name = GetExchangeTzName(exchange)
    tz_exchange = ZoneInfo(tz_name)
    start_dt = datetime.combine(start_d, time(0), tzinfo=tz_exchange)
    end_dt = datetime.combine(end_d, time(0), tzinfo=tz_exchange)
    # index in sql is tz-naive, so discard tz from date range:
    start_dt = pd.Timestamp(start_dt).tz_localize(None)
    end_dt = pd.Timestamp(end_dt).tz_localize(None)
    cols = [c for c in ["open", "close", "auction"] if c in schedDbMetadata[exchange]["columns"]]
    query = "SELECT indexpd, {} FROM {} WHERE indexpd >= '{}' AND indexpd < '{}' ;".format(", ".join(cols), exchange, start_dt, end_dt)
    sched = pd.read_sql(query, db_mem, parse_dates=["indexpd", "open", "close"])
    if sched is None or sched.empty:
        return None
    sched.index = pd.DatetimeIndex(sched["indexpd"])  # .tz_localize(tz_name)
    sched = sched.drop("indexpd", axis=1)
    sched["open"] = sched["open"].dt.tz_convert(tz_exchange)
    sched["close"] = sched["close"].dt.tz_convert(tz_exchange)
    cols = ["open", "close"]
    if "auction" in sched.columns:
        cols.append("auction")
    df = sched[cols].copy()
    rename_cols = {"open": "market_open", "close": "market_close"}
    df.columns = [rename_cols[col] if col in rename_cols else col for col in df.columns]
    return df


def GetExchangeWeekSchedule(exchange, start, end, ignoreHolidays, ignoreWeekends, forceStartMonday=True):
    yfcu.TypeCheckStr(exchange, "exchange")
    if start >= end:
        raise Exception("start={} must be < end={}".format(start, end))
    yfcu.TypeCheckDateEasy(start, "start")
    yfcu.TypeCheckDateEasy(end, "end")
    yfcu.TypeCheckBool(ignoreHolidays, "ignoreHolidays")
    yfcu.TypeCheckBool(ignoreWeekends, "ignoreWeekends")
    yfcu.TypeCheckBool(forceStartMonday, "forceStartMonday")

    debug = False
    # debug = True

    if debug:
        print("GetExchangeWeekSchedule()", locals())

    tz = ZoneInfo(GetExchangeTzName(exchange))
    td_1d = timedelta(days=1)
    if not isinstance(start, datetime):
        start_d = start
    else:
        start_d = start.astimezone(tz).date()
    if not isinstance(end, datetime):
        end_d = end
    else:
        end_d = end.astimezone(tz).date() + td_1d
    dt_now = pd.Timestamp.utcnow().tz_convert(ZoneInfo("UTC"))
    # td7d = timedelta(days=7)
    td7d = pd.DateOffset(days=7)

    sunday_has_trading = exchange in ["TLV"]
    week_starts_sunday = (exchange in ["TLV"]) and (not forceStartMonday)
    if debug:
        print("- week_starts_sunday =", week_starts_sunday)

    if exchange not in yfcd.exchangeToXcalExchange:
        raise Exception("Need to add mapping of exchange {} to xcal".format(exchange))
    cal = GetCalendarViaCache(exchange, start, end)

    sched = GetExchangeSchedule(exchange, start_d, end_d)
    if sched is None or sched.empty:
        return None

    if "auction" in sched.columns:
        sched["close"] = sched["auction"] + yfcd.exchangeAuctionDuration[exchange]
        sched = sched.drop("auction", axis=1)

    if week_starts_sunday:
        periods = sched.index.to_period("W-SAT")
    else:
        periods = sched.index.to_period("W")
    if debug:
        print("- periods:", type(periods), type(periods[0]))
        for p in periods:
            print(p.start_time, p.end_time)
    period_starts = periods.start_time.tz_localize(tz)
    period_intervals = pd.IntervalIndex.from_arrays(period_starts, period_starts+td7d, closed="left")
    if debug:
        print("- period_intervals:", type(period_intervals), type(period_intervals[0]))
        for x in period_intervals:
            print(x.left, x.right)
    g = sched.groupby(period_intervals)
    weeks = g.first().drop("close", axis=1)
    weeks["close"] = g.last()["close"]
    if debug:
        print("- weeks:")
        print(weeks)
        print("- - index")
        for x in weeks.index:
            print(x.left, x.right)

    drop_first = False
    drop_last = False
    if not ignoreHolidays:
        drop_first = start_d > weeks.index[0].left.date()
        if not week_starts_sunday and sunday_has_trading:
            # Week is Monday open -> Sunday close
            last_working_week_end = weeks.index[-1].right
        elif week_starts_sunday:
            # Week is Sunday open -> Thursday close
            last_working_week_end = weeks.index[-1].right -2*td_1d
        else:
            # Week is Monday open -> Friday close
            last_working_week_end = weeks.index[-1].right -2*td_1d
        if debug:
            print("- last_working_week_end =", last_working_week_end)
        if last_working_week_end <= dt_now:
            drop_last = end_d < last_working_week_end.date()
    else:
        prev_sesh = cal.previous_session(weeks["open"].iloc[0].date()).tz_localize(tz)
        if debug:
            print("- prev_sesh =", prev_sesh)
        drop_first = prev_sesh >= weeks.index[0].left
        next_sesh = cal.next_session(weeks["close"].iloc[-1].date()).tz_localize(tz)
        if debug:
            print("- next_sesh =", next_sesh)
        if next_sesh >= dt_now:
            # Week ends in future so inevitably cut off but allow it
            drop_last = False
        else:
            drop_last = next_sesh < weeks.index[-1].right
    if not ignoreWeekends:
        # last_working_week_end = weeks.index[-1].end_time.tz_localize(tz) + timedelta(microseconds=1)
        last_working_week_end = weeks.index[-1].right
        if debug:
            print("- last_working_week_end =", last_working_week_end)
        if last_working_week_end <= dt_now:
            drop_last = drop_last or end_d < last_working_week_end.date()
    if debug:
        print(f"- drop_first={drop_first} drop_last={drop_last}")
    if drop_first and drop_last:
        weeks = weeks.iloc[1:-1]
    elif drop_first:
        weeks = weeks.iloc[1:]
    elif drop_last:
        weeks = weeks.iloc[:-1]

    if weeks.empty:
        weeks = None

    if debug:
        print("- weeks:")
        pprint(weeks)

    if debug:
        print("GetExchangeWeekSchedule() returning")
    return weeks


def MapPeriodToDates(exchange, period, interval):
    yfcu.TypeCheckPeriod(period, "period")

    debug = False
    # debug = True

    if debug:
        print(f"MapPeriodToDates(exchange={exchange}, period={period}, interval={interval})")

    tz_name = GetExchangeTzName(exchange)
    tz_exchange = ZoneInfo(tz_name)

    # Map period to start->end range so logic can intelligently fetch missing data
    td_1d = timedelta(days=1)
    dt_now = pd.Timestamp.utcnow().replace(tzinfo=ZoneInfo("UTC"))
    d_now = dt_now.astimezone(tz_exchange).date()
    sched = GetExchangeSchedule(exchange, d_now-(7*td_1d), d_now+td_1d)
    yf_lag = yfcd.exchangeToYfLag[exchange]
    dt_now_sub_lag = dt_now - yf_lag
    if sched["open"].iloc[-1] > dt_now_sub_lag:
        # Discard days that haven't opened yet
        opens = pd.DatetimeIndex(sched["open"])
        x = opens.get_indexer([dt_now_sub_lag], method="bfill")[0]  # If not exact match, search forward
        sched = sched.iloc[:x]
    last_open_day = sched["open"].iloc[-1].date()
    if debug:
        print(f"- last_open_day={last_open_day} d_now={d_now}")
    end_d = last_open_day + td_1d
    if period == yfcd.Period.Max:
        start_d = date(yfcd.yf_min_year, 1, 1)
    elif period == yfcd.Period.Ytd:
        start_d = date(d_now.year, 1, 1)
    else:
        if isinstance(period, yfcd.Period):
            period = yfcd.periodToTimedelta[period]
        if yfcd.intervalToTimedelta[interval] <= timedelta(days=1):
            start_d = end_d - period
            while not ExchangeOpenOnDay(exchange, start_d):
                start_d += td_1d

            end_d = last_open_day+td_1d

        elif interval == yfcd.Interval.Week:
            if last_open_day.weekday() < 4:
                # last week in-progress so ignore from counting, but include in date range
                last_full_week_start = d_now - timedelta(days=7+d_now.weekday())
            else:
                last_full_week_start = d_now - timedelta(days=d_now.weekday())
            if last_full_week_start > last_open_day:
                last_full_week_start -= timedelta(days=7)
            if debug:
                print("- last_full_week_start =", last_full_week_start)

            start_d = last_full_week_start + timedelta(days=7) - period

        else:
            raise Exception("codepath not implemented")

    if debug:
        print(f"MapPeriodToDates() returning {start_d}->{end_d}")

    return start_d, end_d


def GetExchangeScheduleIntervals(exchange, interval, start, end, discardTimes=None, week7days=True, weekForceStartMonday=True, ignore_breaks=False, exclude_future=True):
    yfcu.TypeCheckStr(exchange, "exchange")
    if start >= end:
        raise Exception("start={} must be < end={}".format(start, end))
    yfcu.TypeCheckIntervalDt(start, interval, "start", strict=False)
    yfcu.TypeCheckIntervalDt(end, interval, "end", strict=False)
    if discardTimes is not None:
        yfcu.TypeCheckBool(discardTimes, "discardTimes")
    yfcu.TypeCheckBool(week7days, "week7days")
    yfcu.TypeCheckBool(ignore_breaks, "ignore_breaks")

    itd = yfcd.intervalToTimedelta[interval]
    intraday = itd < timedelta(days=1)
    if discardTimes is None:
        discardTimes = not intraday
    if discardTimes and intraday:
        raise Exception("discardTimes with intraday is nonsense")
    if interval == yfcd.Interval.Week and week7days and not discardTimes:
        raise Exception("week7days without discardTimes is nonsense")

    debug = False
    # debug = True

    if debug:
        yfc_logger = yfcl.GetLogger("exchange-"+exchange)
        yfc_logger.debug("GetExchangeScheduleIntervals()")
        yfc_logger.debug("- " + str(locals()))

    dt_now = pd.Timestamp.utcnow()
    tz = ZoneInfo(GetExchangeTzName(exchange))
    td_1d = timedelta(days=1)
    if not isinstance(start, datetime):
        start_dt = datetime.combine(start, time(0), tz)
        start_d = start
    else:
        start_dt = start
        start_d = start.astimezone(tz).date()
    if not isinstance(end, datetime):
        end_dt = datetime.combine(end, time(0), tz)
        end_d = end
    else:
        end_dt = end
        end_d = end.astimezone(tz).date() + td_1d

    # First look in cache:
    cache_key = (exchange, interval, start_d, end_d, discardTimes, week7days)  # todo: frozenset?
    if cache_key in schedIntervalsCache:
        s = schedIntervalsCache[cache_key]
        if s is not None and len(s) > 0:
            if isinstance(s.left[0], datetime):
                s = s[s.left >= start_dt]
            if len(s) > 0 and isinstance(s.right[0], datetime):
                s = s[s.right <= end_dt]

            if exclude_future and intraday:
                s = s[s.left <= dt_now]
                if debug:
                    yfc_logger.debug("- returning cached intervals ({}->{} filtered by {}->{})".format(start_d, end_d, start, min(end, dt_now)))
            elif debug:
                yfc_logger.debug("- returning cached intervals ({}->{} filtered by {}->{})".format(start_d, end_d, start, end))
        return s

    if debug:
        yfc_logger.debug("- start_d={}, end_d={}".format(start_d, end_d))

    week_starts_sunday = (exchange in ["TLV"]) and (not week7days)
    if debug:
        yfc_logger.debug(f"- week_starts_sunday = {week_starts_sunday}")

    cal = GetCalendarViaCache(exchange, start_d, end_d)

    # When calculating intervals use dates not datetimes. Cache the result, and then
    # apply datetime limits.
    intervals = None
    istr = yfcd.intervalToString[interval]
    if intraday:
        if itd > timedelta(minutes=30):
            align = "-30m"
        else:
            align = "-" + istr
        ti = cal.trading_index(start_d.isoformat(), (end_d-td_1d).isoformat(), period=istr, intervals=True, force_close=True, ignore_breaks=ignore_breaks, align=align)
        if len(ti) == 0:
            return None
        # Transfer IntervalIndex to DataFrame so can modify
        intervals_df = pd.DataFrame(data={"interval_open": ti.left.tz_convert(tz), "interval_close": ti.right.tz_convert(tz)})
        if "auction" in cal.schedule.columns:
            sched = GetExchangeSchedule(exchange, start_d, end_d)
            sched.index = sched.index.date

            # Will map auction time to an interval by flooring relative to market open.
            # Implemented by flooring then applying offset calculated from floored market open.
            intervals_grp = intervals_df.groupby(intervals_df["interval_open"].dt.date)
            # 1 - calculate offset
            res = 'h' if istr.endswith('h') else istr.replace('m', 'T')
            market_opens = intervals_grp.min()["interval_open"]
            if len(market_opens.dt.time.unique()) == 1:
                open0 = market_opens.iloc[0]
                offset = open0 - open0.floor(res)
                auctions_df = sched[["auction"]].copy()
            else:
                market_opens.name = "day_open"
                market_opens.index.name = "day"
                auctions_df = sched[["auction"]].join(market_opens)
                offset = auctions_df["day_open"] - auctions_df["day_open"].dt.floor(res)
            # 2 perform relative flooring:
            if isinstance(offset, pd.Timedelta) and len(auctions_df["auction"].dt.time.unique()) == 1:
                auction0 = auctions_df["auction"].iloc[0]
                auction0_floor = (auction0-offset).floor(res) + offset
                open_offset = auction0_floor - auction0
                auctions_df["auction_open"] = auctions_df["auction"] + open_offset
            else:
                auctions_df["auction_open"] = (auctions_df["auction"]-offset).dt.floor(res) + offset
            auctions_df["auction_close"] = auctions_df["auction"] + yfcd.exchangeAuctionDuration[exchange]
            auctions_df = auctions_df.drop(["day_open", "auction"], axis=1, errors="ignore")

            # Compare auction intervals against last trading interval
            intervals_df_last = intervals_grp.max()
            intervals_df_ex_last = intervals_df[~intervals_df["interval_open"].isin(intervals_df_last["interval_open"])]
            intervals_df_last.index = intervals_df_last["interval_open"].dt.date
            auctions_df = auctions_df.join(intervals_df_last)
            # - if auction surrounded by trading, discard auction
            f_surround = (auctions_df["auction_open"] >= auctions_df["interval_open"]) & \
                         (auctions_df["auction_close"] <= auctions_df["interval_close"])
            if f_surround.any():
                auctions_df.loc[f_surround, ["auction_open", "auction_close"]] = pd.NaT
            # - if last trading interval surrounded by auction, then replace by auction
            f_surround = (auctions_df["interval_open"] >= auctions_df["auction_open"]) & \
                         (auctions_df["interval_close"] <= auctions_df["auction_close"])
            if f_surround.any():
                auctions_df.loc[f_surround, ["interval_open", "interval_close"]] = pd.NaT
            # - no duplicates, no overlaps
            f_duplicate = (auctions_df["auction_open"] == auctions_df["interval_open"]) & \
                          (auctions_df["auction_close"] == auctions_df["interval_close"])
            if f_duplicate.any():
                print("")
                print(auctions_df[f_duplicate])
                raise Exception("Auction intervals are duplicates of normal trading intervals")
            f_overlap = (auctions_df["auction_open"] >= auctions_df["interval_open"]) & \
                        (auctions_df["auction_open"] < auctions_df["interval_close"])
            if f_overlap.any():
                # First, if total duration is <= interval length, then combine
                d = auctions_df["auction_close"] - auctions_df["interval_open"]
                f = d <= itd
                if f.any():
                    # Combine
                    auctions_df.loc[f, "auction_open"] = auctions_df.loc[f, "interval_open"]
                    auctions_df.loc[f, ["interval_open", "interval_close"]] = pd.NaT
                    f_overlap = f_overlap & (~f)
                if f_overlap.any():
                    print("")
                    print(auctions_df[f_overlap])
                    raise Exception("Auction intervals are overlapping normal trading intervals")
            # - combine
            auctions_df = auctions_df.reset_index(drop=True)
            intervals_df_last = auctions_df.loc[~auctions_df["interval_open"].isna(), ["interval_open", "interval_close"]]
            auctions_df = auctions_df.loc[~auctions_df["auction_open"].isna(), ["auction_open", "auction_close"]]
            rename_cols = {"auction_open": "interval_open", "auction_close": "interval_close"}
            auctions_df.columns = [rename_cols[col] if col in rename_cols else col for col in auctions_df.columns]
            intervals_df = pd.concat([intervals_df_ex_last, intervals_df_last, auctions_df], sort=True).sort_values(by="interval_open").reset_index(drop=True)

        intervals = pd.IntervalIndex.from_arrays(intervals_df["interval_open"], intervals_df["interval_close"], closed="left")

    elif interval == yfcd.Interval.Days1:
        s = GetExchangeSchedule(exchange, start_d, end_d)
        if s is None or s.empty:
            return None
        if exclude_future:
            s = s[s["open"] <= dt_now]
            if s.empty:
                return None
            s = s.copy()
        if debug:
            yfc_logger.debug("- sched:")
            yfc_logger.debug(s)
        if discardTimes:
            open_days = np.array([dt.to_pydatetime().astimezone(tz).date() for dt in s["open"]])
            intervals = yfcd.DateIntervalIndex.from_arrays(open_days, open_days+td_1d, closed="left")
        else:
            if "auction" in s.columns:
                s["close"] = s["auction"] + yfcd.exchangeAuctionDuration[exchange]
            intervals = pd.IntervalIndex.from_arrays(s["open"], s["close"], closed="left")

    elif interval == yfcd.Interval.Week:
        # There is reason for this madness of conditionally igoring holidays & weekends, don't mess with it.
        if week7days:
            week_sched = GetExchangeWeekSchedule(exchange, start, end, ignoreHolidays=False, ignoreWeekends=False, forceStartMonday=True)
            if week_sched is not None:
                if exclude_future:
                    week_sched = week_sched[week_sched["open"] <= dt_now]
                intervals = yfcd.DateIntervalIndex.from_arrays(week_sched.index.left.date, week_sched.index.right.date, closed="left")
        else:
            week_sched = GetExchangeWeekSchedule(exchange, start, end, ignoreHolidays=True, ignoreWeekends=True, forceStartMonday=weekForceStartMonday)
            if week_sched is not None:
                if exclude_future:
                    week_sched = week_sched[week_sched["open"] <= dt_now]
                if discardTimes:
                    intervals = yfcd.DateIntervalIndex.from_arrays(week_sched["open"].dt.date, week_sched["close"].dt.date+td_1d, closed="left")
                else:
                    intervals = pd.IntervalIndex.from_arrays(week_sched["open"], week_sched["close"], closed="left")

    else:
        raise Exception("Need to implement for interval={}".format(interval))

    if cache_key is not None:
        schedIntervalsCache[cache_key] = intervals

    # Only after caching can we prune future intervals
    if exclude_future and intraday:
        intervals = intervals[intervals.left <= dt_now]

    if intervals is not None:
        if isinstance(intervals.left[0], datetime):
            intervals = intervals[(intervals.left >= start_dt) & (intervals.right <= end_dt)]
        if len(intervals) == 0:
            intervals = None

    if debug:
        if intervals is not None:
            yfc_logger.debug(f"GetExchangeScheduleIntervals({istr}) returning interval starts {intervals.left[0]} -> {intervals.left[-1]}")
        else:
            yfc_logger.debug(f"GetExchangeScheduleIntervals({istr}) returning None")
    return intervals


def IsTimestampInActiveSession(exchange, ts):
    yfcu.TypeCheckStr(exchange, "exchange")
    yfcu.TypeCheckDatetime(ts, "ts")

    cal = GetCalendarViaCache(exchange, ts)
    try:
        s = cal.schedule.loc[ts.date().isoformat()]
    except Exception:
        return False

    o = s["open"] ; c = s["close"]
    if "auction" in cal.schedule.columns:
        a = s["auction"]
        if a != pd.NaT:
            c = max(c, s["auction"]+yfcd.exchangeAuctionDuration[exchange])

    return o <= ts and ts < c


def GetTimestampCurrentSession(exchange, ts):
    yfcu.TypeCheckStr(exchange, "exchange")
    yfcu.TypeCheckDatetime(ts, "ts")

    cal = GetCalendarViaCache(exchange, ts)
    try:
        s = cal.schedule.loc[ts.date().isoformat()]
    except Exception:
        return None
    o = s["open"] ; c = s["close"]
    if "auction" in cal.schedule.columns:
        a = s["auction"]
        if a != pd.NaT:
            c = max(c, s["auction"]+yfcd.exchangeAuctionDuration[exchange])

    if o <= ts and ts < c:
        return {"market_open": o, "market_close": c}
    else:
        return None


def GetTimestampMostRecentSession(exchange, ts):
    yfcu.TypeCheckStr(exchange, "exchange")
    yfcu.TypeCheckDatetime(ts, "ts")

    # If 'ts' is currently in an active session then that is most recent

    s = GetTimestampCurrentSession(exchange, ts)
    if s is not None:
        return s
    sched = GetExchangeSchedule(exchange, ts.date()-timedelta(days=6), ts.date()+timedelta(days=1))
    if "auction" in sched.columns:
        sched = sched.copy()
        f = ~(sched["auction"].isna())
        if f.any():
            if f.all():
                sched["close"] = np.maximum(sched["close"], sched["auction"]+yfcd.exchangeAuctionDuration[exchange])
            else:
                sched.loc[f, "close"] = np.maximum(sched.loc[f, "close"], sched.loc[f, "auction"]+yfcd.exchangeAuctionDuration[exchange])
    for i in range(sched.shape[0]-1, -1, -1):
        if sched["open"].iloc[i] <= ts:
            tz = ZoneInfo(GetExchangeTzName(exchange))
            return {"market_open": sched["open"].iloc[i].to_pydatetime().astimezone(tz), "market_close": sched["close"].iloc[i].to_pydatetime().astimezone(tz)}
    raise Exception("Failed to find most recent '{0}' session for ts = {1}".format(exchange, ts))


def GetTimestampNextSession(exchange, ts):
    yfcu.TypeCheckStr(exchange, "exchange")
    yfcu.TypeCheckDatetime(ts, "ts")

    sched = GetExchangeSchedule(exchange, ts.date(), ts.date()+timedelta(days=10))
    if "auction" in sched.columns:
        sched = sched.copy()
        f = ~(sched["auction"].isna())
        if f.any():
            if f.all():
                sched["close"] = np.maximum(sched["close"], sched["auction"]+yfcd.exchangeAuctionDuration[exchange])
            else:
                sched.loc[f, "close"] = np.maximum(sched.loc[f, "close"], sched.loc[f, "auction"]+yfcd.exchangeAuctionDuration[exchange])
    for i in range(sched.shape[0]):
        if ts < sched["open"].iloc[i]:
            tz = ZoneInfo(GetExchangeTzName(exchange))
            return {"market_open": sched["open"].iloc[i].to_pydatetime().astimezone(tz), "market_close": sched["close"].iloc[i].to_pydatetime().astimezone(tz)}
    raise Exception(f"Failed to find next '{exchange}' session for ts = {ts}")


def GetTimestampCurrentInterval(exchange, ts, interval, discardTimes=None, week7days=True, ignore_breaks=False):
    yfcu.TypeCheckStr(exchange, "exchange")
    yfcu.TypeCheckIntervalDt(ts, interval, "ts", strict=False)
    yfcu.TypeCheckInterval(interval, "interval")
    if discardTimes is not None:
        yfcu.TypeCheckBool(discardTimes, "discardTimes")
    yfcu.TypeCheckBool(week7days, "week7days")
    yfcu.TypeCheckBool(ignore_breaks, "ignore_breaks")

    itd = yfcd.intervalToTimedelta[interval]
    intraday = itd < timedelta(days=1)
    if discardTimes is None:
        discardTimes = not intraday
    if discardTimes and intraday:
        raise Exception("discardTimes with intraday is nonsense")
    if interval == yfcd.Interval.Week and week7days and not discardTimes:
        raise Exception("week7days without discardTimes is nonsense")
    if not intraday and not isinstance(ts, datetime) and not discardTimes:
        raise Exception("Requesting daily/multiday interval for date 'ts' without discardTimes is nonsense")

    debug = False
    # debug = True

    if debug:
        print("GetTimestampCurrentInterval()", locals())

    week_starts_sunday = (exchange in ["TLV"]) and (not week7days)

    i = None

    td_1d = timedelta(days=1)
    tz = ZoneInfo(GetExchangeTzName(exchange))
    if interval == yfcd.Interval.Week:
        # Treat week intervals as special case, contiguous from first weekday open to last weekday open.
        # Not necessarily Monday->Friday because of public holidays.
        # Unless 'week7days' is true, which means range from Monday to next Monday.
        if isinstance(ts, datetime):
            ts_day = ts.astimezone(tz).date()
        else:
            ts_day = ts

        if week_starts_sunday:
            if ts_day.weekday() == 6:
                # Already at start of week
                weekStart = ts_day
            else:
                weekStart = ts_day - timedelta(days=ts_day.weekday()+1)
        else:
            weekStart = ts_day - timedelta(days=ts_day.weekday())

        if week7days:
            weekEnd = weekStart + timedelta(days=7)
        else:
            weekEnd = weekStart + timedelta(days=5)
        if debug:
            print("- weekStart = {}".format(weekStart))
            print("- weekEnd = {}".format(weekEnd))
        if not week7days:
            weekSched = GetExchangeSchedule(exchange, weekStart, weekEnd)
            if "auction" in weekSched.columns:
                weekSched["close"] = weekSched["auction"] + yfcd.exchangeAuctionDuration[exchange]
            if debug:
                print("- weekSched:")
                print(weekSched)
            weekStart = weekSched["open"].iloc[0]
            weekEnd = weekSched["close"].iloc[-1]
            if discardTimes:
                weekStart = weekStart.date()
                weekEnd = weekEnd.date() + td_1d
        intervalStart = weekStart
        intervalEnd = weekEnd
        if debug:
            print("- intervalStart = {}".format(intervalStart))
            print("- intervalEnd = {}".format(intervalEnd))
        if isinstance(intervalStart, datetime):
            ts_in_interval = ts >= intervalStart and ts < intervalEnd
        else:
            ts_in_interval = ts_day >= intervalStart and ts_day < intervalEnd
        if ts_in_interval:
            i = {"interval_open": intervalStart, "interval_close": intervalEnd}

    elif interval == yfcd.Interval.Days1:
        if isinstance(ts, datetime):
            ts_day = ts.astimezone(tz).date()
        else:
            ts_day = ts
        if debug:
            print("- ts_day: {}".format(ts_day))
        if ExchangeOpenOnDay(exchange, ts_day):
            if debug:
                print("- exchange open")
            if discardTimes:
                i = {"interval_open": ts_day, "interval_close": ts_day+td_1d}
            else:
                sched = GetExchangeSchedule(exchange, ts_day, ts_day+td_1d)
                if sched is not None and not sched.empty:
                    if "auction" in sched.columns:
                        sched["close"] = sched["auction"] + yfcd.exchangeAuctionDuration[exchange]
                    i = {"interval_open": sched["open"].iloc[0], "interval_close": sched["close"].iloc[0]}
                    if ts < i["interval_open"] or ts >= i["interval_close"]:
                        i = None
        else:
            if debug:
                print("- exchange closed")

    else:
        if IsTimestampInActiveSession(exchange, ts) or IsTimestampInActiveSession(exchange, ts+timedelta(minutes=30)):
            ts = ts.astimezone(tz)
            if exchange in yfcd.exchangesWithAuction:
                itd = max(itd, timedelta(minutes=15))
            intervals = GetExchangeScheduleIntervals(exchange, interval, ts-itd, ts+timedelta(minutes=30)+itd, ignore_breaks=ignore_breaks)
            idx = intervals.get_indexer([ts])
            f = idx != -1
            if f.any():
                i0 = intervals[idx[f]][0]
                i = {"interval_open": i0.left.to_pydatetime(), "interval_close": i0.right.to_pydatetime()}

    if debug:
        print("GetTimestampCurrentInterval() returning: {}".format(i))

    return i


def GetTimestampCurrentInterval_batch(exchange, ts, interval, discardTimes=None, week7days=True, ignore_breaks=False):
    yfcu.TypeCheckStr(exchange, "exchange")
    if isinstance(ts, list):
        ts = np.array(ts)
    yfcu.TypeCheckNpArray(ts, "ts")
    if len(ts) > 0:
        yfcu.TypeCheckIntervalDt(ts[0], interval, "ts", strict=False)
    yfcu.TypeCheckInterval(interval, "interval")
    if discardTimes is not None:
        yfcu.TypeCheckBool(discardTimes, "discardTimes")
    yfcu.TypeCheckBool(week7days, "week7days")
    yfcu.TypeCheckBool(ignore_breaks, "ignore_breaks")

    debug = False
    # debug = True

    if len(ts) == 0:
        if debug:
            print("- ts[] is empty")
            print("GetTimestampCurrentInterval_batch() returning")
        return None

    itd = yfcd.intervalToTimedelta[interval]
    intraday = itd < timedelta(days=1)
    if discardTimes is None:
        discardTimes = not intraday
    if discardTimes and intraday:
        raise Exception("discardTimes with intraday is nonsense")
    if interval == yfcd.Interval.Week and week7days and not discardTimes:
        raise Exception("week7days without discardTimes is nonsense")
    if not intraday and not isinstance(ts[0], datetime) and not discardTimes:
        raise Exception("Requesting daily/multiday interval for date 'ts' without discardTimes is nonsense")

    # For day and week intervals, the time component is ignored (set to 0).

    if debug:
        if len(ts) == 0:
            msg = "GetTimestampCurrentInterval_batch(ts=[]"
        else:
            msg = f"GetTimestampCurrentInterval_batch(ts={ts[0]}->{ts[-1]}"
        msg += f", interval={interval}, discardTimes={discardTimes} week7days={week7days})"
        print(msg)

    n = len(ts)
    tz = ZoneInfo(GetExchangeTzName(exchange))
    intervals = [None]*n

    td_1d = timedelta(days=1)
    tz = ZoneInfo(GetExchangeTzName(exchange))
    ts_is_datetimes = isinstance(ts[0], datetime)
    if ts_is_datetimes:
        ts = pd.to_datetime(ts)
        if ts[0].tzinfo != tz:
            ts = ts.tz_convert(tz)
        ts_day = ts.date
    else:
        ts_day = np.array(ts)
        ts = pd.to_datetime(ts).tz_localize(tz)

    if interval == yfcd.Interval.Week:
        # Treat week intervals as special case, contiguous from first weekday open to last weekday open.
        # Not necessarily Monday->Friday because of public holidays.
        # Unless 'ignoreClosedDays' is true, which means range from Monday to next Monday.
        #
        t0 = ts_day[0]  ; t0 -= timedelta(days=t0.weekday())+timedelta(days=7)
        tl = ts_day[-1] ; tl += timedelta(days=6-tl.weekday())+timedelta(days=7)
        if debug:
            print("t0={} ; tl={}".format(t0, tl))

        if week7days:
            # Monday -> next Monday regardless of exchange schedule
            wd = pd.to_timedelta(ts.weekday, unit='D')
            weekSchedStart = ts - wd
            weekSchedEnd = weekSchedStart + timedelta(days=7)
            weekSchedStart = weekSchedStart.date
            weekSchedEnd = weekSchedEnd.date
        else:
            week_sched = GetExchangeScheduleIntervals(exchange, interval, t0, tl, discardTimes, week7days=False, weekForceStartMonday=False)
            if debug:
                print("- week_sched:")
                print(week_sched)
            weekSchedStart = np.full(n, None)
            weekSchedEnd = np.full(n, None)
            if discardTimes:
                idx = week_sched.get_indexer(ts_day)
            else:
                idx = week_sched.get_indexer(ts)
            f = idx != -1
            if f.any():
                weekSchedStart[f] = week_sched.left[idx[f]]
                weekSchedEnd[f] = week_sched.right[idx[f]]

        intervals = pd.DataFrame(data={"interval_open": weekSchedStart, "interval_close": weekSchedEnd}, index=ts)

    elif interval == yfcd.Interval.Days1:
        t0 = ts_day[0]
        tl = ts_day[len(ts_day)-1]
        sched = GetExchangeSchedule(exchange, t0, tl+timedelta(days=1))
        if "auction" in sched.columns:
            sched["close"] = sched["auction"] + yfcd.exchangeAuctionDuration[exchange]

        ts_df = pd.DataFrame(data={"day": ts_day}, index=ts)
        sched["day"] = sched.index.date
        ts_df = pd.merge(ts_df, sched, how="left", on="day")
        ts_df.index = ts
        if debug:
            print("- ts_df:")
            print(ts_df)

        intervals = ts_df
        if discardTimes:
            intervals["interval_open"] = intervals["open"].dt.date
            intervals["interval_close"] = intervals["interval_open"] + td_1d
            intervals = intervals.drop(["day", "open", "close"], axis=1)
        else:
            intervals["interval_open"] = intervals["open"]
            intervals["interval_close"] = intervals["close"]
            f_out = (intervals.index < intervals["interval_open"]) | (intervals.index >= intervals["interval_close"])
            if f_out.any():
                intervals.loc[f_out, "interval_open"] = pd.NaT
                intervals.loc[f_out, "interval_close"] = pd.NaT
            intervals = intervals.drop(["day", "open", "close"], axis=1)
        if debug:
            print("- intervals:")
            print(intervals)

    else:
        if exchange in yfcd.exchangesWithAuction:
            itd = max(itd, timedelta(minutes=15))
        t0 = ts[0]
        tl = ts[len(ts)-1]
        tis = GetExchangeScheduleIntervals(exchange, interval, t0-itd, tl+itd, ignore_breaks=ignore_breaks)
        tz_tis = tis[0].left.tzinfo
        if ts[0].tzinfo != tz_tis:
            ts = [t.astimezone(tz_tis) for t in ts]
        idx = tis.get_indexer(ts)
        f = idx != -1

        intervals = pd.DataFrame(index=ts)
        intervals["interval_open"] = pd.NaT
        intervals["interval_close"] = pd.NaT
        intervals["interval_open"] = pd.to_datetime(intervals["interval_open"], utc=True)
        intervals["interval_close"] = pd.to_datetime(intervals["interval_close"], utc=True)
        if f.any():
            intervals.loc[intervals.index[f], "interval_open"] = tis.left[idx[f]].tz_convert(tz)
            intervals.loc[intervals.index[f], "interval_close"] = tis.right[idx[f]].tz_convert(tz)
        intervals["interval_open"] = pd.to_datetime(intervals["interval_open"])
        intervals["interval_close"] = pd.to_datetime(intervals["interval_close"])
        intervals["interval_open"] = intervals["interval_open"].dt.tz_convert(tz)
        intervals["interval_close"] = intervals["interval_close"].dt.tz_convert(tz)

    if debug:
        print("intervals:") ; print(intervals)
        print("GetTimestampCurrentInterval_batch() returning")

    return intervals


def GetTimestampNextInterval(exchange, ts, interval, discardTimes=None, week7days=True, ignore_breaks=False):
    yfcu.TypeCheckStr(exchange, "exchange")
    yfcu.TypeCheckIntervalDt(ts, interval, "ts", strict=False)
    yfcu.TypeCheckInterval(interval, "interval")
    if discardTimes is not None:
        yfcu.TypeCheckBool(discardTimes, "discardTimes")
    yfcu.TypeCheckBool(week7days, "week7days")
    yfcu.TypeCheckBool(ignore_breaks, "ignore_breaks")

    itd = yfcd.intervalToTimedelta[interval]
    intraday = itd < timedelta(days=1)
    if discardTimes is None:
        discardTimes = not intraday
    if discardTimes and intraday:
        raise Exception("discardTimes with intraday is nonsense")
    if interval == yfcd.Interval.Week and week7days and not discardTimes:
        raise Exception("week7days without discardTimes is nonsense")

    debug = False
    # debug = True

    if debug:
        print("GetTimestampNextInterval()", locals())

    td_1d = timedelta(days=1)
    tz = ZoneInfo(GetExchangeTzName(exchange))

    ts_is_datetime = isinstance(ts, datetime)
    if debug:
        print("- ts_is_datetime =", ts_is_datetime)
    if not ts_is_datetime:
        ts_day = ts
        ts = datetime.combine(ts_day, time(0), tz)
    else:
        ts_day = ts.astimezone(tz).date()

    if interval == yfcd.Interval.Days1:
        if discardTimes or not isinstance(ts, datetime):
            next_day = ts_day + td_1d
            s = GetTimestampNextSession(exchange, datetime.combine(next_day, time(0), tz))
            if debug:
                print("- s:")
                print(s)
            interval_open = s["market_open"].date()
            interval_close = interval_open+td_1d
        else:
            if ts_is_datetime:
                s = GetTimestampNextSession(exchange, ts)
            else:
                s = GetTimestampNextSession(exchange, ts+td_1d)
            if debug:
                print("- s:")
                print(s)
            interval_open = s["market_open"]
            interval_close = s["market_close"]
        if debug:
            print("GetTimestampNextInterval() returning")
        return {"interval_open": interval_open, "interval_close": interval_close}
    elif interval == yfcd.Interval.Week:
        week_sched = GetExchangeScheduleIntervals(exchange, interval, ts_day, ts_day+14*td_1d, 
                                                  discardTimes=discardTimes, week7days=week7days, weekForceStartMonday=week7days, ignore_breaks=ignore_breaks)
        if debug:
            print("- week_sched:")
            print(week_sched)
        if not ts_is_datetime:
            if discardTimes:
                skip_first = ts_day >= week_sched.left[0]
            else:
                skip_first = ts_day >= week_sched.left[0].date()
        else:
            if discardTimes:
                skip_first = ts_day >= week_sched.left[0]
            else:
                skip_first = ts >= week_sched.left[0]
        i = 1 if skip_first else 0
        interval_open = week_sched.left[i]
        interval_close = week_sched.right[i]

        if debug:
            print("GetTimestampNextInterval() returning")
        return {"interval_open": interval_open, "interval_close": interval_close}

    interval_td = yfcd.intervalToTimedelta[interval]
    c = GetTimestampCurrentInterval(exchange, ts, interval, discardTimes, ignore_breaks=ignore_breaks)
    if debug:
        if c is None:
            print("- currentInterval = None")
        else:
            print("- currentInterval = {} -> {}".format(c["interval_open"], c["interval_close"]))

    next_interval_close = None
    itd = yfcd.intervalToTimedelta[interval]
    if c is None or not IsTimestampInActiveSession(exchange, c["interval_close"]):
        next_sesh = GetTimestampNextSession(exchange, ts)
        if debug:
            print("- next_sesh = {}".format(next_sesh))
        if exchange == "TLV":
            istr = yfcd.intervalToString[interval]
            if itd > timedelta(minutes=30):
                align = "-30m"
            else:
                align = "-"+istr
            d = next_sesh["market_open"].date()
            cal = GetCalendarViaCache(exchange, ts)
            ti = cal.trading_index(d.isoformat(), (d+td_1d).isoformat(), period=istr, intervals=True, force_close=True, align=align)
            next_interval_start = ti.left[0]
        else:
            next_interval_start = next_sesh["market_open"]
        next_interval_close = min(next_interval_start + interval_td, next_sesh["market_close"])
    else:
        day_sched = GetExchangeSchedule(exchange, ts_day, ts_day+td_1d).iloc[0]
        if exchange in yfcd.exchangesWithAuction:
            if c["interval_close"] < day_sched["close"]:
                # Next is normal trading
                next_interval_start = c["interval_close"]
            else:
                # Next is auction
                if itd <= timedelta(minutes=10):
                    next_interval_start = day_sched["auction"]
                else:
                    next_interval_start = day_sched["close"]
            next_interval_close = min(next_interval_start + interval_td,
                                      day_sched["auction"] + yfcd.exchangeAuctionDuration[exchange])
        else:
            next_interval_start = c["interval_close"]
            next_interval_close = min(next_interval_start + interval_td, day_sched["close"])

    if debug:
        print("GetTimestampNextInterval() returning")
    return {"interval_open": next_interval_start, "interval_close": next_interval_close}


def GetTimestampNextInterval_batch(exchange, ts, interval, discardTimes=None, week7days=True, ignore_breaks=False):
    yfcu.TypeCheckStr(exchange, "exchange")
    if isinstance(ts, list):
        ts = np.array(ts)
    yfcu.TypeCheckNpArray(ts, "ts")
    if len(ts) > 0:
        yfcu.TypeCheckIntervalDt(ts[0], interval, "ts", strict=False)
    yfcu.TypeCheckInterval(interval, "interval")
    if discardTimes is not None:
        yfcu.TypeCheckBool(discardTimes, "discardTimes")
    yfcu.TypeCheckBool(week7days, "week7days")
    yfcu.TypeCheckBool(ignore_breaks, "ignore_breaks")

    itd = yfcd.intervalToTimedelta[interval]
    intraday = itd < timedelta(days=1)
    if discardTimes is None:
        discardTimes = not intraday
    if discardTimes and intraday:
        raise Exception("discardTimes with intraday is nonsense")
    if interval == yfcd.Interval.Week and week7days and not discardTimes:
        raise Exception("week7days without discardTimes is nonsense")

    # For day and week intervals, the time component is ignored (set to 0).

    debug = False
    # debug = True

    if debug:
        if len(ts) == 0:
            msg = "GetTimestampNextInterval_batch(ts=[]"
        else:
            msg = f"GetTimestampNextInterval_batch(ts={ts[0]}->{ts[-1]}"
        msg += f", interval={interval}, discardTimes={discardTimes} week7days={week7days})"
        print(msg)

    if len(ts) == 0:
        if debug:
            print("- ts[] is empty")
            print("GetTimestampNextInterval_batch() returning")
        return None

    n = len(ts)
    tz = ZoneInfo(GetExchangeTzName(exchange))
    intervals = [None]*n

    if debug:
        print("- ts:") ; print(ts)

    td_1d = timedelta(days=1)
    tz = ZoneInfo(GetExchangeTzName(exchange))
    ts_is_datetimes = isinstance(ts[0], datetime)
    if debug:
        print("- ts_is_datetimes =", ts_is_datetimes)
    if ts_is_datetimes:
        ts = pd.to_datetime(ts)
        if ts[0].tzinfo != tz:
            ts = ts.tz_convert(tz)
        ts_day = ts.date
    else:
        ts_day = np.array(ts)
        ts = pd.to_datetime(ts).tz_localize(tz)
    if debug:
        print("- ts_day:") ; print(ts_day)

    if interval == yfcd.Interval.Week:
        # Treat week intervals as special case, contiguous from first weekday open to last weekday open.
        # Not necessarily Monday->Friday because of public holidays.
        # Unless 'ignoreClosedDays' is true, which means range from Monday to next Monday.
        #
        t0 = ts_day[0]  ; t0 -= timedelta(days=t0.weekday())+timedelta(days=7)
        tl = ts_day[-1] ; tl += timedelta(days=6-tl.weekday())+timedelta(days=14)
        if debug:
            print("t0={} ; tl={}".format(t0, tl))

        if week7days:
            # Monday -> next Monday regardless of exchange schedule
            wd = pd.to_timedelta(ts.weekday, unit='D')
            weekSchedStart = (ts - wd + timedelta(days=7)).date
            weekSchedEnd = weekSchedStart + timedelta(days=7)
        else:
            week_sched = GetExchangeScheduleIntervals(exchange, interval, t0, tl, discardTimes=discardTimes, week7days=week7days, weekForceStartMonday=week7days, exclude_future=False)
            if debug:
                print("- week_sched:", type(week_sched))
                for x in week_sched:
                    print(x)
            if ts_is_datetimes and not discardTimes:
                idx = week_sched.get_indexer(ts)
            else:
                if isinstance(week_sched, yfcd.DateIntervalIndex):
                    week_sched_daily = week_sched
                else:
                    week_sched_daily = yfcd.DateIntervalIndex.from_arrays(week_sched.left.date, week_sched.right.date+td_1d)
                if debug:
                    print("- week_sched_daily:")
                    for x in week_sched_daily:
                        print(x)
                idx = week_sched_daily.get_indexer(ts_day)
            if debug:
                print("- idx:")
                print(idx)
            f = idx != -1
            if f.any():
                # Point to next interval
                idx[f] += 1
            if (~f).any():
                if ts_is_datetimes and not discardTimes:
                    idx_next = week_sched.left.get_indexer(ts, method="bfill")
                else:
                    idx_next = np.searchsorted(week_sched_daily.left, ts_day)
                idx[~f] = idx_next[~f]
            if debug:
                print("- idx:")
                print(idx)
            weekSchedStart = week_sched.left[idx]
            weekSchedEnd = week_sched.right[idx]

        intervals = pd.DataFrame(data={"interval_open": weekSchedStart, "interval_close": weekSchedEnd}, index=ts)

    elif interval == yfcd.Interval.Days1:
        f_nna = ~pd.isna(ts_day)
        t0 = ts_day[f_nna][0]
        tl = ts_day[f_nna][-1]
        sched = GetExchangeSchedule(exchange, t0, tl+timedelta(days=14))
        if "auction" in sched.columns:
            sched["close"] = sched["auction"] + yfcd.exchangeAuctionDuration[exchange]
        if debug:
            print("- sched:")
            print(sched)

        intervals = pd.DataFrame(index=ts)
        if ts_is_datetimes and not discardTimes:
            open_as_index = pd.to_datetime(sched["open"].to_numpy())
            idx = open_as_index.get_indexer(ts)
            f = idx != -1
            if f.any():
                # Point to next interval
                idx[f] += 1
            if (~f).any():
                idx_next = open_as_index.get_indexer(ts, method="bfill")
                idx[~f] = idx_next[~f]
            intervals["interval_open"] = sched["open"].iloc[idx].to_numpy()
            intervals["interval_close"] = sched["close"].iloc[idx].to_numpy()
        else:
            idx = sched.index.get_indexer(ts_day)
            f = idx != -1
            if f.any():
                # Point to next interval
                idx[f] += 1
            if (~f).any():
                idx_next = sched.index.get_indexer(ts_day, method="bfill")
                idx[~f] = idx_next[~f]
            if discardTimes:
                intervals["interval_open"] = sched.index[idx].date
                intervals["interval_close"] = intervals["interval_open"] + timedelta(days=1)
            else:
                intervals["interval_open"] = sched["open"].iloc[idx].to_numpy()
                intervals["interval_close"] = sched["close"].iloc[idx].to_numpy()

    else:
        itd = yfcd.intervalToTimedelta[interval]
        if exchange in yfcd.exchangesWithAuction:
            itd = max(itd, timedelta(minutes=15))
        t0 = ts[0]
        tl = ts[len(ts)-1]
        # tis = GetExchangeScheduleIntervals(exchange, interval, t0-itd, tl+timedelta(days=14), discardTimes=False, week7days, ignore_breaks=ignore_breaks)
        tis = GetExchangeScheduleIntervals(exchange, interval, t0-itd, tl+timedelta(days=14), ignore_breaks=ignore_breaks, exclude_future=False)
        tz_tis = tis[0].left.tzinfo
        if ts[0].tzinfo != tz_tis:
            ts = [t.astimezone(tz_tis) for t in ts]
        idx = tis.get_indexer(ts)
        f = idx != -1
        if f.any():
            # Point to next interval
            idx[f] += 1
        if (~f).any():
            idx_next = tis.left.get_indexer(ts, method="bfill")
            idx[~f] = idx_next[~f]
        # Now everything mapped

        intervals = pd.DataFrame(index=ts)
        intervals["interval_open"] = tis.left[idx].tz_convert(tz)
        intervals["interval_close"] = tis.right[idx].tz_convert(tz)

    if debug:
        print("intervals (next):") ; print(intervals)
        print("GetTimestampNextInterval_batch() returning")

    return intervals


def TimestampInBreak_batch(exchange, ts, interval):
    yfcu.TypeCheckStr(exchange, "exchange")
    if isinstance(ts, list):
        ts = np.array(ts)
    yfcu.TypeCheckNpArray(ts, "ts")
    if len(ts) > 0:
        yfcu.TypeCheckIntervalDt(ts[0], interval, "ts", strict=False)
    yfcu.TypeCheckInterval(interval, "interval")

    n = len(ts)

    interday = interval in [yfcd.Interval.Days1, yfcd.Interval.Week]#, yfcd.Interval.Months1, yfcd.Interval.Months3]
    if interday:
        return np.full(n, False)

    itd = yfcd.intervalToTimedelta[interval]
    tss = pd.to_datetime(ts)
    df = pd.DataFrame(data={"_date": tss.date}, index=tss)
    cal = GetCalendarViaCache(exchange, df["_date"].iloc[0], df["_date"].iloc[-1])
    s = cal.schedule.copy()
    s["_date"] = s.index.date
    df["_indexBackup"] = df.index
    df = df.merge(s[["break_start", "break_end", "_date"]], how="left", on="_date")
    df.index = df["_indexBackup"]

    return (df.index >= df["break_start"].to_numpy()) & (df.index+itd <= df["break_end"].to_numpy())


def CalcIntervalLastDataDt(exchange, intervalStart, interval, ignore_breaks=False, yf_lag=None):
    # When does Yahoo stop receiving data for this interval?
    yfcu.TypeCheckStr(exchange, "exchange")
    yfcu.TypeCheckIntervalDt(intervalStart, interval, "intervalStart", strict=False)
    yfcu.TypeCheckInterval(interval, "interval")

    debug = False
    # debug = True

    if debug:
        print(f"CalcIntervalLastDataDt(exchange={exchange}, intervalStart={intervalStart}, interval={interval}, yf_lag={yf_lag})")

    i_td = yfcd.intervalToTimedelta[interval]
    intraday = i_td < timedelta(days=1)
    irange = GetTimestampCurrentInterval(exchange, intervalStart, interval, ignore_breaks=ignore_breaks)
    if irange is None:
        return None
    if debug:
        print("- irange:")
        pprint(irange)

    tz = ZoneInfo(GetExchangeTzName(exchange))

    if isinstance(irange["interval_open"], datetime):
        intervalSched = GetExchangeSchedule(exchange, irange["interval_open"].astimezone(tz).date(), irange["interval_close"].astimezone(tz).date()+timedelta(days=1))
    else:
        intervalSched = GetExchangeSchedule(exchange, irange["interval_open"], irange["interval_close"])
    if debug:
        print("- intervalSched:")
        pprint(intervalSched)

    if yf_lag is not None:
        yfcu.TypeCheckTimedelta(yf_lag, "yf_lag")
    else:
        yf_lag = GetExchangeDataDelay(exchange)

    if intervalSched is None or intervalSched.empty:
        # Exchange closed so data cannot update/expire
        lastDataDt = irange["interval_close"]
        if not isinstance(lastDataDt, pd.Timestamp):
            lastDataDt = pd.Timestamp(lastDataDt).tz_localize(tz)
        if debug:
            print("CalcIntervalLastDataDt() returning {} because exchange closed".format(lastDataDt))
        return lastDataDt + yf_lag

    # For daily+ intervals, Yahoo can update prices until next market open, 
    # and update volume until the second-next market open.
    if not intraday:
        # lastDataDt = start of next trading session, even if next day (or later)
        next_sesh = GetTimestampNextSession(exchange, intervalSched["close"].iloc[-1]-timedelta(minutes=1))
        next_sesh = GetTimestampNextSession(exchange, next_sesh['market_close']-timedelta(minutes=1))
        lastDataDt = next_sesh["market_open"] + yf_lag

        # Attempt to handle Yahoo changing weekly interval data after the last market update.
        # Yes this also happens with daily but less often.
        # Also, I never see this happen on USA exchanges, but everywhere else.
        # if not intraday and interval != yfcd.Interval.Days1 and '.' in exchange:
        # Update: I've seen this happen to 1d interval on USA
        lastDataDt += timedelta(hours=4)

        if debug:
            print("CalcIntervalLastDataDt() returning {}".format(lastDataDt))
        return lastDataDt

    # lastDataDt = start of next interval, even if next day (or later)
    nextInterval = GetTimestampNextInterval(exchange, irange["interval_close"]-timedelta(minutes=1), interval, ignore_breaks=ignore_breaks)
    if debug:
        print("- nextInterval:")
        print(nextInterval)
    lastDataDt = nextInterval["interval_open"] + yf_lag

    if debug:
        print("CalcIntervalLastDataDt() returning {}".format(lastDataDt))

    return lastDataDt


def CalcIntervalLastDataDt_batch(exchange, intervalStart, interval, ignore_breaks=False, yf_lag=None):
    # When does Yahoo stop receiving data for this interval?
    yfcu.TypeCheckStr(exchange, "exchange")
    if isinstance(intervalStart, list):
        intervalStart = np.array(intervalStart)
    yfcu.TypeCheckNpArray(intervalStart, "intervalStart")
    yfcu.TypeCheckIntervalDt(intervalStart[0], interval, "intervalStart", strict=False)
    yfcu.TypeCheckInterval(interval, "interval")
    yfcu.TypeCheckBool(ignore_breaks, "ignore_breaks")

    debug = False
    # debug = True

    if debug:
        print("CalcIntervalLastDataDt_batch(len(intervalStart)={}, interval={}, yf_lag={})".format(len(intervalStart), interval, yf_lag))
        print("- intervalStart:")
        print(intervalStart)

    if yf_lag is not None:
        yfcu.TypeCheckTimedelta(yf_lag, "yf_lag")
    else:
        yf_lag = GetExchangeDataDelay(exchange)

    tz = ZoneInfo(GetExchangeTzName(exchange))
    i_td = yfcd.intervalToTimedelta[interval]
    intraday = i_td < timedelta(days=1)
    interday = not intraday

    if interday and isinstance(intervalStart[0], (datetime, pd.Timestamp)):
        intervalStart = [dt.date() for dt in intervalStart]

    intervals = GetTimestampCurrentInterval_batch(exchange, intervalStart, interval, ignore_breaks=ignore_breaks)
    iopens = intervals["interval_open"]
    icloses = intervals["interval_close"]

    if isinstance(intervals["interval_open"].iloc[0], datetime):
        intervals["interval_open_day"] = intervals["interval_open"].dt.date
    else:
        intervals["interval_open_day"] = intervals["interval_open"]
    if debug:
        print("- intervals:")
        print(intervals)

    f_na = intervals["interval_open"].isna().values
    f_nna = ~f_na

    iopen0 = iopens[f_nna].iloc[0]
    iclosel = icloses[f_nna].iloc[-1]
    if debug:
        print(f"- intervals date range: {iopen0} -> {iclosel}")
    is_dt = isinstance(iopen0, datetime)
    if is_dt:
        if isinstance(iopen0, pd.Timestamp):
            iopen0 = iopen0.to_pydatetime()
            iclosel = iclosel.to_pydatetime()
        sched = GetExchangeSchedule(exchange, iopen0.astimezone(tz).date(), iclosel.astimezone(tz).date()+timedelta(days=1))
    else:
        sched = GetExchangeSchedule(exchange, iopen0, iclosel+timedelta(days=1))
    sched = sched.copy()
    sched["day"] = sched["open"].dt.date
    if debug:
        print("- sched:")
        print(sched)

    if interval == yfcd.Interval.Week:
        week_starts = sched["day"].to_period("W").keys().start_time
        if debug:
            print("- week_starts:")
            print(week_starts)
        sched["interval_open_day"] = week_starts
        sched_grp = sched[["interval_open_day", "open", "close"]].groupby(week_starts)
        sched = sched_grp.agg(Open=("open", "first"), Close=("close", "last"), interval_open_day=("interval_open_day", "first")).rename(columns={"Open": "open", "Close": "close"})
        sched["interval_open_day"] = sched["interval_open_day"].dt.date
    else:
        sched["interval_open_day"] = sched["day"]
        sched = sched[["interval_open_day", "open", "close"]]
    if debug:
        print("- sched:")
        print(sched)

    intervals = intervals.merge(sched, on="interval_open_day", how="left")
    intervals = intervals.rename(columns={"close": "market_close", "open": "market_open"})
    if debug:
        print("- intervals:")
        print(intervals)
    mcloses = intervals["market_close"]
    if debug:
        print("- mcloses:", type(mcloses[0]))
        print(mcloses)
        print("- icloses:")
        print(icloses)

    # For daily intervals, Yahoo data is updating until midnight. I guess aftermarket.
    i_td = yfcd.intervalToTimedelta[interval]
    if i_td >= timedelta(days=1):
        # lastDataDt = start of next interval, even if next day (or later)
        next_intervals = GetTimestampNextInterval_batch(exchange, mcloses.to_numpy(), yfcd.Interval.Days1, discardTimes=False, ignore_breaks=ignore_breaks)
        if debug:
            print("- next_intervals:")
            print(next_intervals)

        # Handle Yahoo changing weekly interval data after the last market update.
        # Also happens with daily but less often.
        # Get next trading day after next
        next_intervals = GetTimestampNextInterval_batch(exchange, (next_intervals['interval_close']-pd.Timedelta('1m')).to_numpy(), yfcd.Interval.Days1, discardTimes=False, ignore_breaks=ignore_breaks)

        lastDataDt = next_intervals["interval_open"].to_numpy() + yf_lag
        if f_na.any():
            lastDataDt[f_na] = pd.NaT

        lastDataDt += timedelta(hours=4)

        if debug:
            print("CalcIntervalLastDataDt_batch() returning")
        return lastDataDt

    # lastDataDt = start of next interval, even if next day (or later)
    next_intervals = GetTimestampNextInterval_batch(exchange, intervalStart, interval, ignore_breaks=ignore_breaks)
    if debug:
        print("- next_intervals:")
        print(next_intervals)
    lastDataDt = next_intervals["interval_open"].to_numpy() + yf_lag

    if debug:
        print("CalcIntervalLastDataDt_batch() returning")

    return lastDataDt


def IsPriceDatapointExpired(intervalStart, fetch_dt, repaired, max_age, exchange, interval, ignore_breaks=False, triggerExpiryOnClose=True, yf_lag=None, dt_now=None):
    yfcu.TypeCheckIntervalDt(intervalStart, interval, "intervalStart", strict=False)
    yfcu.TypeCheckDatetime(fetch_dt, "fetch_dt")
    yfcu.TypeCheckBool(repaired, "repaired")
    yfcu.TypeCheckTimedelta(max_age, "max_age")
    yfcu.TypeCheckStr(exchange, "exchange")
    yfcu.TypeCheckInterval(interval, "interval")
    yfcu.TypeCheckBool(triggerExpiryOnClose, "triggerExpiryOnClose")

    # TODO: unit tests for 'repaired' arg

    debug = False
    # debug = True

    if debug:
        print("") ; print("")
        print(f"IsPriceDatapointExpired(exchange={exchange} intervalStart={intervalStart}, fetch_dt={fetch_dt}, max_age={max_age}, interval={interval}, triggerExpiryOnClose={triggerExpiryOnClose}, dt_now={dt_now})")

    if dt_now is not None:
        yfcu.TypeCheckDatetime(dt_now, "dt_now")
    else:
        dt_now = pd.Timestamp.utcnow().tz_convert(ZoneInfo(GetExchangeTzName(exchange)))

    if yf_lag is not None:
        yfcu.TypeCheckTimedelta(yf_lag, "yf_lag")
    else:
        yf_lag = GetExchangeDataDelay(exchange)
    if debug:
        print("yf_lag = {}".format(yf_lag))

    irange = GetTimestampCurrentInterval(exchange, intervalStart, interval, ignore_breaks=ignore_breaks)
    if debug:
        print("- irange = {}".format(irange))

    if irange is None:
        if not isinstance(intervalStart, datetime):
            intervalStart_ts = pd.Timestamp(intervalStart).tz_localize(ZoneInfo(GetExchangeTzName(exchange)))
        else:
            intervalStart_ts = intervalStart
        if debug:
            print("market open? = {}".format(IsTimestampInActiveSession(exchange, intervalStart_ts)))
        raise yfcd.TimestampOutsideIntervalException(exchange, interval, intervalStart)

    intervalEnd = irange["interval_close"]
    if isinstance(intervalEnd, datetime):
        intervalEnd_d = intervalEnd.date()
    else:
        intervalEnd_d = intervalEnd
    if debug:
        print("- intervalEnd_d = {0}".format(intervalEnd_d))

    lastDataDt = CalcIntervalLastDataDt(exchange, intervalStart, interval, ignore_breaks=ignore_breaks, yf_lag=yf_lag)
    if debug:
        print("- lastDataDt = {}".format(lastDataDt))

    if repaired:
        # Give Yahoo 1 week to fix their data, so yfinance doesn't have to repair
        if debug:
            print("- adding 1wk to lastDataDt because data was repaired")
        lastDataDt += timedelta(days=7)

    # Decide if was fetched after last Yahoo update
    if interval in [yfcd.Interval.Days1, yfcd.Interval.Week]:
        if fetch_dt >= lastDataDt:
            # interval already closed before fetch, nothing to do.
            if debug:
                print("- fetch_dt > lastDataDt so return FALSE")
            return False
    else:
        interval_already_closed = fetch_dt > lastDataDt
        if interval_already_closed:
            # interval already closed before fetch, nothing to do.
            if debug:
                print("- fetch_dt > lastDataDt so return FALSE")
            return False

    expire_dt = fetch_dt + max_age
    if debug:
        print("- expire_dt = {0}".format(expire_dt))
    if expire_dt <= dt_now:
        if debug:
            print("- expire_dt <= dt_now")
        if IsTimestampInActiveSession(exchange, expire_dt - yf_lag):
            if debug:
                print("- ... and expire_dt in active session so return TRUE")
            return True
        elif IsTimestampInActiveSession(exchange, dt_now - yf_lag):
            if debug:
                print("- ... and dt_now in active session so return TRUE")
            return True
        elif expire_dt < dt_now:
            intervals = GetExchangeScheduleIntervals(exchange, yfcd.Interval.Days1, expire_dt, dt_now, discardTimes=False, week7days=False, ignore_breaks=False)
            if intervals is not None and not intervals.empty:
                if debug:
                    print("- ... and trading occurred since so return TRUE")
                return True

    if triggerExpiryOnClose:
        if debug:
            print("- checking if triggerExpiryOnClose ...")
            print("- - fetch_dt            = {}".format(fetch_dt))
            print("- - lastDataDt = {}".format(lastDataDt))
            print("- - dt_now              = {}".format(dt_now))
        if fetch_dt < lastDataDt:
            if lastDataDt <= dt_now:
                # Even though fetched data hasn't fully aged, the candle has since closed so treat as expired
                if debug:
                    print("- triggerExpiryOnClose and interval closed so return TRUE")
                return True
        if interval in [yfcd.Interval.Days1, yfcd.Interval.Week]:
            # If last fetch was anytime within interval, even post-market,
            # and dt_now is next day (or later) then trigger
            if fetch_dt.date() <= intervalEnd_d and dt_now.date() > intervalEnd_d:
                if debug:
                    print("- triggerExpiryOnClose and interval midnight passed since fetch so return TRUE")
                return True

    if debug:
        print("- reached end of function, returning FALSE")
    return False


def IdentifyMissingIntervals(exchange, start, end, interval, knownIntervalStarts, week7days=True, ignore_breaks=False):
    yfcu.TypeCheckStr(exchange, "exchange")
    yfcu.TypeCheckDateEasy(start, "start")
    yfcu.TypeCheckDateEasy(end, "end")
    if start >= end:
        raise Exception("start={} must be < end={}".format(start, end))
    if knownIntervalStarts is not None:
        if not isinstance(knownIntervalStarts, list) and not isinstance(knownIntervalStarts, np.ndarray):
            raise Exception("'knownIntervalStarts' must be list or numpy array not {0}".format(type(knownIntervalStarts)))
        if len(knownIntervalStarts) > 0:
            if interval in [yfcd.Interval.Days1, yfcd.Interval.Week]:
                # Must be date
                yfcu.TypeCheckDateStrict(knownIntervalStarts[0], "knownIntervalStarts")
            else:
                # Must be datetime
                yfcu.TypeCheckDatetime(knownIntervalStarts[0], "knownIntervalStarts")
                if knownIntervalStarts[0].tzinfo is None:
                    raise Exception("'knownIntervalStarts' dates must be timezone-aware")
    yfcu.TypeCheckBool(ignore_breaks, "ignore_breaks")

    debug = False
    # debug = True

    if debug:
        print(f"IdentifyMissingIntervals-{yfcd.intervalToString[interval]}(start={start} end={end})")
        print("- knownIntervalStarts:")
        pprint(knownIntervalStarts)

    intervals = GetExchangeScheduleIntervals(exchange, interval, start, end, week7days=week7days, ignore_breaks=ignore_breaks, exclude_future=True)
    if intervals is None or intervals.empty:
        raise yfcd.NoIntervalsInRangeException(interval, start, end)
    if debug:
        print("- intervals:")
        pprint(intervals)

    if knownIntervalStarts is not None:
        intervalStarts = intervals.left
        if isinstance(intervalStarts[0], (datetime, pd.Timestamp)):
            intervalStarts = [i.timestamp() for i in intervalStarts.to_pydatetime()]
            knownIntervalStarts = [x.timestamp() for x in knownIntervalStarts]
        if debug:
            print("- intervalStarts:")
            print(intervalStarts)
        f_missing = yfcu.np_isin_optimised(intervalStarts, knownIntervalStarts, invert=True)
    else:
        f_missing = np.full(intervals.shape[0], True)

    intervals_missing_df = pd.DataFrame(data={"open": intervals[f_missing].left, "close": intervals[f_missing].right}, index=np.where(f_missing)[0])
    if debug:
        print("- intervals_missing_df:")
        print(intervals_missing_df)

    if debug:
        print("IdentifyMissingIntervals() returning")
    return intervals_missing_df


def IdentifyMissingIntervalRanges(exchange, start, end, interval, knownIntervalStarts, ignore_breaks=False, minDistanceThreshold=5):
    yfcu.TypeCheckStr(exchange, "exchange")
    yfcu.TypeCheckIntervalDt(start, interval, "start", strict=True)
    yfcu.TypeCheckIntervalDt(end, interval, "end", strict=True)
    if start >= end:
        raise Exception("start={} must be < end={}".format(start, end))
    if knownIntervalStarts is not None:
        if not isinstance(knownIntervalStarts, list) and not isinstance(knownIntervalStarts, np.ndarray):
            raise Exception("'knownIntervalStarts' must be list or numpy array not {0}".format(type(knownIntervalStarts)))
        if len(knownIntervalStarts) > 0:
            if interval in [yfcd.Interval.Days1, yfcd.Interval.Week]:
                # Must be date or datetime
                yfcu.TypeCheckDateEasy(knownIntervalStarts[0], "knownIntervalStarts")
                if isinstance(knownIntervalStarts[0], datetime) and knownIntervalStarts[0].tzinfo is None:
                    raise Exception("'knownIntervalStarts' datetimes must be timezone-aware")
            else:
                # Must be datetime
                yfcu.TypeCheckDatetime(knownIntervalStarts[0], "knownIntervalStarts")
                if knownIntervalStarts[0].tzinfo is None:
                    raise Exception("'knownIntervalStarts' dates must be timezone-aware")
    yfcu.TypeCheckBool(ignore_breaks, "ignore_breaks")

    debug = False
    # debug = True

    if debug:
        print("IdentifyMissingIntervalRanges()")
        print(f"- start={start}, end={end}, interval={interval}")
        print("- knownIntervalStarts:")
        pprint(knownIntervalStarts)

    intervals = GetExchangeScheduleIntervals(exchange, interval, start, end, ignore_breaks=ignore_breaks)
    if intervals is None or intervals.empty:
        raise yfcd.NoIntervalsInRangeException(interval, start, end)
    if debug:
        print("- intervals:", type(intervals))
        for i in intervals:
            print(i)

    intervals_missing_df = IdentifyMissingIntervals(exchange, start, end, interval, knownIntervalStarts)
    if debug:
        print("- intervals_missing_df:")
        pprint(intervals_missing_df)

    f_missing = np.full(intervals.shape[0], False) ; f_missing[intervals_missing_df.index] = True

    # Merge together near ranges if the distance between is below threshold.
    # This is to reduce web requests
    i_true = np.where(f_missing)[0]
    for i in range(len(i_true)-1):
        i0 = i_true[i]
        i1 = i_true[i+1]
        if i1-i0 <= minDistanceThreshold+1:
            # Mark all intervals between as missing, thus merging together
            # the pair of missing ranges
            f_missing[i0+1:i1] = True
    if debug:
        print("- f_missing:")
        pprint(f_missing)

    # Scan for contiguous sets of missing intervals:
    ranges = []
    i_true = np.where(f_missing)[0]
    if len(i_true) > 0:
        start = None ; end = None
        for i in range(len(f_missing)):
            v = f_missing[i]
            if v:
                if start is None:
                    start = i ; end = i
                else:
                    if i == (end+1):
                        end = i
                    else:
                        r = (intervals[start].left, intervals[end].right)
                        ranges.append(r)
                        start = i ; end = i

            if i == (len(f_missing) - 1):
                r = (intervals[start].left, intervals[end].right)
                ranges.append(r)

    if debug:
        print("- ranges:")
        pprint(ranges)

    if debug:
        print("IdentifyMissingIntervalRanges() returning")

    if len(ranges) == 0:
        return None
    return ranges


def ConvertToDatetime(dt, tz=None):
    # Convert numpy.datetime64 -> pandas.Timestamp -> python datetime
    if isinstance(dt, np.datetime64):
        dt = pd.Timestamp(dt)
    if isinstance(dt, pd.Timestamp):
        dt = dt.to_pydatetime()
    if tz is not None:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tz)
        else:
            dt = dt.astimezone(tz)
    return dt


def DtSubtractPeriod(dt, period):
    yfcu.TypeCheckDateEasy(dt, "dt")
    yfcu.TypeCheckPeriod(period, "period")

    if period == yfcd.Period.Ytd:
        if isinstance(dt, datetime):
            return datetime(dt.year, 1, 1, tzinfo=dt.tzinfo)
        else:
            return date(dt.year, 1, 1)

    elif period == yfcd.Period.Max:
        raise Exception("Codepath not implemented")

    else:
        # 'period' should be type Timedelta
        return dt - period

