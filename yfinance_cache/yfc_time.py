from pprint import pprint

from datetime import datetime, date, time, timedelta
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo

import sys ; sys.path.insert(0, "/home/gonzo/ReposForks/exchange_calendars.dev")
import exchange_calendars as xcal

import pandas as pd
import numpy as np
import sqlite3 as sql

from . import yfc_dat as yfcd
from . import yfc_cache_manager as yfcm
from . import yfc_utils as yfcu


def TypeCheckStr(var, varName):
    if not isinstance(var, str):
        raise Exception("'{}' must be str not {}".format(varName, type(var)))
def TypeCheckBool(var, varName):
    if not isinstance(var, bool):
        raise Exception("'{}' must be bool not {}".format(varName, type(var)))
def TypeCheckDateEasy(var, varName):
    if not (isinstance(var, date) or isinstance(var, datetime)):
        raise Exception("'{}' must be date not {}".format(varName, type(var)))
    if isinstance(var, datetime):
        if var.tzinfo is None:
            raise Exception("'{}' if datetime must be timezone-aware".format(varName))
        elif not isinstance(var.tzinfo, ZoneInfo):
            raise Exception("'{}' tzinfo must be ZoneInfo".format(varName))
def TypeCheckDateStrict(var, varName):
    if isinstance(var, pd.Timestamp):
        # While Pandas missing support for 'zoneinfo' must deny
        raise Exception("'{}' must be date not {}".format(varName, type(var)))
    if not (isinstance(var, date) and not isinstance(var, datetime)):
        raise Exception("'{}' must be date not {}".format(varName, type(var)))
def TypeCheckDatetime(var, varName):
    if not isinstance(var, datetime):
        raise Exception("'{}' must be datetime not {}".format(varName, type(var)))
    if var.tzinfo is None:
        raise Exception("'{}' if datetime must be timezone-aware".format(varName))
    elif not isinstance(var.tzinfo, ZoneInfo):
        raise Exception("'{}' tzinfo must be ZoneInfo".format(varName))
def TypeCheckTimedelta(var, varName):
    if not isinstance(var, timedelta):
        raise Exception("'{}' must be timedelta not {}".format(varName, type(var)))
def TypeCheckInterval(var, varName):
    if not isinstance(var, yfcd.Interval):
        raise Exception("'{}' must be yfcd.Interval not {}".format(varName, type(var)))
def TypeCheckIntervalDt(dt, interval, varName, strict=True):
    if strict and interval in [yfcd.Interval.Days1, yfcd.Interval.Week]:
        TypeCheckDateStrict(dt, varName)
    else:
        TypeCheckDateEasy(dt, varName)
def TypeCheckPeriod(var, varName):
    if not isinstance(var, yfcd.Period):
        raise Exception("'{}' must be yfcd.Period not {}".format(varName, type(var)))
def TypeCheckNpArray(var, varName):
    if not isinstance(var, np.ndarray):
        raise Exception("'{}' must be numpy array not {}".format(varName, type(var)))


exchangeTzCache = {}
def GetExchangeTzName(exchange):
    TypeCheckStr(exchange, "exchange")

    if exchange not in exchangeTzCache:
        tz = yfcm.ReadCacheDatum("exchange-"+exchange, "tz")
        if tz is None:
            raise Exception("Do not know timezone for exchange '{}'".format(exchange))
        exchangeTzCache[exchange] = tz
    else:
        tz = exchangeTzCache[exchange]
    return tz
def SetExchangeTzName(exchange, tz):
    TypeCheckStr(exchange, "exchange")
    TypeCheckStr(tz, "tz")

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


def GetExchangeDataDelay(exchange):
    TypeCheckStr(exchange, "exchange")

    d = yfcm.ReadCacheDatum("exchange-"+exchange, "yf_lag")
    if d is None:
        d = yfcd.exchangeToYfLag[exchange]
    return d


def GetCalendar(exchange):
    global calCache

    cal_name = yfcd.exchangeToXcalExchange[exchange]

    if cal_name in calCache:
        return calCache[cal_name]

    if cal_name in {"JPX", "XTKS"}:
        # These won't go before 1997
        start = "1997"
    else:
        start = str(yfcd.yf_min_year)
    cal = xcal.get_calendar(cal_name, start=start, cache=True)

    df = cal.schedule
    tz = ZoneInfo(GetExchangeTzName(exchange))
    df["open"] = df["open"].dt.tz_convert(tz)
    df["close"] = df["close"].dt.tz_convert(tz)

    if (exchange in yfcd.exchangesWithAuction) and ("auction" not in df.columns):
        df["auction"] = df["close"] + yfcd.exchangeAuctionDelay[exchange]

    df["idx_nanos"] = df.index.values.astype("int64")

    calCache[cal_name] = cal

    return cal


def ExchangeOpenOnDay(exchange, d):
    TypeCheckStr(exchange, "exchange")
    TypeCheckDateStrict(d, "d")

    if exchange not in yfcd.exchangeToXcalExchange:
        raise Exception("Need to add mapping of exchange {} to xcal".format(exchange))
    cal = GetCalendar(exchange)

    return d.isoformat() in cal.schedule.index


def GetExchangeSchedule(exchange, start_d, end_d):
    TypeCheckStr(exchange, "exchange")
    TypeCheckDateStrict(start_d, "start_d")
    TypeCheckDateStrict(end_d, "end_d")
    if start_d >= end_d:
        raise Exception("start_d={} must < end_d={}".format(start_d, end_d))

    debug = False
    # debug = True

    if debug:
        print("GetExchangeSchedule(exchange={}, start_d={}, end_d={}".format(exchange, start_d, end_d))

    if exchange not in yfcd.exchangeToXcalExchange:
        raise Exception("Need to add mapping of exchange {} to xcal".format(exchange))

    end_d_sub1 = end_d-timedelta(days=1)

    num_years = end_d.year - start_d.year + 1
    if num_years <= 2:
        # Cache
        cache_key = (exchange, start_d.year, num_years)
        if cache_key in schedCache:
            s = schedCache[cache_key]
        else:
            cal = GetCalendar(exchange)
            s = cal.schedule.loc[str(start_d.year):str(end_d_sub1.year)].copy()
            schedCache[cache_key] = s
    else:
        cal = GetCalendar(exchange)
        s = cal.schedule

    if s is not None:
        start_ts = pd.Timestamp(start_d)
        end_ts = pd.Timestamp(end_d_sub1)
        slice_start = s["idx_nanos"].values.searchsorted(start_ts.value, side="left")
        slice_end = s["idx_nanos"].values.searchsorted(end_ts.value, side="right")
        sched = s[slice_start:slice_end]
    else:
        sched = None

    if (sched is None) or sched.shape[0] == 0:
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
        cal = GetCalendar(exchange)
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
    if (sched is None) or sched.shape[0] == 0:
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
    ##


def GetExchangeWeekSchedule(exchange, start, end, weeklyUseYahooDef=True):
    TypeCheckStr(exchange, "exchange")
    if start >= end:
        raise Exception("start={} must be < end={}".format(start, end))
    TypeCheckDateEasy(start, "start")
    TypeCheckDateEasy(end, "end")

    debug = False
    # debug = True

    if debug:
        print("GetExchangeWeekSchedule(exchange={}, start={}, end={})".format(exchange, start, end))

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

    week_starts_sunday = (exchange in ["TLV"]) and (not weeklyUseYahooDef)
    if debug:
        print("- week_starts_sunday =", week_starts_sunday)

    if exchange not in yfcd.exchangeToXcalExchange:
        raise Exception("Need to add mapping of exchange {} to xcal".format(exchange))
    cal = GetCalendar(exchange)

    open_dts = GetExchangeSchedule(exchange, start_d, end_d)["open"]

    if len(open_dts) == 0:
        return None
    open_dts = pd.DatetimeIndex(open_dts).tz_convert(tz).tz_localize(None)

    if debug:
        print("- open_dts:")
        print(open_dts)

    if week_starts_sunday:
        weeks = open_dts.groupby(open_dts.to_period("W-SAT"))
    else:
        weeks = open_dts.groupby(open_dts.to_period("W"))
    weeks_keys = sorted(list(weeks.keys()))
    weeks_keys_arr = pd.arrays.PeriodArray(pd.Series(weeks_keys))

    if debug:
        print("- weeks:")
        for k in weeks:
            print("- {}->{}".format(k.start_time, k.end_time))
            print(weeks[k].date)
        print("")

    if weeklyUseYahooDef:
        td_7d = timedelta(days=7)
        week_starts = weeks_keys_arr.start_time.date
        week_ends = week_starts + td_7d
        week_ranges = np.stack([week_starts, week_ends], axis=1)
    else:
        week_ranges = np.array([(w[0].date(), w[-1].date()) for w in weeks.values()])
        week_ranges[:,1] += td_1d

    if debug:
        print("- week_ranges:")
        pprint(week_ranges)

    first_week_cutoff = False
    last_week_cutoff = False
    if weeklyUseYahooDef:
        k = weeks_keys[0]
        prev_sesh = cal.previous_session(weeks[k][0].date())
        if debug:
            print("- prev_sesh:", prev_sesh)
        first_week_cutoff = prev_sesh >= k.start_time

        k = weeks_keys[-1]
        next_sesh = cal.next_session(weeks[k][-1].date())
        if debug:
            print("- next_sesh:", next_sesh)
        last_week_cutoff = next_sesh <= k.end_time

    else:
        # Add one day to start and end. If returns more open days, then means
        # above date range cuts off weeks.
        open_dts_wrap = cal.schedule.loc[(start_d-td_1d).isoformat():end_d.isoformat()]["open"]
        open_dts_wrap = pd.DatetimeIndex(open_dts_wrap).tz_convert(tz).tz_localize(None)
        if week_starts_sunday:
            weeks2 = open_dts_wrap.groupby(open_dts_wrap.to_period("W-SAT"))
        else:
            weeks2 = open_dts_wrap.groupby(open_dts_wrap.to_period("W"))

        if debug:
            print("- open_dts_wrap:")
            print(open_dts_wrap)

        if debug:
            print("- weeks2:")
            # pprint(weeks2)
            for k in weeks2:
                print("- ", k)
                print(weeks2[k].date)

        k0 = sorted(list(weeks.keys()))[0]
        m0 = sorted(list(weeks2.keys()))[0]
        if k0 == m0:
            if weeks2[m0][0] < weeks[k0][0]:
                first_week_cutoff = True

        kn1 = sorted(list(weeks.keys()))[-1]
        mn1 = sorted(list(weeks2.keys()))[-1]
        if kn1 == mn1:
            if weeks2[mn1][-1] > weeks[kn1][-1]:
                last_week_cutoff = True

    if debug:
        print("- first_week_cutoff:", first_week_cutoff)
        print("- last_week_cutoff:", last_week_cutoff)

    week_ranges = np.sort(week_ranges, axis=0)
    if last_week_cutoff:
        week_ranges = np.delete(week_ranges, -1, axis=0)
    if first_week_cutoff:
        week_ranges = np.delete(week_ranges, 0, axis=0)
    if week_ranges.shape[0] == 0:
        week_ranges = None
    week_ranges = week_ranges.tolist()

    if debug:
        print("- week_ranges:")
        print(week_ranges)
        print("GetExchangeWeekSchedule() returning")
    return week_ranges


def GetExchangeScheduleIntervals(exchange, interval, start, end, weeklyUseYahooDef=True):
    TypeCheckStr(exchange, "exchange")
    if start >= end:
        raise Exception("start={} must be < end={}".format(start, end))
    TypeCheckIntervalDt(start, interval, "start", strict=True)
    TypeCheckIntervalDt(end, interval, "end", strict=True)

    debug = False
    # debug = True

    if debug:
        print("GetExchangeScheduleIntervals(interval={}, start={} (), end={}, weeklyUseYahooDef={})".format(interval, start, end, weeklyUseYahooDef))
        print("- types: start={} end={}".format(type(start), type(end)))

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
    cache_key = None
    cache_key = (exchange, interval, start_d, end_d, weeklyUseYahooDef)
    if cache_key in schedIntervalsCache:
        s = schedIntervalsCache[cache_key]
        if isinstance(s.left[0], datetime):
            s = s[s.left>=start_dt]
        if isinstance(s.right[0], datetime):
            s = s[s.right<=end_dt]
        if debug:
            print("- returning cached intervals ({}->{} filtered by {}->{})".format(start_d, end_d, start, end))
        return s

    if debug:
        print("- start_d={}, end_d={}".format(start_d, end_d))

    week_starts_sunday = (exchange in ["TLV"]) and (not weeklyUseYahooDef)
    if debug:
        print("- week_starts_sunday =", week_starts_sunday)

    if exchange not in yfcd.exchangeToXcalExchange:
        raise Exception("Need to add mapping of exchange {} to xcal".format(exchange))
    cal = GetCalendar(exchange)

    # When calculating intervals use dates not datetimes. Cache the result, and then
    # apply datetime limits.
    intervals = None
    istr = yfcd.intervalToString[interval]
    td = yfcd.intervalToTimedelta[interval]
    if istr.endswith('h') or istr.endswith('m'):
        if td > timedelta(minutes=30):
            align = '30m'
        else:
            align = istr
        ti = cal.trading_index(start_d.isoformat(), (end_d-td_1d).isoformat(), period=istr, intervals=True, force_close=True, align=align)
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
            res = istr.replace('h', 'H') if istr.endswith('h') else istr.replace('m', 'T')
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
                f = d <= td
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
        if s is None or s.shape[0] == 0:
            return None
        open_days = np.array([dt.to_pydatetime().astimezone(tz).date() for dt in s["open"]])
        intervals = yfcd.DateIntervalIndex.from_arrays(open_days, open_days+td_1d, closed="left")

    elif interval == yfcd.Interval.Week:
        week_ranges = GetExchangeWeekSchedule(exchange, start, end, weeklyUseYahooDef)
        if week_ranges is not None:
            intervals = yfcd.DateIntervalIndex.from_arrays([w[0] for w in week_ranges], [w[1] for w in week_ranges], closed="left")

    else:
        raise Exception("Need to implement for interval={}".format(interval))

    if not cache_key is None:
        schedIntervalsCache[cache_key] = intervals

    if isinstance(intervals.left[0], datetime):
        intervals = intervals[intervals.left >= start_dt]
    if isinstance(intervals.right[0], datetime):
        intervals = intervals[intervals.right <= end_dt]
    if intervals.shape[0] == 0:
        raise Exception("WARNING: No intervals generated for date range {} -> {}".format(start, end))
        return None
    if debug:
        print("- intervals: [0]={}->{} [-1]={}->{}".format(intervals.left[0], intervals.right[0], intervals.left[-1], intervals.right[-1]))

    if debug:
        print("GetExchangeScheduleIntervals() returning")

    return intervals


def IsTimestampInActiveSession(exchange, ts):
    TypeCheckStr(exchange, "exchange")
    TypeCheckDatetime(ts, "ts")

    cal = GetCalendar(exchange)
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
    TypeCheckStr(exchange, "exchange")
    TypeCheckDatetime(ts, "ts")

    cal = GetCalendar(exchange)
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
    TypeCheckStr(exchange, "exchange")
    TypeCheckDatetime(ts, "ts")

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
        if sched["open"][i] <= ts:
            tz = ZoneInfo(GetExchangeTzName(exchange))
            return {"market_open": sched["open"][i].to_pydatetime().astimezone(tz), "market_close": sched["close"][i].to_pydatetime().astimezone(tz)}
    raise Exception("Failed to find most recent '{0}' session for ts = {1}".format(exchange, ts))


def GetTimestampNextSession(exchange, ts):
    TypeCheckStr(exchange, "exchange")
    TypeCheckDatetime(ts, "ts")

    sched = GetExchangeSchedule(exchange, ts.date(), ts.date()+timedelta(days=7))
    if "auction" in sched.columns:
        sched = sched.copy()
        f = ~(sched["auction"].isna())
        if f.any():
            if f.all():
                sched["close"] = np.maximum(sched["close"], sched["auction"]+yfcd.exchangeAuctionDuration[exchange])
            else:
                sched.loc[f, "close"] = np.maximum(sched.loc[f, "close"], sched.loc[f, "auction"]+yfcd.exchangeAuctionDuration[exchange])
    for i in range(sched.shape[0]):
        if ts < sched["open"][i]:
            tz = ZoneInfo(GetExchangeTzName(exchange))
            return {"market_open": sched["open"][i].to_pydatetime().astimezone(tz), "market_close": sched["close"][i].to_pydatetime().astimezone(tz)}
    raise Exception("Failed to find next '{0}' session for ts = {1}".format(exchange, ts))


def GetTimestampCurrentInterval(exchange, ts, interval, weeklyUseYahooDef=True):
    TypeCheckStr(exchange, "exchange")
    TypeCheckIntervalDt(ts, interval, "ts", strict=False)
    TypeCheckInterval(interval, "interval")
    TypeCheckBool(weeklyUseYahooDef, "weeklyUseYahooDef")

    # For day and week intervals, the time component is ignored (set to 0).

    debug = False
    # debug = True

    if debug:
        print("GetTimestampCurrentInterval(ts={}, interval={}, weeklyUseYahooDef={})".format(ts, interval, weeklyUseYahooDef))

    week_starts_sunday = (exchange in ["TLV"]) and (not weeklyUseYahooDef)

    i = None

    tz = ZoneInfo(GetExchangeTzName(exchange))
    if interval == yfcd.Interval.Week:
        # Treat week intervals as special case, contiguous from first weekday open to last weekday open.
        # Not necessarily Monday->Friday because of public holidays.
        # Unless 'weeklyUseYahooDef' is true, which means range from Monday to Friday.
        # UPDATE: Extending range to Sunday midnight aka next Monday
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

        if weeklyUseYahooDef:
            weekEnd = weekStart + timedelta(days=7)
        else:
            weekEnd = weekStart + timedelta(days=5)
        if debug:
            print("- weekStart = {}".format(weekStart))
            print("- weekEnd = {}".format(weekEnd))
        if not weeklyUseYahooDef:
            weekSched = GetExchangeSchedule(exchange, weekStart, weekEnd)
            weekStart = weekSched["open"][0].date()
            weekEnd = weekSched["close"][-1].date()+timedelta(days=1)
        intervalStart = weekStart
        intervalEnd = weekEnd
        if debug:
            print("- intervalStart = {}".format(intervalStart))
            print("- intervalEnd = {}".format(intervalEnd))
        if ts_day >= intervalStart:
            if (ts_day < intervalEnd):
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
            i = {"interval_open": ts_day, "interval_close": ts_day+timedelta(days=1)}
        else:
            if debug:
                print("- exchange closed")

    else:
        if IsTimestampInActiveSession(exchange, ts) or IsTimestampInActiveSession(exchange, ts+timedelta(minutes=30)):
            ts = ts.astimezone(tz)
            td = yfcd.intervalToTimedelta[interval]
            if exchange in yfcd.exchangesWithAuction:
                td = max(td, timedelta(minutes=15))
            intervals = GetExchangeScheduleIntervals(exchange, interval, ts-td, ts+timedelta(minutes=30)+td)
            idx = intervals.get_indexer([ts])
            f = idx != -1
            if f.any():
                i0 = intervals[idx[f]][0]
                i = {"interval_open": i0.left.to_pydatetime(), "interval_close": i0.right.to_pydatetime()}

    if debug:
        print("GetTimestampCurrentInterval() returning: {}".format(i))

    return i


def GetTimestampCurrentInterval_batch(exchange, ts, interval, weeklyUseYahooDef=True):
    TypeCheckStr(exchange, "exchange")
    if isinstance(ts, list):
        ts = np.array(ts)
    TypeCheckNpArray(ts, "ts")
    if len(ts) > 0:
        TypeCheckIntervalDt(ts[0], interval, "ts", strict=False)
    TypeCheckInterval(interval, "interval")
    TypeCheckBool(weeklyUseYahooDef, "weeklyUseYahooDef")

    # For day and week intervals, the time component is ignored (set to 0).

    debug = False
    # debug = True

    if debug:
        if len(ts) == 0:
            print("GetTimestampCurrentInterval_batch(ts=[], interval={}, weeklyUseYahooDef={})".format(interval, weeklyUseYahooDef))
        else:
            print("GetTimestampCurrentInterval_batch(ts[0]={}, interval={}, weeklyUseYahooDef={})".format(ts[0], interval, weeklyUseYahooDef))

    if len(ts) == 0:
        if debug:
            print("- ts[] is empty")
            print("GetTimestampCurrentInterval_batch() returning")
        return None

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
        # Unless 'weeklyUseYahooDef' is true, which means range from Monday to Friday.
        #
        t0 = ts_day[0]  ; t0 -= timedelta(days=t0.weekday())+timedelta(days=7)
        tl = ts_day[-1] ; tl += timedelta(days=6-tl.weekday())+timedelta(days=7)
        if debug:
            print("t0={} ; tl={}".format(t0, tl))

        if weeklyUseYahooDef:
            # Monday -> next Monday regardless of exchange schedule
            wd = pd.to_timedelta(ts.weekday, unit='D')
            weekSchedStart = ts - wd
            weekSchedEnd = weekSchedStart + timedelta(days=7)
            weekSchedStart = weekSchedStart.date
            weekSchedEnd = weekSchedEnd.date
        else:
            week_sched = GetExchangeScheduleIntervals(exchange, interval, t0, tl, weeklyUseYahooDef)
            weekSchedStart = np.full(n, None)
            weekSchedEnd = np.full(n, None)
            left = pd.to_datetime(week_sched.left).tz_localize(tz)
            right = pd.to_datetime(week_sched.right).tz_localize(tz)

            week_sched = pd.IntervalIndex.from_arrays(left, right, closed="left")
            idx = week_sched.get_indexer(ts)
            f = idx != -1
            if f.any():
                weekSchedStart[f] = week_sched.left[idx[f]].date
                weekSchedEnd[f] = week_sched.right[idx[f]].date

        intervals = pd.DataFrame(data={"interval_open": weekSchedStart, "interval_close": weekSchedEnd}, index=ts)

    elif interval == yfcd.Interval.Days1:
        t0 = ts_day[0]
        tl = ts_day[len(ts_day)-1]
        sched = GetExchangeSchedule(exchange, t0, tl+timedelta(days=1))
        #
        ts_day = pd.to_datetime(ts_day)
        ts_day_df = pd.DataFrame(index=ts_day)
        intervals = pd.merge(ts_day_df, sched, how="left", left_index=True, right_index=True)
        rename_cols = {"open": "interval_open", "close": "interval_close"}
        intervals.columns = [rename_cols[col] if col in rename_cols else col for col in intervals.columns]
        intervals["interval_open"] = intervals["interval_open"].dt.date
        intervals["interval_close"] = intervals["interval_close"].dt.date + td_1d

    else:
        td = yfcd.intervalToTimedelta[interval]
        if exchange == "ASX":
            td = max(td, timedelta(minutes=15))
        t0 = ts[0]
        tl = ts[len(ts)-1]
        tis = GetExchangeScheduleIntervals(exchange, interval, t0-td, tl+td)
        if debug:
            print("- trading index:", type(tis))
            for ti in tis:
                print(ti)
        tz_tis = tis[0].left.tzinfo
        if ts[0].tzinfo != tz_tis:
            ts = [t.astimezone(tz_tis) for t in ts]
        idx = tis.get_indexer(ts)
        f = idx != -1
        #
        intervals = pd.DataFrame(index=ts)
        intervals["interval_open"] = pd.NaT
        intervals["interval_close"] = pd.NaT
        if f.any():
            intervals.loc[intervals.index[f], "interval_open"] = tis.left[idx[f]].tz_convert(tz)
            intervals.loc[intervals.index[f], "interval_close"] = tis.right[idx[f]].tz_convert(tz)

    if debug:
        print("GetTimestampCurrentInterval_batch() returning")

    return intervals


def GetTimestampNextInterval(exchange, ts, interval, weeklyUseYahooDef=True):
    TypeCheckStr(exchange, "exchange")
    TypeCheckIntervalDt(ts, interval, "ts", strict=False)
    TypeCheckInterval(interval, "interval")
    TypeCheckBool(weeklyUseYahooDef, "weeklyUseYahooDef")

    debug = False
    # debug = True

    if debug:
        print("GetTimestampNextInterval(exchange={}, ts={}, interval={})".format(exchange, ts, interval))

    td_1d = timedelta(days=1)
    tz = ZoneInfo(GetExchangeTzName(exchange))
    ts_d = ts.astimezone(tz).date()

    if interval in [yfcd.Interval.Days1, yfcd.Interval.Week]:
        if interval == yfcd.Interval.Days1:
            next_day = ts_d + td_1d
            s = GetTimestampNextSession(exchange, datetime.combine(next_day, time(0), tz))
            interval_open = s["market_open"].date()
            interval_close = interval_open+td_1d
        else:
            week_sched = GetExchangeWeekSchedule(exchange, ts_d, ts_d+14*td_1d, weeklyUseYahooDef)
            if ts_d >= week_sched[0][0]:
                week_sched = week_sched[1:]
            interval_open = week_sched[0][0]
            interval_close = week_sched[0][1]
        return {"interval_open": interval_open, "interval_close": interval_close}

    interval_td = yfcd.intervalToTimedelta[interval]
    c = GetTimestampCurrentInterval(exchange, ts, interval)
    if debug:
        if c is None:
            print("- currentInterval = None")
        else:
            print("- currentInterval = {} -> {}".format(c["interval_open"], c["interval_close"]))

    next_interval_close = None
    td = yfcd.intervalToTimedelta[interval]
    if (c is None) or not IsTimestampInActiveSession(exchange, c["interval_close"]):
        next_sesh = GetTimestampNextSession(exchange, ts)
        if debug:
            print("- next_sesh = {}".format(next_sesh))
        if exchange == "TLV":
            istr = yfcd.intervalToString[interval]
            if td > timedelta(minutes=30):
                align = '30m'
            else:
                align = istr
            d = next_sesh["market_open"].date()
            ti = GetCalendar(exchange).trading_index(d.isoformat(), (d+td_1d).isoformat(), period=istr, intervals=True, force_close=True, align=align)
            next_interval_start = ti.left[0]
        else:
            next_interval_start = next_sesh["market_open"]
    else:
        if exchange in yfcd.exchangesWithAuction:
            day_sched = GetExchangeSchedule(exchange, ts_d, ts_d+td_1d).iloc[0]
            if c["interval_close"] < day_sched["close"]:
                # Next is normal trading
                next_interval_start = c["interval_close"]
            else:
                # Next is auction
                if td <= timedelta(minutes=10):
                    next_interval_start = day_sched["auction"]
                else:
                    next_interval_start = day_sched["close"]
                next_interval_close = day_sched["auction"] + yfcd.exchangeAuctionDuration[exchange]
        else:
            next_interval_start = c["interval_close"]

    if next_interval_close is None:
        next_interval_close = next_interval_start + interval_td

    if debug:
        print("GetTimestampNextInterval() returning")
    return {"interval_open": next_interval_start, "interval_close": next_interval_close}


def CalcIntervalLastDataDt(exchange, intervalStart, interval, yf_lag=None):
    # When does Yahoo stop receiving data for this interval?
    TypeCheckStr(exchange, "exchange")
    TypeCheckIntervalDt(intervalStart, interval, "intervalStart", strict=False)
    TypeCheckInterval(interval, "interval")

    debug = False
    # debug = True

    if debug:
        print("CalcIntervalLastDataDt(intervalStart={}, interval={})".format(intervalStart, interval))

    if yf_lag is not None:
        TypeCheckTimedelta(yf_lag, "yf_lag")
    else:
        yf_lag = GetExchangeDataDelay(exchange)

    tz = ZoneInfo(GetExchangeTzName(exchange))

    irange = GetTimestampCurrentInterval(exchange, intervalStart, interval)
    if irange is None:
        raise Exception("Failed to map {} to interval".format(intervalStart))
    if debug:
        print("- irange:")
        pprint(irange)

    if isinstance(irange["interval_open"], datetime):
        intervalSched = GetExchangeSchedule(exchange, irange["interval_open"].astimezone(tz).date(), irange["interval_close"].astimezone(tz).date()+timedelta(days=1))
    else:
        intervalSched = GetExchangeSchedule(exchange, irange["interval_open"], irange["interval_close"])
    if debug:
        print("- intervalSched:")
        pprint(intervalSched)

    intervalEnd = irange["interval_close"]
    if isinstance(intervalEnd, datetime):
        intervalEnd_dt = intervalEnd
    else:
        intervalEnd_dt = datetime.combine(intervalEnd, time(0), ZoneInfo(GetExchangeTzName(exchange)))

    lastDataDt = min(intervalEnd_dt, intervalSched["close"][-1]) + yf_lag

    # For some exchanges, Yahoo has trades that occurred soon afer official market close, e.g. Johannesburg:
    if exchange in ["JNB"]:
        late_data_allowance = timedelta(minutes=15)
    else:
        late_data_allowance = timedelta(0)

    if (interval in [yfcd.Interval.Days1, yfcd.Interval.Week]) or (intervalEnd_dt == intervalSched["close"][-1]):
        # Is daily/weekly interval or last interval of day:
        lastDataDt += late_data_allowance

    if debug:
        print("CalcIntervalLastDataDt() returning {}".format(lastDataDt))

    return lastDataDt


def CalcIntervalLastDataDt_batch(exchange, intervalStart, interval, yf_lag=None):
    # When does Yahoo stop receiving data for this interval?
    TypeCheckStr(exchange, "exchange")
    if isinstance(intervalStart, list):
        intervalStart = np.array(intervalStart)
    TypeCheckNpArray(intervalStart, "intervalStart")
    TypeCheckIntervalDt(intervalStart[0], interval, "intervalStart", strict=False)
    TypeCheckInterval(interval, "interval")

    debug = False
    # debug = True

    if debug:
        print("CalcIntervalLastDataDt_batch(interval={}, yf_lag={})".format(interval, yf_lag))

    if yf_lag is not None:
        TypeCheckTimedelta(yf_lag, "yf_lag")
    else:
        yf_lag = GetExchangeDataDelay(exchange)

    n = len(intervalStart)
    tz = ZoneInfo(GetExchangeTzName(exchange))

    intervals = GetTimestampCurrentInterval_batch(exchange, intervalStart, interval)
    if isinstance(intervals["interval_open"].iloc[0], datetime):
        iopens = intervals["interval_open"]
        icloses = intervals["interval_close"]
    else:
        iopens = intervals["interval_open"].values
        icloses = intervals["interval_close"].values
    if debug:
        print("- intervals:")
        print(intervals)

    marketCloses = np.array([None]*n)
    iopen0 = iopens[0]
    iclosel = icloses[len(icloses)-1]
    if isinstance(iopen0, datetime):
        if isinstance(iopen0, pd.Timestamp):
            iopen0 = iopen0.to_pydatetime()
            iclosel = iclosel.to_pydatetime()
        sched = GetExchangeSchedule(exchange, iopen0.astimezone(tz).date(), iclosel.astimezone(tz).date()+timedelta(days=1))
    else:
        sched = GetExchangeSchedule(exchange, iopen0, iclosel+timedelta(days=1))
    sched = sched.copy()
    sched["day"] = sched["open"].dt.date
    is_dt = isinstance(iopens[0], datetime)
    if is_dt:
        iopen0 = iopens[0]
        if isinstance(iopen0, pd.Timestamp):
            iclose_days = [i.astimezone(tz).date() for i in icloses]
        else:
            iclose_days = [i.astimezone(tz).date() for i in icloses]
    else:
        iclose_days = [i-timedelta(days=1) for i in icloses]
    icloses_df = pd.DataFrame(data={"iopen": iopens, "iclose": icloses, "day": iclose_days})
    icloses_df2 = icloses_df.merge(sched[["day", "close"]], on="day", how="left")
    f_na = icloses_df2["close"].isna()
    if f_na.any():
        if interval in [yfcd.Interval.Week, yfcd.Interval.Months1, yfcd.Interval.Months3]:
            # Search back a little
            attempts = 4  # Worst-case = 9/11, have to search back from Friday to Monday to find actual open day
            while f_na.any() and attempts > 0:
                icloses_df2 = icloses_df2.drop("close", axis=1)
                icloses_df2.loc[f_na, "day"] -= timedelta(days=1)
                icloses_df2 = icloses_df2.merge(sched[["day", "close"]], on="day", how="left")
                attempts -= 1
                f_na = icloses_df2["close"].isna()
        if f_na.any():
            raise Exception("Lost data in merge")
    icloses_df = icloses_df2
    marketCloses = icloses_df["close"].dt.to_pydatetime()
    if debug:
        print("- icloses_df:")
        print(icloses_df)

    if (marketCloses == None).any():
        raise Exception("Failed to map some intervals to schedule")

    dc0 = icloses[0]
    if isinstance(dc0, datetime):
        intervalEnd_dt = [x.to_pydatetime().astimezone(tz) for x in icloses]
    else:
        intervalEnd_dt = [datetime.combine(dc, time(0), tz) for dc in icloses]

    lastDataDt = np.minimum(intervalEnd_dt, marketCloses) + yf_lag

    # For some exchanges, Yahoo has trades that occurred soon afer official market close, e.g. Johannesburg:
    if exchange in ["JNB"]:
        late_data_allowance = timedelta(minutes=15)
        if interval in [yfcd.Interval.Days1, yfcd.Interval.Week]:
            lastDataDt += late_data_allowance
        else:
            lastDataDt[intervalEnd_dt == marketCloses] += late_data_allowance

    if debug:
        print("CalcIntervalLastDataDt_batch() returning")

    return lastDataDt


def IsPriceDatapointExpired(intervalStart, fetch_dt, max_age, exchange, interval, triggerExpiryOnClose=True, yf_lag=None, dt_now=None):
    TypeCheckIntervalDt(intervalStart, interval, "intervalStart", strict=False)
    TypeCheckDatetime(fetch_dt, "fetch_dt")
    TypeCheckTimedelta(max_age, "max_age")
    TypeCheckStr(exchange, "exchange")
    TypeCheckInterval(interval, "interval")
    TypeCheckBool(triggerExpiryOnClose, "triggerExpiryOnClose")

    debug = False
    # debug = True

    if debug:
        print("") ; print("")
        print("IsPriceDatapointExpired(intervalStart={}, fetch_dt={}, max_age={}, triggerExpiryOnClose={}, dt_now={})".format(intervalStart, fetch_dt, max_age, triggerExpiryOnClose, dt_now))

    if dt_now is not None:
        TypeCheckDatetime(dt_now, "dt_now")
    else:
        dt_now = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))

    if yf_lag is not None:
        TypeCheckTimedelta(yf_lag, "yf_lag")
    else:
        yf_lag = GetExchangeDataDelay(exchange)
    if debug:
        print("yf_lag = {}".format(yf_lag))

    irange = GetTimestampCurrentInterval(exchange, intervalStart, interval)
    if debug:
        print("- irange = {}".format(irange))

    if irange is None:
        print("market open? = {}".format(IsTimestampInActiveSession(exchange, intervalStart)))
        raise Exception("Failed to map '{}'' to '{}' interval range".format(intervalStart, interval))

    intervalEnd = irange["interval_close"]
    if isinstance(intervalEnd, datetime):
        intervalEnd_d = intervalEnd.date()
    else:
        intervalEnd_d = intervalEnd
    if debug:
        print("- intervalEnd_d = {0}".format(intervalEnd_d))

    lastDataDt = CalcIntervalLastDataDt(exchange, intervalStart, interval, yf_lag)
    if debug:
        print("- lastDataDt = {}".format(lastDataDt))

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

    expire_dt = fetch_dt+max_age
    if debug:
        print("- expire_dt = {0}".format(expire_dt))
    if expire_dt < lastDataDt and expire_dt <= dt_now:
        if debug:
            print("- expire_dt < lastDataDt and expire_dt <= dt_now so return TRUE")
        return True

    if triggerExpiryOnClose:
        if debug:
            print("- checking if triggerExpiryOnClose ...")
            print("- - fetch_dt            = {}".format(fetch_dt))
            print("- - lastDataDt = {}".format(lastDataDt))
            print("- - dt_now              = {}".format(dt_now))
        if (fetch_dt < lastDataDt) and (lastDataDt <= dt_now):
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


def IdentifyMissingIntervals(exchange, start, end, interval, knownIntervalStarts, weeklyUseYahooDef=True):
    TypeCheckStr(exchange, "exchange")
    TypeCheckDateEasy(start, "start")
    TypeCheckDateEasy(end, "end")
    if start >= end:
        raise Exception("start={} must be < end={}".format(start, end))
    if knownIntervalStarts is not None:
        if not isinstance(knownIntervalStarts, list) and not isinstance(knownIntervalStarts, np.ndarray):
            raise Exception("'knownIntervalStarts' must be list or numpy array not {0}".format(type(knownIntervalStarts)))
        if len(knownIntervalStarts) > 0:
            if interval in [yfcd.Interval.Days1, yfcd.Interval.Week]:
                # Must be date
                TypeCheckDateStrict(knownIntervalStarts[0], "knownIntervalStarts")
            else:
                # Must be datetime
                TypeCheckDatetime(knownIntervalStarts[0], "knownIntervalStarts")
                if knownIntervalStarts[0].tzinfo is None:
                    raise Exception("'knownIntervalStarts' dates must be timezone-aware")

    debug = False
    # debug = True

    if debug:
        print("IdentifyMissingIntervals()")
        print("- start={}, end={}".format(start, end))
        print("- knownIntervalStarts:")
        pprint(knownIntervalStarts)

    intervals = GetExchangeScheduleIntervals(exchange, interval, start, end, weeklyUseYahooDef)
    if (intervals is None) or (intervals.shape[0] == 0):
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


def IdentifyMissingIntervalRanges(exchange, start, end, interval, knownIntervalStarts, weeklyUseYahooDef=True, minDistanceThreshold=5):
    TypeCheckStr(exchange, "exchange")
    TypeCheckIntervalDt(start, interval, "start", strict=True)
    TypeCheckIntervalDt(end, interval, "end", strict=True)
    if start >= end:
        raise Exception("start={} must be < end={}".format(start, end))
    if knownIntervalStarts is not None:
        if not isinstance(knownIntervalStarts, list) and not isinstance(knownIntervalStarts, np.ndarray):
            raise Exception("'knownIntervalStarts' must be list or numpy array not {0}".format(type(knownIntervalStarts)))
        if len(knownIntervalStarts) > 0:
            if interval in [yfcd.Interval.Days1, yfcd.Interval.Week]:
                # Must be date or datetime
                TypeCheckDateEasy(knownIntervalStarts[0], "knownIntervalStarts")
                if isinstance(knownIntervalStarts[0], datetime) and knownIntervalStarts[0].tzinfo is None:
                    raise Exception("'knownIntervalStarts' datetimes must be timezone-aware")
            else:
                # Must be datetime
                TypeCheckDatetime(knownIntervalStarts[0], "knownIntervalStarts")
                if knownIntervalStarts[0].tzinfo is None:
                    raise Exception("'knownIntervalStarts' dates must be timezone-aware")

    debug = False
    # debug = True

    if debug:
        print("IdentifyMissingIntervalRanges()")
        print("- start={}, end={}".format(start, end))
        print("- knownIntervalStarts:")
        pprint(knownIntervalStarts)

    intervals = GetExchangeScheduleIntervals(exchange, interval, start, end, weeklyUseYahooDef)
    if intervals is None or intervals.shape[0] == 0:
        raise yfcd.NoIntervalsInRangeException(interval, start, end)
    if debug:
        print("- intervals:")
        for i in intervals:
            print(i)

    intervals_missing_df = IdentifyMissingIntervals(exchange, start, end, interval, knownIntervalStarts, weeklyUseYahooDef)
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
    TypeCheckDateEasy(dt, "dt")
    TypeCheckPeriod(period, "period")

    if period == yfcd.Period.Ytd:
        if isinstance(dt, datetime):
            return datetime(dt.year, 1, 1, tzinfo=dt.tzinfo)
        else:
            return date(dt.year, 1, 1)

    if period == yfcd.Period.Days1:
        rd = relativedelta(days=1)
    elif period == yfcd.Period.Days5:
        rd = relativedelta(days=5)
    elif period == yfcd.Period.Week:
        rd = relativedelta(days=7)
    elif period == yfcd.Period.Months1:
        rd = relativedelta(months=1)
    elif period == yfcd.Period.Months3:
        rd = relativedelta(months=3)
    elif period == yfcd.Period.Months6:
        rd = relativedelta(months=6)
    elif period == yfcd.Period.Years1:
        rd = relativedelta(years=1)
    elif period == yfcd.Period.Years2:
        rd = relativedelta(years=2)
    elif period == yfcd.Period.Years5:
        rd = relativedelta(years=5)
    elif period == yfcd.Period.Years10:
        rd = relativedelta(years=10)
    else:
        raise Exception("Unknown period value '{}'".format(period))

    return dt - rd


def GetSystemTz():
    dt = datetime.utcnow().astimezone()

    # tz = dt.tzinfo
    tzn = dt.tzname()
    if tzn == "BST":
        # Confirmed that ZoneInfo figures out DST
        tzn = "GB"
    tz = ZoneInfo(tzn)
    return tz
