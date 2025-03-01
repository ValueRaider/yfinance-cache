from datetime import datetime, date, timedelta, time
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo
import re
from pprint import pprint
import numpy as np
import math
import pandas as pd

from . import yfc_dat as yfcd


class CustomNanCheckingDataFrame(pd.DataFrame):
    def __init__(self, *args, **kwargs):
        super(CustomNanCheckingDataFrame, self).__init__(*args, **kwargs)
        self.check_nans()

    def __setitem__(self, key, value):
        super(CustomNanCheckingDataFrame, self).__setitem__(key, value)
        self.check_nans()

    @classmethod
    def concat(cls, objs, *args, **kwargs):
        result = super(CustomNanCheckingDataFrame, cls).concat(objs, *args, **kwargs)
        result.check_nans()
        return result
    
    @classmethod
    def merge(cls, *args, **kwargs):
        result = super(CustomNanCheckingDataFrame, cls).merge(*args, **kwargs)
        result.check_nans()
        return result
    
    def check_nans(self):
        if 'Repaired?' not in self.columns:
            return
        if self['Repaired?'].isna().any():
            raise Exception("NaNs detected in column 'Repaired?'!")


def TypeCheckStr(var, varName):
    if not isinstance(var, str):
        raise TypeError(f"'{varName}' must be str not {type(var)}")
def TypeCheckBool(var, varName):
    if not isinstance(var, (bool, np.bool_)):
        raise TypeError(f"'{varName}' must be bool not {type(var)}")
def TypeCheckFloat(var, varName):
    if not isinstance(var, (float, np.float32, np.float64)):
        raise TypeError(f"'{varName}' must be float not {type(var)}")
def TypeCheckInt(var, varName):
    if isinstance(var, bool) or not isinstance(var, (int, np.int32, np.int64)):
        raise TypeError(f"'{varName}' must be int not {type(var)}")
def TypeCheckIterable(var, varName):
    if not isinstance(var, (list, set, np.ndarray, pd.Series)):
        raise TypeError(f"'{varName}' must be iterable not {type(var)}")

def TypeCheckDateEasy(var, varName):
    if not (isinstance(var, date) or isinstance(var, datetime)):
        raise TypeError(f"'{varName}' must be date not {type(var)}")
    if isinstance(var, datetime):
        if var.tzinfo is None:
            raise TypeError(f"'{varName}' if datetime must be timezone-aware".format(varName))
        elif not isinstance(var.tzinfo, ZoneInfo):
            raise TypeError(f"'{varName}' tzinfo must be ZoneInfo not {type(var.tzinfo)}")
def TypeCheckDateStrict(var, varName):
    if isinstance(var, pd.Timestamp):
        # While Pandas missing support for 'zoneinfo' must deny
        raise TypeError(f"'{varName}' must be date not {type(var)}")
    if not (isinstance(var, date) and not isinstance(var, datetime)):
        raise TypeError(f"'{varName}' must be date not {type(var)}")
def TypeCheckDatetime(var, varName):
    if not isinstance(var, datetime):
        raise TypeError(f"'{varName}' must be datetime not {type(var)}")
    if var.tzinfo is None:
        raise TypeError(f"'{varName}' if datetime must be timezone-aware".format(varName))
    elif not isinstance(var.tzinfo, ZoneInfo):
        raise TypeError(f"'{varName}' tzinfo must be ZoneInfo not {type(var.tzinfo)}")
def TypeCheckYear(var, varName):
    if not isinstance(var, int):
        raise Exception("'{}' must be int not {}".format(varName, type(var)))
    if var < 1900 or var > 2200:
        raise Exception("'{}' must be in range 1900-2200 not {}".format(varName, var))
def TypeCheckTimedelta(var, varName):
    if not isinstance(var, timedelta):
        raise TypeError(f"'{varName}' must be timedelta not {type(var)}")
def TypeCheckInterval(var, varName):
    if not isinstance(var, yfcd.Interval):
        raise TypeError(f"'{varName}' must be yfcd.Interval not {type(var)}")
def TypeCheckIntervalDt(var, interval, varName, strict=True):
    try:
        if interval in [yfcd.Interval.Days1, yfcd.Interval.Week]:
            if strict:
                TypeCheckDateStrict(var, varName)
            else:
                TypeCheckDateEasy(var, varName)
        else:
            if strict:
                TypeCheckDatetime(var, varName)
            else:
                TypeCheckDateEasy(var, varName)
    except Exception as e:
        raise TypeError(str(e) + " for interval "+yfcd.intervalToString[interval] + f' (value = {var})')

def TypeCheckPeriod(var, varName):
    if not isinstance(var, yfcd.Period) and not isinstance(var, (timedelta, pd.Timedelta, relativedelta)):
        raise TypeError(f"'{varName}' must be Timedelta or yfcd.Period not {type(var)}")

def TypeCheckNpArray(var, varName):
    if not isinstance(var, np.ndarray):
        raise TypeError(f"'{varName}' must be numpy array not {type(var)}")
def TypeCheckDataFrame(var, varName):
    if not isinstance(var, pd.DataFrame):
        raise TypeError(f"'{varName}' must be pd.DataFrame not {type(var)}")
def TypeCheckDatetimeIndex(var, varName):
    if not isinstance(var, pd.DatetimeIndex):
        raise TypeError(f"'{varName}' must be pd.DatetimeIndex not {type(var)}")


def JsonEncodeValue(value):
    if isinstance(value, date):
        return value.isoformat()
    elif isinstance(value, timedelta):
        e = "timedelta-{0}".format(value.total_seconds())
        return e
    raise TypeError()


def JsonDecodeDict(value):
    for k in value.keys():
        v = value[k]
        if isinstance(v, str) and v.startswith("timedelta-"):
            try:
                sfx = '-'.join(v.split('-')[1:])
                sfxf = float(sfx)
                value[k] = timedelta(seconds=sfxf)
            except Exception:
                pass
        else:
            # TODO: add suffix "date-" or "datetime-". Will need to upgrade existing cache
            decoded = False
            try:
                value[k] = date.fromisoformat(v)
                decoded = True
            except Exception:
                pass
            if not decoded:
                try:
                    value[k] = datetime.fromisoformat(v)
                    decoded = True
                except Exception:
                    pass

    return value


def GetSigFigs(n):
    if n == 0:
        return 0
    n_str = str(n).replace('.', '')
    m = re.match(r'0*[1-9](\d*[1-9])?', n_str)
    sf = len(m.group())

    return sf


def GetMagnitude(n):
    m = 0
    if n >= 1.0:
        while n >= 1.0:
            n *= 0.1
            m += 1
    else:
        while n < 1.0:
            n *= 10.0
            m -= 1

    return m


def CalculateRounding(n, sigfigs):
    if GetSigFigs(round(n)) >= sigfigs:
        return 0
    else:
        return sigfigs - GetSigFigs(round(n))


def ProcessUserDt(dt, tz_name):
    d = None
    tz = ZoneInfo(tz_name)
    if isinstance(dt, str):
        d = datetime.strptime(dt, "%Y-%m-%d").date()
        dt = datetime.combine(d, time(0), tz)
    elif isinstance(dt, date) and not isinstance(dt, datetime):
        d = dt
        dt = datetime.combine(dt, time(0), tz)
    elif not isinstance(dt, datetime):
        raise Exception("Argument 'dt' must be str, date or datetime")
    dt = dt.replace(tzinfo=tz) if dt.tzinfo is None else dt.astimezone(tz)

    if d is None and dt.time() == time(0):
        d = dt.date()

    return dt, d


def RDtoDO(rd):
    # Convert a relativedelta to Pandas.DateOffset
    return pd.DateOffset(years=rd.years,
                         months=rd.months,
                         days=rd.days,
                         hours=rd.hours,
                         minutes=rd.minutes,
                         seconds=rd.seconds)


def GetCSF0(df):
    if "Stock Splits" not in df:
        raise Exception("DataFrame does not contain column 'Stock Splits")
    if df.shape[0] == 0:
        raise Exception("DataFrame is empty")

    ss = df["Stock Splits"].copy()
    ss[ss == 0.0] = 1.0

    if "CSF" in df.columns:
        csf = df["CSF"]
    else:
        ss_rcp = 1.0/ss
        csf = ss_rcp.sort_index(ascending=False).cumprod().sort_index(ascending=True).shift(-1, fill_value=1.0)
    csf0 = csf.iloc[0]

    ss0 = ss.iloc[0]
    if ss0 != 1.0:
        csf0 *= 1.0/ss0

    return csf0


def GetCDF0(df, close_day_before=None):
    if "CDF" not in df:
        raise Exception("DataFrame does not contain column 'CDF")
    if df.shape[0] == 0:
        raise Exception("DataFrame is empty")

    df = df.sort_index(ascending=True)

    cdf = df["CDF"].iloc[0]
    if cdf != 1.0:
        # Yahoo's dividend adjustment has tiny variation (~1e-6),
        # so use mean to minimise accuracy loss of adjusted->deadjust->adjust
        i = np.argmax(df["Dividends"] != 0.0)
        cdf_mean = df["CDF"].iloc[0:i].mean()
        if abs(cdf_mean-cdf)/cdf > 0.0001:
            raise Exception("Mean CDF={} is sig. different to CDF[0]={}".format(cdf_mean, cdf))
        cdf = cdf_mean

    div0 = df["Dividends"].iloc[0]
    if div0 != 0.0:
        if close_day_before is None:
            raise Exception("Dividend in most recent row so need to know yesterday's close")
        cdf *= (close_day_before-div0)/close_day_before

    return cdf



def ChunkDatesIntoYfFetches(schedule, maxDays, overlapDays):
    TypeCheckDataFrame(schedule, "schedule")
    TypeCheckInt(maxDays, "maxDays")
    TypeCheckInt(overlapDays, "overlapDays")

    debug = False
    # debug = True

    if debug:
        print("ChunkDatesIntoYfFetches()")
        print("- schedule:")
        print(schedule)
        print(schedule["close"].iloc[0].tz)
        print("- maxDays =", maxDays)
        print("- overlap =", overlapDays)

    s = schedule.copy()
    n = s.shape[0]
    step = np.full(n, pd.Timedelta(days=1))
    step[1:] = (s.index.date[1:] - s.index.date[:-1])
    s["step"] = step ; s["step"] = s["step"].dt.days

    # groupStarts = [s.index[0]]
    groupStarts = [0]
    groupEnds = []
    grpSize = s["step"].iloc[0]
    # TODO: probably need to compile this loop
    i = 1
    ctr = 0
    while i < s.shape[0]:
        ctr += 1
        if ctr > 1000:
            raise Exception("infinite loop detected")

        size = s["step"].iloc[i]
        if grpSize + size <= maxDays:
            # Add to group
            grpSize += size
        else:
            # Close current group
            # groupEnds.append(s.index[i])
            groupEnds.append(i)
            # Start new group, 2 indices back
            i -= 2
            # nextStart = s.index[i]
            # groupStarts.append(nextStart)
            groupStarts.append(i)
            grpSize = s["step"].iloc[i]
        i += 1

    tz = schedule["close"].iloc[0].tz

    # groupEnds.append(schedule.index[-1] + pd.Timedelta(days=1))
    # groups = [[groupStarts[i], groupEnds[i]] for i in range(len(groupStarts))]
    # groups = [[groupStarts[i].tz_localize(tz), groupEnds[i].tz_localize(tz)] for i in range(len(groupStarts))]
    # return groups

    if debug:
        # print("- groups:")
        # pprint([ (groupStarts[i], groupEnds[i]) for i in range(len(groupStarts))])
        print("- groupStarts")
        pprint(groupStarts)
        print("- groupEnds")
        pprint(groupEnds)

    groups = []
    td_1d = pd.Timedelta(days=1)
    for i in range(len(groupStarts)):
        g = {}
        g["fetch start"] = s.index[groupStarts[i]].tz_localize(tz)
        g["core start"] = s.index[groupStarts[i]+1].tz_localize(tz)
        if i == len(groupStarts)-1:
            g["core end"] = s.index[-1].tz_localize(tz)
            g["core end"] = max(g["core end"], g["core start"]+td_1d)
            g["fetch end"] = g["core end"] + td_1d
        else:
            g["core end"] = s.index[groupEnds[i]-1].tz_localize(tz)
            g["fetch end"] = s.index[groupEnds[i]].tz_localize(tz)
        groups.append(g)

    return groups


def VerifyPricesDf(h, df_yf, interval, rtol=0.0001, vol_rtol=0.005, exit_first_error=False, quiet=False, debug=False):
    if df_yf.empty:
        raise Exception("VerifyPricesDf() has been given empty df_yf")

    f_diff_all = pd.Series(np.full(h.shape[0], False), h.index)
    errors_str = ''

    interday = interval in [yfcd.Interval.Days1, yfcd.Interval.Week]#, yfcd.Interval.Months1, yfcd.Interval.Months3]
    istr = yfcd.intervalToString[interval]


    # Test: no NaNs in dividends & stock splits
    f_na = h["Dividends"].isna()
    if f_na.any():
        if not quiet:
            print(f"WARNING: {np.sum(f_na)}/{h.shape[0]} NaNs detected in dividends")
        f_diff_all = f_diff_all | f_na
        errors_str = 'Dividends'
    f_na = h["Stock Splits"].isna()
    if f_na.any():
        if not quiet:
            print(f"WARNING: {np.sum(f_na)}/{h.shape[0]} NaNs detected in stock splits")
        f_diff_all = f_diff_all | f_na
        errors_str += ';Splits'

    # Drop NaNs from YF data:
    df_yf = df_yf[~df_yf[yfcd.yf_price_data_cols].isna().any(axis=1)]

    # Drop mismatching indices for value check
    h = h[h.index.isin(df_yf.index)].copy()
    h = h[h['Final?'].to_numpy()]
    df_yf = df_yf[df_yf.index.isin(h.index)]
    n = h.shape[0]

    # Apply dividend-adjustment
    h_adj = h.copy()
    for c in ["Open", "Close", "Low", "High"]:
        h_adj["Adj " + c] = h_adj[c].to_numpy() * h_adj["CDF"].to_numpy()
        h_adj = h_adj.drop(c, axis=1)
    df_yf_adj = df_yf.copy()
    if interval == yfcd.Interval.Week:
        for c in ['Open', 'High', 'Low', 'Close']:
            df_yf_adj = df_yf_adj.drop(c, axis=1)
    else:
        adj_f = df_yf["Adj Close"].to_numpy() / df_yf["Close"].to_numpy()
        df_yf_adj = df_yf_adj.drop("Close", axis=1)
        for c in ["Open", "Low", "High"]:
            df_yf_adj["Adj " + c] = df_yf_adj[c].to_numpy() * adj_f
            df_yf_adj = df_yf_adj.drop(c, axis=1)

    # Verify dividends
    # - first compare dates
    c = "Dividends"
    h_divs = h.loc[h[c] != 0.0, [c, "FetchDate"]].copy().dropna()
    yf_divs = df_yf.loc[df_yf[c] != 0.0, c]
    dts_missing_from_cache = yf_divs.index[~yf_divs.index.isin(h_divs.index)]
    dts_missing_from_yf = h_divs.index[~h_divs.index.isin(yf_divs.index)]
    divs_bad = False
    if len(dts_missing_from_cache) > 0:
        if not quiet:
            print(f"WARNING: Dividends missing from cached {istr}: {dts_missing_from_cache.date.astype(str)}")
        for dt in dts_missing_from_cache:
            f_diff_all.loc[dt] = True
        if 'Dividends' not in errors_str:
            errors_str += ';Dividends'
        if exit_first_error:
            f_diff_all = f_diff_all.rename(errors_str)
            return f_diff_all
    if len(dts_missing_from_yf) > 0:
        if not quiet:
            print(f"WARNING: Cached {istr} contains dividends missing from Yahoo: {dts_missing_from_yf.date.astype(str)}")
        for dt in dts_missing_from_yf:
            f_diff_all.loc[dt] = True
        if 'Dividends' not in errors_str:
            errors_str += ';Dividends'
        if exit_first_error:
            f_diff_all = f_diff_all.rename(errors_str)
            return f_diff_all
    # - now compare values
    h_divs = h_divs[h_divs.index.isin(yf_divs.index)]
    yf_divs = yf_divs[yf_divs.index.isin(h_divs.index)]
    if not yf_divs.empty:
        f_close = np.isclose(h_divs[c].to_numpy(), yf_divs.to_numpy(), rtol=rtol)
        f_close = pd.Series(f_close, h_divs.index)
        f_diff = ~f_close
        if f_diff.any():
            n_diff = np.sum(f_diff)
            if not quiet:
                print(f"WARNING: {istr}: {n_diff}/{n} differences in column {c}")
            if not quiet:
                df_diffs = h_divs[f_diff].join(yf_divs[f_diff], lsuffix="_cache", rsuffix="_yf")
                df_diffs.index = df_diffs.index.tz_convert(h_divs.index.tz)
                df_diffs = df_diffs.join(h['Close'].rename('Close_yfc'))
                df_diffs = df_diffs.join(df_yf['Close'].rename('Close_yf'))
                if interday:
                    df_diffs.index = df_diffs.index.tz_convert(df_yf.index.tz).date
                df_diffs["error"] = df_diffs[c+"_cache"] - df_diffs[c+"_yf"]
                df_diffs["error %"] = (df_diffs["error"]*100 / df_diffs[c+"_yf"]).round(1).astype(str) + '%'
                print(df_diffs)
            f_diff_all = f_diff_all | f_diff
            if 'Dividends' not in errors_str:
                errors_str += ';Dividends'
            if exit_first_error:
                f_diff_all = f_diff_all.rename(errors_str)
                return f_diff_all

    # Verify stock splits
    # - first compare dates
    c = "Stock Splits"
    h_ss = h.loc[h[c] != 0.0, [c, "FetchDate"]].copy().dropna()
    yf_ss = df_yf.loc[df_yf[c] != 0.0, c]
    dts_missing_from_cache = yf_ss.index[~yf_ss.index.isin(h_ss.index)]
    dts_missing_from_yf = h_ss.index[~h_ss.index.isin(yf_ss.index)]
    splits_bad = False
    if len(dts_missing_from_cache) > 0:
        if not quiet:
            print(f"WARNING: Splits missing from cached {istr}: {dts_missing_from_cache.date.astype(str)}")
        for dt in dts_missing_from_cache:
            f_diff_all.loc[dt] = True
    if len(dts_missing_from_yf) > 0:
        if not quiet:
            print(f"WARNING: Cached {istr} contains splits missing from Yahoo: {dts_missing_from_yf.date.astype(str)}")
        for dt in dts_missing_from_yf:
            f_diff_all.loc[dt] = True
        if 'Stock Splits' not in errors_str:
            errors_str += ';Stock Splits'
        if exit_first_error:
            f_diff_all = f_diff_all.rename(errors_str)
            return f_diff_all
    # - now compare values
    h_ss = h_ss[h_ss.index.isin(yf_ss.index)]
    yf_ss = yf_ss[yf_ss.index.isin(h_ss.index)]
    if not yf_ss.empty:
        f_close = pd.Series(np.isclose(h_ss[c].to_numpy(), yf_ss.to_numpy(), rtol=rtol), yf_ss.index)
        f_diff = ~f_close
        if f_diff.any():
            n_diff = np.sum(f_diff)
            if not quiet:
                print(f"WARNING: {istr}: {n_diff}/{n} differences in column {c}")
            df_diffs = h_ss.join(yf_ss[f_diff], lsuffix="_cache", rsuffix="_yf")
            df_diffs.index = df_diffs.index.tz_convert(h_ss.index.tz)
            if interday:
                df_diffs.index = df_diffs.index.tz_convert(df_yf.index.tz).date
            df_diffs["error"] = df_diffs[c+"_cache"] - df_diffs[c+"_yf"]
            df_diffs["error %"] = (df_diffs["error"]*100 / df_diffs[c+"_yf"]).round(2).astype(str) + '%'
            if not quiet:
                print(df_diffs)
            f_diff_all = f_diff_all | f_diff
            if 'Splits' not in errors_str:
                errors_str += ';Splits'
            splits_bad = True

    # Verify capital gains
    # - first compare dates
    c = "Capital Gains"
    if c in df_yf.columns:
        yf_cg = df_yf.loc[df_yf[c] != 0.0, c]
        if not yf_cg.empty:
            if c not in h.columns:
                for dt in yf_cg.index:
                    f_diff_all.loc[dt] = True
                if not quiet:
                    print(f"ERROR: Cached {istr} missing column 'Capital Gains")
                if 'Capital Gains' not in errors_str:
                    errors_str += ';Capital Gains'
                if exit_first_error:
                    f_diff_all = f_diff_all.rename(errors_str)
                    return f_diff_all
            else:
                h_cg = h.loc[h[c] != 0.0, [c, "FetchDate"]].copy().dropna()
                dts_missing_from_cache = yf_cg.index[~yf_cg.index.isin(h_cg.index)]
                dts_missing_from_yf = h_cg.index[~h_cg.index.isin(yf_cg.index)]
                if len(dts_missing_from_cache) > 0:
                    if not quiet:
                        print("WARNING: Capital gains missing from cached {istr}:")
                        print("- ", dts_missing_from_cache.date())
                    for dt in dts_missing_from_cache:
                        f_diff_all.loc[dt] = True
                if len(dts_missing_from_yf) > 0 and not quiet:
                    print("ERROR: Cached {istr} contains capital gains missing from Yahoo:")
                    print([d.date() for d in dts_missing_from_yf])
                    for dt in dts_missing_from_yf:
                        f_diff_all.loc[dt] = True
                    if 'Capital Gains' not in errors_str:
                        errors_str += ';Capital Gains'
                    if exit_first_error:
                        f_diff_all = f_diff_all.rename(errors_str)
                        return f_diff_all
                # - now compare values
                h_cg = h_cg[h_cg.index.isin(yf_cg.index)]
                yf_cg = yf_cg[yf_cg.index.isin(h_cg.index)]
                if not yf_cg.empty:
                    f_close = pd.Series(np.isclose(h_cg[c].to_numpy(), yf_cg.to_numpy(), rtol=rtol), yf_cg.index)
                    f_diff = ~f_close
                    if f_diff.any():
                        n_diff = np.sum(f_diff)
                        if not quiet:
                            print(f"WARNING: {istr}: {n_diff}/{n} differences in column {c}")
                        df_diffs = h_cg.join(yf_cg[f_diff], lsuffix="_cache", rsuffix="_yf")
                        df_diffs.index = df_diffs.index.tz_convert(h_cg.index.tz)
                        if interday:
                            df_diffs.index = df_diffs.index.tz_convert(df_yf.index.tz).date
                        df_diffs["error"] = df_diffs[c+"_cache"] - df_diffs[c+"_yf"]
                        df_diffs["error %"] = (df_diffs["error"]*100 / df_diffs[c+"_yf"]).round(2).astype(str) + '%'
                        if not quiet:
                            print(df_diffs)
                        f_diff_all = f_diff_all | f_diff
                        if 'Capital Gains' not in errors_str:
                            errors_str += ';Capital Gains'

    def _print_sig_diffs(df, df_yf, column, rtol):
        c = column
        f_close = np.isclose(df[c].to_numpy(), df_yf[c].to_numpy(), rtol=rtol)
        f_diff = ~f_close
        if c == 'Volume':
            f_yfZeroVol = df_yf[c].to_numpy() == 0
            if f_yfZeroVol.any():
                # Ignore differences where YF volume = 0, because what has happened
                # is cached data contains repair but now too old for YF to repair
                f_diff[f_yfZeroVol] = False

        if not f_diff.any():
            return False
        else:
            cols = ["FetchDate"]
            if "Adj" in column:
                cols.append("LastDivAdjustDt")
            else:
                cols.append("LastSplitAdjustDt")
            cols.append("Repaired?")
            cols.append(c)
            # yahoo_cols = [c]
            yahoo_cols = [c, "Repaired?"]
            df_diffs = df.loc[f_diff, cols].join(df_yf.loc[f_diff, yahoo_cols], lsuffix="_cache", rsuffix="_yf")
            df_diffs.index = df_diffs.index.tz_convert(df.index[0].tz)

            df_diffs["error"] = df_diffs[c+"_cache"] - df_diffs[c+"_yf"]
            df_diffs["error %"] = (df_diffs["error"]*100 / df_diffs[c+"_yf"]).round(2).astype(str) + '%'

            # Combine the 'Repaired?' columns
            df_diffs["Repaired?"] = "cache="
            f = df_diffs["Repaired?_cache"].to_numpy()
            df_diffs.loc[f,"Repaired?"] += 'Y'
            df_diffs.loc[~f,"Repaired?"] += 'N'
            df_diffs["Repaired?"] += ' yf='
            f = df_diffs["Repaired?_yf"].to_numpy()
            df_diffs.loc[f,"Repaired?"] += 'Y'
            df_diffs.loc[~f,"Repaired?"] += 'N'
            df_diffs = df_diffs.drop(["Repaired?_cache", "Repaired?_yf"], axis=1)

            df_diffs["FetchDate"] = df_diffs["FetchDate"].dt.tz_convert(df.index.tz)
            df_diffs["FetchDate"] = df_diffs["FetchDate"].dt.strftime("%Y-%m-%d %H:%M:%S%z")
            if "LastDivAdjustDt" in df_diffs.columns:
                df_diffs["LastDivAdjustDt"] = df_diffs["LastDivAdjustDt"].dt.tz_convert(df.index.tz)
                df_diffs["LastDivAdjustDt"] = df_diffs["LastDivAdjustDt"].dt.strftime("%Y-%m-%d %H:%M:%S")
            if "LastSplitAdjustDt" in df_diffs.columns:
                df_diffs["LastSplitAdjustDt"] = df_diffs["LastSplitAdjustDt"].dt.tz_convert(df.index.tz)
                df_diffs["LastSplitAdjustDt"] = df_diffs["LastSplitAdjustDt"].dt.strftime("%Y-%m-%d %H:%M:%S")
            if interday:
                df_diffs.index = df_diffs.index.date
            f_diff_n = sum(f_diff)
            msg = f"WARNING: {istr}: {f_diff_n}/{n} sig. diffs in column {c} with rtol={rtol}"
            print(msg)
            print(df_diffs)
            return True

    # Verify volumes match
    c = "Volume"
    f_close = np.isclose(h[c].to_numpy(), df_yf[c].to_numpy(), vol_rtol)
    f_close = pd.Series(f_close, h.index)
    f_diff_vol = ~f_close
    if f_diff_vol.any():
        # Use looser tolerance if different 'Repaired?' states
        f_repair_mismatch = np.logical_or(h["Repaired?"].to_numpy(), df_yf["Repaired?"].to_numpy())
        if f_repair_mismatch.any():
            loose_tol = 1.0
            f_diff_vol[f_repair_mismatch] = ~np.isclose(h[c].to_numpy()[f_repair_mismatch], df_yf[c].to_numpy()[f_repair_mismatch], rtol=loose_tol)
    f_yfZeroVol = df_yf[c].to_numpy() == 0
    if f_yfZeroVol.any():
        # Ignore differences where YF volume = 0, because what has happened
        # is cached data contains repair but now too old for YF to repair
        f_diff_vol[f_yfZeroVol] = False
        if debug:
            msg = f"ignoring {np.sum(f_yfZeroVol)} diffs where YF volume = 0"
            print("- " + msg)
    if f_diff_vol.any():
        if debug:
            _print_sig_diffs(h, df_yf, "Volume", vol_rtol)
        elif not quiet:
            msg = f"WARNING: {istr}: {np.sum(f_diff_vol)}/{n} differences in 'Volume'"
            # If very few date(times), append to string
            if not interday and np.sum(f_diff_vol) == 1:
                msg += f" @ {h.index[f_diff_vol]}"
            elif interday and np.sum(f_diff_vol) < 2:
                msg += f" @ {h.index.date[f_diff_vol]}"
            print(msg)
        f_diff_all = f_diff_all | f_diff_vol
        if 'Volume' not in errors_str:
            errors_str += ';Volume'

    f_diff_prices = pd.Series(np.full(h.shape[0], False), h.index)
    for c in ["Open", "Close", "High", "Low"]:
        f_close = np.isclose(h[c].to_numpy(), df_yf[c].to_numpy(), rtol=rtol)
        f_close = pd.Series(f_close, h.index)
        f_diff_c = ~f_close
        if f_diff_c.any():
            # Use looser tolerance if different 'Repaired?' states
            f_repair_mismatch = np.logical_xor(h["Repaired?"].to_numpy(), df_yf["Repaired?"].to_numpy())
            if f_repair_mismatch.any():
                loose_tol = 0.1
                f_diff_c[f_repair_mismatch] = ~np.isclose(h[c].to_numpy()[f_repair_mismatch], df_yf[c].to_numpy()[f_repair_mismatch], rtol=loose_tol)
        if f_diff_c.any():
            if debug:
                _print_sig_diffs(h, df_yf, c, rtol)
            elif not quiet:
                msg = f"WARNING: {istr}: {np.sum(f_diff_c)}/{n} differences in '{c}'"
                # If very few date(times), append to string
                if not interday and np.sum(f_diff_c) == 1:
                    msg += f" @ {h.index[f_diff_c]}"
                elif interday and np.sum(f_diff_c) < 2:
                    msg += f" @ {h.index.date[f_diff_c]}"
                print(msg)
            f_diff_prices = f_diff_prices | f_diff_c
    prices_bad = f_diff_prices.any()
    if prices_bad:
        f_diff_all = f_diff_all | f_diff_prices
        errors_str += ';Prices'

    if not divs_bad and not splits_bad and not prices_bad:
        f_diff_divs = pd.Series(np.full(h.shape[0], False), h.index)
        if interday:
            # Yahoo div-adjusts interday data, so check my div adjustment
            # Use looser tolerance if different 'Repaired?' states
            f_repair_mismatch = np.logical_xor(h_adj["Repaired?"].to_numpy(), df_yf_adj["Repaired?"].to_numpy())
            for c in ["Open", "Close", "High", "Low"]:
                c = "Adj "+c
                f_close = np.isclose(h_adj[c].to_numpy(), df_yf_adj[c].to_numpy(), rtol=rtol)
                f_close = pd.Series(f_close, h.index)
                if f_repair_mismatch.any():
                    loose_tol = 0.1
                    f_close2 = np.isclose(h_adj[c].to_numpy()[f_repair_mismatch], df_yf_adj[c].to_numpy()[f_repair_mismatch], rtol=loose_tol)
                    f_close[f_repair_mismatch] = f_close2
                f_diff_c = (~f_close) & (~f_diff_all)
                f_diff_divs = f_diff_divs | f_diff_c
        f_diff_divs = f_diff_divs & ~f_diff_all
        if f_diff_divs.any():
            if debug:
                print("Bad div-adjustments detected:")
                if not f_diff_all.any():
                    print("- no other differences")
                for c in ['Close', 'Open', 'High', 'Low']:
                    if _print_sig_diffs(h_adj, df_yf_adj, "Adj "+c, rtol):
                        break
            elif not quiet:
                print(f"{np.sum(f_diff_divs)}/{h.shape[0]} div-adjustment errors")

        if f_diff_divs.any():
            only_div_errors = not f_diff_all.any()
            if only_div_errors and interval == yfcd.Interval.Week:
                # ignore the div diffs IFF they are limited to ex-div intervals, 
                # and intervals are multiday. This is because yfinance now handles
                # them correctly, but YFC doesn't.
                subset = True
                div_dts = df_yf_adj.index[df_yf_adj['Dividends']!=0]
                for dt in f_diff_divs.index[f_diff_divs]:
                    if dt not in div_dts:
                        subset = False
                        break
                if subset:
                    f_diff_divs = None
            if f_diff_divs is not None:
                f_diff_all = f_diff_all | f_diff_divs
                if 'Dividends' not in errors_str:
                    errors_str += ';Dividends'

    f_diff_all = f_diff_all.rename(errors_str)
    return f_diff_all


def np_isin_optimised(a, b, invert=False):
    if not isinstance(a, np.ndarray):
        a = np.array(a)
    if not isinstance(b, np.ndarray):
        b = np.array(b)
    if a.dtype.hasobject or b.dtype.hasobject:
        # Apparently not optimised in numpy, faster to DIY
        # https://github.com/numpy/numpy/issues/14997#issuecomment-560516888
        b_set = set(b)
        x = np.array([elem in b_set for elem in a])
        if invert:
            x = ~x
    else:
        if invert:
            x = np.isin(a, b, invert=True)
        else:
            x = np.isin(a, b)
    return x


def np_weighted_mean_and_std(values, weights):
    # print("values:")
    # print(values)
    # print("weights:")
    # print(weights)
    # mean = np.mean(values)
    mean = np.average(values, weights=weights)
    # print(f"mean = {mean}")
    dev = (values - mean)**2
    # print("dev:")
    # print(dev)
    std2 = np.mean(dev)
    # print(f"std2 = {std2}")
    std2 = np.average(dev, weights=weights)
    # print(f"std2 = {std2}")

    std = math.sqrt(std2)
    # print(f"std = {std}")

    # std_pct = std / mean
    # print(f"std_pct = {std_pct}")

    # return std_pct
    return mean, std

def display_progress_bar(completed, total):
    """Function to display progress bar with percentage completion."""
    # percentage = (completed / total) * 100
    bar_length = 48  # Number of characters in the progress bar
    completed_length = int(bar_length * completed // total)
    bar = "*" * completed_length + " " * (bar_length - completed_length)
    # print(f"\rProgress: |{bar}| {percentage:.0f}% Completed", end='', flush=True)
    print(f"\r[{bar}]  {completed} of {total} completed", end='', flush=True)
