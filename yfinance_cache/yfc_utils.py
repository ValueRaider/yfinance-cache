from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
import re
from pprint import pprint
import numpy as np
import math
import pandas as pd

from . import yfc_dat as yfcd


def TypeCheckStr(var, varName):
    if not isinstance(var, str):
        raise TypeError(f"'{varName}' must be str not {type(var)}")
def TypeCheckBool(var, varName):
    if not isinstance(var, bool):
        raise TypeError(f"'{varName}' must be bool not {type(var)}")
def TypeCheckFloat(var, varName):
    if not isinstance(var, (float, np.float32, np.float64)):
        raise TypeError(f"'{varName}' must be float not {type(var)}")
def TypeCheckInt(var, varName):
    if not isinstance(var, (int, np.int32, np.int64)):
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
            raise TypeError(f"'{varName}' tzinfo must be ZoneInfo".format(varName))
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
        raise TypeError(f"'{varName}' tzinfo must be ZoneInfo".format(varName))
def TypeCheckYear(var, varName):
    if not isinstance(var, int):
        raise Exception("'{}' must be int not {}".format(varName, type(var)))
    if var < 1900 or var > 2200:
        raise Exception("'{}' must be in range 1900-2200 not {}".format(varName, type(var)))
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
        raise TypeError(str(e) + " for interval "+yfcd.intervalToString[interval])

def TypeCheckPeriod(var, varName):
    if not isinstance(var, yfcd.Period):
        raise TypeError(f"'{varName}' must be yfcd.Period not {type(var)}")

def TypeCheckNpArray(var, varName):
    if not isinstance(var, np.ndarray):
        raise TypeError(f"'{varName}' must be numpy array not {type(var)}")
def TypeCheckDataFrame(var, varName):
    if not isinstance(var, pd.DataFrame):
        raise TypeError(f"'{varName}' must be pd.DataFrame not {type(var)}")
def TypeCheckDatetimeIndex(var, varName):
    if not isinstance(var, pd.DatetimeIndex):
        raise TypeError(f"'{varName}' must be pd.DatetimeIndex not {type(var)}")


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
    csf0 = csf[0]

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

    cdf = df["CDF"][0]
    if cdf != 1.0:
        # Yahoo's dividend adjustment has tiny variation (~1e-6),
        # so use mean to minimise accuracy loss of adjusted->deadjust->adjust
        i = np.argmax(df["Dividends"] != 0.0)
        cdf_mean = df["CDF"].iloc[0:i].mean()
        if abs(cdf_mean-cdf)/cdf > 0.0001:
            raise Exception("Mean CDF={} is sig. different to CDF[0]={}".format(cdf_mean, cdf))
        cdf = cdf_mean

    div0 = df["Dividends"][0]
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


def VerifyPricesDf(h, df_yf, interval, rtol=0.0001, vol_rtol=0.003, quiet=False, debug=False):
    if df_yf.empty:
        raise Exception("VerifyPricesDf() has been given empty df_yf")

    f_diff_all = pd.Series(np.full(h.shape[0], False), h.index)

    interday = interval in [yfcd.Interval.Days1, yfcd.Interval.Week, yfcd.Interval.Months1, yfcd.Interval.Months3]


    # Test: no NaNs in dividends & stock splits
    f_na = h[["Dividends", "Stock Splits"]].isna().any(axis=1)
    if f_na.any():
        if not quiet:
            msg = "WARNING: NaNs detected in dividends & stock splits"
            print(msg)
        print(f"{np.sum(f_na)}/{h.shape[0]} NaNs detected in dividends & stock splits")
        f_diff_all = f_diff_all | f_na

    # Test: index should match
    f_missing = ~df_yf.index.isin(h.index)
    if f_missing.any():
        dts_missing_from_cache = df_yf.index[f_missing]
    else:
        dts_missing_from_cache = []
    #
    f_orphan = pd.Series(~h.index.isin(df_yf.index), index=h.index)
    if f_orphan.any():
        dts_missing_from_yf = h.index[f_orphan]
        n_missing = len(dts_missing_from_yf)
        n_missing_pct = n_missing/h.shape[0]
        if not quiet:
            if not interday and not interval == yfcd.Interval.Mins1 and n_missing_pct < 0.005:
                msg = "WARNING: Cache contains intervals not returned by Yahoo, may be result of historic repair rather than error:"
            else:
                msg = "WARNING: These cached intervals not returned by Yahoo:"
            print(msg)
            print("-", dts_missing_from_yf)
    else:
        dts_missing_from_yf = []

    # Drop NaNs from YF data:
    df_yf = df_yf[~df_yf[yfcd.yf_price_data_cols].isna().any(axis=1)]

    # Drop mismatching indices for value check
    h = h[h.index.isin(df_yf.index)]
    df_yf = df_yf[df_yf.index.isin(h.index)]
    if h.shape[0] != df_yf.shape[0]:
        print("h:") ; print(h)
        print("df_yf:") ; print(df_yf)
        missing_from_h = df_yf.index[~df_yf.index.isin(h.index)]
        print("missing_from_h:", missing_from_h)
        missing_from_yf = h.index[~h.index.isin(df_yf.index)]
        print("missing_from_yf:", missing_from_yf)
        print(f"- h: {h.index[0]} -> {h.index[-1]}")
        print(f"- df_yf: {df_yf.index[0]} -> {df_yf.index[-1]}")
        raise Exception("Different #rows")
    n = h.shape[0]

    # Apply dividend-adjustment
    h_adj = h.copy()
    for c in ["Open", "Close", "Low", "High"]:
        h_adj["Adj " + c] = h_adj[c].to_numpy() * h_adj["CDF"].to_numpy()
        h_adj = h_adj.drop(c, axis=1)
    df_yf_adj = df_yf.copy()
    adj_f = df_yf["Adj Close"].to_numpy() / df_yf["Close"].to_numpy()
    df_yf_adj = df_yf_adj.drop("Close", axis=1)
    for c in ["Open", "Low", "High"]:
        df_yf_adj["Adj " + c] = df_yf_adj[c].to_numpy() * adj_f
        df_yf_adj = df_yf_adj.drop(c, axis=1)

    # Verify dividends
    # - first compare dates
    c = "Dividends"
    h_divs = h.loc[h[c] != 0.0, c].copy().dropna()
    yf_divs = df_yf.loc[df_yf[c] != 0.0, c]
    dts_missing_from_cache = yf_divs.index[~yf_divs.index.isin(h_divs.index)]
    dts_missing_from_yf = h_divs.index[~h_divs.index.isin(yf_divs.index)]
    divs_bad = False
    if len(dts_missing_from_cache) > 0:
        if not quiet:
            print("WARNING: Dividends missing from cache:")
            print("- ", dts_missing_from_cache)
        for dt in dts_missing_from_cache:
            f_diff_all.loc[dt] = True
        divs_bad = True
    if len(dts_missing_from_yf) > 0 and not quiet:
        print("ERROR: Cache contains dividends missing from Yahoo:")
        print(dts_missing_from_yf)
    # - now compare values
    h_divs = h_divs[h_divs.index.isin(yf_divs.index)]
    yf_divs = yf_divs[yf_divs.index.isin(h_divs.index)]
    f_close = np.isclose(h_divs.to_numpy(), yf_divs.to_numpy(), rtol=rtol)
    f_close = pd.Series(f_close, h_divs.index)
    f_diff = ~f_close
    if f_diff.any():
        n_diff = np.sum(f_diff)
        print(f"{n_diff}/{n} differences in column {c}")
        df_diffs = pd.DataFrame(h_divs[f_diff]).join(yf_divs[f_diff], lsuffix="_cache", rsuffix="_Yahoo")
        df_diffs["error"] = df_diffs[c+"_cache"] - df_diffs[c+"_Yahoo"]
        df_diffs["error %"] = (df_diffs["error"]*100 / df_diffs[c+"_Yahoo"]).round(1).astype(str) + '%'
        f_diff_all = f_diff_all | f_diff
        divs_bad = True

    # Verify stock splits
    # - first compare dates
    c = "Stock Splits"
    h_ss = h.loc[h[c] != 0.0, c].copy().dropna()
    yf_ss = df_yf.loc[df_yf[c] != 0.0, c]
    dts_missing_from_cache = yf_ss.index[~yf_ss.index.isin(h_ss.index)]
    dts_missing_from_yf = h_ss.index[~h_ss.index.isin(yf_ss.index)]
    if len(dts_missing_from_cache) > 0:
        if not quiet:
            print("WARNING: Stock splits missing from cache:")
            print("- ", dts_missing_from_cache)
        for dt in dts_missing_from_cache:
            f_diff_all.loc[dt] = True
    # - now compare values
    h_ss = h_ss[h_ss.index.isin(yf_ss.index)]
    yf_ss = yf_ss[yf_ss.index.isin(h_ss.index)]
    f_close = np.isclose(h_ss.to_numpy(), yf_ss.to_numpy(), rtol=rtol)
    f_diff = ~f_close
    if f_diff.any():
        n_diff = np.sum(f_diff)
        if not quiet:
            print(f"{n_diff}/{n} differences in column {c}")
        df_diffs = pd.DataFrame(h_ss[f_diff]).join(yf_ss[f_diff], lsuffix="_cache", rsuffix="_Yahoo")
        df_diffs["error"] = df_diffs[c+"_cache"] - df_diffs[c+"_Yahoo"]
        df_diffs["error %"] = (df_diffs["error"]*100 / df_diffs[c+"_Yahoo"]).round(2).astype(str) + '%'
        raise Exception("Need to test handling stock split mismatches - prune stock-split store?")

    def _print_sig_diffs(df, df_yf, column, rtol):
        c = column
        f_close = np.isclose(df[c].to_numpy(), df_yf[c].to_numpy(), rtol=rtol)
        f_diff = ~f_close
        if f_diff.any():
            df_diffs = df.loc[f_diff, ["FetchDate", c]].join(df_yf.loc[f_diff, [c]], lsuffix="_cache", rsuffix="_Yahoo")
            df_diffs.index = df_diffs.index.tz_convert(df.index[0].tz)

            df_diffs["error"] = df_diffs[c+"_cache"] - df_diffs[c+"_Yahoo"]
            df_diffs["error %"] = (df_diffs["error"]*100 / df_diffs[c+"_Yahoo"]).round(2).astype(str) + '%'

            f_diff_n = sum(f_diff)
            print(f"- {f_diff_n}/{n} sig. diffs in column {c} with rtol={rtol}")
            print(df_diffs)

    # Verify volumes match
    c = "Volume"
    f_close = np.isclose(h[c].to_numpy(), df_yf[c].to_numpy(), vol_rtol)
    f_close = pd.Series(f_close, h.index)
    f_yfZeroVol = df_yf[c].to_numpy() == 0
    if f_yfZeroVol.any():
        # Ignore differences where YF volume = 0, because what has happened
        # is cached data contains repair but now too old for YF to repair
        if debug:
            msg = f"ignoring {np.sum(f_yfZeroVol & ~f_close)} diffs where YF volume = 0"
            print("- " + msg)
        f_close[f_yfZeroVol] = True
    f_diff_vol = ~f_close
    if f_diff_vol.any():
        if debug:
            _print_sig_diffs(h, df_yf, "Volume", vol_rtol)
        elif not quiet:
            print(f"WARNING: {np.sum(f_diff_vol)}/{n} differences in 'Volume'")
        f_diff_all = f_diff_all | f_diff_vol

    for c in ["Open", "Close", "High", "Low"]:
        f_close = np.isclose(h[c].to_numpy(), df_yf[c].to_numpy(), rtol)
        f_close = pd.Series(f_close, h.index)
        f_diff_c = ~f_close
        if f_diff_c.any():
            if debug:
                _print_sig_diffs(h, df_yf, c, rtol)
            elif not quiet:
                print(f"WARNING: {np.sum(f_diff_vol)}/{n} differences in '{c}'")
            f_diff_all = f_diff_all | f_diff_c

    if not divs_bad:
        f_diff_divs = pd.Series(np.full(h.shape[0], False), h_adj.index)
        if interday:
            # Yahoo div-adjusts interday data, so check my div adjustment
            for c in ["Open", "Close", "High", "Low"]:
                c = "Adj "+c
                f_close = np.isclose(h_adj[c].to_numpy(), df_yf_adj[c].to_numpy(), rtol=0.0005)
                f_close = pd.Series(f_close, h.index)
                f_diff_c = (~f_close) & (~f_diff_all)
                f_diff_divs = f_diff_divs | f_diff_c
        f_diff_divs = f_diff_divs & ~f_diff_all
        if f_diff_divs.any():
            if debug:
                print("Bad div-adjustments detected:")
                if not f_diff_all.any():
                    print("- no other differences")
                _print_sig_diffs(h_adj, df_yf_adj, "Adj Open", rtol)
            elif not quiet:
                print(f"{np.sum(f_diff_divs)}/{h.shape[0]} div-adjustment errors")

        f_diff_all = f_diff_all | f_diff_divs

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
