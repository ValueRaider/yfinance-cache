from enum import Enum, IntEnum
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta

import numpy as np
import pandas as pd

yf_price_data_cols = ['Open', 'High', 'Low', 'Close', 'Adj Close']
yf_data_cols = yf_price_data_cols+['Volume', 'Dividends', 'Stock Splits']
yf_min_year = 1950


class Period(Enum):
    Days1 = 0
    Days5 = 1
    Months1 = 10
    Months3 = 11
    Months6 = 12
    Years1 = 20
    Years2 = 21
    Years5 = 22
    Ytd = 24
    Max = 30
periodToString = {}
periodToString[Period.Days1] = "1d"
periodToString[Period.Days5] = "5d"
periodToString[Period.Months1] = "1mo"
periodToString[Period.Months3] = "3mo"
periodToString[Period.Months6] = "6mo"
periodToString[Period.Years1] = "1y"
periodToString[Period.Years2] = "2y"
periodToString[Period.Years5] = "5y"
periodToString[Period.Ytd] = "ytd"
periodToString[Period.Max] = "max"
periodStrToEnum = {v: k for k, v in periodToString.items()}
periodToTimedelta = {}
periodToTimedelta[Period.Days1] = timedelta(days=1)
periodToTimedelta[Period.Days5] = timedelta(days=7)
periodToTimedelta[Period.Months1] = relativedelta(months=1)
periodToTimedelta[Period.Months3] = relativedelta(months=3)
periodToTimedelta[Period.Months6] = relativedelta(months=6)
periodToTimedelta[Period.Years1] = relativedelta(years=1)
periodToTimedelta[Period.Years2] = relativedelta(years=2)
periodToTimedelta[Period.Years5] = relativedelta(years=5)


# Months3 = 0
# Months1 = 2
class Interval(Enum):
    Week = 5
    Days1 = 10
    Hours1 = 20
    Mins90 = 21
    Mins60 = 22
    Mins30 = 23
    Mins15 = 24
    Mins5 = 25
    Mins2 = 26
    Mins1 = 27
intervalToString = {}
intervalToString[Interval.Mins1] = "1m"
intervalToString[Interval.Mins2] = "2m"
intervalToString[Interval.Mins5] = "5m"
intervalToString[Interval.Mins15] = "15m"
intervalToString[Interval.Mins30] = "30m"
intervalToString[Interval.Mins60] = "60m"
intervalToString[Interval.Mins90] = "90m"
intervalToString[Interval.Hours1] = "1h"
intervalToString[Interval.Days1] = "1d"
intervalToString[Interval.Week] = "1wk"
# intervalToString[Interval.Months1] = "1mo"
# intervalToString[Interval.Months3] = "3mo"
intervalStrToEnum = {v: k for k, v in intervalToString.items()}
intervalToTimedelta = {}
intervalToTimedelta[Interval.Mins1] = timedelta(minutes=1)
intervalToTimedelta[Interval.Mins2] = timedelta(minutes=2)
intervalToTimedelta[Interval.Mins5] = timedelta(minutes=5)
intervalToTimedelta[Interval.Mins15] = timedelta(minutes=15)
intervalToTimedelta[Interval.Mins30] = timedelta(minutes=30)
intervalToTimedelta[Interval.Mins60] = timedelta(minutes=60)
intervalToTimedelta[Interval.Mins90] = timedelta(minutes=90)
intervalToTimedelta[Interval.Hours1] = timedelta(hours=1)
intervalToTimedelta[Interval.Days1] = timedelta(days=1)
intervalToTimedelta[Interval.Week] = timedelta(days=7)
# intervalToTimedelta[Interval.Months1] = relativedelta(months=1)
# intervalToTimedelta[Interval.Months3] = relativedelta(months=3)


exchangeToXcalExchange = {}
# USA:
exchangeToXcalExchange["NYQ"] = "XNYS"
exchangeToXcalExchange["ASE"] = exchangeToXcalExchange["NYQ"]
exchangeToXcalExchange["PCX"] = exchangeToXcalExchange["NYQ"]  # NYSE Arca
exchangeToXcalExchange["PNK"] = exchangeToXcalExchange["NYQ"]  # OTC pink
exchangeToXcalExchange["OQX"] = exchangeToXcalExchange["NYQ"]  # OTCQX
exchangeToXcalExchange["OEM"] = exchangeToXcalExchange["NYQ"]  # OTC EXMKT
exchangeToXcalExchange["OQB"] = exchangeToXcalExchange["NYQ"]  # OTCQB
exchangeToXcalExchange["NCM"] = "NASDAQ"
exchangeToXcalExchange["NAS"] = exchangeToXcalExchange["NCM"]
exchangeToXcalExchange["NGM"] = exchangeToXcalExchange["NCM"]
exchangeToXcalExchange["NMS"] = exchangeToXcalExchange["NCM"]
exchangeToXcalExchange["BTS"] = exchangeToXcalExchange["NYQ"]  # Cboe BZX formerly known as BATS
exchangeToXcalExchange["CXI"] = "XCBF"  # CBOE Futures
exchangeToXcalExchange['NYM'] = "CMES"  # NY Mercantile aka NYMEX, CME group. But xcal not perfect match to YF (3 days diff over 2 years)
exchangeToXcalExchange['CMX'] = "CMES"  # COMEX, CME group. But xcal not perfect match to YF (1 day diff over 2 years)
exchangeToXcalExchange['CBT'] = "CMES"  # CBOT, CME group. But xcal not perfect match to YF (1 day diff over 2 years)
exchangeToXcalExchange['NYB'] = 'IEPA'  # ICE Futures. But xcal not perfect match to YF (1 day diff over 2 years)
exchangeToXcalExchange['CME'] = 'CMES'  # Chicago Mercantile aka CME
# Canada:
exchangeToXcalExchange["TOR"] = "XTSE"  # Toronto
exchangeToXcalExchange["VAN"] = exchangeToXcalExchange["TOR"]  # TSX Venture
exchangeToXcalExchange["CNQ"] = exchangeToXcalExchange["TOR"]  # CSE. TSX competitor, but has same hours
exchangeToXcalExchange["NEO"] = exchangeToXcalExchange["TOR"]  # Canada Cboe. VERY similar to Toronto, I'm too lazy to add NEO to exchange_calendars
# Europe:
exchangeToXcalExchange["LSE"] = "XLON"  # London
exchangeToXcalExchange["IOB"] = exchangeToXcalExchange["LSE"]
exchangeToXcalExchange["AMS"] = "XAMS"  # Amsterdam
exchangeToXcalExchange["ATH"] = "ASEX"  # Athens
exchangeToXcalExchange["BER"] = "XHAM"  # Berlin. not in xcal but looks like Hamburg
exchangeToXcalExchange["BRU"] = "XBRU"  # Brussels
exchangeToXcalExchange["BUD"] = "XBUD"  # Budapest
exchangeToXcalExchange["BVB"] = "XBSE"  # Bucharest
exchangeToXcalExchange["CPH"] = "XCSE"  # Copenhagen
exchangeToXcalExchange["EBS"] = "XSWX"  # Zurich
exchangeToXcalExchange["FRA"] = "XFRA"  # Frankfurt. Germany also has XETRA but that's part of Frankfurt exchange
exchangeToXcalExchange["GER"] = "XFRA"  # Frankfurt
exchangeToXcalExchange["DUS"] = "XDUS"  # Dusseldorf
exchangeToXcalExchange["HAM"] = "XHAM"  # Hamburg
exchangeToXcalExchange["HEL"] = "XHEL"  # Helsinki
exchangeToXcalExchange["ICE"] = "XICE"  # Iceland
exchangeToXcalExchange["ISE"] = "XDUB"  # Ireland
exchangeToXcalExchange["LIS"] = "XLIS"  # Lisbon
exchangeToXcalExchange["MCE"] = "XMAD"  # Madrid
exchangeToXcalExchange["MIL"] = "XMIL"  # Milan
exchangeToXcalExchange["OSL"] = "XOSL"  # Oslo
exchangeToXcalExchange["PAR"] = "XPAR"  # Paris
exchangeToXcalExchange["PRA"] = "XPRA"  # Prague
exchangeToXcalExchange["STO"] = "XSTO"  # Stockholm
exchangeToXcalExchange['STU'] = 'XHAM'  # Stuttgart. not in xcal but looks like Hamburg
exchangeToXcalExchange["VIE"] = "XWBO"  # Vienna
exchangeToXcalExchange["WSE"] = "XWAR"  # Warsaw
# Other:
exchangeToXcalExchange["TLV"] = "XTAE"  # Israel
exchangeToXcalExchange["JNB"] = "XJSE"  # Johannesburg, South Africa
exchangeToXcalExchange["SAO"] = "BVMF"  # Sao Paulo, Brazil
exchangeToXcalExchange["SGO"] = "XSGO"  # Santiago, Chile
exchangeToXcalExchange["BVC"] = "XBOG"  # Bogota, Colombia
exchangeToXcalExchange["BUE"] = "XBUE"  # Buenos Aires, Argentina
exchangeToXcalExchange["MEX"] = "XMEX"  # Mexico
exchangeToXcalExchange["JPX"] = "JPX"   # Tokyo
exchangeToXcalExchange['OSA'] = exchangeToXcalExchange["JPX"]  # Osaka. Not in xcal so assume
exchangeToXcalExchange['SHZ'] = 'XSHG'  # Shenzen
exchangeToXcalExchange["TAI"] = "XTAI"  # Taiwan
exchangeToXcalExchange["TWO"] = "XTAI"  # Taipai OTC, Taiwan. Closes 5 minutes before TWSE, otherwise same.
exchangeToXcalExchange["KSC"] = "XKRX"  # Korea
exchangeToXcalExchange["KOE"] = exchangeToXcalExchange["KSC"]
exchangeToXcalExchange["SES"] = "XSES"  # Singapore
exchangeToXcalExchange["HKG"] = "XHKG"  # Hong Kong
exchangeToXcalExchange["ASX"] = "ASX"   # Australia
exchangeToXcalExchange["NZE"] = "XNZE"  # New Zealand
exchangeToXcalExchange["SAU"] = "XSAU"  # Saudi Arabia
exchangeToXcalExchange['BSE'] = 'XBOM'  # Bombai, India
exchangeToXcalExchange['NSI'] = 'XBOM'  # National Stock Exchange of India. Schedule appears identical to Bombai
exchangeToXcalExchange['PHS'] = 'XPHS'  # Philippines
exchangeToXcalExchange['IST'] = 'XIST'  # Istanbul, Turkey
exchangeToXcalExchange['JKT'] = 'XIDX'  # Jakarta, Indonesia
# FX
exchangeToXcalExchange["CCY"] = "24/5"  # Didn't stop trading on Jimmy Carter's federal holiday
exchangeToXcalExchange["CCC"] = "24/7"  # Crypto 24/7

exchangesWithBreaks = {"HKG"}

# Yahoo specify data delays here:
# https://help.yahoo.com/kb/SLN2310.html?guccounter=1
exchangeToYfLag = {}
# USA:
exchangeToYfLag["NYQ"] = timedelta(0)
exchangeToYfLag["ASE"] = exchangeToYfLag["NYQ"]
exchangeToYfLag["PCX"] = exchangeToYfLag["NYQ"]
exchangeToYfLag['NYM'] = timedelta(minutes=10)
exchangeToYfLag["PNK"] = timedelta(minutes=15)
exchangeToYfLag["OQX"] = timedelta(minutes=15)
exchangeToYfLag["OEM"] = exchangeToYfLag["OQX"]
exchangeToYfLag["OQB"] = exchangeToYfLag["OQX"]
exchangeToYfLag["NCM"] = exchangeToYfLag["NYQ"]
exchangeToYfLag["NAS"] = exchangeToYfLag["NCM"]
exchangeToYfLag["NGM"] = exchangeToYfLag["NCM"]
exchangeToYfLag["NMS"] = exchangeToYfLag["NCM"]
exchangeToYfLag["BTS"] = exchangeToYfLag["NYQ"]
exchangeToYfLag["CXI"] = timedelta(minutes=15)
exchangeToYfLag['CMX'] = timedelta(minutes=30)
exchangeToYfLag["CBT"] = timedelta(minutes=10)
exchangeToYfLag["NYB"] = timedelta(minutes=30)
exchangeToYfLag['CME'] = timedelta(minutes=10)
# Canada:
exchangeToYfLag["TOR"] = timedelta(0)
exchangeToYfLag["VAN"] = exchangeToYfLag["TOR"]
exchangeToYfLag["CNQ"] = exchangeToYfLag["TOR"]
exchangeToYfLag["NEO"] = timedelta(0)
# Europe:
exchangeToYfLag["LSE"] = timedelta(minutes=20)
exchangeToYfLag["IOB"] = timedelta(minutes=20)
exchangeToYfLag["AMS"] = timedelta(minutes=15)
exchangeToYfLag["ATH"] = timedelta(minutes=15)
exchangeToYfLag["BER"] = timedelta(minutes=15)
exchangeToYfLag["BRU"] = timedelta(minutes=15)
exchangeToYfLag["BUD"] = timedelta(minutes=15)
exchangeToYfLag["BVB"] = timedelta(minutes=15)
exchangeToYfLag["CPH"] = timedelta(0)
exchangeToYfLag["EBS"] = timedelta(minutes=30)
exchangeToYfLag["FRA"] = timedelta(minutes=15)
exchangeToYfLag["GER"] = timedelta(minutes=15)
exchangeToYfLag["DUS"] = timedelta(minutes=15)
exchangeToYfLag["HAM"] = timedelta(minutes=15)
exchangeToYfLag["HEL"] = timedelta(0)
exchangeToYfLag["ICE"] = timedelta(0)
exchangeToYfLag["ISE"] = timedelta(minutes=15)
exchangeToYfLag["LIS"] = timedelta(minutes=15)
exchangeToYfLag["MCE"] = timedelta(minutes=15)
exchangeToYfLag["MIL"] = timedelta(minutes=20)
exchangeToYfLag["OSL"] = timedelta(minutes=15)
exchangeToYfLag["PAR"] = timedelta(minutes=15)
exchangeToYfLag["PRA"] = timedelta(minutes=20)
exchangeToYfLag["STO"] = timedelta(0)
exchangeToYfLag["STU"] = timedelta(minutes=15)
exchangeToYfLag["VIE"] = timedelta(minutes=15)
exchangeToYfLag["WSE"] = timedelta(minutes=15)
# Other:
exchangeToYfLag["TLV"] = timedelta(minutes=20)
exchangeToYfLag["JNB"] = timedelta(minutes=15)
exchangeToYfLag["SAO"] = timedelta(minutes=15)
exchangeToYfLag["SGO"] = timedelta(minutes=15)
exchangeToYfLag["BVC"] = timedelta(minutes=15)  # Guess because Yahoo don't specify
exchangeToYfLag["BUE"] = timedelta(minutes=30)
exchangeToYfLag["MEX"] = timedelta(minutes=20)
exchangeToYfLag["JPX"] = timedelta(minutes=20)
exchangeToYfLag["OSA"] = timedelta(minutes=30)
exchangeToYfLag["SHZ"] = timedelta(minutes=30)
exchangeToYfLag["TAI"] = timedelta(minutes=20)
exchangeToYfLag["TWO"] = timedelta(minutes=20)
exchangeToYfLag["KSC"] = timedelta(minutes=20)
exchangeToYfLag["KOE"] = timedelta(minutes=20)
exchangeToYfLag["SES"] = timedelta(minutes=20)
exchangeToYfLag["HKG"] = timedelta(minutes=15)
exchangeToYfLag["ASX"] = timedelta(minutes=20)
exchangeToYfLag["NZE"] = timedelta(minutes=20)
exchangeToYfLag["SAU"] = timedelta(minutes=15)
exchangeToYfLag['BSE'] = timedelta(minutes=15)
exchangeToYfLag['NSI'] = timedelta(0)
exchangeToYfLag['PHS'] = timedelta(minutes=15)
exchangeToYfLag['IST'] = timedelta(minutes=15)
exchangeToYfLag['JKT'] = timedelta(minutes=10)
# FX:
exchangeToYfLag["CCY"] = timedelta(0)
exchangeToYfLag["CCC"] = timedelta(0)

# Indices, not real exchanges
exchangeToXcalExchange['DJI'] = exchangeToXcalExchange["NYQ"]  # Dow Jones
exchangeToXcalExchange['SNP'] = exchangeToXcalExchange["NYQ"]  # SNP
exchangeToXcalExchange['NYS'] = exchangeToXcalExchange["NYQ"]  # NYSE
exchangeToXcalExchange['NIM'] = exchangeToXcalExchange['NCM']  # Nasdaq GIDS
exchangeToXcalExchange["CGI"] = exchangeToXcalExchange['CXI']  # Cboe Indices
exchangeToXcalExchange['WCB'] = exchangeToXcalExchange["CXI"]  # Chicago Options
exchangeToXcalExchange['FGI'] = exchangeToXcalExchange['LSE']  # FTSE Index
exchangeToYfLag['DJI'] = timedelta(0)
exchangeToYfLag['SNP'] = timedelta(0)
exchangeToYfLag['NYS'] = exchangeToYfLag['NYQ']  # guess
exchangeToYfLag['NIM'] = timedelta(0)  # guess
exchangeToYfLag["CGI"] = timedelta(minutes=15)
exchangeToYfLag["WCB"] = exchangeToYfLag["CGI"]  # guess
exchangeToYfLag['FGI'] = timedelta(minutes=15)

# After-market auctions:
exchangesWithAuction = set()
exchangeAuctionDelay = {}
exchangeAuctionDuration = {}
# ASX after-market auction starts 10m after close but no end time given. So allow 1 minute
exchangesWithAuction.add("ASX")
exchangeAuctionDelay["ASX"] = timedelta(minutes=10)
exchangeAuctionDuration["ASX"] = timedelta(minutes=1)
# TLB after-market auction starts 9m after close but no end time given. So allow 1 minute
exchangesWithAuction.add("TLV")
exchangeAuctionDelay["TLV"] = timedelta(minutes=9)
exchangeAuctionDuration["TLV"] = timedelta(minutes=2)  # One extra minute because of randomised start time


yfMaxFetchRange = {}
yfMaxFetchRange[Interval.Mins1] = timedelta(days=7)
yfMaxFetchRange[Interval.Mins2] = timedelta(days=60)
yfMaxFetchRange[Interval.Mins5] = timedelta(days=60)
yfMaxFetchRange[Interval.Mins15] = timedelta(days=60)
yfMaxFetchRange[Interval.Mins30] = timedelta(days=60)
yfMaxFetchRange[Interval.Mins90] = timedelta(days=60)
yfMaxFetchRange[Interval.Mins60] = timedelta(days=730)
yfMaxFetchRange[Interval.Hours1] = timedelta(days=730)
yfMaxFetchRange[Interval.Days1] = None
yfMaxFetchRange[Interval.Week] = None
# yfMaxFetchRange[Interval.Months1] = None
# yfMaxFetchRange[Interval.Months3] = None

yfMaxFetchLookback = {}
yfMaxFetchLookback[Interval.Mins1] = timedelta(days=30)
yfMaxFetchLookback[Interval.Mins2] = timedelta(days=60)
yfMaxFetchLookback[Interval.Mins5] = timedelta(days=60)
yfMaxFetchLookback[Interval.Mins15] = timedelta(days=60)
yfMaxFetchLookback[Interval.Mins30] = timedelta(days=60)
yfMaxFetchLookback[Interval.Mins90] = timedelta(days=60)
yfMaxFetchLookback[Interval.Mins60] = timedelta(days=730)
yfMaxFetchLookback[Interval.Hours1] = timedelta(days=730)
yfMaxFetchLookback[Interval.Days1] = None
yfMaxFetchLookback[Interval.Week] = None
# yfMaxFetchLookback[Interval.Months1] = None
# yfMaxFetchLookback[Interval.Months3] = None

listing_date_check_tols = {}
listing_date_check_tols[Interval.Days1] = timedelta(days=7)
listing_date_check_tols[Interval.Week] = timedelta(days=14)
# listing_date_check_tols[Interval.Months1] = timedelta(days=35)
# listing_date_check_tols[Interval.Months3] = timedelta(days=35*3)


from multiprocessing import Manager, current_process, Lock
import threading

_manager = None
_manager_lock = threading.Lock()

def get_manager():
    global _manager
    with _manager_lock:
        if _manager is None:
            if current_process().name == 'MainProcess':
                _manager = Manager()
            else:
                # For non-main processes, use threading locks instead
                return None
        return _manager

# Initialize exchange_locks with thread locks by default
exchange_locks = {e:Lock() for e in exchangeToXcalExchange.keys()}

# Only use Manager locks in main process
if current_process().name == 'MainProcess':
    try:
        manager = get_manager()
        if manager is not None:
            exchange_locks = {e:manager.Lock() for e in exchangeToXcalExchange.keys()}
    except Exception as e:
        # Fallback to thread locks if Manager fails
        print(f"Warning: Failed to initialize Manager locks: {e}")

    
class Financials(Enum):
    IncomeStmt = 0
    BalanceSheet = 1
    CashFlow = 2

class ReportingPeriod(Enum):
    Interim = 0
    Full = 1

class Confidence(IntEnum):
    Low = 0
    Medium = 1
    High = 2

confidence_to_buffer = {}
confidence_to_buffer[Confidence.High] = timedelta(days=2)
confidence_to_buffer[Confidence.Medium] = timedelta(days=7)
confidence_to_buffer[Confidence.Low] = timedelta(days=40)


class NoIntervalsInRangeException(Exception):
    def __init__(self, interval, start_dt, end_dt, *args):
        super().__init__(args)
        self.interval = interval
        self.start_dt = start_dt
        self.end_dt = end_dt

    def __str__(self):
        return ("No {} intervals found between {}->{}".format(self.interval, self.start_dt, self.end_dt))


class NoPriceDataInRangeException(Exception):
    def __init__(self, tkr, interval, start_dt, end_dt, *args):
        super().__init__(args)
        self.tkr = tkr
        self.interval = interval
        self.start_dt = start_dt
        self.end_dt = end_dt

    def __str__(self):
        return ("No {}-price data fetched for ticker {} between dates {} -> {}".format(self.interval, self.tkr, self.start_dt, self.end_dt))


class TimestampOutsideIntervalException(Exception):
    def __init__(self, exchange, interval, ts, *args):
        super().__init__(args)
        self.exchange = exchange
        self.interval = interval
        self.ts = ts

    def __str__(self):
        return (f"Failed to map '{self.ts}' to '{self.interval}' interval on exchange '{self.exchange}'")


class AmbiguousComparisonException(Exception):
    def __init__(self, value1, value2, operation, true_prob=None):
        if not isinstance(operation, str):
            raise TypeError(f"operation must be a string not {type(operation)}")
        if true_prob is not None and not isinstance(true_prob, (int, float)):
            raise TypeError(f"true_prob must be numeric not {type(true_prob)}")

        self.value1 = value1
        self.value2 = value2
        self.operation = operation
        self.true_prob = true_prob

    def __str__(self):
        msg = f"Ambiguous whether {self.value1} {self.operation} {self.value2}"
        if self.true_prob is not None:
            msg += f" (true with probability {self.true_prob*100:.1f}%)"
        return msg


class DateInterval:
    def __init__(self, left, right, closed=None):
        if not isinstance(left, date) or isinstance(left, datetime):
            raise TypeError("'left' must be date object not datetime")

        self.left = left
        self.right = right

        if closed is None:
            self.closed = None
        else:
            if closed not in ["left", "right"]:
                raise Exception("closed must be left or right")
            self.closed = closed

    def __eq__(self, other):
        return self.left == other.left and self.right == other.right and self.closed == other.closed

    def __str__(self):
        s = ""
        if self.closed == "left":
            s += '['
        else:
            s += '('
        s += str(self.left) + ', ' + str(self.right)
        if self.closed == "right":
            s += ']'
        else:
            s += ')'
        return s

    def __repr__(self):
        return self.__str__()


class DateIntervalIndex:
    def __init__(self, intervals):
        if not isinstance(intervals, (list, np.ndarray, pd.Series)):
            raise TypeError(f"'intervals' must be iterable not '{type(intervals)}'")
        if not isinstance(intervals, np.ndarray):
            self.array = np.array(intervals)
        else:
            self.array = intervals

        self._left = np.array([x.left for x in self.array])
        self._right = np.array([x.right for x in self.array])
        self._right_inc = self._right - timedelta(days=1)

    @classmethod
    def from_arrays(cls, left, right, closed=None):
        if len(left) != len(right):
            raise Exception("left and right must be equal length")
        if isinstance(left, pd.Series):
            intervals = [DateInterval(left.iloc[i], right.iloc[i], closed) for i in range(len(left))]
        else:
            intervals = [DateInterval(left[i], right[i], closed) for i in range(len(left))]
        return cls(intervals)

    @property
    def left(self):
        return self._left

    @property
    def right(self):
        return self._right

    @property
    def shape(self):
        return (len(self.array), 2)

    @property
    def empty(self):
        return self.shape[0] == 0

    def __len__(self):
        return self.shape[0]

    def sort_values(self):
        return DateIntervalIndex(self.array[np.argsort(self._left)])

    def get_indexer(self, values):
        idx_right = np.searchsorted(self._right_inc, values)

        idx_left = np.searchsorted(self._left, values, side="right")
        idx_left -= 1

        f_match = idx_right == idx_left

        idx = idx_left
        idx[~f_match] = -1
        return idx

    def __getitem__(self, i):
        v = self.array[i]
        if isinstance(v, np.ndarray):
            v = DateIntervalIndex(v)
        return v

    def __setitem__(self, i, v):
        raise Exception("immutable")

    def __eq__(self, other):
        if not isinstance(other, DateIntervalIndex):
            return False
        if len(self.array) != len(other.array):
            return False
        return np.equal(self.array, other.array)

    def equals(self, other):
        e = self == other
        if isinstance(e, np.ndarray):
            e = e.all()
        return e

    def __str__(self):
        s = "DateIntervalIndex([ "
        for x in self.array:
            s += x.__str__() + " , "
        s += "])"
        return s

    def __repr__(self):
        return self.__str__()


def uniform_prob_lt(X, Y):

    def is_scalar(val):
        return not isinstance(val, tuple)

    if is_scalar(X) and is_scalar(Y):
        return float(X < Y)

    if is_scalar(X):
        Y_min, Y_max = Y
        if X < Y_min:
            return 1.0
        elif X >= Y_max:
            return 0.0
        else:
            return (Y_max - X) / (Y_max - Y_min)

    if is_scalar(Y):
        X_min, X_max = X
        if Y <= X_min:
            return 0.0
        elif Y > X_max:
            return 1.0
        else:
            return (Y - X_min) / (X_max - X_min)

    # To handle ranges, decompose into weighted sum
    # of simple sub-ranges.

    X_min, X_max = X
    Y_min, Y_max = Y

    # Non-Overlapping Ranges
    if X_max <= Y_min:
        return 1.0
    elif Y_max <= X_min:
        return 0.0

    # Identical Ranges
    elif X_min == Y_min and X_max == Y_max:
        return 0.5

    # Ensure X < Y
    elif X_min > Y_min:
        return 1.0 - uniform_prob_lt(Y, X)
    elif X_min == Y_min and X_max > Y_max:
        return 1.0 - uniform_prob_lt(Y, X)

    elif X_min < Y_min:
        # Split on Y_min
        p_x_lt_ymin = (Y_min-X_min)/(X_max-X_min)
        return p_x_lt_ymin + uniform_prob_lt((Y_min, X_max), (Y_min, Y_max)) * (1.0-p_x_lt_ymin)

    elif X_min == Y_min and X_max < Y_max:
        # Split on X_max
        ratio = (X_max-X_min)/(Y_max-Y_min)
        return ratio*uniform_prob_lt((X_min, X_max), (Y_min, X_max)) + (1.0 - ratio)

    # Unexpected scenario
    raise ValueError(f"Unexpected scenario: X={X}, Y={Y}")


class ComparableRelativedelta(relativedelta):
    def _have_same_attributes(self, other):
        attrs = ['years', 'months', 'days', 'leapdays', 'hours', 'minutes', 'seconds', 'microseconds', 'year', 'month', 'day', 'weekday']

        for a in attrs:
            if getattr(self, a, 0) == 0:
                if getattr(other, a, 0) != 0:
                    return False
        return True

    def __str__(self):
        s = ''

        a = 'years'
        x = getattr(self, a, 0)
        if x != 0:
            s += f'{x}y'

        a = 'months'
        x = getattr(self, a, 0)
        if x != 0:
            s += f'{x}mo'

        a = 'days'
        x = getattr(self, a, 0)
        if x != 0:
            s += f'{x}d'

        a = 'hours'
        x = getattr(self, a, 0)
        if x != 0:
            s += f'{x}h'

        a = 'minutes'
        x = getattr(self, a, 0)
        if x != 0:
            s += f'{x}m'

        if s == '':
            s = '0'

        return s

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        if isinstance(other, relativedelta):
            attrs = ['years', 'months', 'days', 'leapdays', 'hours', 'minutes', 'seconds', 'microseconds', 'year', 'month', 'day', 'weekday']
            return all(getattr(self, attr, 0) == getattr(other, attr, 0) for attr in attrs)

        raise NotImplementedError(f'Not implemented ComparableRelativedelta={self} == {type(other)}={other}')

    def __lt__(self, other):
        if isinstance(other, (TimedeltaEstimate, TimedeltaRangeEstimate)):
            return other.__gt__(self)

        elif isinstance(other, (relativedelta, timedelta)):
            reference_date = date(2000, 1, 1)
            result_date_self = reference_date + self
            result_date_other = reference_date + other

            if not self._have_same_attributes(other):
                if abs(result_date_self - result_date_other) < timedelta(days=7):
                    raise AmbiguousComparisonException(self, other, '<')

            return result_date_self < result_date_other

        raise NotImplementedError(f'Not implemented ComparableRelativedelta={self} < {type(other)}={other}')

    def __le__(self, other):
        return self < other

    def __gt__(self, other):
        if isinstance(other, (TimedeltaEstimate, TimedeltaRangeEstimate)):
            return other.__lt__(self)

        elif isinstance(other, (relativedelta, timedelta)):
            reference_date = date(2000, 1, 1)
            result_date_self = reference_date + self
            result_date_other = reference_date + other

            if not self._have_same_attributes(other):
                if abs(result_date_self - result_date_other) < timedelta(days=7):
                    raise AmbiguousComparisonException(self, other, '>')

            return result_date_self > result_date_other
        raise NotImplementedError(f'Not implemented ComparableRelativedelta={self} > {type(other)}={other}')

    def __ge__(self, other):
        return self > other


class TimedeltaEstimate():
    def __init__(self, td, confidence):
        if not isinstance(confidence, Confidence):
            raise Exception("'confidence' must be a 'Confidence' object, not {0}".format(type(confidence)))
        if not isinstance(td, (timedelta, pd.Timedelta, ComparableRelativedelta)):
            raise Exception("'td' must be a 'timedelta' object or None, not {0}".format(type(td)))
        if isinstance(td, ComparableRelativedelta) and confidence != Confidence.High:
            td = timedelta(days=td.years*365 +td.months*30 +td.days)
        self.td = td
        self.confidence = confidence
        self.uncertainty = confidence_to_buffer[confidence]

    def copy(self):
        return TimedeltaEstimate(self.td, self.confidence)

    def __str__(self):
        tdstr = f'{self.td.days} days'
        s = f"{tdstr} (conf={self.confidence}/2)"
        return s

    def __repr__(self):
        return self.__str__()

    def __abs__(self):
        return TimedeltaEstimate(abs(self.td), self.confidence)

    def __neg__(self):
        return TimedeltaEstimate(-self.td, self.confidence)

    def __eq__(self, other):
        if isinstance(other, TimedeltaEstimate):
            return self.td == other.td and self.confidence == other.confidence
        raise NotImplementedError(f'Not implemented {self} == {type(other)}={other}')

    def isclose(self, other):
        if isinstance(other, TimedeltaEstimate):
            # return abs(self.td - other.td) <= max(self.uncertainty, other.uncertainty)
            return abs(self.td - other.td) <= (self.uncertainty + other.uncertainty)

        elif isinstance(other, (timedelta, pd.Timedelta, ComparableRelativedelta)):
            return abs(self.td - other) <= self.uncertainty
        
        raise NotImplementedError(f'Not implemented {self} is-close-to {type(other)}={other}')

    def __iadd__(self, other):
        if isinstance(other, (timedelta, relativedelta, ComparableRelativedelta)):
            self.td += other
            return self
        raise NotImplementedError(f'Not implemented {self} += {type(other)}={other}')

    def __add__(self, other):
        if isinstance(other, (timedelta, relativedelta, ComparableRelativedelta)):
            return TimedeltaEstimate(self.td + other, self.confidence)
        elif isinstance(other, date):
            return DateEstimate(self.td + other, self.confidence)
        elif isinstance(other, DateEstimate):
            return DateEstimate(self.td + other.date, min(self.confidence, other.confidence))
        raise NotImplementedError(f'Not implemented {self} + {type(other)}={other}')

    def __radd__(self, other):
        return self.__add__(other)

    def __sub__(self, other):
        if isinstance(other, (timedelta, relativedelta, ComparableRelativedelta)):
            return TimedeltaEstimate(self.td - other, self.confidence)
        elif isinstance(other, date):
            return DateEstimate(self.td - other, self.confidence)
        raise NotImplementedError(f'Not implemented {type(self)}{self} - {type(other)}={other}')

    def __rsub__(self, other):
        if isinstance(other, date):
            return DateEstimate(other - self.td, self.confidence)
        raise NotImplementedError(f'Not implemented {self} rsub {type(other)}={other}')

    def __mul__(self, other):
        if isinstance(other, (int, float)):
            return TimedeltaEstimate(self.td * other, self.confidence)
        raise NotImplementedError(f'Not implemented {self} * {type(other)}={other}')

    def __rmul__(self, other):
        if isinstance(other, (int, float)):
            return self.__mul__(other)
        raise NotImplementedError(f'Not implemented {type(other)}={other} * {self}')

    def __imul__(self, other):
        if isinstance(other, (int, float)):
            self.td *= other
            return self
        raise NotImplementedError(f'Not implemented {self} *= {type(other)}={other}')

    def prob_lt(self, other):
        if isinstance(other, TimedeltaRangeEstimate):
            return other.prob_gt(self)

        elif isinstance(other, TimedeltaRange):
            if self.td + self.uncertainty < other.td1:
                return 1.0
            elif other.td2 <= self.td - self.uncertainty:
                return 0.0
            else:
                return uniform_prob_lt((self.td-self.uncertainty, self.td+self.uncertainty),
                                      (other.td1, other.td2))

        elif isinstance(other, TimedeltaEstimate):
            if self.td + self.uncertainty < other.td - other.uncertainty:
                return 1.0
            elif other.td + other.uncertainty <= self.td - self.uncertainty:
                return 0.0
            else:
                return uniform_prob_lt((self.td-self.uncertainty, self.td+self.uncertainty),
                                      (other.td-other.uncertainty, other.td+other.uncertainty))

        elif isinstance(other, (relativedelta, timedelta, ComparableRelativedelta)):
            if self.td + self.uncertainty < other:
                return 1.0
            elif other <= self.td - self.uncertainty:
                return 0.0
            else:
                return uniform_prob_lt((self.td-self.uncertainty, self.td+self.uncertainty),
                                       other)

        raise NotImplementedError(f'Not implemented {self} < {type(other)}={other}')
    def __lt__(self, other):
        x = self.prob_lt(other)
        if x in [0.0, 1.0]:
            return x == 1.0
        else:
            raise AmbiguousComparisonException(self, other, '<', x)
    def prob_gt(self, other):
        return 1.0 - self.prob_lt(other)
    def __gt__(self, other):
        x = self.prob_gt(other)
        if x in [0.0, 1.0]:
            return x == 1.0
        else:
            raise AmbiguousComparisonException(self, other, '>', x)
    def prob_le(self, other):
        return self.prob_lt(other)
    def __le__(self, other):
        return self < other
    def prob_ge(self, other):
        return self.prob_gt(other)
    def __ge__(self, other):
        return self > other

    def __mod__(self, other):
        if isinstance(other, timedelta):
            if self.td < timedelta(0):
                raise NotImplementedError('Not implemented modulus of negative TimedeltaEstimate')
            else:
                td = self.td
                while td > other:
                    td -= other
                return TimedeltaEstimate(td, self.confidence)
        raise NotImplementedError(f'Not implemented {self} modulus-of {type(other)}={other}')

    def __truediv__(self, other):
        if isinstance(other, (int, float, np.int64)):
            return TimedeltaEstimate(self.td / other, self.confidence)
        raise NotImplementedError(f'Not implemented {self} / {type(other)}={other}')


class TimedeltaRangeEstimate():
    def __init__(self, td1, td2, confidence):
        if not isinstance(confidence, Confidence):
            raise Exception("'confidence' must be a 'Confidence' object, not {0}".format(type(confidence)))
        if (td1 is not None) and not isinstance(td1, (timedelta, pd.Timedelta)):
            raise Exception("'td1' must be a 'timedelta' object or None, not {0}".format(type(td1)))
        if (td2 is not None) and not isinstance(td2, (timedelta, pd.Timedelta)):
            raise Exception("'td2' must be a 'timedelta' object or None, not {0}".format(type(td2)))
        if td2 <= td1:
            swap = td1 ; td1 = td2 ; td2 = swap
        self.td1 = td1
        self.td2 = td2
        self.confidence = confidence
        self.uncertainty = confidence_to_buffer[confidence]

    def __str__(self):
        s = f"{self.td1.days}->{self.td2.days} days (conf={self.confidence}/2)"
        return s

    def __repr__(self):
        return self.__str__()

    def __abs__(self):
        return TimedeltaRangeEstimate(abs(self.td1), abs(self.td2), self.confidence)

    def isclose(self, other):
        raise NotImplementedError(f'Not implemented {self} is-close-to {type(other)}={other}')

    def __neg__(self):
        return TimedeltaRangeEstimate(-self.td2, -self.td1, self.confidence)

    def __invert__(self):
        raise NotImplementedError(f'Not implemented {self} invert')

    def __eq__(self, other):
        if isinstance(other, TimedeltaRangeEstimate):
            return self.td1 == other.td1 and self.td2 == other.td2 and self.confidence == other.confidence
        raise NotImplementedError(f'Not implemented {self} == {type(other)}={other}')

    def __add__(self, other):
        if isinstance(other, date):
            return DateRangeEstimate(self.td1 + other, self.td2 + other, self.confidence)
        elif isinstance(other, DateEstimate):
            return DateRangeEstimate(self.td1 + other.date, self.td2 + other.date, min(self.confidence, other.confidence))
        elif isinstance(other, timedelta):
            return TimedeltaRangeEstimate(self.td1 + other, self.td2 + other, self.confidence)
        raise NotImplementedError(f'Not implemented {self} + {type(other)}={other}')

    def __radd__(self, other):
        return self.__add__(other)

    def __mul__(self, other):
        if isinstance(other, (int, float)):
            return TimedeltaRangeEstimate(self.td1 * other, self.td2 * other, self.confidence)
        raise NotImplementedError(f'Not implemented {self} * {type(other)}={other}')

    def __rmul__(self, other):
        if isinstance(other, (int, float)):
            return self.__mul__(other)
        raise NotImplementedError(f'Not implemented {type(other)}={other} * {self}')

    def prob_lt(self, other):
        if isinstance(other, (timedelta, pd.Timedelta, ComparableRelativedelta)):
            if self.td2 + self.uncertainty < other:
                return 1.0
            elif other <= self.td1 - self.uncertainty:
                return 0.0
            else:
                return uniform_prob_lt((self.td1-self.uncertainty, self.td2+self.uncertainty), 
                                      other)

        elif isinstance(other, TimedeltaEstimate):
            if self.td2 + self.uncertainty < other.td - other.uncertainty:
                return 1.0
            elif other.td + other.uncertainty <= self.td1 - self.uncertainty:
                return 0.0
            else:
                return uniform_prob_lt((self.td1-self.uncertainty, self.td2+self.uncertainty), 
                                      (other.td-other.uncertainty, other.td+other.uncertainty))

        elif isinstance(other, TimedeltaRangeEstimate):
            if self.td2 + self.uncertainty < other.td1 - other.uncertainty:
                return 1.0
            elif other.td2 + other.uncertainty <= self.td1 - self.uncertainty:
                return 0.0
            else:
                return uniform_prob_lt((self.td1-self.uncertainty, self.td2+self.uncertainty), 
                                      (other.td1-other.uncertainty, other.td2+other.uncertainty))

        raise NotImplementedError(f'Not implemented {self} < {type(other)}={other}')
    def __lt__(self, other):
        x = self.prob_lt(other)
        if x in [0.0, 1.0]:
            return x == 1.0
        else:
            raise AmbiguousComparisonException(self, other, '<', x)
    def prob_gt(self, other):
        return 1.0 - self.prob_lt(other)
    def __gt__(self, other):
        x = self.prob_gt(other)
        if x in [0.0, 1.0]:
            return x == 1.0
        else:
            raise AmbiguousComparisonException(self, other, '>', x)
    def prob_le(self, other):
        return self.prob_lt(other)
    def __le__(self, other):
        return self < other
    def prob_ge(self, other):
        return self.prob_gt(other)
    def __ge__(self, other):
        return self > other
        

class TimedeltaRange():
    def __init__(self, td1, td2):
        if (td1 is not None) and not isinstance(td1, (timedelta, pd.Timedelta)):
            raise Exception("'td1' must be a 'timedelta' object or None, not {0}".format(type(td1)))
        if (td2 is not None) and not isinstance(td2, (timedelta, pd.Timedelta)):
            raise Exception("'td2' must be a 'timedelta' object or None, not {0}".format(type(td2)))
        if td2 <= td1:
            swap = td1 ; td1 = td2 ; td2 = swap
        self.td1 = td1
        self.td2 = td2

    def __str__(self):
        s = f"{self.td1.days}->{self.td2.days} days"
        return s

    def __repr__(self):
        return self.__str__()

    def __abs__(self):
        if self.td2 <= timedelta(0):
            return TimedeltaRange(-self.td1, -self.td2)
        elif self.td1 >= timedelta(0):
            return self
        else:
            # Straddles zero
            return TimedeltaRange(timedelta(0), self.td2)

    def __add__(self, other):
        if isinstance(other, date):
            return DateRange(other+self.td1, other+self.td2)
        elif isinstance(other, DateEstimate):
            return DateRangeEstimate(other.date+self.td1, other.date+self.td2, other.confidence)
        elif isinstance(other, timedelta):
            return TimedeltaRange(self.td1 + other, self.td2 + other)
        raise NotImplementedError(f'Not implemented {self} + {type(other)}={other}')

    def __radd__(self, other):
        return self.__add__(other)

    def __mul__(self, other):
        if isinstance(other, (int, float)):
            return TimedeltaRange(self.td1 * other, self.td2 * other)
        raise NotImplementedError(f'Not implemented {self} * {type(other)}={other}')

    def __rmul__(self, other):
        if isinstance(other, (int, float)):
            return self.__mul__(other)
        raise NotImplementedError(f'Not implemented {type(other)}={other} * {self}')

    def prob_lt(self, other):
        if isinstance(other, (timedelta, pd.Timedelta, ComparableRelativedelta)):
            if self.td2 < other:
                return 1.0
            else:
                return 0.0
        elif isinstance(other, TimedeltaRange):
            if self.td2 < other.td1:
                return 1.0
            elif other.td2 <= self.td1:
                return 0.0
            else:
                return uniform_prob_lt((self.td1, self.td2), 
                                      (other.td1, other.td2))
        elif isinstance(other, TimedeltaEstimate):
            return other.prob_gt(self)
        raise NotImplementedError(f'Not implemented {self} < {type(other)}={other}')
    def __lt__(self, other):
        x = self.prob_lt(other)
        if x in [0.0, 1.0]:
            return x == 1.0
        else:
            raise AmbiguousComparisonException(self, other, '<', x)
    def prob_gt(self, other):
        return 1.0 - self.prob_lt(other)
    def __gt__(self, other):
        x = self.prob_gt(other)
        if x in [0.0, 1.0]:
            return x == 1.0
        else:
            raise AmbiguousComparisonException(self, other, '>', x)
    def prob_le(self, other):
        return self.prob_lt(other)
    def __le__(self, other):
        return self < other
    def prob_ge(self, other):
        return self.prob_gt(other)
    def __ge__(self, other):
        return self > other


class DateRangeEstimate():
    def __init__(self, start, end, confidence):
        if (start is not None) and not isinstance(start, date):
            raise Exception("'start' must be a 'date' object or None, not {0}".format(type(start)))
        if (end is not None) and not isinstance(end, date):
            raise Exception("'end' must be a 'date' object or None, not {0}".format(type(end)))
        if not isinstance(confidence, Confidence):
            raise Exception("'confidence' must be a 'Confidence' object, not {0}".format(type(confidence)))
        self.start = start
        self.end = end
        self.confidence = confidence
        self.uncertainty = confidence_to_buffer[confidence]

    def copy(self):
        return DateRangeEstimate(self.start, self.end, self.confidence)

    def __str__(self):
        s = f"{self.start}->{self.end} (conf={self.confidence}/2)"
        return s

    def __repr__(self):
        return self.__str__()

    def __abs__(self):
        raise NotImplementedError(f'Not implemented {self} abs')

    def __eq__(self, other):
        if isinstance(other, DateRangeEstimate):
            return self.start == other.start and self.end == other.end and self.confidence == other.confidence
        raise NotImplementedError(f'Not implemented {self} == {type(other)}={other}')

    def isclose(self, other):
        if isinstance(other, date):
            if (self.start - self.uncertainty) <= other and other <= (self.end + self.uncertainty):
                return True
            else:
                return False
        elif isinstance(other, DateRange):
            if ((self.start - self.uncertainty) <= other.start and other.start <= (self.end + self.uncertainty)) or\
               ((self.start - self.uncertainty) <= other.end and other.end <= (self.end + self.uncertainty)):
                return True
            else:
                return False
        elif isinstance(other, DateEstimate):
            if (self.start - self.uncertainty) <= (other.date+other.uncertainty) and (self.end + self.uncertainty) >= (other.date-other.uncertainty):
                return True
            else:
                return False
        elif isinstance(other, DateRangeEstimate):
            self_start_min = self.start - self.uncertainty
            self_end_max   = self.end + self.uncertainty
            other_start_min = other.start - other.uncertainty
            other_end_max   = other.end + other.uncertainty
            return (other_start_min <= self_end_max) and (self_start_min <= other_end_max)

        raise NotImplementedError(f'Not implemented {self} is-close-to {type(other)}={other}')

    def __neg__(self):
        raise NotImplementedError(f'Not implemented {self} negate')

    def __invert__(self):
        raise NotImplementedError(f'Not implemented {self} invert')

    def __iadd__(self, other):
        if isinstance(other, (timedelta, pd.Timedelta)):
            self.start += other
            self.end += other
            return self
        raise NotImplementedError(f'Not implemented {self} += {type(other)}={other}')

    def __add__(self, other):
        if isinstance(other, (timedelta, ComparableRelativedelta)):
            return DateRangeEstimate(self.start + other, self.end + other, self.confidence)
        elif isinstance(other, TimedeltaEstimate):
            conf = min(self.confidence, other.confidence)
            return DateRangeEstimate(self.start + other.td, self.end + other.td, self.confidence)
        raise NotImplementedError(f'Not implemented {self} + {type(other)}={other}')

    def __radd__(self, other):
        return self.__add__(other)

    def __isub__(self, other):
        if isinstance(other, timedelta):
            self.start -= other
            self.end -= other
            return self
        raise NotImplementedError(f'Not implemented {self} -= {type(other)}={other}')

    def __sub__(self, other):
        if isinstance(other, date):
            return TimedeltaRangeEstimate(self.start - other, self.end - other, self.confidence)
        elif isinstance(other, DateEstimate):
            conf = min(self.confidence, other.confidence)
            return TimedeltaRangeEstimate(self.start - other.date, self.end - other.date, conf)
        elif isinstance(other, DateRange):
            return TimedeltaRangeEstimate(self.start - other.start, self.end - other.end, self.confidence)
        elif isinstance(other, DateRangeEstimate):
            conf = min(self.confidence, other.confidence)
            return TimedeltaRangeEstimate(self.start - other.start, self.end - other.end, conf)
        raise NotImplementedError(f'Not implemented {self} - {type(other)}={other}')

    def __rsub__(self, other):
        return -self.__sub__(other)

    def prob_lt(self, other):
        if isinstance(other, date):
            if self.end + self.uncertainty < other:
                return 1.0
            elif other <= self.start - self.uncertainty:
                return 0.0
            else:
                return uniform_prob_lt((self.start-self.uncertainty, self.end+self.uncertainty),
                                      other)

        elif isinstance(other, DateEstimate):
            if self.end + self.uncertainty < other.date - other.uncertainty:
                return 1.0
            elif other.date + other.uncertainty <= self.start - self.uncertainty:
                return 0.0
            else:
                return uniform_prob_lt((self.start-self.uncertainty, self.end+self.uncertainty),
                                      (other.date-other.uncertainty, other.date+other.uncertainty))

        elif isinstance(other, DateRangeEstimate):
            if self.end + self.uncertainty < other.start - other.uncertainty:
                return 1.0
            elif other.end + other.uncertainty <= self.start - self.uncertainty:
                return 0.0
            else:
                return uniform_prob_lt((self.start-self.uncertainty, self.end+self.uncertainty),
                                      (other.start-other.uncertainty, other.end+other.uncertainty))

        raise NotImplementedError(f'Not implemented {self} < {type(other)}={other}')
    def __lt__(self, other):
        x = self.prob_lt(other)
        if x in [0.0, 1.0]:
            return x == 1.0
        else:
            raise AmbiguousComparisonException(self, other, '<', x)
    def prob_gt(self, other):
        return 1.0 - self.prob_lt(other)
    def __gt__(self, other):
        x = self.prob_gt(other)
        if x in [0.0, 1.0]:
            return x == 1.0
        else:
            raise AmbiguousComparisonException(self, other, '>', x)
    def prob_le(self, other):
        return self.prob_lt(other)
    def __le__(self, other):
        return self < other
    def prob_ge(self, other):
        return self.prob_gt(other)
    def __ge__(self, other):
        return self > other


class DateRange():
    def __init__(self, start, end):
        if (start is not None) and not isinstance(start, date):
            raise Exception("'start' must be a 'date' object or None, not {0}".format(type(start)))
        if (end is not None) and not isinstance(end, date):
            raise Exception("'end' must be a 'date' object or None, not {0}".format(type(end)))
        self.start = start
        self.end = end

    def copy(self):
        return DateRange(self.start, self.end)

    def __str__(self):
        s = f"{self.start}->{self.end}"
        return s

    def __repr__(self):
        return self.__str__()

    def __abs__(self):
        raise NotImplementedError(f'Not implemented {self} abs')

    def __eq__(self, other):
        if isinstance(other, DateRange):
            return self.start == other.start and self.end == other.end
        elif isinstance(other, (date, DateEstimate)):
            return False
        raise NotImplementedError(f'Not implemented {self} == {type(other)}={other}')

    def isclose(self, other):
        if isinstance(other, DateRange):
            return (other.start <= self.end) and (self.start <= other.end)
        elif isinstance(other, date):
            return self.start <= other and other <= self.end
        elif isinstance(other, DateEstimate):
            return self.start <= other.date+other.uncertainty and other.date-other.uncertainty <= self.end
        elif isinstance(other, DateRangeEstimate):
            return other.isclose(self)
        raise NotImplementedError(f'Not implemented {self} is-close-to {type(other)}={other}')

    def prob_lt(self, other):
        if isinstance(other, date):
            if self.start >= other:
                return 0.0
            elif self.end < other:
                return 1.0
            else:
                return uniform_prob_lt((self.start, self.end), other)
        elif isinstance(other, DateRange):
            if other.end <= self.start:
                return 0.0
            elif self.end < other.start:
                return 1.0
            else:
                return uniform_prob_lt((self.start, self.end), (other.start, other.end))
        elif isinstance(other, DateEstimate):
            return other.prob_gt(self)
        raise NotImplementedError(f'Not implemented {self} < {type(other)}={other}')
    def __lt__(self, other):
        x = self.prob_lt(other)
        if x in [0.0, 1.0]:
            return x == 1.0
        else:
            raise AmbiguousComparisonException(self, other, '<', x)
    def prob_gt(self, other):
        return 1.0 - self.prob_lt(other)
    def __gt__(self, other):
        x = self.prob_gt(other)
        if x in [0.0, 1.0]:
            return x == 1.0
        else:
            raise AmbiguousComparisonException(self, other, '>', x)
    def prob_le(self, other):
        return self.prob_lt(other)
    def __le__(self, other):
        return self < other
    def prob_ge(self, other):
        return self.prob_gt(other)
    def __ge__(self, other):
        return self > other

    def __iadd__(self, other):
        if isinstance(other, (timedelta, pd.Timedelta)):
            self.start += other
            self.end += other
            return self
        raise NotImplementedError(f'Not implemented {self} += {type(other)}={other}')

    def __add__(self, other):
        if isinstance(other, timedelta):
            return DateRange(self.start + other, self.end + other)
        raise NotImplementedError(f'Not implemented {self} + {type(other)}={other}')

    # def __radd__(self, other):
    #     raise NotImplementedError(f'Not implemented {self} radd {type(other)}={other}')
    def __radd__(self, other):
        return self.__add__(other)

    def __isub__(self, other):
        raise NotImplementedError(f'Not implemented {self} -= {type(other)}={other}')

    def __sub__(self, other):
        if isinstance(other, date):
            return TimedeltaRange(self.start - other, self.end - other)
        elif isinstance(other, DateRange):
            return TimedeltaRange(self.start - other.end, self.end - other.start)
        elif isinstance(other, DateEstimate):
            return TimedeltaRangeEstimate(self.start - other.date, self.end - other.date, other.confidence)
        elif isinstance(other, DateRangeEstimate):
            return TimedeltaRangeEstimate(self.start - other.end, self.end - other.start, other.confidence)
        elif isinstance(other, timedelta):
            return DateRange(self.start - other, self.end - other)
        raise NotImplementedError(f'Not implemented {self} - {type(other)}={other}')

    def __rsub__(self, other):
        if isinstance(other, date):
            return TimedeltaRange(other - self.start, other - self.end)
        raise NotImplementedError(f'Not implemented {self} rsub {type(other)}={other}')

    def __neg__(self):
        raise NotImplementedError(f'Not implemented {self} negate')

    def __invert__(self):
        raise NotImplementedError(f'Not implemented {self} invert')


class DateEstimate():
    def __init__(self, dt, confidence):
        if not isinstance(confidence, Confidence):
            raise Exception("'confidence' must be a 'Confidence' object, not {0}".format(type(confidence)))
        if isinstance(dt, (datetime, pd.Timestamp)) or not isinstance(dt, (date, DateEstimate)):
            raise Exception("'dt' must be a 'date' object or None, not {0}".format(type(dt)))
        if isinstance(dt, DateEstimate):
            self.date = dt.date
            self.confidence = min(dt.confidence, confidence)
        else:
            self.date = dt
            self.confidence = confidence
        self.uncertainty = confidence_to_buffer[confidence]

    def copy(self):
        return DateEstimate(self.date, self.confidence)

    def __str__(self):
        s = f"{self.date} (conf={self.confidence}/2)"
        return s

    def __repr__(self):
        return self.__str__()

    def prob_lt(self, other):
        if isinstance(other, date):
            if self.date + self.uncertainty < other:
                return 1.0
            elif other <= self.date - self.uncertainty:
                return 0.0
            return uniform_prob_lt((self.date-self.uncertainty, self.date+self.uncertainty), other)
        elif isinstance(other, DateEstimate):
            if self.date + self.uncertainty < other.date - other.uncertainty:
                return 1.0
            elif other.date + other.uncertainty <= self.date - self.uncertainty:
                return 0.0
            return uniform_prob_lt((self.date-self.uncertainty, self.date+self.uncertainty), 
                                  (other.date-other.uncertainty, other.date+other.uncertainty))
        elif isinstance(other, DateRange):
            p = uniform_prob_lt((self.date-self.uncertainty, self.date+self.uncertainty), 
                               (other.start, other.end))
            return p
        elif isinstance(other, DateRangeEstimate):
            return other.__ge__(self)

        raise NotImplementedError(f'Not implemented {self} < {type(other)}={other}')
    def __lt__(self, other):
        x = self.prob_lt(other)
        if x in [0.0, 1.0]:
            return x == 1.0
        else:
            raise AmbiguousComparisonException(self, other, '<', x)
    def prob_gt(self, other):
        return 1.0 - self.prob_lt(other)
    def __gt__(self, other):
        x = self.prob_gt(other)
        if x in [0.0, 1.0]:
            return x == 1.0
        else:
            raise AmbiguousComparisonException(self, other, '>', x)
    def prob_le(self, other):
        return self.prob_lt(other)
    def __le__(self, other):
        return self < other
    def prob_ge(self, other):
        return self.prob_gt(other)
    def __ge__(self, other):
        return self > other

    def __abs__(self):
        raise NotImplementedError(f'Not implemented {self} abs')

    def __eq__(self, other):
        if isinstance(other, DateEstimate):
            return self.date == other.date and self.confidence == other.confidence
        elif isinstance(other, date):
            if self.isclose(other):
                raise AmbiguousComparisonException(self, other, '==')
            else:
                return False
        raise NotImplementedError(f'Not implemented {self} == {type(other)}={other}')

    def isclose(self, other):
        if isinstance(other, DateEstimate):
            return abs(self.date - other.date) <= min(self.uncertainty, other.uncertainty)
        elif isinstance(other, DateRangeEstimate):
            return other.isclose(self)
        else:
            return abs(self.date - other) <= self.uncertainty
        raise NotImplementedError(f'Not implemented {self} is-close-to {type(other)}={other}')

    def __iadd__(self, other):
        if isinstance(other, (timedelta, relativedelta, ComparableRelativedelta)):
            self.date += other
            return self
        raise NotImplementedError(f'Not implemented {self} += {type(other)}={other}')

    def __add__(self, other):
        if isinstance(other, TimedeltaEstimate):
            return DateEstimate(self.date+other.td, min(self.confidence, other.confidence))
        elif isinstance(other, (timedelta, relativedelta, ComparableRelativedelta)):
            return DateEstimate(self.date+other, self.confidence)
        elif isinstance(other, TimedeltaRange):
            return DateRangeEstimate(self.date + other.td1, self.date + other.td2, self.confidence)
        raise NotImplementedError(f'Not implemented {self} + {type(other)}={other}')

    def __radd__(self, other):
        return self.__add__(other)

    def __isub__(self, other):
        if isinstance(other, (timedelta, relativedelta, ComparableRelativedelta)):
            self.date -= other
            return self
        raise NotImplementedError(f'Not implemented {self} -= {type(other)}={other}')

    def __sub__(self, other):
        if isinstance(other, (timedelta, relativedelta, ComparableRelativedelta)):
            return DateEstimate(self.date-other, self.confidence)
        elif isinstance(other, TimedeltaEstimate):
            return DateEstimate(self.date-other.td, min(self.confidence, other.confidence))
        elif isinstance(other, DateEstimate):
            td = self.date - other.date
            c0 = self.confidence
            c1 = other.confidence
            return TimedeltaEstimate(td, min(c0, c1))
        elif isinstance(other, date):
            return TimedeltaEstimate(self.date-other, self.confidence)
        elif isinstance(other, DateRange):
            return TimedeltaRangeEstimate(self.date - other.start, self.date - other.end, self.confidence)
        elif isinstance(other, DateRangeEstimate):
            return TimedeltaRangeEstimate(self.date - other.start, self.date - other.end, min(self.confidence, other.confidence))
        raise NotImplementedError(f'Not implemented {self} - {type(other)}={other}')

    def __rsub__(self, other):
        if isinstance(other, DateEstimate):
            return other - self
        elif isinstance(other, date):
            return TimedeltaEstimate(other - self.date, self.confidence)
        raise NotImplementedError(f'Not implemented {self} rsub {type(other)}={other}')

    def __neg__(self):
        raise NotImplementedError(f'Not implemented {self} negate')

    def __invert__(self):
        raise NotImplementedError(f'Not implemented {self} invert')

