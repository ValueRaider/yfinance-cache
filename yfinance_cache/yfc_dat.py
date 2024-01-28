from enum import Enum
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta

import numpy as np
import pandas as pd

yf_price_data_cols = ['Open', 'High', 'Low', 'Close', 'Adj Close']
yf_data_cols = yf_price_data_cols+['Volume', 'Dividends', 'Stock Splits']
yf_min_year = 1950


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
        return (self == other).all()

    def __str__(self):
        s = "DateIntervalIndex([ "
        for x in self.array:
            s += x.__str__() + " , "
        s += "])"
        return s

    def __repr__(self):
        return self.__str__()


class Period(Enum):
    Days1 = 0
    Days5 = 1
    Week = 2
    Months1 = 10
    Months3 = 11
    Months6 = 12
    Years1 = 20
    Years2 = 21
    Years5 = 22
    Years10 = 23
    Ytd = 24
    Max = 30
periodToString = {}
periodToString[Period.Days1] = "1d"
periodToString[Period.Days5] = "5d"
periodToString[Period.Week] = "1wk"
periodToString[Period.Months1] = "1mo"
periodToString[Period.Months3] = "3mo"
periodToString[Period.Months6] = "6mo"
periodToString[Period.Years1] = "1y"
periodToString[Period.Years2] = "2y"
periodToString[Period.Years5] = "5y"
periodToString[Period.Years10] = "10y"
periodToString[Period.Ytd] = "ytd"
periodToString[Period.Max] = "max"
periodStrToEnum = {v: k for k, v in periodToString.items()}
periodToTimedelta = {}
periodToTimedelta[Period.Days1] = timedelta(days=1)
periodToTimedelta[Period.Days5] = timedelta(days=5)
periodToTimedelta[Period.Week] = timedelta(days=7)
periodToTimedelta[Period.Months1] = relativedelta(months=1)
periodToTimedelta[Period.Months3] = relativedelta(months=3)
periodToTimedelta[Period.Months6] = relativedelta(months=6)
periodToTimedelta[Period.Years1] = relativedelta(years=1)
periodToTimedelta[Period.Years2] = relativedelta(years=2)
periodToTimedelta[Period.Years5] = relativedelta(years=5)
periodToTimedelta[Period.Years10] = relativedelta(years=10)


class Interval(Enum):
    Months3 = 0
    Months1 = 2
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
intervalToString[Interval.Months1] = "1mo"
intervalToString[Interval.Months3] = "3mo"
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
intervalToTimedelta[Interval.Months1] = relativedelta(months=1)
intervalToTimedelta[Interval.Months3] = relativedelta(months=3)


exchangeToXcalExchange = {}
# USA:
exchangeToXcalExchange["NYQ"] = "XNYS"
exchangeToXcalExchange["ASE"] = exchangeToXcalExchange["NYQ"]
exchangeToXcalExchange["PCX"] = exchangeToXcalExchange["NYQ"]  # NYSE Arca
exchangeToXcalExchange["PNK"] = exchangeToXcalExchange["NYQ"]  # OTC
exchangeToXcalExchange["NCM"] = "NASDAQ"
exchangeToXcalExchange["NGM"] = exchangeToXcalExchange["NCM"]
exchangeToXcalExchange["NMS"] = exchangeToXcalExchange["NCM"]
exchangeToXcalExchange["BTS"] = exchangeToXcalExchange["NYQ"]  # Cboe BZX formerly known as BATS
# Canada:
exchangeToXcalExchange["TOR"] = "XTSE"  # Toronto
exchangeToXcalExchange["VAN"] = exchangeToXcalExchange["TOR"]  # TSX Venture
exchangeToXcalExchange["CNQ"] = exchangeToXcalExchange["TOR"]  # CSE. TSX competitor, but has same hours
# Europe:
exchangeToXcalExchange["LSE"] = "XLON"  # London
exchangeToXcalExchange["IOB"] = exchangeToXcalExchange["LSE"]
exchangeToXcalExchange["AMS"] = "XAMS"  # Amsterdam
exchangeToXcalExchange["ATH"] = "ASEX"  # Athens
exchangeToXcalExchange["BRU"] = "XBRU"  # Brussels
exchangeToXcalExchange["CPH"] = "XCSE"  # Copenhagen
exchangeToXcalExchange["EBS"] = "XSWX"  # Zurich
exchangeToXcalExchange["FRA"] = "XFRA"  # Frankfurt. Germany also has XETRA but that's part of Frankfurt exchange
exchangeToXcalExchange["GER"] = "XFRA"  # Frankfurt
exchangeToXcalExchange["HAM"] = exchangeToXcalExchange["GER"] # Hamburg, assume same as Frankfurt
exchangeToXcalExchange["HEL"] = "XHEL"  # Helsinki
exchangeToXcalExchange["ISE"] = "XDUB"  # Ireland
exchangeToXcalExchange["MCE"] = "XMAD"  # Madrid
exchangeToXcalExchange["MIL"] = "XMIL"  # Milan
exchangeToXcalExchange["OSL"] = "XOSL"  # Oslo
exchangeToXcalExchange["PAR"] = "XPAR"  # Paris
exchangeToXcalExchange["STO"] = "XSTO"  # Stockholm
exchangeToXcalExchange["VIE"] = "XWBO"  # Vienna
exchangeToXcalExchange["WSE"] = "XWAR"  # Warsaw
# Other:
exchangeToXcalExchange["TLV"] = "XTAE"  # Israel
exchangeToXcalExchange["JNB"] = "XJSE"  # Johannesburg, South Africa
exchangeToXcalExchange["SAO"] = "BVMF"  # Sao Paulo, Brazil
exchangeToXcalExchange["SGO"] = "XSGO"  # Santiago, Chile
exchangeToXcalExchange["BVC"] = "XBOG"  # Bogota, Colombia
exchangeToXcalExchange["MEX"] = "XMEX"  # Mexico
exchangeToXcalExchange["JPX"] = "JPX"   # Tokyo
exchangeToXcalExchange["TAI"] = "XTAI"  # Taiwan
exchangeToXcalExchange["KSC"] = "XKRX"  # Korea
exchangeToXcalExchange["SES"] = "XSES"  # Singapore
exchangeToXcalExchange["HKG"] = "XHKG"  # Hong Kong
exchangeToXcalExchange["ASX"] = "ASX"   # Australia
exchangeToXcalExchange["NZE"] = "XNZE"  # New Zealand

exchangesWithBreaks = {"HKG"}

# Yahoo specify data delays here:
# https://help.yahoo.com/kb/SLN2310.html?guccounter=1
exchangeToYfLag = {}
# USA:
exchangeToYfLag["NYQ"] = timedelta(seconds=0)
exchangeToYfLag["ASE"] = exchangeToYfLag["NYQ"]
exchangeToYfLag["PCX"] = exchangeToYfLag["NYQ"]
exchangeToYfLag["PNK"] = timedelta(minutes=15)
exchangeToYfLag["NCM"] = exchangeToYfLag["ASE"]
exchangeToYfLag["NGM"] = exchangeToYfLag["ASE"]
exchangeToYfLag["NMS"] = exchangeToYfLag["ASE"]
exchangeToYfLag["BTS"] = exchangeToYfLag["NYQ"]
# Canada:
exchangeToYfLag["TOR"] = timedelta(seconds=0)
exchangeToYfLag["VAN"] = exchangeToYfLag["TOR"]
exchangeToYfLag["CNQ"] = exchangeToYfLag["TOR"]
# Europe:
exchangeToYfLag["LSE"] = timedelta(minutes=20)
exchangeToYfLag["IOB"] = timedelta(minutes=20)
exchangeToYfLag["AMS"] = timedelta(minutes=15)
exchangeToYfLag["ATH"] = timedelta(minutes=15)
exchangeToYfLag["BRU"] = timedelta(minutes=15)
exchangeToYfLag["CPH"] = timedelta(0)
exchangeToYfLag["EBS"] = timedelta(minutes=30)
exchangeToYfLag["FRA"] = timedelta(minutes=15)
exchangeToYfLag["GER"] = timedelta(minutes=15)
exchangeToYfLag["HAM"] = exchangeToYfLag["GER"]
exchangeToYfLag["HEL"] = timedelta(0)
exchangeToYfLag["ISE"] = timedelta(minutes=15)
exchangeToYfLag["MCE"] = timedelta(minutes=15)
exchangeToYfLag["MIL"] = timedelta(minutes=20)
exchangeToYfLag["OSL"] = timedelta(minutes=15)
exchangeToYfLag["PAR"] = timedelta(minutes=15)
exchangeToYfLag["STO"] = timedelta(0)
exchangeToYfLag["VIE"] = timedelta(minutes=15)
exchangeToYfLag["WSE"] = timedelta(minutes=15)
# Other:
exchangeToYfLag["TLV"] = timedelta(minutes=20)
exchangeToYfLag["JNB"] = timedelta(minutes=15)
exchangeToYfLag["SAO"] = timedelta(minutes=15)
exchangeToYfLag["SGO"] = timedelta(minutes=15)
exchangeToYfLag["BVC"] = timedelta(minutes=15)  # Guess because Yahoo don't specify
exchangeToYfLag["MEX"] = timedelta(minutes=20)
exchangeToYfLag["JPX"] = timedelta(minutes=20)
exchangeToYfLag["TAI"] = timedelta(minutes=20)
exchangeToYfLag["KSC"] = timedelta(minutes=20)
exchangeToYfLag["SES"] = timedelta(minutes=20)
exchangeToYfLag["HKG"] = timedelta(minutes=15)
exchangeToYfLag["ASX"] = timedelta(minutes=20)
exchangeToYfLag["NZE"] = timedelta(minutes=20)

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
yfMaxFetchRange[Interval.Months1] = None
yfMaxFetchRange[Interval.Months3] = None

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
yfMaxFetchLookback[Interval.Months1] = None
yfMaxFetchLookback[Interval.Months3] = None

listing_date_check_tols = {}
listing_date_check_tols[Interval.Days1] = timedelta(days=7)
listing_date_check_tols[Interval.Week] = timedelta(days=14)
listing_date_check_tols[Interval.Months1] = timedelta(days=35)
listing_date_check_tols[Interval.Months3] = timedelta(days=35*3)


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

