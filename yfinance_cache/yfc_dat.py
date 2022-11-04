from enum import Enum
from datetime import timedelta
from zoneinfo import ZoneInfo
import numpy as np

yf_data_cols = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume', 'Dividends', 'Stock Splits']
yf_min_year = 1950


class DateInterval:
	def __init__(self, left, right, closed=None):
		self.left = left
		self.right = right

		if closed is None:
			self.closed = None
		else:
			if not closed in ["left","right"]:
				raise Exception("closed must be left or right")
			self.closed = closed

	def __eq__(self, other):
		return self.left==other.left and self.right==other.right and self.closed==other.closed

	def __str__(self):
		s = ""
		if self.closed=="left":
			s += '['
		else:
			s += '('
		s += str(self.left) + ', ' + str(self.right)
		if self.closed=="right":
			s += ']'
		else:
			s += ')'
		return s

	def __repr__(self):
		return self.__str__()

class DateIntervalIndex:
	def __init__(self, intervals):
		if not isinstance(intervals, np.ndarray):
			self.array = np.array(intervals)
		else:
			self.array = intervals

	@classmethod
	def from_arrays(cls, left, right, closed=None):
		if len(left) != len(right):
			raise Exception("left and right must be equal length")
		l = [DateInterval(left[i], right[i], closed) for i in range(len(left))]
		return cls(l)

	@property
	def left(self):
		return np.array([x.left for x in self.array])

	@property
	def right(self):
		return np.array([x.right for x in self.array])

	@property
	def shape(self):
		return (len(self.array),2)

	def __getitem__(self, i):
		v = self.array[i]
		if isinstance(v, np.ndarray):
			v = DateIntervalIndex(v)
		return v

	def __setitem__(self,i,v):
		raise Exception("immutable")

	def __eq__(self,other):
		if not isinstance(other, DateIntervalIndex):
			return False
		if len(self.array) != len(other.array):
			return False
		return np.equal(self.array, other.array).all()

	def __str__(self):
		s = "[ "
		for x in self.array:
			s += x.__str__() + " , "
		s += "]"
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
periodStrToEnum = {v:k for k,v in periodToString.items()}

class Interval(Enum):
	Mins1 = 0
	Mins2 = 1
	Mins5 = 2
	Mins15 = 3
	Mins30 = 4
	Mins60 = 5
	Mins90 = 6
	Hours1 = 10
	Days1 = 20
	Week = 30
	Months1 = 40
	Months3 = 41
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
intervalStrToEnum = {v:k for k,v in intervalToString.items()}
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
# intervalToTimedelta[Interval.Months1] = None ## irregular time interval
# intervalToTimedelta[Interval.Months3] = None ## irregular time interval


exchangeToXcalExchange = {}
# USA:
exchangeToXcalExchange["NYQ"] = "XNYS"
exchangeToXcalExchange["ASE"] = exchangeToXcalExchange["NYQ"]
exchangeToXcalExchange["PCX"] = exchangeToXcalExchange["NYQ"]  # NYSE Arca
exchangeToXcalExchange["PNK"] = exchangeToXcalExchange["NYQ"]  # OTC
exchangeToXcalExchange["NCM"] = "NASDAQ"
exchangeToXcalExchange["NGM"] = exchangeToXcalExchange["NCM"]
exchangeToXcalExchange["NMS"] = exchangeToXcalExchange["NCM"]
# Canada:
exchangeToXcalExchange["TOR"] = "XTSE" # Toronto
exchangeToXcalExchange["VAN"] = exchangeToXcalExchange["TOR"] # TSX Venture
exchangeToXcalExchange["CNQ"] = exchangeToXcalExchange["TOR"] # CSE. TSX competitor, but has same hours
# Europe:
exchangeToXcalExchange["LSE"] = "XLON" # London
exchangeToXcalExchange["IOB"] = exchangeToXcalExchange["LSE"]
exchangeToXcalExchange["AMS"] = "XAMS" # Amsterdam
exchangeToXcalExchange["EBS"] = "XSWX" # Zurich
exchangeToXcalExchange["ISE"] = "XDUB" # Ireland
exchangeToXcalExchange["MCE"] = "XMAD" # Madrid
exchangeToXcalExchange["MIL"] = "XMIL" # Milan
exchangeToXcalExchange["OSL"] = "XOSL" # Oslo
exchangeToXcalExchange["PAR"] = "XPAR" # Paris
exchangeToXcalExchange["GER"] = "XFRA" # Frankfurt. Germany also has XETRA but that's part of Frankfurt exchange
exchangeToXcalExchange["STO"] = "XSTO" # Stockholm
# Other:
exchangeToXcalExchange["TLV"] = "XTAE" # Israel
exchangeToXcalExchange["JNB"] = "XJSE" # Johannesburg
exchangeToXcalExchange["SAO"] = "BVMF" # Sao Paulo
exchangeToXcalExchange["JPX"] = "JPX"  # Tokyo
exchangeToXcalExchange["ASX"] = "ASX" # Australia
exchangeToXcalExchange["NZE"] = "XNZE" # New Zealand

## Yahoo specify data delays here:
## https://help.yahoo.com/kb/SLN2310.html?guccounter=1
exchangeToYfLag = {}
# USA:
exchangeToYfLag["NYQ"] = timedelta(seconds=0)
exchangeToYfLag["ASE"] = exchangeToYfLag["NYQ"]
exchangeToYfLag["PCX"] = exchangeToYfLag["NYQ"]
exchangeToYfLag["PNK"] = timedelta(minutes=15)
exchangeToYfLag["NCM"] = exchangeToYfLag["ASE"]
exchangeToYfLag["NGM"] = exchangeToYfLag["ASE"]
exchangeToYfLag["NMS"] = exchangeToYfLag["ASE"]
# Canada:
exchangeToYfLag["TOR"] = timedelta(seconds=0)
exchangeToYfLag["VAN"] = exchangeToYfLag["TOR"]
exchangeToYfLag["CNQ"] = exchangeToYfLag["TOR"]
# Europe:
exchangeToYfLag["LSE"] = timedelta(minutes=20)
exchangeToYfLag["IOB"] = timedelta(minutes=20)
exchangeToYfLag["AMS"] = timedelta(minutes=15)
exchangeToYfLag["EBS"] = timedelta(minutes=30)
exchangeToYfLag["GER"] = timedelta(minutes=15)
exchangeToYfLag["ISE"] = timedelta(minutes=15)
exchangeToYfLag["MCE"] = timedelta(minutes=15)
exchangeToYfLag["MIL"] = timedelta(minutes=20)
exchangeToYfLag["OSL"] = timedelta(minutes=15)
exchangeToYfLag["PAR"] = timedelta(minutes=15)
exchangeToYfLag["STO"] = timedelta(0)
# Other:
exchangeToYfLag["TLV"] = timedelta(minutes=20)
exchangeToYfLag["JNB"] = timedelta(minutes=15)
exchangeToYfLag["SAO"] = timedelta(minutes=15)
exchangeToYfLag["JPX"] = timedelta(minutes=20)
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
exchangeAuctionDuration["TLV"] = timedelta(minutes=2) # One extra minute because of randomised start time


class NoIntervalsInRangeException(Exception):
	def __init__(self,interval,start_dt,end_dt,*args):
		super().__init__(args)
		self.interval = interval
		self.start_dt = start_dt
		self.end_dt = end_dt

	def __str__(self):
		return ("No {} intervals found between {}->{}".format(self.interval, self.start_dt, self.end_dt))
		
class NoPriceDataInRangeException(Exception):
	def __init__(self,tkr,interval,start_dt,end_dt,*args):
		super().__init__(args)
		self.tkr = tkr
		self.interval = interval
		self.start_dt = start_dt
		self.end_dt = end_dt

	def __str__(self):
		return ("No {}-price data fetched for ticker {} between dates {} -> {}".format(self.interval, self.tkr, self.start_dt, self.end_dt))
