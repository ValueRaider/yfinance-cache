from enum import Enum
import os
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
import re

class OperatingSystem(Enum):
	Windows = 1
	Linux = 2
	OSX = 3


def GetOperatingSystem():
	if os.name == "nt":
		return OperatingSystem.Windows
	elif os.name == "posix":
		return OperatingSystem.Linux
	else:
		raise Exception("Unknwon os.name = '{0}'".format(os.name))


def GetUserCacheDirpath():
	_os = GetOperatingSystem()
	if _os == OperatingSystem.Windows:
		dp = os.getenv("APPDATA")
		raise Exception("Not tested. Does this make sense as cache dir in Windows? - {0}".format(dp))
	elif _os == OperatingSystem.Linux:
		dp = os.path.join(os.getenv("HOME"), ".cache")
	else:
		raise Exception("Not implemented: cache dirpath under OS '{0}'".format(_os))
	return dp


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
		if isinstance(v,str) and v.startswith("timedelta-"):
			try:
				sfx = '-'.join(v.split('-')[1:])
				sfxf = float(sfx)
				value[k] = timedelta(seconds=sfxf)
			except:
				pass
		else:
			## TODO: add suffix "date-" or "datetime-". Will need to upgrade existing cache
			decoded = False
			try:
				value[k] = date.fromisoformat(v)
				decoded = True
			except:
				pass
			if not decoded:
				try:
					value[k] = datetime.fromisoformat(v)
					decoded = True
				except:
					pass

	return value


def GetSigFigs(n):
	if n == 0:
		return 0
	n_str = str(n).replace('.', '')
	m = re.match( r'0*[1-9](\d*[1-9])?', n_str)
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
	elif round(n,0)==n:
		return 0
	else:
		return sigfigs - GetSigFigs(round(n))


def ReverseYahooBackAdjust(df, pre_csf=None):
	# Reverse Yahoo's back adjustment. 
	# Note: Yahoo always returns split-adjusted price, so reverse that

	# If 'df' does not contain all stock splits until present, then
	# set 'pre_csf' to cumulative stock split factor just after last 'df' date
	last_dt = df.index[-1]
	dt_now = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
	# thr = 5
	thr = 10 # Extend threshold for weekly data
	if (dt_now-last_dt) > timedelta(days=thr):
		if pre_csf is None:
			raise Exception("Data is older than {} days, need to set 'pre_csf' arg to capture all stock splits since".format(tkr))

	# Cumulative dividend factor:
	cdf = df["Adj Close"] / df["Close"]
	
	# Cumulative stock-split factor
	ss = df["Stock Splits"].copy()
	ss[ss==0.0] = 1.0
	ss_rcp = 1.0/ss
	csf = ss_rcp.sort_index(ascending=False).cumprod().sort_index(ascending=True)
	csf = csf.shift(-1, fill_value=1.0)
	if not pre_csf is None:
		csf *= pre_csf
	csf_rcp = 1.0/csf

	# Reverse Yahoo's split adjustment:
	data_cols = ["Open","High","Low","Close","Dividends"]
	for dc in data_cols:
		df[dc] = df[dc] * csf_rcp
	df["Volume"] *= csf

	# Drop 'Adj Close', replace with scaling factors:
	df = df.drop("Adj Close",axis=1)
	df["CSF"] = csf
	df["CDF"] = cdf
	
	return df
