import sys
sys.path.append("/home/gonzo/Repos/yfinance-cache/src/yfinance_cache")
import yfc_ticker as yfc
from yfc_utils import *

from pprint import pprint

from datetime import datetime

tkr = "INTC"
dat = yfc.Ticker(tkr)

# pprint(dat.info)
# pprint({k:dat.info[k] for k in dat.info if "ividen" in k})
# pprint({k:dat.info[k] for k in dat.info if "change" in k})

# dn = dat.info["lastDividendDate"]
# dt = datetime.fromtimestamp(dn)
# print(dt)

# qe = dat.quarterly_earnings
# print(qe)

# qf = dat.quarterly_financials
# print(qf)

# print(dat.calendar)
# print(dat.recommendations)
# print(dat.major_holders)
# print(dat.institutional_holders)

# h = dat.history()
# h = dat.history(period=Period.Days5)
# h = dat.history(period=Period.Days5,
# 				interval=Interval.Days1)
h = dat.history(period=Period.Days5,
				interval=Interval.Days1, 
				actions=False)
				# prepost=False,
print(h)
