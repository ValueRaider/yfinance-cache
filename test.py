import sys
sys.path.append("/home/gonzo/Repos/yfinance-cache/src/yfinance_cache")
import yfc_ticker as yfc
from yfc_utils import *

from pprint import pprint

from datetime import datetime

tkr = "INTC"
dat = yfc.Ticker(tkr)

# pprint(dat.info)
# pprint({k:dat.info[k] for k in dat.info if "dividend" in k.lower()})
# pprint({k:dat.info[k] for k in dat.info if "exchange" in k.lower()})
# pprint({k:dat.info[k] for k in dat.info if "market" in k.lower()})
# pprint({k:dat.info[k] for k in dat.info if "time" in k.lower()})

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

h = dat.history(period=Period.Months1,
				interval=Interval.Days1, 
				prepost=False,
				actions=False)
# h = dat.history(period=Period.Days5,
# 				interval=Interval.Days1, 
# 				prepost=False,
# 				actions=False)
# h = dat.history(period=Period.Days1,
# 				interval=Interval.Hours1,
# 				prepost=False,
# 				actions=False)
print(h)
# print(h.shape)
# print(h.columns)
# print(h.index)
# d = h.index[-1]
# print(d)
