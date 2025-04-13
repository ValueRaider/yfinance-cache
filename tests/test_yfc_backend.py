import unittest
from pprint import pprint

from .context import yfc_dat as yfcd
from .context import yfc_time as yfct
from .context import yfc_cache_manager as yfcm
from .context import yfc_prices_manager as yfcp
from .context import yfc_utils as yfcu
from .context import yfc_ticker as yfc
from .context import session_gbl
from .utils import Test_Base

import yfinance as yf

import tempfile

import pandas as pd
import numpy as np
from time import sleep
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo
import pytz
import os
import appdirs

## 2022 calendar:
## X* = day X is public holiday that closed exchange
##
## USA
##  -- February --
##  Mo   Tu   We   Th   Fr   Sa   Su
##  7    8    9    10   11   12   13
##  14   15   16   17   18   19   20
##  21*  22   23   24   25   26   27
##  28
##
## New Zealand
##  -- April --
##  Mo   Tu   We   Th   Fr   Sa   Su
##  4    5    6    7    8    9    10
##  11   12   13   14   15*  16   17
##  18*  19   20   21   22   23   24
##  25*
##
## London
##  -- April --
##  Mo   Tu   We   Th   Fr   Sa   Su
##  4    5    6    7    8    9    10
##  11   12   13   14   15*  16   17
##  18*  19   20   21   22   23   24
##  25
##
## Taiwan
##  -- February --
##  Mo   Tu   We   Th   Fr   Sa   Su
##  -    1*   2*   3*   4*   5*   6*
##  7    8    9    10   11   12   13
##  14   15   16   17   18   19   20
##  21*  22   23   24   25   26   27
##  28*

# class Test_Yfc_Backend(unittest.TestCase):
class Test_Yfc_Backend(Test_Base):

    def setUp(self):
        self.tempCacheDir = tempfile.TemporaryDirectory()
        yfcm.SetCacheDirpath(self.tempCacheDir.name)

        self.session = session_gbl

        self.usa_tkr = "INTC"
        self.usa_market = "us_market"
        self.usa_exchange = "NMS"
        self.usa_market_tz_name = 'America/New_York'
        self.usa_market_tz = ZoneInfo('America/New_York')
        self.usa_market_open_time  = time(9, 30)
        self.usa_market_close_time = time(16)
        self.usa_dat = yfc.Ticker(self.usa_tkr, session=self.session)

        self.td1h = timedelta(hours=1)
        self.td1d = timedelta(days=1)

    def tearDown(self):
        self.tempCacheDir.cleanup()
        self.session.close()

    def test_yf_lag(self):
        ## Only use high-volume stocks:
        tkr_candidates = ["AZN.L", "ASML.AS", "BHG.JO", "INTC", "MEL.NZ"]

        dt_now = pd.Timestamp.utcnow().replace(tzinfo=ZoneInfo("UTC"))

        for tkr in tkr_candidates:
            dat = yfc.Ticker(tkr, session=self.session)
            if not yfct.IsTimestampInActiveSession(dat.fast_info["exchange"], dt_now):
                continue
            expected_lag = yfcd.exchangeToYfLag[dat.fast_info["exchange"]]

            dat = yfc.Ticker(tkr, session=None) # Use live data

            # First call with temp-cache means will calculate lag:
            lag = dat.yf_lag
            if lag > expected_lag:
                diff = lag - expected_lag
            else:
                diff = expected_lag - lag
            tolerance = timedelta(minutes=10)
            try:
                self.assertLess(diff, tolerance)
            except:
                pprint("lag: {0}".format(lag))
                pprint("expected_lag: {0}".format(expected_lag))
                pprint("diff: {0}".format(diff))
                raise

            # Confirm that fetching from cache returns same value:
            lag_cache = dat.yf_lag
            self.assertEqual(lag, lag_cache)

    def test_history_backend_usa(self):
        # index should always be DatetimeIndex

        yfct.SetExchangeTzName(self.usa_exchange, self.usa_market_tz_name)

        intervals = ["30m", "1h", "1d"]
        start_d = date.today() -self.td1d
        start_d = start_d - timedelta(days=start_d.weekday())
        while not yfct.ExchangeOpenOnDay(self.usa_exchange, start_d):
            start_d -= self.td1d
        end_d = start_d +self.td1d
        for interval in intervals:
            df = self.usa_dat.history(start=start_d, end=end_d, interval=interval)
            self.assertTrue(isinstance(df.index, pd.DatetimeIndex))

        interval = "1wk"
        start_d -= timedelta(days=start_d.weekday())
        while not yfct.ExchangeOpenOnDay(self.usa_exchange, start_d):
            start_d -= timedelta(days=7)
        end_d = start_d+timedelta(days=5)
        df = self.usa_dat.history(start=start_d, end=end_d, interval=interval)
        self.assertTrue(isinstance(df.index, pd.DatetimeIndex))

    def test_detect_stock_listing(self):
        # RYDE listed on 2024-03-06
        lday = date(2024, 3, 6)
        # Stress-test listing-date detection:
        tkr="RYDE"
        dat = yfc.Ticker(tkr, session=self.session)

        # Init cache:
        dat.history(start="2024-06-01", end="2025-01-01", interval="1d")

        # If detection failed, then next call will fail
        start="2024-05-20"
        try:
            df = dat.history(start=start, end="2025-01-01", interval="1d")
        except:
            raise Exception("history() failed, indicates problem with detecting/handling listing-date")

        self.assertEqual(lday, dat._listing_day)

    def test_reverseYahooAdjust(self):
        tkr = '8TRA.DE'
        interval = yfcd.Interval.Days1
        dat = yfc.Ticker(tkr)

        exchange = dat.info['exchange']
        if "exchangeTimezoneName" in dat.info:
            tz_name = dat.info["exchangeTimezoneName"]
        else:
            tz_name = dat.info["timeZoneFullName"]
        lday = date(2019, 6, 1)
        hm = yfcp.HistoriesManager(tkr, exchange, tz_name, lday, self.session, None)

        # Step 1: add a dividend into system
        div = 0.7
        div_date = date(2023, 6, 2)
        div_close_before = 18.79
        fetch_dt = pd.Timestamp(datetime(2023, 6, 1, 14, 1)).tz_localize(tz_name)
        divs_df = pd.DataFrame(index=[div_date], data={'Close before':[div_close_before], 'Dividends':[div], "FetchDate":[fetch_dt]})
        divs_df.index = pd.to_datetime(divs_df.index).tz_localize(tz_name)
        divs_df['Close repaired?'] = False
        hm.GetHistory("Events").UpdateDividends(divs_df)

        # Step 2: test _reverseYahooAdjust()
        df = pd.read_csv(os.path.join('./tests/Adjustment/TestCase_missingDivAdjust', tkr.replace('.','-')+"-missing-div-adjust.csv"), index_col="Date")
        df.index = pd.to_datetime(df.index)
        df2 = hm.GetHistory(interval)._reverseYahooAdjust(df)

        df_correct = pd.read_csv(os.path.join('./tests/Adjustment/TestCase_missingDivAdjust', tkr.replace('.','-')+"-missing-div-adjust-fixed.csv"), index_col="Date")
        self.verify_df(df2, df_correct, 1e-10)

if __name__ == '__main__':
    unittest.main()

    # # Run tests sequentially:
    # import inspect
    # test_src = inspect.getsource(Test_Yfc_Backend)
    # unittest.TestLoader.sortTestMethodsUsing = lambda _, x, y: (
    #     test_src.index(f"def {x}") - test_src.index(f"def {y}")
    # )
    # unittest.main(verbosity=2)
