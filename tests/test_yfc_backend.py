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

    def test_detect_stock_listing1(self):
        # HLTH listed on 24/25 SEP 2021
        # Stress-test listing-date detection:
        tkr="HLTH"
        dat = yfc.Ticker(tkr, session=self.session)

        # Init cache:
        dat.history(start="2021-09-24", end="2021-10-03", interval="1d")

        # If detection failed, then next call will fail
        start="2021-09-20"
        try:
            df = dat.history(start=start, end="2021-10-03", interval="1d")
        except:
            raise Exception("history() failed, indicates problem with detecting/handling listing-date")

    def test_detect_stock_listing2(self):
        # HLTH listed on 24/25 SEP 2021
        # Stress-test listing-date detection:
        tkr="HLTH"
        dat = yfc.Ticker(tkr, session=self.session)

        # Init cache:
        dat.history(period="2y", interval="1d")

        # If detection failed, then next call will fail
        start="2021-09-20"
        try:
            df = dat.history(start=start, end="2021-10-03", interval="1d")
        except:
            raise Exception("history() failed, indicates problem with detecting/handling listing-date")

    def test_history_bug_pnl(self):
        # Ticker PNL.L missing 90 minutes of trading on morning of 2022-07-18, 
        # and Yahoo not returning NaN rows in place. So YFC needs to insert NaN rows

        tkr="PNL.L"
        exchange="LSE"
        tz_name="Europe/London"
        tz=ZoneInfo(tz_name)
        dat = yfc.Ticker(tkr, session=self.session)

        dt0 = datetime(2022, 7, 18, 8, 0, tzinfo=tz)
        dt1 = datetime(2022, 7, 18, 9, 0, tzinfo=tz)

        start = datetime(2022, 7, 18, 8, 0, tzinfo=tz)
        end   = datetime(2022, 7, 18, 10, 0, tzinfo=tz)
        df = dat.history(start=start, end=end, interval="1h", keepna=True)
        self.assertTrue(df.index[0]==dt0)
        self.assertTrue(df.index[1]==dt1)

        end = datetime(2022, 7, 18, 16, 0, tzinfo=tz)
        df = dat.history(start=start, end=end, interval="1h", keepna=True)
        self.assertTrue(df.index[0]==dt0)
        self.assertTrue(df.index[1]==dt1)

    def test_GetCDF0(self):
        tkr = "I3E.L"
        tz = ZoneInfo("Europe/London")

        # Setup DataFrame with real data:

        columns = ["Close", "Adj Close", "Dividends"]

        df_rows = []

        dt = datetime.combine(date(2022, 10, 14), time(0), tz)
        prices = [23.2, 23.2, 0]
        r = pd.Series(data=prices, index=columns, name=dt)
        df_rows.append(r)
        dt = datetime.combine(date(2022, 10, 13), time(0), tz)
        prices = [23.55, 23.55, 0.1425]
        r = pd.Series(data=prices, index=columns, name=dt)
        df_rows.append(r)
        dt = datetime.combine(date(2022, 10, 12), time(0), tz)
        prices = [24.15, 24.01, 0]
        r = pd.Series(data=prices, index=columns, name=dt)
        df_rows.append(r)
        dt = datetime.combine(date(2022, 10, 11), time(0), tz)
        prices = [24.2, 24.06, 0]
        r = pd.Series(data=prices, index=columns, name=dt)
        df_rows.append(r)

        dt = datetime.combine(date(2022, 9, 16), time(0), tz)
        prices = [23.4, 23.26, 0]
        r = pd.Series(data=prices, index=columns, name=dt)
        df_rows.append(r)
        dt = datetime.combine(date(2022, 9, 15), time(0), tz)
        prices = [24.5, 24.36, 0.1425]
        r = pd.Series(data=prices, index=columns, name=dt)
        df_rows.append(r)
        dt = datetime.combine(date(2022, 9, 14), time(0), tz)
        prices = [24.7, 24.41, 0]
        r = pd.Series(data=prices, index=columns, name=dt)
        df_rows.append(r)
        dt = datetime.combine(date(2022, 9, 13), time(0), tz)
        prices = [24.35, 24.07, 0]
        r = pd.Series(data=prices, index=columns, name=dt)
        df_rows.append(r)

        dt = datetime.combine(date(2022, 8, 12), time(0), tz)
        prices = [29.95, 29.6, 0]
        r = pd.Series(data=prices, index=columns, name=dt)
        df_rows.append(r)
        dt = datetime.combine(date(2022, 8, 11), time(0), tz)
        prices = [29.7, 29.35, 0.1425]
        r = pd.Series(data=prices, index=columns, name=dt)
        df_rows.append(r)
        dt = datetime.combine(date(2022, 8, 10), time(0), tz)
        prices = [29.3, 28.82, 0]
        r = pd.Series(data=prices, index=columns, name=dt)
        df_rows.append(r)
        dt = datetime.combine(date(2022, 8, 9), time(0), tz)
        prices = [29.15, 28.67, 0]
        r = pd.Series(data=prices, index=columns, name=dt)
        df_rows.append(r)

        df = pd.DataFrame(df_rows)
        df["CDF"] = df["Adj Close"] / df["Close"]

        df = df.sort_index(ascending=False)

        # Test: no dividends in data
        df_tc = df.iloc[0:1]
        cdf0 = yfcu.GetCDF0(df_tc)
        self.assertEqual(cdf0, 1.0)

        # Test: 1x dividend in data, in middle of table
        df_tc = df.iloc[0:3]
        cdf0_answer = df_tc["CDF"].iloc[-1]
        try:
            cdf0 = yfcu.GetCDF0(df_tc)
            self.assertEqual(cdf0, cdf0_answer)
        except:
            print("df_tc:")
            print(df_tc)
            raise

        # Test: 1x dividend in data, in oldest row
        df_tc = df.iloc[0:2]
        close_before = df["Close"].iloc[2]
        cdf0_answer = df["CDF"].iloc[2]
        try:
            cdf0 = yfcu.GetCDF0(df_tc, close_before)
            self.assertAlmostEqual(cdf0, cdf0_answer, delta=0.0002)
        except:
            print("df_tc:")
            print(df_tc)
            raise

        # Test: 1x dividend in data, at most recent row:
        df_tc = df.iloc[1:3]
        cdf0_answer = df_tc["CDF"].iloc[-1]
        try:
            cdf0 = yfcu.GetCDF0(df_tc)
            self.assertEqual(cdf0, cdf0_answer)
        except:
            print("df_tc:")
            print(df_tc)
            raise

    def test_reverseYahooAdjust(self):
        tkr = '8TRA.DE'
        interval = yfcd.Interval.Days1
        dat = yfc.Ticker(tkr)

        exchange = dat.info['exchange']
        if "exchangeTimezoneName" in dat.info:
            tz_name = dat.info["exchangeTimezoneName"]
        else:
            tz_name = dat.info["timeZoneFullName"]
        hm = yfcp.HistoriesManager(tkr, exchange, tz_name, self.session, None)

        # Step 1: add a dividend into system
        div = 0.7
        div_date = date(2023, 6, 2)
        div_close_before = 18.79
        fetch_dt = pd.Timestamp(datetime(2023, 6, 1, 14, 1)).tz_localize(tz_name)
        divs_df = pd.DataFrame(index=[div_date], data={'Close day before':[div_close_before], 'Dividends':[div], "FetchDate":[fetch_dt]})
        divs_df.index = pd.to_datetime(divs_df.index).tz_localize(tz_name)
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
