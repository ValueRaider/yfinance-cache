import unittest
from pprint import pprint

from .context import yfc_dat as yfcd
from .context import yfc_time as yfct
from .context import yfc_cache_manager as yfcm
from .context import yfc_ticker as yfc

import tempfile

import pandas as pd
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo
import os

class Test_Yfc_Interface(unittest.TestCase):

    def setUp(self):
        self.market = "us_market"
        self.exchange = "NMS"
        self.market_tz = ZoneInfo('US/Eastern')
        self.market_open_time  = time(hour=9, minute=30)
        self.market_close_time = time(hour=16, minute=0)

        self.tkr = "INTC"
        self.tempCacheDir = tempfile.TemporaryDirectory()
        yfcm.SetCacheDirpath(self.tempCacheDir.name)

        import requests_cache
        self.session = requests_cache.CachedSession('yfinance.cache')
        self.session.headers['User-agent'] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0"

        self.dat = yfc.Ticker(self.tkr, session=self.session)

        self.monday  = date(year=2022, month=2, day=7)
        self.tuesday = date(year=2022, month=2, day=8)
        self.wednesday=date(year=2022, month=2, day=9)
        self.thursday =date(year=2022, month=2, day=10)
        self.friday  = date(year=2022, month=2, day=11)
        self.saturday= date(year=2022, month=2, day=12)


    def tearDown(self):
        self.tempCacheDir.cleanup()
        self.session.close()


    ## Test day interval fetched same day
    def test_yf_lag(self):
        dt_now = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
        if not yfct.IsTimestampInActiveSession(self.dat.info["exchange"], dt_now):
            # If market closed, can't run this test
            return

        # First call with temp-cache means will calculate lag:
        lag = self.dat.yf_lag
        expected_lag = timedelta(minutes=15)
        diff = abs(lag - expected_lag)
        tolerance = timedelta(minutes=1)
        try:
            self.assertLess(diff, tolerance)
        except:
            pprint("lag: {0}".format(lag))
            pprint("expected_lag: {0}".format(expected_lag))
            pprint("diff: {0}".format(lag - expected_lag))
            raise

        # Confirm that fetching from cache returns same value:
        lag_cache = self.dat.yf_lag
        self.assertEqual(lag, lag_cache)


    def test_history_basics1(self):
        self.assertFalse(os.path.isdir(os.path.join(self.tempCacheDir.name, self.tkr)))

        # A fetch of prices, then another fetch of same prices, should return identical

        ## Daily 
        df1 = self.dat.history(interval="1d", start=self.monday, end=self.tuesday)
        self.assertEqual(df1.shape[0], 2)
        df2 = self.dat.history(interval="1d", start=self.monday, end=self.tuesday)
        try:
            self.assertTrue(df1.equals(df2))
        except:
            print("df1:")
            print(df1)
            print("")
            print("df2:")
            print(df2)
            print("")
            raise

        ## Hourly
        df1 = self.dat.history(interval="1h", start=self.monday, end=self.tuesday)
        self.assertEqual(df1.shape[0], 14)
        df2 = self.dat.history(interval="1h", start=self.monday, end=self.tuesday)
        try:
            self.assertTrue(df1.equals(df2))
        except:
            print("df1:")
            print(df1)
            print("")
            print("df2:")
            print(df2)
            print("")
            raise


    def test_history_basics2(self):
        # Fetching 2 consecutive prices ranges, then fetching 3rd of total range should
        # return contiguous table

        dfa = self.dat.history(interval="1d", start=self.monday, end=self.monday)
        dfb = self.dat.history(interval="1d", start=self.tuesday,end=self.tuesday)
        df1 = pd.concat([dfa,dfb])

        df2 = self.dat.history(interval="1d", start=self.monday, end=self.tuesday)
        self.assertTrue(df1.equals(df2))


    def test_history_basics_hour(self):
        # Check fetching single hour
        start_dt = datetime.combine(self.monday, time(10, 30), self.market_tz)
        end_dt = start_dt + timedelta(hours=1)
        df1 = self.dat.history(interval="1h", start=start_dt, end=end_dt)
        self.assertEqual(df1.shape[0], 1)
        self.assertEqual(df1.index[0], start_dt)
        df2 = self.dat.history(interval="1h", start=start_dt, end=end_dt)
        self.assertTrue(df1.equals(df2))


    def test_history_overlaps(self):
        df1 = self.dat.history(interval="1d", start=self.monday, end=self.wednesday)
        df2 = self.dat.history(interval="1d", start=self.tuesday,end=self.thursday)
        ## Where these overlap should be identical
        self.assertTrue(df1.iloc[1:3].reset_index(drop=True).equals(df2.iloc[0:2].reset_index(drop=True)))

        ## Then fetching full date range is identical to unique rows in df1+df2
        df3 = self.dat.history(interval="1d", start=self.monday, end=self.thursday)
        self.assertTrue(df3.equals(pd.concat([df1.iloc[0:1], df2])))


if __name__ == '__main__':
    unittest.main()
