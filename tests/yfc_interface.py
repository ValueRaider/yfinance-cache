import unittest
from pprint import pprint

from .context import yfc_dat as yfcd
from .context import yfc_time as yfct
from .context import yfc_cache_manager as yfcm
from .context import yfc_ticker as yfc

import yfinance as yf

import tempfile

import pandas as pd
import numpy as np
from time import sleep
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
        ## Only use high-volume stocks:
        tkr_candidates = ["AZN.L", "ASML.AS", "IMP.JO", "INTC"]

        dt_now = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))

        for tkr in tkr_candidates:
            dat = yfc.Ticker(tkr, session=self.session)
            if not yfct.IsTimestampInActiveSession(dat.info["exchange"], dt_now):
                continue
            expected_lag = yfcd.exchangeToYfLag[dat.info["exchange"]]
            # print("tkr = {}".format(tkr))
            # print("expected_lag = {}".format(expected_lag))
            print("Testing tkr {}".format(tkr))

            dat = yfc.Ticker(tkr, session=None) # Use live data

            # First call with temp-cache means will calculate lag:
            lag = dat.yf_lag
            # print("lag = {}".format(lag))
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

        ## Daily with actions
        df1 = self.dat.history(interval="1d", start=self.monday, end=self.tuesday, actions=True)
        self.assertTrue("Dividends" in df1.columns.values)
        self.assertTrue("Stock Splits" in df1.columns.values)
        self.assertEqual(df1.shape[0], 2)
        df2 = self.dat.history(interval="1d", start=self.monday, end=self.tuesday, actions=True)
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


    def test_matches_yf_daily(self):
        dat_yf = yf.Ticker(self.tkr, session=self.session)

        start_day_str = "2022-06-20"
        end_day_str = "2022-06-24"

        start_dt = datetime.combine(date(2022,6,20), self.market_open_time)
        end_dt = datetime.combine(date(2022,6,24), self.market_close_time)

        for aa in [False,True]:
            for ba in [False,True]:
                if aa and ba:
                    continue

                df_yf = dat_yf.history(start=start_day_str, end=end_day_str, auto_adjust=aa, back_adjust=ba)

                df_yfc = self.dat.history(start=start_dt, end=end_dt, auto_adjust=aa, back_adjust=ba)

                data_cols = ["Open","High","Low","Close","Volume","Dividends","Stock Splits"]
                for c in data_cols:
                    try:
                        self.assertTrue(df_yf[c].equals(df_yfc[c]))
                    except:
                        print("aa={}, ba={}, c={}".format(aa, ba, c))
                        raise


    def test_history_live(self):
        # Fetch during live trading session
        tkr_candidates = ["IMP.JO", "INTC"]
        interval = yfcd.Interval.Days1
        #
        dt_now = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
        for tkr in tkr_candidates:
            dat = yfc.Ticker(tkr, session=None)
            exchange = dat.info["exchange"]
            yfct.SetExchangeTzName(exchange, dat.info["exchangeTimezoneName"])
            if not yfct.GetTimestampCurrentInterval(exchange, dt_now, interval) is None:
                print("Testing tkr {}".format(tkr))

                start_dt = dt_now - timedelta(days=7)
                end_dt = dt_now

                d = start_dt.date()
                expected_interval_dates = []
                while d <= end_dt.date():
                    if yfct.ExchangeOpenOnDay(exchange, d):
                        expected_interval_dates.append(d)
                    d += timedelta(days=1)

                ## Check that table covers expected date range
                df1 = dat.history(interval="1d", start=start_dt.date(), end=end_dt.date())
                self.assertTrue(np.array_equal(expected_interval_dates, df1.index.date))

                # Refetch before data aged, should return identical table
                sleep(1)
                df2 = dat.history(interval="1d", start=start_dt.date(), max_age=timedelta(minutes=1), end=end_dt.date())
                try:
                    self.assertTrue(df1.equals(df2))
                except:
                    print("df1:")
                    pprint(df1)
                    print("df2:")
                    print(df2)
                    raise

                # Refetch after data aged, last row should be different
                sleep(10)
                n = df1.shape[0]
                df3 = dat.history(interval="1d", start=start_dt.date(), max_age=timedelta(seconds=1), end=end_dt.date())
                try:
                    self.assertTrue(np.array_equal(expected_interval_dates, df3.index.date))
                except:
                    print("expected_interval_dates:")
                    pprint(expected_interval_dates)
                    print("df3:")
                    print(df3)
                    raise
                try:
                    self.assertTrue(df1.iloc[0:n-1].equals(df3.iloc[0:n-1]))
                    self.assertFalse(df1.iloc[n-1:n].equals(df3.iloc[n-1:n]))
                except:
                    print("df1:")
                    print(df1)
                    print("df3:")
                    print(df3)
                    raise

                ## Finally, check it matches YF:
                dat_yf = yf.Ticker(self.tkr, session=self.session)
                df_yf = dat_yf.history(interval="1d", start=start_dt.date(), end=end_dt.date())
                data_cols = ["Open","High","Low","Close","Volume","Dividends","Stock Splits"]
                for c in data_cols:
                    try:
                        self.assertTrue(df_yf[c].equals(df3[c]))
                    except:
                        print("Difference in column {}".format(c))
                        print("")
                        print("df_yf:")
                        print(df_yf)
                        print("")
                        print("df_yfc:")
                        print(df3)
                        print("")
                        raise

    def test_history_live_1d_evening(self):
        # Fetch during evening after active session
        tkr_candidates = ["IMP.JO", "INTC"]
        interval = yfcd.Interval.Days1
        #
        dt_now = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
        for tkr in tkr_candidates:
            dat = yfc.Ticker(tkr, session=None)
            exchange = dat.info["exchange"]
            yfct.SetExchangeTzName(exchange, dat.info["exchangeTimezoneName"])
            if yfct.ExchangeOpenOnDay(exchange, dt_now.date()):
                sched = yfct.GetExchangeSchedule(exchange, dt_now.date(), dt_now.date())
                if (not sched is None) and dt_now > sched["market_close"][0]:
                    print("Testing tkr {}".format(tkr))

                    start_dt = dt_now - timedelta(days=7)
                    end_dt = dt_now

                    d = start_dt.date()
                    expected_interval_dates = []
                    while d <= end_dt.date():
                        if yfct.ExchangeOpenOnDay(exchange, d):
                            expected_interval_dates.append(d)
                        d += timedelta(days=1)

                    ## Check that table covers expected date range
                    df1 = dat.history(interval="1d", start=start_dt.date(), end=end_dt.date())
                    self.assertTrue(np.array_equal(expected_interval_dates, df1.index.date))

                    ## Finally, check it matches YF:
                    dat_yf = yf.Ticker(self.tkr, session=self.session)
                    df_yf = dat_yf.history(interval="1d", start=start_dt.date(), end=end_dt.date())
                    data_cols = ["Open","High","Low","Close","Volume","Dividends","Stock Splits"]
                    for c in data_cols:
                        try:
                            self.assertTrue(df_yf[c].equals(df1[c]))
                        except:
                            print("Difference in column {}".format(c))
                            print("")
                            print("df_yf:")
                            print(df_yf)
                            print("")
                            print("df_yfc:")
                            print(df1)
                            print("")
                            raise

    def test_history_live_1w_evening(self):
        # Fetch during evening after active session
        tkr_candidates = ["IMP.JO", "INTC"]
        interval = yfcd.Interval.Week
        #
        dt_now = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
        for tkr in tkr_candidates:
            dat = yfc.Ticker(tkr, session=self.session)
            exchange = dat.info["exchange"]
            yfct.SetExchangeTzName(exchange, dat.info["exchangeTimezoneName"])
            if yfct.ExchangeOpenOnDay(exchange, dt_now.date()):
                sched = yfct.GetExchangeSchedule(exchange, dt_now.date(), dt_now.date())
                if (not sched is None) and dt_now > sched["market_close"][0]:
                    print("Testing tkr {}".format(tkr))

                    start_dt = dt_now - timedelta(days=dt_now.weekday())
                    end_dt = start_dt+timedelta(days=4)

                    # Add a 2nd week
                    start_dt -= timedelta(days=7)

                    d = start_dt.date()
                    expected_interval_dates = []
                    while d < end_dt.date():
                        ## Yahoo returns weekly data as starting Monday even if market closed then.
                        if d.weekday() == 0:
                            expected_interval_dates.append(d)
                        d += timedelta(days=7-d.weekday())

                    ## Check that table covers expected date range
                    df1 = dat.history(interval="1wk", start=start_dt.date(), end=end_dt.date())
                    try:
                        self.assertTrue(np.array_equal(expected_interval_dates, df1.index.date))
                    except:
                        print("expected_interval_dates")
                        pprint(expected_interval_dates)
                        print("df1:")
                        print(df1)
                        raise

                    ## Finally, check it matches YF:
                    dat_yf = yf.Ticker(self.tkr, session=self.session)
                    df_yf = dat_yf.history(interval="1wk", start=start_dt.date(), end=end_dt.date())
                    data_cols = ["Open","High","Low","Close","Volume","Dividends","Stock Splits"]
                    for c in data_cols:
                        try:
                            self.assertTrue(df_yf[c].equals(df1[c]))
                        except:
                            print("Difference in column {}".format(c))
                            print("")
                            print("df_yf:")
                            print(df_yf)
                            print("")
                            print("df_yfc:")
                            print(df1)
                            print("")
                            raise


if __name__ == '__main__':
    unittest.main()
