import unittest
from pprint import pprint

from .context import yfc_dat as yfcd
from .context import yfc_time as yfct
from .context import yfc_cache_manager as yfcm
from .context import yfc_utils as yfcu
from .context import yfc_ticker as yfc

import yfinance as yf

import tempfile

import pandas as pd
import numpy as np
from time import sleep
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo
import pytz
import os

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
##  7*   8    9    10   11   12   13
##  14   15   16   17   18   19   20
##  21   22   23   24   25   26   27
##  28

## TODO:
## Test for handling days without trades. Happens most on Toronto exchange
## Test using 'tz' argument in history()

class Test_Yfc_Interface(unittest.TestCase):

    def setUp(self):
        self.tempCacheDir = tempfile.TemporaryDirectory()
        yfcm.SetCacheDirpath(self.tempCacheDir.name)

        self.session = None
        import requests_cache
        self.session = requests_cache.CachedSession(os.path.join(yfcu.GetUserCacheDirpath(),'yfinance.cache'))
        self.session.headers['User-agent'] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0"

        self.usa_tkr = "INTC"
        self.usa_market = "us_market"
        self.usa_exchange = "NMS"
        self.usa_market_tz = ZoneInfo('US/Eastern')
        self.usa_market_open_time  = time(hour=9, minute=30)
        self.usa_market_close_time = time(hour=16, minute=0)
        self.usa_dat = yfc.Ticker(self.usa_tkr, session=self.session)

        self.nze_tkr = "MEL.NZ"
        self.nze_market = "nz_market"
        self.nze_exchange = "NZE"
        self.nze_market_tz = ZoneInfo('Pacific/Auckland')
        self.nze_market_open_time  = time(hour=10, minute=0)
        self.nze_market_close_time = time(hour=16, minute=45)
        self.nze_dat = yfc.Ticker(self.nze_tkr, session=self.session)


    def tearDown(self):
        self.tempCacheDir.cleanup()
        self.session.close()


    ## Test day interval fetched same day
    def test_yf_lag(self):
        ## Only use high-volume stocks:
        tkr_candidates = ["AZN.L", "ASML.AS", "IMP.JO", "INTC", "MEL.NZ"]

        dt_now = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))

        for tkr in tkr_candidates:
            dat = yfc.Ticker(tkr, session=self.session)
            if not yfct.IsTimestampInActiveSession(dat.info["exchange"], dt_now):
                continue
            expected_lag = yfcd.exchangeToYfLag[dat.info["exchange"]]

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
        intervals = ["30m", "1h", "1d", "1wk"]
        start_d = date(2022,7,11)
        end_d = date(2022,7,12)
        for interval in intervals:
            df = self.usa_dat.history(start=start_d, end=end_d, interval=interval)
            self.assertTrue(isinstance(df.index, pd.DatetimeIndex))


    def test_history_basics1_usa(self):
        # A fetch of prices, then another fetch of same prices, should return identical

        self.assertFalse(os.path.isdir(os.path.join(self.tempCacheDir.name, self.usa_tkr)))

        ## Daily 
        df1 = self.usa_dat.history(interval="1d", start=date(2022,2,7), end=date(2022,2,15))
        try:
            self.assertEqual(df1.shape[0], 6)
        except:
            print("df1:")
            print(df1)
            print("")
            raise
        df2 = self.usa_dat.history(interval="1d", start=date(2022,2,7), end=date(2022,2,15))
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
        self.assertTrue((df2.index.time==time(0)).all())

        ## Daily with actions
        df1 = self.usa_dat.history(interval="1d", start=date(2022,2,7), end=date(2022,2,15), actions=True)
        self.assertTrue("Dividends" in df1.columns.values)
        self.assertTrue("Stock Splits" in df1.columns.values)
        self.assertEqual(df1.shape[0], 6)
        df2 = self.usa_dat.history(interval="1d", start=date(2022,2,7), end=date(2022,2,15), actions=True)
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
        self.assertTrue((df2.index.time==time(0)).all())

        ## Hourly
        df1 = self.usa_dat.history(interval="1h", start=date(2022,2,7), end=date(2022,2,9))
        self.assertEqual(df1.shape[0], 14)
        df2 = self.usa_dat.history(interval="1h", start=date(2022,2,7), end=date(2022,2,9))
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

        ## Weekly 
        df1 = self.usa_dat.history(interval="1wk", start=date(2022,2,7), end=date(2022,2,19))
        self.assertEqual(df1.shape[0], 2)
        df2 = self.usa_dat.history(interval="1wk", start=date(2022,2,7), end=date(2022,2,19))
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


    def test_history_basics1_nze(self):
        # A fetch of prices, then another fetch of same prices, should return identical

        self.assertFalse(os.path.isdir(os.path.join(self.tempCacheDir.name, self.nze_tkr)))

        ## Daily 
        df1 = self.nze_dat.history(interval="1d", start=date(2022,2,8), end=date(2022,2,15))
        self.assertEqual(df1.shape[0], 5)
        df2 = self.nze_dat.history(interval="1d", start=date(2022,2,8), end=date(2022,2,15))
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
        self.assertTrue((df2.index.time==time(0)).all())

        ## Daily with actions
        df1 = self.nze_dat.history(interval="1d", start=date(2022,2,8), end=date(2022,2,15), actions=True)
        self.assertTrue("Dividends" in df1.columns.values)
        self.assertTrue("Stock Splits" in df1.columns.values)
        self.assertEqual(df1.shape[0], 5)
        df2 = self.nze_dat.history(interval="1d", start=date(2022,2,8), end=date(2022,2,15), actions=True)
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
        df1 = self.nze_dat.history(interval="1h", start=date(2022,2,8), end=date(2022,2,10))
        self.assertEqual(df1.shape[0], 14)
        df2 = self.nze_dat.history(interval="1h", start=date(2022,2,8), end=date(2022,2,10))
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

        ## Weekly 
        df1 = self.nze_dat.history(interval="1wk", start=date(2022,2,7), end=date(2022,2,19))
        self.assertEqual(df1.shape[0], 2)
        df2 = self.nze_dat.history(interval="1wk", start=date(2022,2,7), end=date(2022,2,19))
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


    def test_history_basics2_usa(self):
        # Fetching 2 consecutive prices ranges, then fetching 3rd of total range should
        # return contiguous table

        dfa = self.usa_dat.history(interval="1d", start=date(2022,2,7), end=date(2022,2,8))
        dfb = self.usa_dat.history(interval="1d", start=date(2022,2,8), end=date(2022,2,9))
        df1 = pd.concat([dfa,dfb])

        df2 = self.usa_dat.history(interval="1d", start=date(2022,2,7), end=date(2022,2,9))
        self.assertTrue(df1.equals(df2))


    def test_history_basics2_nze(self):
        # Fetching 2 consecutive prices ranges, then fetching 3rd of total range should
        # return contiguous table

        dfa = self.nze_dat.history(interval="1d", start=date(2022,2,8), end=date(2022,2,9))
        dfb = self.nze_dat.history(interval="1d", start=date(2022,2,9), end=date(2022,2,10))
        df1 = pd.concat([dfa,dfb])

        df2 = self.nze_dat.history(interval="1d", start=date(2022,2,8), end=date(2022,2,10))
        self.assertTrue(df1.equals(df2))


    def test_history_basics_hour_usa(self):
        # Check fetching single hour
        start_dt = datetime.combine(date(2022,2,7), time(10, 30), self.usa_market_tz)
        end_dt = start_dt + timedelta(hours=1)
        df1 = self.usa_dat.history(interval="1h", start=start_dt, end=end_dt)
        self.assertEqual(df1.shape[0], 1)
        self.assertEqual(df1.index[0], start_dt)
        df2 = self.usa_dat.history(interval="1h", start=start_dt, end=end_dt)
        self.assertTrue(df1.equals(df2))


    def test_history_basics_hour_nze(self):
        # Check fetching single hour
        start_dt = datetime.combine(date(2022,2,8), time(10, 0), self.nze_market_tz)
        end_dt = start_dt + timedelta(hours=1)
        df1 = self.nze_dat.history(interval="1h", start=start_dt, end=end_dt)
        self.assertEqual(df1.shape[0], 1)
        self.assertEqual(df1.index[0], start_dt)
        df2 = self.nze_dat.history(interval="1h", start=start_dt, end=end_dt)
        self.assertTrue(df1.equals(df2))


    def test_history_overlaps_usa(self):
        df1 = self.usa_dat.history(interval="1d", start=date(2022,2,7), end=date(2022,2,10))
        df2 = self.usa_dat.history(interval="1d", start=date(2022,2,8), end=date(2022,2,11))
        ## Where these overlap should be identical
        try:
            self.assertTrue(df1.iloc[1:3].reset_index(drop=True).equals(df2.iloc[0:2].reset_index(drop=True)))
        except:
            print("df1:")
            print(df1)
            print("df2:")
            print(df2)
            raise

        ## Then fetching full date range is identical to unique rows in df1+df2
        df3 = self.usa_dat.history(interval="1d", start=date(2022,2,7), end=date(2022,2,11))
        self.assertTrue(df3.equals(pd.concat([df1.iloc[0:1], df2])))


    def test_history_overlaps_nze(self):
        df1 = self.nze_dat.history(interval="1d", start=date(2022,2,8), end=date(2022,2,11))
        df2 = self.nze_dat.history(interval="1d", start=date(2022,2,9), end=date(2022,2,12))
        ## Where these overlap should be identical
        self.assertTrue(df1.iloc[1:3].reset_index(drop=True).equals(df2.iloc[0:2].reset_index(drop=True)))

        ## Then fetching full date range is identical to unique rows in df1+df2
        df3 = self.nze_dat.history(interval="1d", start=date(2022,2,8), end=date(2022,2,12))
        self.assertTrue(df3.equals(pd.concat([df1.iloc[0:1], df2])))


    def test_matches_yf_daily_usa(self):
        dat_yf = yf.Ticker(self.usa_tkr, session=self.session)

        start_day_str = "2022-06-20"
        end_day_str = "2022-06-25"

        for aa in [False,True]:
            for ba in [False,True]:
                if aa and ba:
                    continue

                df_yf = dat_yf.history(start=start_day_str, end=end_day_str, auto_adjust=aa, back_adjust=ba)
                df_yfc = self.usa_dat.history(start=start_day_str, end=end_day_str, auto_adjust=aa, back_adjust=ba)

                # df_yf = dat_yf.history(start=start_dt, end=end_dt, auto_adjust=aa, back_adjust=ba)
                # df_yfc = self.usa_dat.history(start=start_dt, end=end_dt, auto_adjust=aa, back_adjust=ba)

                data_cols = ["Open","High","Low","Close","Volume","Dividends","Stock Splits"]
                for c in data_cols:
                    try:
                        self.assertTrue(df_yf[c].equals(df_yfc[c]))
                    except:
                        print("df_yf:")
                        print(df_yf[c])
                        print("df_yfc:")
                        print(df_yfc[[c,"FetchDate"]])
                        print("aa={}, ba={}, c={}".format(aa, ba, c))
                        raise


    def test_matches_yf_daily_nze(self):
        dat_yf = yf.Ticker(self.nze_tkr, session=self.session)

        start_day_str = "2022-06-13"
        end_day_str = "2022-06-18"

        for aa in [False,True]:
            for ba in [False,True]:
                if aa and ba:
                    continue

                df_yf = dat_yf.history(start=start_day_str, end=end_day_str, auto_adjust=aa, back_adjust=ba)
                df_yfc = self.nze_dat.history(start=start_day_str, end=end_day_str, auto_adjust=aa, back_adjust=ba)

                data_cols = ["Open","High","Low","Close","Volume","Dividends","Stock Splits"]
                for c in data_cols:
                    try:
                        self.assertTrue(df_yf[c].equals(df_yfc[c]))
                    except:
                        print("df_yf:")
                        print(df_yf)
                        print("df_yfc:")
                        print(df_yfc)
                        print("aa={}, ba={}, c={}".format(aa, ba, c))
                        raise


    def test_history_final(self):
        # Test 'Final?' column
        # tkr_candidates = ["IMP.JO", "INTC", "MEL.NZ"]
        tkr_candidates = ["INTC"]
        interval = yfcd.Interval.Days1

        dt_now_utc = datetime.utcnow()
        dt_now = dt_now_utc.replace(tzinfo=ZoneInfo("UTC"))

        start_d = dt_now_utc.date() -timedelta(days=7)
        end_d = dt_now_utc.date() +timedelta(days=1)

        # First, check during a live session
        for tkr in tkr_candidates:
            dat = yfc.Ticker(tkr, session=self.session)
            exchange = dat.info["exchange"]
            yfct.SetExchangeTzName(exchange, dat.info["exchangeTimezoneName"])
            if yfct.IsTimestampInActiveSession(exchange, dt_now):
                df1 = dat.history(interval="1d", start=start_d, end=end_d)
                n = df1.shape[0]
                try:
                    self.assertTrue((df1["Final?"].iloc[0:n-1]==True).all())
                    self.assertFalse(df1["Final?"].iloc[n-1])
                except:
                    print("start={} , end={}".format(start_d, end_d))
                    print("df1:")
                    print(df1)
                    raise

        return
 
        # Second, check while exchange closed
        for tkr in tkr_candidates:
            dat = yfc.Ticker(tkr, session=self.session)
            exchange = dat.info["exchange"]
            yfct.SetExchangeTzName(exchange, dat.info["exchangeTimezoneName"])
            if not yfct.IsTimestampInActiveSession(exchange, dt_now):
                df1 = dat.history(interval="1d", start=start_dt.date(), end=end_d)
                n = df1.shape[0]
                self.assertTrue((df1["Final?"]==True).all())


    def test_history_live(self):
        # Fetch during live trading session
        tkr_candidates = ["IMP.JO", "INTC", "MEL.NZ"]
        interval = yfcd.Interval.Days1

        dt_now = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
        start_dt = dt_now - timedelta(days=7)
        end_dt = dt_now+timedelta(days=1)
        end_d = end_dt.date()+timedelta(days=1)

        for tkr in tkr_candidates:
            dat = yfc.Ticker(tkr, session=self.session)
            exchange = dat.info["exchange"]
            yfct.SetExchangeTzName(exchange, dat.info["exchangeTimezoneName"])
            if yfct.IsTimestampInActiveSession(exchange, dt_now):
                d = start_dt.date()
                expected_interval_dates = []
                # tz = pytz.timezone(dat.info["exchangeTimezoneName"])
                tz = ZoneInfo(dat.info["exchangeTimezoneName"])
                while d < end_dt.date():
                    if yfct.ExchangeOpenOnDay(exchange, d):
                        # dt = datetime.combine(d, time(0))
                        # dt = tz.localize(dt)
                        dt = datetime.combine(d, time(0), tz)
                        expected_interval_dates.append(dt)
                    d += timedelta(days=1)

                ## Check that table covers expected date range
                df1 = dat.history(interval="1d", start=start_dt.date(), end=end_d)
                try:
                    self.assertTrue(np.array_equal(expected_interval_dates, df1.index))
                except:
                    print("expected_interval_dates:")
                    pprint(expected_interval_dates)
                    print("df1.index:")
                    pprint(df1.index)
                    edt0 = expected_interval_dates[0]
                    adt0 = df1.index[0]
                    print("expected dt0 = {} (tz={})".format(edt0, edt0.tzinfo))
                    print("actual dt0 = {} (tz={})".format(adt0, adt0.tzinfo))
                    print(expected_interval_dates == df1.index)
                    raise

                # Refetch before data aged, should return identical table
                sleep(1)
                df2 = dat.history(interval="1d", start=start_dt.date(), end=end_d, max_age=timedelta(minutes=1))
                try:
                    self.assertTrue(df1.equals(df2))
                except:
                    for c in df1.columns:
                        if not df1[c].equals(df2[c]):
                            print("Column {} different".format(c))
                            print(df1[c])
                            print(df2[c])
                            print(df1[c]==df2[c])
                            break
                    print("df1:")
                    pprint(df1)
                    print("df1 cols: {}".format(["'{}'".format(x) for x in df1.columns]))
                    print("df2:")
                    print(df2)
                    print("df2 cols: {}".format(["'{}'".format(x) for x in df2.columns]))
                    raise
                return

                # Refetch after data aged, last row should be different
                sleep(10)
                n = df1.shape[0]
                df3 = dat.history(interval="1d", start=start_dt.date(), end=end_d, max_age=timedelta(seconds=1))
                try:
                    self.assertTrue(np.array_equal(expected_interval_dates, df3.index))
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

                ## Check it matches YF:
                dat_yf = yf.Ticker(tkr, session=self.session)
                df_yf = dat_yf.history(interval="1d", start=start_dt.date(), end=end_d)
                data_cols = ["Open","High","Low","Close","Volume","Dividends","Stock Splits"]
                for c in data_cols:
                    try:
                        self.assertTrue(df_yf[c].equals(df1[c]))
                    except:
                        print("")
                        print("df_yf:")
                        print(df_yf)
                        print("")
                        print("df_yfc:")
                        print(df1)
                        print("")
                        print("Difference in column {}".format(c))
                        raise

    def test_history_live_1h_evening(self):
        # Fetch during evening after active session
        tkr_candidates = ["IMP.JO", "INTC", "MEL.NZ"]
        interval = yfcd.Interval.Hours1
        #
        dt_now = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
        d_now = dt_now.date()
        for tkr in tkr_candidates:
            dat = yfc.Ticker(tkr, session=None)
            exchange = dat.info["exchange"]
            yfct.SetExchangeTzName(exchange, dat.info["exchangeTimezoneName"])
            if yfct.ExchangeOpenOnDay(exchange, d_now):
                sched = yfct.GetExchangeSchedule(exchange, d_now, d_now+timedelta(days=1))
                if (not sched is None) and dt_now > sched["market_close"][0]:
                    tz = ZoneInfo(dat.info["exchangeTimezoneName"])
                    start_dt = sched["market_open"][0]
                    end_dt = sched["market_close"][0]

                    dt = sched["market_open"][0]
                    expected_interval_starts = []
                    while dt < sched["market_close"][0]:
                        expected_interval_starts.append(dt)
                        dt += timedelta(hours=1)

                    ## Check that table covers expected date range
                    df1 = dat.history(interval="1h", start=start_dt, end=end_dt)
                    try:
                        self.assertTrue(np.array_equal(expected_interval_starts, df1.index))
                    except:
                        print("expected_interval_starts:")
                        pprint(expected_interval_starts)
                        print("df1.index:")
                        pprint(df1.index)
                        raise

                    ## Finally, check it matches YF:
                    dat_yf = yf.Ticker(tkr, session=self.session)
                    df_yf = dat_yf.history(interval="1h", start=start_dt, end=end_dt)
                    data_cols = ["Open","High","Low","Close","Volume","Dividends","Stock Splits"]
                    for c in data_cols:
                        try:
                            self.assertTrue(df_yf[c].equals(df1[c]))
                        except:
                            print("")
                            print("df_yf:")
                            print(df_yf)
                            print("")
                            print("df_yfc:")
                            print(df1)
                            print("")
                            print("Difference in column {}".format(c))
                            raise

    def test_history_live_1d_evening(self):
        # Fetch during evening after active session
        tkr_candidates = ["IMP.JO", "INTC", "MEL.NZ"]
        interval = yfcd.Interval.Days1
        #
        dt_now = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
        d_now = dt_now.date()
        for tkr in tkr_candidates:
            dat = yfc.Ticker(tkr, session=None)
            exchange = dat.info["exchange"]
            yfct.SetExchangeTzName(exchange, dat.info["exchangeTimezoneName"])
            if yfct.ExchangeOpenOnDay(exchange, d_now):
                sched = yfct.GetExchangeSchedule(exchange, d_now, d_now+timedelta(days=1))
                if (not sched is None) and dt_now > sched["market_close"][0]:
                    start_dt = dt_now - timedelta(days=7)
                    end_dt = dt_now
                    tz = ZoneInfo(dat.info["exchangeTimezoneName"])

                    d = start_dt.date()
                    expected_interval_dates = []
                    while d < end_dt.date():
                        if yfct.ExchangeOpenOnDay(exchange, d):
                            dt = datetime.combine(d, time(0), tz)
                            expected_interval_dates.append(dt)
                        d += timedelta(days=1)

                    ## Check that table covers expected date range
                    df1 = dat.history(interval="1d", start=start_dt.date(), end=end_dt.date())
                    try:
                        self.assertTrue(np.array_equal(expected_interval_dates, df1.index))
                    except:
                        print("expected_interval_dates:")
                        pprint(expected_interval_dates)
                        print("df1.index:")
                        pprint(df1.index)
                        raise

                    ## Finally, check it matches YF:
                    dat_yf = yf.Ticker(tkr, session=self.session)
                    df_yf = dat_yf.history(interval="1d", start=start_dt.date(), end=end_dt.date())
                    data_cols = ["Open","High","Low","Close","Volume","Dividends","Stock Splits"]
                    for c in data_cols:
                        try:
                            self.assertTrue(df_yf[c].equals(df1[c]))
                        except:
                            print("")
                            print("df_yf:")
                            print(df_yf)
                            print("")
                            print("df_yfc:")
                            print(df1)
                            print("")
                            print("Difference in column {}".format(c))
                            raise

    def test_history_live_1w_evening(self):
        # Fetch during evening after active session
        tkr_candidates = ["IMP.JO", "INTC"]
        interval = yfcd.Interval.Week
        #
        dt_now = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
        d_now = dt_now.date()
        for tkr in tkr_candidates:
            dat = yfc.Ticker(tkr, session=self.session)
            exchange = dat.info["exchange"]
            yfct.SetExchangeTzName(exchange, dat.info["exchangeTimezoneName"])
            if yfct.ExchangeOpenOnDay(exchange, d_now):
                sched = yfct.GetExchangeSchedule(exchange, d_now, d_now+timedelta(days=1))
                if (not sched is None) and dt_now > sched["market_close"][0]:
                    start_dt = dt_now - timedelta(days=dt_now.weekday())
                    end_dt = start_dt+timedelta(days=4)
                    tz = ZoneInfo(dat.info["exchangeTimezoneName"])

                    # Add a 2nd week
                    start_dt -= timedelta(days=7)

                    d = start_dt.date()
                    expected_interval_dates = []
                    while d < end_dt.date():
                        ## Yahoo returns weekly data as starting Monday even if market closed then.
                        if d.weekday() == 0:
                            # expected_interval_dates.append(d)
                            dt = datetime.combine(d, time(0), tz)
                            expected_interval_dates.append(dt)
                        d += timedelta(days=7-d.weekday())

                    ## Check that table covers expected date range
                    df1 = dat.history(interval="1wk", start=start_dt.date(), end=end_dt.date())
                    try:
                        self.assertTrue(np.array_equal(expected_interval_dates, df1.index))
                    except:
                        print("expected_interval_dates")
                        pprint(expected_interval_dates)
                        print("df1:")
                        print(df1)
                        raise

                    ## Finally, check it matches YF:
                    dat_yf = yf.Ticker(tkr, session=self.session)
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

    def test_history_jse_is_weird(self):
        # JSE market closes at 5pm local time, but if request 1h data 
        # including today then it returns a row with timestamp today@5pm and 0 volume.
        # This mysterious row disappears tomorrow.
        interval = "1h"
        tkr = "IMP.JO"

        dt_now = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
        d_now = dt_now.date()
        dat = yfc.Ticker(tkr, session=None)
        exchange = dat.info["exchange"]
        yfct.SetExchangeTzName(exchange, dat.info["exchangeTimezoneName"])
        if yfct.ExchangeOpenOnDay(exchange, d_now):
            sched = yfct.GetExchangeSchedule(exchange, d_now, d_now+timedelta(days=1))
            if (not sched is None) and dt_now > sched["market_close"][0]:
                start_dt = dt_now - timedelta(days=7)
                end_dt = dt_now
                tz = ZoneInfo(dat.info["exchangeTimezoneName"])

                ## Test is simple. Fetch should complete, and no 5pm in table
                df = dat.history(start=d_now, interval="1h")
                try:
                    self.assertEqual(df.shape[0], 8)
                    self.assertEqual(df.index[-1].time(), time(16))
                except:
                    print(df)
                    raise

    def test_usa_public_holidays_1w(self):
        # Reproduces bug where 'refetch algorithm' broke if 
        # date range included a public holiday:
        start_d = date(2022,2,14)
        # Monday 21-Feb-2022 is public holiday
        end_d = date(2022,2,26)
        interval = yfcd.Interval.Week

        df1 = self.usa_dat.history(start=start_d, end=end_d,interval=interval)
        sleep(1)
        df2 = self.usa_dat.history(start=start_d, end=end_d,interval=interval)
        try:
            self.assertTrue(df1.equals(df2))
        except:
            print("df1:")
            print(df1)
            print("")
            print("df2:")
            print(df2)
            raise

    def test_dead_days_live(self):
        # Some days, some stocks simply have no trade volume. 
        # Most times I see this on Toroto exchange.
        # e.g. EPL.V 2022-07-08. But only before market close apparently.
        #
        # If daily interval, Yahoo fills in row using prior and next day.
        # But if intraday then Yahoo just returns NaN

        tkr="EPL.V"
        dat = yfc.Ticker(tkr)
        start_d = date(2022,7,4)
        end_d = date(2022,7,9)

        exchange = dat.info["exchange"]
        yfct.SetExchangeTzName(exchange, dat.info["exchangeTimezoneName"])

        dt_now = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
        if yfct.IsTimestampInActiveSession(exchange, dt_now):
            df = dat.history(start=start_d, end=end_d, interval="1d", keepna=True)
            idx = 2 # 3rd row = 2022-7-6
            self.assertTrue(np.isnan(df["Close"][idx]) or df["Volume"][idx]==0)

            df = dat.history(start=start_d, end=start_d+timedelta(days=1), interval="1h", keepna=True)
            idx = 1
            self.assertTrue(np.isnan(df["Close"][idx]))

if __name__ == '__main__':
    unittest.main()
