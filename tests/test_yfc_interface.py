import unittest
import tempfile
from pprint import pprint

from .context import yfc_dat as yfcd
from .context import yfc_time as yfct
from .context import yfc_cache_manager as yfcm
from .context import yfc_utils as yfcu
from .context import yfc_ticker as yfc
from .context import session_gbl
from .utils import Test_Base
import pickle as pkl

import yfinance as yf

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
##  7*   8    9    10   11   12   13
##  14   15   16   17   18   19   20
##  21   22   23   24   25   26   27
##  28

## TODO:
## Test for handling days without trades. Happens most on Toronto exchange

class Test_Yfc_Interface(Test_Base):

    def setUp(self):
        self.tempCacheDir = tempfile.TemporaryDirectory()
        yfcm.SetCacheDirpath(self.tempCacheDir.name)
        yfcm._option_manager.calendar.accept_unexpected_Yahoo_intervals = True

        self.session = session_gbl

        self.tkrs = ["MEL.NZ", "BHG.JO", "INTC"]
        self.tkrs.append("GME") # Stock split recently
        self.tkrs.append("BHP.AX") # ASX market has auction
        # self.tkrs.append("ICL.TA") # TLV market has auction and odd times  # disabling until I restore 'yahooWeeklyDef'

        self.usa_tkr = "INTC"
        self.usa_market = "us_market"
        self.usa_exchange = "NMS"
        self.usa_market_tz_name = 'US/Eastern'
        self.usa_market_tz = ZoneInfo(self.usa_market_tz_name)
        self.usa_market_open_time  = time(hour=9, minute=30)
        self.usa_market_close_time = time(hour=16, minute=0)
        self.usa_dat = yfc.Ticker(self.usa_tkr, session=self.session)

        self.nze_tkr = "MEL.NZ"
        self.nze_market = "nz_market"
        self.nze_exchange = "NZE"
        self.nze_market_tz_name = 'Pacific/Auckland'
        self.nze_market_tz = ZoneInfo(self.nze_market_tz_name)
        self.nze_market_open_time  = time(hour=10, minute=0)
        self.nze_market_close_time = time(hour=16, minute=45)
        self.nze_dat = yfc.Ticker(self.nze_tkr, session=self.session)


    def tearDown(self):
        self.tempCacheDir.cleanup()
        self.session.close()


    def test_history_basics1_usa(self):
        # A fetch of prices, then another fetch of same prices, should return identical

        self.assertFalse(os.path.isdir(os.path.join(self.tempCacheDir.name, self.usa_tkr, "history-1d.pkl")))

        week_start = date.today()
        week_start -= timedelta(days=28)
        week_start -= timedelta(days=week_start.weekday())
        td_1d = timedelta(days=1)
        td_7d = timedelta(days=7)

        ## Daily 
        df1 = self.usa_dat.history(interval="1d", start=week_start, end=week_start+td_7d)
        try:
            self.assertGreaterEqual(df1.shape[0], 3)
        except:
            print("df1:")
            print(df1)
            print("")
            raise
        df2 = self.usa_dat.history(interval="1d", start=week_start, end=week_start+td_7d)
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
        df1 = self.usa_dat.history(interval="1d", start=week_start, end=week_start+td_7d, actions=True)
        self.assertTrue("Dividends" in df1.columns.values)
        self.assertTrue("Stock Splits" in df1.columns.values)
        self.assertGreaterEqual(df1.shape[0], 3)
        df2 = self.usa_dat.history(interval="1d", start=week_start, end=week_start+td_7d, actions=True)
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
        df1 = self.usa_dat.history(interval="1h", start=week_start, end=week_start+td_1d*2)
        try:
            self.assertGreaterEqual(df1.shape[0], 7)
        except:
            print("df1:")
            print(df1)
            raise
        df2 = self.usa_dat.history(interval="1h", start=week_start, end=week_start+td_1d*2)
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
        df1 = self.usa_dat.history(interval="1wk", start=week_start, end=week_start+td_1d*14)
        self.assertEqual(df1.shape[0], 2)
        df2 = self.usa_dat.history(interval="1wk", start=week_start, end=week_start+td_1d*14)
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

        ## 1-minute
        df1 = self.usa_dat.history(interval='1m', period='1d')
        df2 = self.usa_dat.history(interval='1m', period='1d')
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

        self.assertFalse(os.path.isdir(os.path.join(self.tempCacheDir.name, self.nze_tkr, "history-1d.pkl")))

        week_start = date.today()
        week_start -= timedelta(days=28)
        week_start -= timedelta(days=week_start.weekday())
        td_1d = timedelta(days=1)
        td_7d = timedelta(days=7)

        ## Daily 
        df1 = self.nze_dat.history(interval="1d", start=week_start, end=week_start+td_7d)
        self.assertGreaterEqual(df1.shape[0], 3)
        df2 = self.nze_dat.history(interval="1d", start=week_start, end=week_start+td_7d)
        try:
            self.verify_df(df1, df2)
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
        df1 = self.nze_dat.history(interval="1d", start=week_start, end=week_start+td_7d, actions=True)
        self.assertTrue("Dividends" in df1.columns.values)
        self.assertTrue("Stock Splits" in df1.columns.values)
        self.assertGreaterEqual(df1.shape[0], 3)
        df2 = self.nze_dat.history(interval="1d", start=week_start, end=week_start+td_7d, actions=True)
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
        df1 = self.nze_dat.history(interval="1h", start=week_start, end=week_start+td_1d*2)
        self.assertGreaterEqual(df1.shape[0], 7)
        df2 = self.nze_dat.history(interval="1h", start=week_start, end=week_start+td_1d*2)
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
        df1 = self.nze_dat.history(interval="1wk", start=week_start, end=week_start+td_7d*2)
        self.assertEqual(df1.shape[0], 2)
        df2 = self.nze_dat.history(interval="1wk", start=week_start, end=week_start+td_7d*2)
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
        self.verify_df(df1, df2)


    def test_history_basics_hour_usa(self):
        # Check fetching single hour

        yfct.SetExchangeTzName(self.usa_exchange, self.usa_market_tz_name)

        start_d = date.today() - timedelta(days=1)
        while not yfct.ExchangeOpenOnDay(self.usa_exchange, start_d):
            start_d -= timedelta(days=1)
        start_dt = datetime.combine(start_d, time(10,30), self.usa_market_tz)
        end_dt = start_dt + timedelta(hours=1)
        df1 = self.usa_dat.history(interval="1h", start=start_dt, end=end_dt)
        self.assertEqual(df1.shape[0], 1)
        self.assertEqual(df1.index[0], start_dt)
        df2 = self.usa_dat.history(interval="1h", start=start_dt, end=end_dt)
        self.assertTrue(df1.equals(df2))


    def test_history_basics_hour_nze(self):
        # Check fetching single hour

        yfct.SetExchangeTzName(self.nze_exchange, self.nze_market_tz_name)

        start_d = date.today() - timedelta(days=1)
        while not yfct.ExchangeOpenOnDay(self.nze_exchange, start_d):
            start_d -= timedelta(days=1)
        start_dt = datetime.combine(start_d, time(10,0), self.nze_market_tz)
        end_dt = start_dt + timedelta(hours=1)
        df1 = self.nze_dat.history(interval="1h", start=start_dt, end=end_dt)
        self.assertEqual(df1.shape[0], 1)
        self.assertEqual(df1.index[0], start_dt)
        df2 = self.nze_dat.history(interval="1h", start=start_dt, end=end_dt)
        self.verify_df(df1, df2)


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
        self.verify_df(df1.iloc[1:3], df2.iloc[0:2])

        ## Then fetching full date range is identical to unique rows in df1+df2
        df3 = self.nze_dat.history(interval="1d", start=date(2022,2,8), end=date(2022,2,12))
        self.verify_df(df3, pd.concat([df1.iloc[0:1], df2]))


    def test_matches_yf_daily_usa(self):
        dat_yf = yf.Ticker(self.usa_tkr, session=self.session)

        start_day_str = "2022-06-20"
        end_day_str = "2022-06-25"

        for aa in [False,True]:
            df_yf = dat_yf.history(start=start_day_str, end=end_day_str, auto_adjust=aa, back_adjust=False)
            df_yfc = self.usa_dat.history(start=start_day_str, end=end_day_str, adjust_divs=aa)

            for c in yfcd.yf_data_cols:
                if not c in df_yf.columns:
                    continue
                elif c == "Adj Close" and not c in df_yfc.columns:
                    continue
                try:
                    if aa or c == "Adj Close":
                        self.assertTrue(np.isclose(df_yf[c].values, df_yfc[c].values, rtol=5e-6).all())
                    else:
                        self.assertTrue(np.equal(df_yf[c].values, df_yfc[c].values).all())
                except:
                    f = ~np.equal(df_yf[c].values, df_yfc[c].values)
                    print("df_yf:")
                    print(df_yf)
                    print("df_yfc:")
                    print(df_yfc)
                    print("aa={}, c={}".format(aa, c))
                    last_dt = df_yfc.index[f][-1]
                    v1 = df_yfc.loc[last_dt][c]
                    v2 =  df_yf.loc[last_dt][c]
                    print("Last diff: {}: {} - {} = {}".format(last_dt, v1, v2, v1-v2))
                    raise


    def test_matches_yf_daily_nze(self):
        dat_yf = yf.Ticker(self.nze_tkr, session=self.session)

        start_day_str = "2022-06-13"
        end_day_str = "2022-06-18"

        for aa in [False,True]:
            df_yf = dat_yf.history(start=start_day_str, end=end_day_str, auto_adjust=aa, back_adjust=False)
            df_yfc = self.nze_dat.history(start=start_day_str, end=end_day_str, adjust_divs=aa)

            for c in yfcd.yf_data_cols:
                if not c in df_yf.columns:
                    continue
                elif c == "Adj Close" and not c in df_yfc.columns:
                    continue
                try:
                    if aa or "Adj" in c:
                        self.assertTrue(np.isclose(df_yf[c].values, df_yfc[c].values, rtol=5e-6).all())
                    else:
                        self.assertTrue(np.equal(df_yf[c].values, df_yfc[c].values).all())
                except:
                    print("df_yf:")
                    print(df_yf[[c]])
                    print("df_yfc:")
                    print(df_yfc[[c]])
                    print("aa={}, c={}".format(aa, c))
                    raise


    def test_history_final(self):
        # Test 'Final?' column
        tkr_candidates = ["BHG.JO", "INTC", "MEL.NZ"]
        interval = yfcd.Interval.Days1

        dt_now_utc = pd.Timestamp.utcnow()
        dt_now = dt_now_utc.replace(tzinfo=ZoneInfo("UTC"))

        start_d = dt_now_utc.date() -timedelta(days=7)
        end_d = dt_now_utc.date() +timedelta(days=1)

        # First, check during a live session
        for tkr in tkr_candidates:
            dat = yfc.Ticker(tkr, session=self.session)
            exchange = dat.fast_info["exchange"]
            yfct.SetExchangeTzName(exchange, dat.fast_info["timezone"])
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

    def test_history_final_v2(self):
        # Test 'Final?' column
        tkr_candidates = ["BHG.JO", "INTC", "MEL.NZ"]

        dt_now_utc = pd.Timestamp.utcnow()
        dt_now = dt_now_utc.replace(tzinfo=ZoneInfo("UTC"))

        # start_d = dt_now_utc.date() - timedelta(days=7)
        # end_d = dt_now_utc.date() + timedelta(days=1)

        for tkr in tkr_candidates:
            dat = yfc.Ticker(tkr, session=self.session)
            exchange = dat.fast_info["exchange"]
            yfct.SetExchangeTzName(exchange, dat.fast_info["timezone"])

            # df = dat.history(interval="1d", period="1wk")
            # n = df.shape[0]
            # try:
            #     self.assertTrue((df["Final?"].iloc[0:n-1]==True).all())
            #     self.assertFalse(df["Final?"].iloc[n-1])
            # except:
            #     print(f"tkr={tkr}")
            #     print("df:") ; print(df)
            #     raise

            # df = dat.history(interval="1h", period="1d")
            # n = df.shape[0]
            # try:
            #     self.assertTrue((df["Final?"].iloc[0:n-1]==True).all())
            #     self.assertFalse(df["Final?"].iloc[n-1])
            # except:
            #     print(f"tkr={tkr}")
            #     print("df:") ; print(df)
            #     raise

            df = dat.history(interval="1wk", period="1mo")
            n = df.shape[0]
            try:
                self.assertTrue((df["Final?"].iloc[0:n-1]==True).all())
                self.assertFalse(df["Final?"].iloc[n-1])
            except:
                print(f"tkr={tkr}")
                print("df:") ; print(df)
                raise


    def test_periods(self):
        tkrs = self.tkrs
        periods = [p for p in yfcd.Period]
        #
        for tkr in tkrs:
            dat_yf = yf.Ticker(tkr, session=self.session)
            for p in periods:
                self.tempCacheDir.cleanup() ; self.tempCacheDir = tempfile.TemporaryDirectory()
                yfcm.SetCacheDirpath(self.tempCacheDir.name)
                yfcm._option_manager.calendar.accept_unexpected_Yahoo_intervals = True
                dat_yfc = yfc.Ticker(tkr, session=self.session)
                try:
                    df_yf = dat_yf.history(period=yfcd.periodToString[p], auto_adjust=False, repair=True)
                except Exception as e:
                    if "No data found for this date range" in str(e):
                        # Skip
                        continue
                    else:
                        raise
                # Remove any rows when exchange was closed. Yahoo can be naughty and fill in rows when exchange closed.
                sched = yfct.GetExchangeSchedule(dat_yfc.fast_info["exchange"], df_yf.index[0].date(), df_yf.index[-1].date()+timedelta(days=1))
                f_open = yfcu.np_isin_optimised(df_yf.index.date, sched["open"].dt.date)
                f_div_split = (df_yf['Dividends']!=0).to_numpy() | (df_yf['Stock Splits']!=0).to_numpy()
                f_open = f_open | f_div_split
                df_yf = df_yf[f_open]
                df_yf_backup = df_yf.copy()

                df_yfc = dat_yfc.history(period=p, adjust_divs=False)
                if df_yfc is None and p == yfcd.Period.Days1:
                    continue

                ## How Yahoo maps period -> start_date is mysterious, so need to account for my different mapping:
                td = abs(df_yf.index.min() - df_yfc.index.min())
                if td < timedelta(days=28):
                    start_ts = max(df_yf.index.min(), df_yfc.index.min())
                    df_yf = df_yf.loc[start_ts:]
                    df_yfc = df_yfc.loc[start_ts:]
                    if df_yfc.shape[0] == 0:
                        raise Exception("df_yfc is empty")

                try:
                    self.assertEqual(df_yf.shape[0], df_yfc.shape[0])
                except:
                    print("df_yf: {}".format(df_yf.shape))
                    print(df_yf)
                    print("df_yfc: {}".format(df_yfc.shape))
                    print(df_yfc)
                    missing_from_yf = df_yfc.index[yfcu.np_isin_optimised(df_yfc.index, df_yf.index, invert=True)]
                    missing_from_yfc = df_yf.index[yfcu.np_isin_optimised(df_yf.index, df_yfc.index, invert=True)]
                    if len(missing_from_yf)>0:
                        print("missing_from_yf:")
                        print(missing_from_yf)
                    if len(missing_from_yfc)>0:
                        print("missing_from_yfc:")
                        # print(missing_from_yfc)
                        print(df_yf.loc[missing_from_yfc])
                    print("Different shapes")
                    raise
                try:
                    self.assertTrue(np.equal(df_yf.index, df_yfc.index).all())
                except:
                    print("df_yf:",df_yf.shape)
                    print(df_yf)
                    print("df_yfc:",df_yfc.shape)
                    print(df_yfc)
                    print("Index different")
                    raise
                f = df_yfc["Final?"].values
                self.verify_df(df_yfc, df_yf, rtol=1e-15)

                # Fetch from cache should match
                df_yf = df_yf_backup.copy()
                df_yfc = dat_yfc.history(period=p, adjust_divs=False)
                start_ts = max(df_yf.index.min(), df_yfc.index.min())
                df_yf = df_yf.loc[start_ts:]
                df_yfc = df_yfc.loc[start_ts:]
                self.verify_df(df_yfc, df_yf, rtol=1e-15)

    def test_periods_with_persistent_caching(self):
        tkrs = self.tkrs
        periods = [p for p in yfcd.Period]
        #
        for tkr in tkrs:
            dat_yf = yf.Ticker(tkr, session=self.session)
            dat_yfc = yfc.Ticker(tkr, session=self.session)
            for p in periods:
                try:
                    df_yf = dat_yf.history(period=yfcd.periodToString[p], auto_adjust=False, repair=True)
                except Exception as e:
                    if "No data found for this date range" in str(e):
                        # Skip
                        continue
                    else:
                        raise
                # Remove any rows when exchange was closed. Yahoo can be naughty and fill in rows when exchange closed.
                sched = yfct.GetExchangeSchedule(dat_yfc.fast_info["exchange"], df_yf.index[0].date(), df_yf.index[-1].date()+timedelta(days=1))
                f_open = yfcu.np_isin_optimised(df_yf.index.date, sched["open"].dt.date)
                f_div_split = (df_yf['Dividends']!=0).to_numpy() | (df_yf['Stock Splits']!=0).to_numpy()
                f_open = f_open | f_div_split
                df_yf = df_yf[f_open]

                df_yfc = dat_yfc.history(period=p, adjust_divs=False)
                if df_yfc is None and p == yfcd.Period.Days1:
                    continue

                ## How Yahoo maps period -> start_date is mysterious, so need to account for my different mapping:
                td = abs(df_yf.index.min() - df_yfc.index.min())
                if td < timedelta(days=28):
                    start_ts = max(df_yf.index.min(), df_yfc.index.min())
                    df_yf = df_yf.loc[start_ts:]
                    df_yfc = df_yfc.loc[start_ts:]
                    if df_yfc.shape[0] == 0:
                        raise Exception("df_yfc is empty")

                self.verify_df(df_yfc, df_yf, rtol=1e-15)

                # Fetch from cache should match
                df_yfc = dat_yfc.history(period=p, adjust_divs=False)
                if not df_yfc is None:
                    df_yfc = df_yfc.loc[start_ts:]
                self.verify_df(df_yfc, df_yf, rtol=1e-15)


    def test_history_live(self):
        # Fetch during live trading session
        tkr_candidates = self.tkrs
        interval = yfcd.Interval.Days1

        # Exclude low-volume tickers
        del tkr_candidates[tkr_candidates.index("MEL.NZ")]

        dt_now = pd.Timestamp.utcnow().replace(tzinfo=ZoneInfo("UTC"))
        start_dt = dt_now - timedelta(days=7)
        end_dt = dt_now+timedelta(days=1)
        start_d = start_dt.date()
        end_d = end_dt.date()

        for tkr in tkr_candidates:
            dat = yfc.Ticker(tkr)
            exchange = dat.fast_info["exchange"]
            yfct.SetExchangeTzName(exchange, dat.fast_info["timezone"])
            if yfct.IsTimestampInActiveSession(exchange, dt_now):
                d = start_dt.date()
                expected_interval_dates = []
                tz = ZoneInfo(dat.fast_info["timezone"])
                while d < end_dt.date():
                    if yfct.ExchangeOpenOnDay(exchange, d):
                        dt = datetime.combine(d, time(0), tz)
                        expected_interval_dates.append(dt)
                    d += timedelta(days=1)

                df1 = dat.history(interval="1d", start=start_d, end=end_d)

                dat_yf = yf.Ticker(tkr, session=self.session)
                df_yf = dat_yf.history(interval="1d", start=start_d, end=end_d)

                ## Check that table covers expected date range
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

                ## Check it matches YF:
                self.verify_df(df1, df_yf)

                # Refetch before data aged, should return identical table
                sleep(1)
                df2 = dat.history(interval="1d", start=start_d, end=end_d, max_age=timedelta(minutes=1))
                self.verify_df(df1, df2)
                self.assertEqual(df1["FetchDate"].iloc[-1], df2["FetchDate"].iloc[-1])

                # Refetch after data aged, last row should be different
                sleep(3)
                df3 = dat.history(interval="1d", start=start_d, end=end_d, max_age=timedelta(seconds=1))
                try:
                    self.assertEqual(len(expected_interval_dates), df3.shape[0])
                except:
                    print("Different shapes")
                    print("df1:")
                    print(df1)
                    print("df3:")
                    print(df3)
                    raise
                try:
                    self.assertTrue(np.array_equal(expected_interval_dates, df3.index))
                except:
                    print("expected_interval_dates:")
                    pprint(expected_interval_dates)
                    print("df3:")
                    print(df3.index)
                    edt0 = expected_interval_dates[0]
                    adt0 = df3.index[0]
                    print("expected dt0 = {} (tz={})".format(edt0, edt0.tzinfo))
                    print("actual dt0 = {} (tz={})".format(adt0, adt0.tzinfo))
                    print(expected_interval_dates == df3.index)
                    raise
                try:
                    # In 3 seconds maybe no trading occurred. But at least YFC will record new fetch
                    self.assertNotEqual(df1["FetchDate"].iloc[-1], df3["FetchDate"].iloc[-1])
                except:
                    print("df1:")
                    print(df1)
                    print("df3:")
                    print(df3)
                    raise

    def test_history_live_1h_evening(self):
        # Fetch during evening after active session
        tkr_candidates = self.tkrs
        interval = yfcd.Interval.Hours1
        #
        tkr_candidates = ["BHG.JO"]
        #
        dt_now = pd.Timestamp.utcnow().replace(tzinfo=ZoneInfo("UTC"))
        td_1d = timedelta(days=1)
        for tkr in tkr_candidates:
            dat = yfc.Ticker(tkr, session=None)
            exchange = dat.fast_info["exchange"]
            tz = dat.fast_info["timezone"]
            yfct.SetExchangeTzName(exchange, tz)
            d_now = dt_now.astimezone(ZoneInfo(tz)).date()
            if yfct.ExchangeOpenOnDay(exchange, d_now):
                sched = yfct.GetExchangeSchedule(exchange, d_now, d_now+timedelta(days=1))
                if (not sched is None) and dt_now > sched["close"][0]:
                    tz = ZoneInfo(dat.fast_info["timezone"])

                    start = d_now
                    end = d_now+td_1d

                    dt = sched["open"][0]
                    if tkr.endswith(".TA"):
                        # Align back to 9:30am
                        dt = datetime.combine(dt.date(), time(9,30), tzinfo=tz)
                    expected_interval_starts = []
                    while dt < sched["close"][0]:
                        expected_interval_starts.append(dt)
                        dt += timedelta(hours=1)

                    ## Check that table covers expected date range
                    df1 = dat.history(interval="1h", start=start, end=end)
                    try:
                        self.assertTrue(np.array_equal(expected_interval_starts, df1.index))
                    except:
                        print("sched:")
                        print(sched)
                        print("expected_interval_starts:")
                        pprint(expected_interval_starts)
                        print("df1.index:")
                        pprint(df1.index)
                        raise

                    ## Finally, check it matches YF:
                    df1 = dat.history(start=start, end=end, interval="1h", adjust_divs=False)
                    dat_yf = yf.Ticker(tkr, session=self.session)
                    # Note: Yahoo doesn't dividend-adjust hourly. Also have to prepend a day to
                    #       get correct volume for start date
                    df_yf = dat_yf.history(start=start-td_1d, interval="1h", repair=True)
                    df_yf = df_yf.loc[df_yf.index.date>=start]
                    # Discard 0-volume data at market close
                    sched = yfct.GetExchangeSchedule(dat.fast_info["exchange"], df_yf.index.date.min(), df_yf.index.date.max()+td_1d)
                    sched["_date"] = sched.index.date
                    df_yf["_date"] = df_yf.index.date
                    answer2 = df_yf.merge(sched, on="_date", how="left", validate="many_to_one")
                    answer2.index = df_yf.index ; df_yf = answer2
                    f_drop = (df_yf["Volume"]==0).values & (df_yf.index>=df_yf["close"])
                    df_yf = df_yf[~f_drop].drop("_date",axis=1)
                    # YF hourly volume is not split-adjusted, so adjust:
                    ss = df_yf["Stock Splits"].copy()
                    ss[ss==0.0] = 1.0
                    ss_rcp = 1.0/ss
                    csf = ss_rcp.sort_index(ascending=False).cumprod().sort_index(ascending=True).shift(-1, fill_value=1.0)
                    df_yf["Volume"] /= csf
                    df_yf = df_yf[df_yf.index.date<end]
                    self.verify_df(df1, df_yf, rtol=1e-10)

    def test_history_live_1d_evening(self):
        # Fetch during evening after active session
        tkr_candidates = self.tkrs
        interval = yfcd.Interval.Days1
        #
        dt_now = pd.Timestamp.utcnow().replace(tzinfo=ZoneInfo("UTC"))
        d_now = dt_now.date()
        for tkr in tkr_candidates:
            dat = yfc.Ticker(tkr, session=None)
            exchange = dat.fast_info["exchange"]
            yfct.SetExchangeTzName(exchange, dat.fast_info["timezone"])
            if yfct.ExchangeOpenOnDay(exchange, d_now):
                sched = yfct.GetExchangeSchedule(exchange, d_now, d_now+timedelta(days=1))
                if (not sched is None) and dt_now > sched["close"][0]:
                    start_dt = dt_now - timedelta(days=7)
                    end_dt = dt_now
                    tz = ZoneInfo(dat.fast_info["timezone"])

                    d = start_dt.date()
                    expected_interval_dates = []
                    while d < end_dt.date():
                        if yfct.ExchangeOpenOnDay(exchange, d):
                            dt = datetime.combine(d, time(0), tz)
                            expected_interval_dates.append(dt)
                        d += timedelta(days=1)

                    ## Check that table covers expected date range
                    df1 = dat.history(interval="1d", start=start_dt.date(), end=end_dt.date(), keepna=True)
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
                    df_yf = dat_yf.history(interval="1d", start=start_dt.date(), end=end_dt.date(), keepna=True)
                    self.verify_df(df1, df_yf, rtol=1e-10)

    def test_history_live_1w_evening(self):
        # Fetch during evening after active session
        tkr_candidates = self.tkrs
        interval = yfcd.Interval.Week
        #
        dt_now = pd.Timestamp.utcnow().replace(tzinfo=ZoneInfo("UTC"))
        d_now = dt_now.date()
        for tkr in tkr_candidates:
            dat = yfc.Ticker(tkr, session=self.session)

            exchange = dat.fast_info["exchange"]
            yfct.SetExchangeTzName(exchange, dat.fast_info["timezone"])
            if yfct.ExchangeOpenOnDay(exchange, d_now):
                sched = yfct.GetExchangeSchedule(exchange, d_now, d_now+timedelta(days=1))
                if (not sched is None) and dt_now > sched["close"][0]:
                    start_dt = dt_now - timedelta(days=dt_now.weekday())
                    end_dt = start_dt+timedelta(days=5)
                    tz = ZoneInfo(dat.fast_info["timezone"])

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
                    df1 = dat.history(interval="1wk", start=start_dt.date(), end=end_dt.date(), keepna=True)
                    try:
                        self.assertTrue(np.array_equal(expected_interval_dates, df1.index))
                    except:
                        print("Date range: {} -> {}".format(start_dt, end_dt))
                        print("expected_interval_dates")
                        pprint(expected_interval_dates)
                        print("df1:")
                        print(df1)
                        raise

                    ## Finally, check it matches YF:
                    dat_yf = yf.Ticker(tkr, session=self.session)
                    df_yf = dat_yf.history(interval="1wk", start=start_dt.date(), end=end_dt.date(), keepna=True)
                    self.verify_df(df1, df_yf)

    def test_history_jse_is_weird(self):
        # JSE market closes at 5pm local time, but if request 1h data 
        # including today then it returns a row with timestamp today@5pm and 0 volume.
        # This mysterious row disappears tomorrow.
        interval = "1h"
        tkr = "IMP.JO"
        dat = yfc.Ticker(tkr, session=self.session)
        exchange = dat.fast_info["exchange"]
        tz_name = dat.fast_info["timezone"]
        tz = ZoneInfo(tz_name)
        yfct.SetExchangeTzName(exchange, tz_name)

        dt_now = pd.Timestamp.utcnow().replace(tzinfo=ZoneInfo("UTC"))
        # d_now = dt_now.date()
        d_now = dt_now.astimezone(tz).date()
        td_1d = timedelta(days=1)
        if yfct.ExchangeOpenOnDay(exchange, d_now):
            sched = yfct.GetExchangeSchedule(exchange, d_now-5*td_1d, d_now+td_1d)
            if not sched is None:
                for i in range(sched.shape[0]-1, -1, -1):
                    if dt_now >= sched["close"].iloc[i]:
                        break
                d = sched["close"].iloc[i].date()
                df = dat.history(start=d, end=d+td_1d, interval="1h")

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
        dat = yfc.Ticker(tkr, session=self.session)
        start_d = date(2022,7,4)
        end_d = date(2022,7,9)

        exchange = dat.fast_info["exchange"]
        yfct.SetExchangeTzName(exchange, dat.fast_info["timezone"])

        dt_now = pd.Timestamp.utcnow().replace(tzinfo=ZoneInfo("UTC"))
        if yfct.IsTimestampInActiveSession(exchange, dt_now):
            df = dat.history(start=start_d, end=end_d, interval="1d", keepna=True)
            idx = 2 # 3rd row = 2022-7-6
            self.assertTrue(np.isnan(df["Close"][idx]) or df["Volume"][idx]==0)

            df = dat.history(start=start_d, end=start_d+timedelta(days=1), interval="1h", keepna=True)
            idx = 1
            self.assertTrue(np.isnan(df["Close"][idx]))

    def test_bug_prepending_nans(self):
        tkr = "QDEL"
        dat = yfc.Ticker(tkr, session=self.session)
        # Init cache:
        dat.history(period="1wk")

        # Modify cache:
        fp = os.path.join(self.tempCacheDir.name, tkr, "history-1d.pkl")
        with open(fp, 'rb') as f:
            d = pkl.load(f)
        df = d["data"]
        # - remove today
        if df.index[-1].date()==date.today():
            df = df.drop(df.index[-1])
        # - remove all before last
        if df.shape[0]>1:
            df = df.drop(df.index[0:df.shape[0]-1])
        if df.shape[0]!=1:
            raise Exception("df should have 1 row")
        # - set last interval to non-final
        dt = df.index[0]
        df.loc[dt,"Final?"] = False
        fetch_dt = datetime.combine(dt.date(), time(12), tzinfo=dt.tz).astimezone(df.loc[dt,"FetchDate"].tz)
        df.loc[dt,"FetchDate"] = fetch_dt
        d["data"] = df
        with open(fp, 'wb') as f:
            pkl.dump(d, f, 4)

        dat = yfc.Ticker(tkr, session=self.session)
        df = dat.history(start=date.today()-timedelta(days=7))
        self.assertFalse(df["Close"].isna().any())



if __name__ == '__main__':
    unittest.main()

    # # Run tests sequentially:
    # import inspect
    # test_src = inspect.getsource(Test_Yfc_Interface)
    # unittest.TestLoader.sortTestMethodsUsing = lambda _, x, y: (
    #     test_src.index(f"def {x}") - test_src.index(f"def {y}")
    # )
    # unittest.main(verbosity=2)
