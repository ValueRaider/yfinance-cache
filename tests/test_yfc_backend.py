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
##  4    5    6    7    8    9    10
##  11   12   13   14   15*  16   17
##  18*  19*  20   21   22   23   24
##  25*

class Test_Yfc_Backend(unittest.TestCase):

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
        self.usa_market_tz_name = 'America/New_York'
        self.usa_market_tz = ZoneInfo('America/New_York')
        self.usa_market_open_time  = time(hour=9, minute=30)
        self.usa_market_close_time = time(hour=16, minute=0)
        self.usa_dat = yfc.Ticker(self.usa_tkr, session=self.session)

        self.nze_tkr = "MEL.NZ"
        self.nze_market = "nz_market"
        self.nze_exchange = "NZE"
        self.nze_market_tz_name = 'Pacific/Auckland'
        self.nze_market_tz = ZoneInfo('Pacific/Auckland')
        self.nze_market_open_time  = time(hour=10, minute=0)
        self.nze_market_close_time = time(hour=16, minute=45)
        self.nze_dat = yfc.Ticker(self.nze_tkr, session=self.session)


    def tearDown(self):
        self.tempCacheDir.cleanup()
        self.session.close()


    def test_yf_lag(self):
        ## Only use high-volume stocks:
        tkr_candidates = ["AZN.L", "ASML.AS", "BHG.JO", "INTC", "MEL.NZ"]

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


    def test_CalcIntervalLastDataDt_USA_hourly(self):
        interval = yfcd.Interval.Hours1

        day = date(2022,2,7)

        exchange = self.usa_exchange
        tz = self.usa_market_tz
        yfct.SetExchangeTzName(exchange, self.usa_market_tz_name)
        market_close_dt = datetime.combine(day, self.usa_market_close_time, tz)

        lag = timedelta(0)
        dts = []
        answers = []
        for h in range(9,16):
            dt = datetime.combine(day, time(h,30), tz)
            dts.append(dt)
            dt_last = min(dt+timedelta(hours=1), market_close_dt)
            answers.append(dt_last+lag)
        for i in range(len(dts)):
            response = yfct.CalcIntervalLastDataDt(exchange, dts[i], interval, yf_lag=lag)
            try:
                self.assertEqual(response, answers[i])
            except:
                print("dt = {}".format(dts[i]))
                print("response = {}".format(response))
                print("answer = {}".format(answers[i]))
                raise

        lag = timedelta(minutes=15)
        dts = []
        answers = []
        for h in range(9,16):
            dt = datetime.combine(day, time(h,30), tz)
            dts.append(dt)
            dt_last = min(dt+timedelta(hours=1), market_close_dt)
            answers.append(dt_last+lag)
        for i in range(len(dts)):
            response = yfct.CalcIntervalLastDataDt(exchange, dts[i], interval, yf_lag=lag)
            try:
                self.assertEqual(response, answers[i])
            except:
                print("dt = {}".format(dts[i]))
                print("response = {}".format(response))
                print("answer = {}".format(answers[i]))
                raise


    def test_CalcIntervalLastDataDt_USA_daily(self):
        interval = yfcd.Interval.Days1

        exchange = self.usa_exchange
        tz = self.usa_market_tz
        yfct.SetExchangeTzName(exchange, self.usa_market_tz_name)

        lag = timedelta(0)
        dts = []
        answers = []
        for d in range(7,12):
            day = date(2022,2,d)
            dt = datetime.combine(day, time(14,30), tz)
            dts.append(dt)
            dt_last = datetime.combine(day, self.usa_market_close_time, tz)
            answers.append(dt_last+lag)
        for i in range(len(dts)):
            response = yfct.CalcIntervalLastDataDt(exchange, dts[i], interval, yf_lag=lag)
            try:
                self.assertEqual(response, answers[i])
            except:
                print("dt = {}".format(dts[i]))
                print("response = {}".format(response))
                print("answer = {}".format(answers[i]))
                raise

        lag = timedelta(minutes=15)
        dts = []
        answers = []
        for d in range(7,12):
            day = date(2022,2,d)
            dt = datetime.combine(day, time(14,30), tz)
            dts.append(dt)
            dt_last = datetime.combine(day, self.usa_market_close_time, tz)
            answers.append(dt_last+lag)
        for i in range(len(dts)):
            response = yfct.CalcIntervalLastDataDt(exchange, dts[i], interval, yf_lag=lag)
            try:
                self.assertEqual(response, answers[i])
            except:
                print("dt = {}".format(dts[i]))
                print("response = {}".format(response))
                print("answer = {}".format(answers[i]))
                raise


    def test_CalcIntervalLastDataDt_USA_weekly(self):
        interval = yfcd.Interval.Week

        exchange = self.usa_exchange
        tz = self.usa_market_tz
        yfct.SetExchangeTzName(exchange, self.usa_market_tz_name)

        lag = timedelta(0)
        dts = []
        answers = []
        week_start_day = date(2022,2,7)
        answer = datetime.combine(week_start_day, self.usa_market_close_time, tz)+timedelta(days=4)
        for d in range(7,12):
            day = date(2022,2,d)
            dt = datetime.combine(day, time(14,30), tz)
            dts.append(dt)
            answers.append(answer+lag)
        week_start_day = date(2022,2,14)
        answer = datetime.combine(week_start_day, self.usa_market_close_time, tz)+timedelta(days=4)
        for d in range(14,19):
            day = date(2022,2,d)
            dt = datetime.combine(day, time(14,30), tz)
            dts.append(dt)
            answers.append(answer+lag)
        for i in range(len(dts)):
            response = yfct.CalcIntervalLastDataDt(exchange, dts[i], interval, yf_lag=lag)
            try:
                self.assertEqual(response, answers[i])
            except:
                print("dt = {}".format(dts[i]))
                print("response = {}".format(response))
                print("answer = {}".format(answers[i]))
                raise


    def test_CalcIntervalLastDataDt_NZE_hourly(self):
        interval = yfcd.Interval.Hours1

        day = date(2022,4,4)

        exchange = self.nze_exchange
        tz = self.nze_market_tz
        yfct.SetExchangeTzName(exchange, self.nze_market_tz_name)
        market_close_dt = datetime.combine(day, self.nze_market_close_time, tz)

        lag = timedelta(0)
        dts = []
        answers = []
        for h in range(10,17):
            dt = datetime.combine(day, time(h), tz)
            dts.append(dt)
            dt_last = min(dt+timedelta(hours=1), market_close_dt)
            answers.append(dt_last+lag)
        for i in range(len(dts)):
            response = yfct.CalcIntervalLastDataDt(exchange, dts[i], interval, yf_lag=lag)
            try:
                self.assertEqual(response, answers[i])
            except:
                print("dt = {}".format(dts[i]))
                print("response = {}".format(response))
                print("answer = {}".format(answers[i]))
                raise

        lag = timedelta(minutes=15)
        dts = []
        answers = []
        for h in range(10,17):
            dt = datetime.combine(day, time(h), tz)
            dts.append(dt)
            dt_last = min(dt+timedelta(hours=1), market_close_dt)
            answers.append(dt_last+lag)
        for i in range(len(dts)):
            response = yfct.CalcIntervalLastDataDt(exchange, dts[i], interval, yf_lag=lag)
            try:
                self.assertEqual(response, answers[i])
            except:
                print("dt = {}".format(dts[i]))
                print("response = {}".format(response))
                print("answer = {}".format(answers[i]))
                raise


    def test_CalcIntervalLastDataDt_NZE_daily(self):
        interval = yfcd.Interval.Days1

        exchange = self.nze_exchange
        tz = self.nze_market_tz
        yfct.SetExchangeTzName(exchange, self.nze_market_tz_name)

        lag = timedelta(0)
        dts = []
        answers = []
        for d in range(4,9):
            day = date(2022,4,d)
            dt = datetime.combine(day, time(14), tz)
            dts.append(dt)
            dt_last = datetime.combine(day, self.nze_market_close_time, tz)
            answers.append(dt_last+lag)
        for i in range(len(dts)):
            response = yfct.CalcIntervalLastDataDt(exchange, dts[i], interval, yf_lag=lag)
            try:
                self.assertEqual(response, answers[i])
            except:
                print("dt = {}".format(dts[i]))
                print("response = {}".format(response))
                print("answer = {}".format(answers[i]))
                raise

        lag = timedelta(minutes=15)
        dts = []
        answers = []
        for d in range(4,9):
            day = date(2022,4,d)
            dt = datetime.combine(day, time(14), tz)
            dts.append(dt)
            dt_last = datetime.combine(day, self.nze_market_close_time, tz)
            answers.append(dt_last+lag)
        for i in range(len(dts)):
            response = yfct.CalcIntervalLastDataDt(exchange, dts[i], interval, yf_lag=lag)
            try:
                self.assertEqual(response, answers[i])
            except:
                print("dt = {}".format(dts[i]))
                print("response = {}".format(response))
                print("answer = {}".format(answers[i]))
                raise


    def test_CalcIntervalLastDataDt_NZE_weekly(self):
        interval = yfcd.Interval.Week

        exchange = self.nze_exchange
        tz = self.nze_market_tz
        yfct.SetExchangeTzName(exchange, self.nze_market_tz_name)

        lag = timedelta(0)
        dts = []
        answers = []
        week_start_day = date(2022,4,4)
        answer = datetime.combine(date(2022,4,8), self.nze_market_close_time, tz)
        for d in range(4,9):
            day = date(2022,4,d)
            dt = datetime.combine(day, time(14), tz)
            dts.append(dt)
            answers.append(answer+lag)
        week_start_day = date(2022,4,11)
        answer = datetime.combine(date(2022,4,14), self.nze_market_close_time, tz)
        for d in range(11,16):
            day = date(2022,4,d)
            dt = datetime.combine(day, time(14), tz)
            dts.append(dt)
            answers.append(answer+lag)
        for i in range(len(dts)):
            response = yfct.CalcIntervalLastDataDt(exchange, dts[i], interval, yf_lag=lag)
            try:
                self.assertEqual(response, answers[i])
            except:
                print("dt = {}".format(dts[i]))
                print("response = {}".format(response))
                print("answer = {}".format(answers[i]))
                raise


    def test_CalcIntervalLastDataDt_USA_hourly_batch(self):
        interval = yfcd.Interval.Hours1

        exchange = self.usa_exchange
        tz = self.usa_market_tz
        yfct.SetExchangeTzName(exchange, self.usa_market_tz_name)

        lags = [timedelta(0), timedelta(minutes=15)]

        start_d = date.today()
        td_1d = timedelta(days=1)
        week_start_d = start_d - td_1d*start_d.weekday()
        week2_start_d = week_start_d -7*td_1d
        week1_start_d = week2_start_d -7*td_1d
        days =  [week1_start_d+x*td_1d for x in [0,1,2,3,4]]
        days += [week2_start_d+x*td_1d for x in [0,1,2,3,4]]

        times = [time(h,30) for h in range(9,16)]
        dts = []
        for d in days:
            if yfct.ExchangeOpenOnDay(exchange, d):
                for t in times:
                    dts.append(datetime.combine(d, t, tz))

        for lag in lags:
            responses = yfct.CalcIntervalLastDataDt_batch(exchange, dts, interval, yf_lag=lag)
            for i in range(len(dts)):
                answer = yfct.CalcIntervalLastDataDt(exchange, dts[i], interval, yf_lag=lag)
                try:
                    self.assertEqual(responses[i], answer)
                except:
                    print("dt = {}".format(dts[i]))
                    print("response = {}".format(responses[i]))
                    print("answer = {}".format(answer))
                    raise


    def test_history_backend_usa(self):
        # index should always be DatetimeIndex

        yfct.SetExchangeTzName(self.usa_exchange, self.usa_market_tz_name)

        intervals = ["30m", "1h", "1d"]
        td_1d = timedelta(days=1)
        start_d = date.today() -td_1d
        start_d = start_d - timedelta(days=start_d.weekday())
        while not yfct.ExchangeOpenOnDay(self.usa_exchange, start_d):
            start_d -= td_1d
        end_d = start_d +td_1d
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
        # Ticker PNL.L missing 90minutes of trading on morning of 2022-07-18, 
        # and Yahoo not returning NaN rows in place. So YFC needs to insert NaN rows

        tkr="PNL.L"
        exchange="LSE"
        tz_name="Europe/London"
        tz=ZoneInfo(tz_name)
        dat = yfc.Ticker(tkr, session=self.session)

        dt0 = datetime(2022,7,18, 8,0,tzinfo=tz)
        dt1 = datetime(2022,7,18, 9,0,tzinfo=tz)

        start = datetime(2022,7,18,8,0,tzinfo=tz)
        end = datetime(2022,7,18,10,0,tzinfo=tz)
        df = dat.history(start=start, end=end, interval="1h", keepna=True)
        self.assertTrue(df.index[0]==dt0)
        self.assertTrue(df.index[1]==dt1)

        end = datetime(2022,7,18,16,0,tzinfo=tz)
        df = dat.history(start=start, end=end, interval="1h", keepna=True)
        self.assertTrue(df.index[0]==dt0)
        self.assertTrue(df.index[1]==dt1)


    def test_GetCDF0(self):
        tkr = "I3E.L"
        tz = ZoneInfo("Europe/London")

        # Setup DataFrame with real data:

        columns = ["Close", "Adj Close", "Dividends"]

        df_rows = []

        dt = datetime.combine(date(2022,10,14), time(0), tz)
        prices = [23.2, 23.2, 0]
        r = pd.Series(data=prices, index=columns, name=dt)
        df_rows.append(r)
        dt = datetime.combine(date(2022,10,13), time(0), tz)
        prices = [23.55, 23.55, 0.1425]
        r = pd.Series(data=prices, index=columns, name=dt)
        df_rows.append(r)
        dt = datetime.combine(date(2022,10,12), time(0), tz)
        prices = [24.15, 24.01, 0]
        r = pd.Series(data=prices, index=columns, name=dt)
        df_rows.append(r)
        dt = datetime.combine(date(2022,10,11), time(0), tz)
        prices = [24.2, 24.06, 0]
        r = pd.Series(data=prices, index=columns, name=dt)
        df_rows.append(r)

        dt = datetime.combine(date(2022,9,16), time(0), tz)
        prices = [23.4, 23.26, 0]
        r = pd.Series(data=prices, index=columns, name=dt)
        df_rows.append(r)
        dt = datetime.combine(date(2022,9,15), time(0), tz)
        prices = [24.5, 24.36, 0.1425]
        r = pd.Series(data=prices, index=columns, name=dt)
        df_rows.append(r)
        dt = datetime.combine(date(2022,9,14), time(0), tz)
        prices = [24.7, 24.41, 0]
        r = pd.Series(data=prices, index=columns, name=dt)
        df_rows.append(r)
        dt = datetime.combine(date(2022,9,13), time(0), tz)
        prices = [24.35, 24.07, 0]
        r = pd.Series(data=prices, index=columns, name=dt)
        df_rows.append(r)

        dt = datetime.combine(date(2022,8,12), time(0), tz)
        prices = [29.95, 29.6, 0]
        r = pd.Series(data=prices, index=columns, name=dt)
        df_rows.append(r)
        dt = datetime.combine(date(2022,8,11), time(0), tz)
        prices = [29.7, 29.35, 0.1425]
        r = pd.Series(data=prices, index=columns, name=dt)
        df_rows.append(r)
        dt = datetime.combine(date(2022,8,10), time(0), tz)
        prices = [29.3, 28.82, 0]
        r = pd.Series(data=prices, index=columns, name=dt)
        df_rows.append(r)
        dt = datetime.combine(date(2022,8,9), time(0), tz)
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


if __name__ == '__main__':
    unittest.main()

    # # Run tests sequentially:
    # import inspect
    # test_src = inspect.getsource(Test_Yfc_Backend)
    # unittest.TestLoader.sortTestMethodsUsing = lambda _, x, y: (
    #     test_src.index(f"def {x}") - test_src.index(f"def {y}")
    # )
    # unittest.main(verbosity=2)
