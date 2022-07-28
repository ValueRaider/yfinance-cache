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
        self.usa_market_tz_name = 'US/Eastern'
        self.usa_market_tz = ZoneInfo('US/Eastern')
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

        days = [date(2022,2,d) for d in [7,8,9,10,11 , 14,15,16,17,18]]
        times = [time(h,30) for h in range(9,16)]
        dts = []
        for d in days:
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
        intervals = ["30m", "1h", "1d", "1wk"]
        start_d = date(2022,7,11)
        end_d = date(2022,7,12)
        for interval in intervals:
            df = self.usa_dat.history(start=start_d, end=end_d, interval=interval)
            self.assertTrue(isinstance(df.index, pd.DatetimeIndex))



if __name__ == '__main__':
    unittest.main()
