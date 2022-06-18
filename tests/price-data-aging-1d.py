import unittest

import sys
sys.path.append("/home/gonzo/Repos/yfinance-cache/src/yfinance_cache")
import yfc_dat as yfcd
import yfc_time as yfct

from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

from pprint import pprint

class Test_PriceDataAging_1D(unittest.TestCase):

    def setUp(self):
        self.market = "us_market"
        self.exchange = "NMS"
        self.market_tz = ZoneInfo('US/Eastern')
        self.market_open_time  = time(hour=9, minute=30)
        self.market_close_time = time(hour=16, minute=0)

        self.monday  = date(year=2022, month=2, day=7)
        self.tuesday = date(year=2022, month=2, day=8)
        self.friday  = date(year=2022, month=2, day=11)
        self.saturday= date(year=2022, month=2, day=12)

    ## Test day interval fetched same day
    def test_sameDay(self):
        interval = yfcd.Interval.Days1
        interval_start_dt = datetime.combine(self.monday, self.market_open_time, self.market_tz)

        max_ages = []
        fetch_dts = []
        dt_nows = []
        expire_on_candle_closes = []
        yf_lags = []
        answers = []

        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=5),  self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.monday, time(hour=12, minute=34), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=5),  self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.monday, time(hour=12, minute=35), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(True)



        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=5),  self.market_tz))
        max_ages.append(timedelta(minutes=60))
        dt_nows.append(  datetime.combine(self.monday, time(hour=13, minute=4), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=5),  self.market_tz))
        max_ages.append(timedelta(hours=1))
        dt_nows.append(  datetime.combine(self.monday, time(hour=13, minute=5), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(True)



        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=55), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.monday, time(hour=16, minute=0),  self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=55), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.monday, time(hour=16, minute=0),  self.market_tz))
        expire_on_candle_closes.append(True) ; yf_lags.append(timedelta(minutes=0))
        answers.append(True)

        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=55), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.monday, time(hour=16, minute=0),  self.market_tz))
        expire_on_candle_closes.append(True) ; yf_lags.append(timedelta(minutes=1))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=55), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.monday, time(hour=16, minute=1),  self.market_tz))
        expire_on_candle_closes.append(True) ; yf_lags.append(timedelta(minutes=1))
        answers.append(True)



        fetch_dts.append(datetime.combine(self.monday,  time(hour=15, minute=55), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.tuesday, time(hour=9,  minute=0),  self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday,  time(hour=15, minute=55), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.tuesday, time(hour=9, minute=30),  self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday,  time(hour=15, minute=55), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.tuesday, time(hour=9, minute=30),  self.market_tz))
        expire_on_candle_closes.append(True) ; yf_lags.append(None)
        answers.append(True)

        for i in range(len(fetch_dts)):
            fetch_dt = fetch_dts[i]
            max_age = max_ages[i]
            dt_now = dt_nows[i]
            expire_on_candle_close = expire_on_candle_closes[i]
            yf_lag = yf_lags[i]
            answer = answers[i]
            response = yfct.IsPriceDatapointExpired(interval_start_dt, fetch_dt, max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
            try:
                self.assertEqual(response, answer)
            except:
                print("fetch_dt = {0}".format(fetch_dt))
                print("max_age = {0}".format(max_age))
                print("dt_now = {0}".format(dt_now))
                print("answer:")
                pprint(answer)
                print("response:")
                pprint(response)
                raise

    ## Test Friday interval fetched same day when dt_now is next day (weekend)
    def test_nextDayWeekend(self):
        interval = yfcd.Interval.Days1
        interval_start_dt = datetime.combine(self.friday, self.market_open_time, self.market_tz)

        max_ages = []
        fetch_dts = []
        dt_nows = []
        expire_on_candle_closes = []
        yf_lags = []
        answers = []

        max_ages.append(timedelta(minutes=30))
        fetch_dts.append(datetime.combine(self.friday,   time(hour=14, minute=55), self.market_tz))
        dt_nows.append(  datetime.combine(self.saturday, time(hour=9, minute=10), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(True)

        max_ages.append(timedelta(minutes=30))
        fetch_dts.append(datetime.combine(self.friday,   time(hour=15, minute=55), self.market_tz))
        dt_nows.append(  datetime.combine(self.saturday, time(hour=9, minute=10), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(False)

        max_ages.append(timedelta(minutes=30))
        fetch_dts.append(datetime.combine(self.friday,   time(hour=15, minute=55), self.market_tz))
        dt_nows.append(  datetime.combine(self.saturday, time(hour=9, minute=10), self.market_tz))
        expire_on_candle_closes.append(True) ; yf_lags.append(None)
        answers.append(True)

        for i in range(len(fetch_dts)):
            fetch_dt = fetch_dts[i]
            max_age = max_ages[i]
            dt_now = dt_nows[i]
            expire_on_candle_close = expire_on_candle_closes[i]
            yf_lag = yf_lags[i]
            answer = answers[i]
            response = yfct.IsPriceDatapointExpired(interval_start_dt, fetch_dt, max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
            try:
                self.assertEqual(response, answer)
            except:
                print("fetch_dt = {0}".format(fetch_dt))
                print("max_age = {0}".format(max_age))
                print("dt_now = {0}".format(dt_now))
                print("answer:")
                pprint(answer)
                print("response:")
                pprint(response)
                raise

    ## Batch test day interval during same day
    def test_intraDay_batch(self):
        interval = yfcd.Interval.Days1
        interval_start_dt = datetime.combine(self.monday, self.market_open_time, self.market_tz)

        max_age = timedelta(minutes=30)
        expire_on_candle_close = False ; yf_lag = None
        dt_now = datetime.combine(self.monday, time(hour=13, minute=10), self.market_tz)

        interval_start_dts = []
        fetch_dts = []

        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=9, minute=5), self.market_tz))
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=30), self.market_tz))
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=13, minute=0), self.market_tz))

        responses = yfct.IsPriceDatapointExpired_batch(interval_start_dts, fetch_dts, max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
        for i in range(len(responses)):
            answer = yfct.IsPriceDatapointExpired(interval_start_dts[i], fetch_dts[i], max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
            try:
                self.assertEqual(responses[i], answer)
            except:
                print("interval = {0}".format(interval))
                print("max_age = {0}".format(max_age))
                print("interval_start_dt = {0}".format(interval_start_dts))
                print("dt_now = {0}".format(dt_now))
                print("fetch_dt:")
                pprint(fetch_dts[i])
                print("answer:")
                pprint(answer)
                print("response:")
                pprint(responses[i])
                raise

    ## Batch test day interval just after market close
    def test_lateDay_batch(self):
        interval = yfcd.Interval.Days1
        interval_start_dt = datetime.combine(self.monday, self.market_open_time, self.market_tz)

        max_age = timedelta(minutes=30)
        expire_on_candle_close = False ; yf_lag = None
        dt_now = datetime.combine(self.monday, time(hour=16, minute=15), self.market_tz)

        interval_start_dts = []
        fetch_dts = []

        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=14, minute=0), self.market_tz))
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=40), self.market_tz))
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=55), self.market_tz))

        responses = yfct.IsPriceDatapointExpired_batch(interval_start_dts, fetch_dts, max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
        for i in range(len(responses)):
            answer = yfct.IsPriceDatapointExpired(interval_start_dts[i], fetch_dts[i], max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
            try:
                self.assertEqual(responses[i], answer)
            except:
                print("interval = {0}".format(interval))
                print("max_age = {0}".format(max_age))
                print("interval_start_dt = {0}".format(interval_start_dts))
                print("dt_now = {0}".format(dt_now))
                print("fetch_dt:")
                pprint(fetch_dts[i])
                print("answer:")
                pprint(answer)
                print("response:")
                pprint(responses[i])
                raise

    ## Batch test day interval next day during market open
    def test_nextDay_batch1(self):
        interval = yfcd.Interval.Days1
        interval_start_dt = datetime.combine(self.monday, self.market_open_time, self.market_tz)

        max_age = timedelta(minutes=30)
        expire_on_candle_close = False ; yf_lag = None
        dt_now = datetime.combine(self.tuesday, time(hour=9, minute=45), self.market_tz)

        interval_start_dts = []
        fetch_dts = []

        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=5), self.market_tz))
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=55), self.market_tz))
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=17, minute=0), self.market_tz))

        responses = yfct.IsPriceDatapointExpired_batch(interval_start_dts, fetch_dts, max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
        for i in range(len(responses)):
            answer = yfct.IsPriceDatapointExpired(interval_start_dts[i], fetch_dts[i], max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
            try:
                self.assertEqual(responses[i], answer)
            except:
                print("interval = {0}".format(interval))
                print("max_age = {0}".format(max_age))
                print("interval_start_dt = {0}".format(interval_start_dts))
                print("dt_now = {0}".format(dt_now))
                print("fetch_dt:")
                pprint(fetch_dts[i])
                print("answer:")
                pprint(answer)
                print("response:")
                pprint(responses[i])
                raise

    ## Batch test day interval next day during market open with 24-hour age
    def test_nextDay_batch2(self):
        interval = yfcd.Interval.Days1
        interval_start_dt = datetime.combine(self.monday, self.market_open_time, self.market_tz)

        max_age = timedelta(days=1)
        expire_on_candle_close = False ; yf_lag = None
        dt_now = datetime.combine(self.tuesday, time(hour=9, minute=45), self.market_tz)

        interval_start_dts = []
        fetch_dts = []

        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=9, minute=30), self.market_tz))
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=10, minute=0), self.market_tz))

        responses = yfct.IsPriceDatapointExpired_batch(interval_start_dts, fetch_dts, max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
        for i in range(len(responses)):
            answer = yfct.IsPriceDatapointExpired(interval_start_dts[i], fetch_dts[i], max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
            try:
                self.assertEqual(responses[i], answer)
            except:
                print("interval = {0}".format(interval))
                print("max_age = {0}".format(max_age))
                print("interval_start_dt = {0}".format(interval_start_dts))
                print("dt_now = {0}".format(dt_now))
                print("fetch_dt:")
                pprint(fetch_dts[i])
                print("answer:")
                pprint(answer)
                print("response:")
                pprint(responses[i])
                raise

    ## Batch test Friday interval next day Saturday
    def test_nextDay_batch3(self):
        interval = yfcd.Interval.Days1
        interval_start_dt = datetime.combine(self.friday, self.market_open_time, self.market_tz)

        max_age = timedelta(minutes=30)
        expire_on_candle_close = False ; yf_lag = None
        dt_now = datetime.combine(self.saturday, time(hour=10, minute=45), self.market_tz)

        interval_start_dts = []
        fetch_dts = []

        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.friday, time(hour=15, minute=5), self.market_tz))
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.friday, time(hour=15, minute=55), self.market_tz))
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.friday, time(hour=17, minute=0), self.market_tz))

        responses = yfct.IsPriceDatapointExpired_batch(interval_start_dts, fetch_dts, max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
        for i in range(len(responses)):
            answer = yfct.IsPriceDatapointExpired(interval_start_dts[i], fetch_dts[i], max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
            try:
                self.assertEqual(responses[i], answer)
            except:
                print("interval = {0}".format(interval))
                print("max_age = {0}".format(max_age))
                print("interval_start_dt = {0}".format(interval_start_dts))
                print("dt_now = {0}".format(dt_now))
                print("fetch_dt:")
                pprint(fetch_dts[i])
                print("answer:")
                pprint(answer)
                print("response:")
                pprint(responses[i])
                raise


if __name__ == '__main__':
    unittest.main()
