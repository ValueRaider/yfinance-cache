import unittest

import sys
sys.path.append("/home/gonzo/Repos/yfinance-cache/src/yfinance_cache")
from yfc_time import *

from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

from pprint import pprint

class Test_PriceDataAging_1H(unittest.TestCase):

    def setUp(self):
        self.market = "us_market"
        self.exchange = "NMS"
        self.market_tz = ZoneInfo('US/Eastern')
        self.market_close_time = time(hour=16, minute=0)

        self.monday  = date(year=2022, month=2, day=7)
        self.tuesday = date(year=2022, month=2, day=8)
        self.friday  = date(year=2022, month=2, day=11)
        self.saturday= date(year=2022, month=2, day=12)

    ## 1h interval, fetched during or v.soon after, tested during or v.soon after
    def test_duringDay(self):
        interval = Interval.Hours1
        interval_start_dt = datetime.combine(self.monday, time(hour=12, minute=30), self.market_tz)

        max_ages = []
        fetch_dts = []
        dt_nows = []
        expire_on_candle_closes = []
        yf_lags = []
        answers = []

        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=35), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.monday, time(hour=13, minute=4),  self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=35), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.monday, time(hour=13, minute=5),  self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(True)

        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=35), self.market_tz))
        max_ages.append(timedelta(minutes=60))
        dt_nows.append(  datetime.combine(self.monday, time(hour=13, minute=30), self.market_tz))
        expire_on_candle_closes.append(False)
        yf_lags.append(timedelta(minutes=1))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=35), self.market_tz))
        max_ages.append(timedelta(minutes=60))
        dt_nows.append(  datetime.combine(self.monday, time(hour=13, minute=30), self.market_tz))
        expire_on_candle_closes.append(True)
        yf_lags.append(timedelta(minutes=1))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=35), self.market_tz))
        max_ages.append(timedelta(minutes=60))
        dt_nows.append(  datetime.combine(self.monday, time(hour=13, minute=31), self.market_tz))
        expire_on_candle_closes.append(True)
        yf_lags.append(timedelta(minutes=1))
        answers.append(True)

        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=35), self.market_tz))
        max_ages.append(timedelta(hours=1))
        dt_nows.append(  datetime.combine(self.monday, time(hour=13, minute=30), self.market_tz))
        expire_on_candle_closes.append(True)
        yf_lags.append(timedelta(minutes=1))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=35), self.market_tz))
        max_ages.append(timedelta(hours=1))
        dt_nows.append(  datetime.combine(self.monday, time(hour=13, minute=31), self.market_tz))
        expire_on_candle_closes.append(True)
        yf_lags.append(timedelta(minutes=1))
        answers.append(True)

        for i in range(len(fetch_dts)):
            fetch_dt = fetch_dts[i]
            max_age = max_ages[i]
            dt_now = dt_nows[i]
            expire_on_candle_close = expire_on_candle_closes[i]
            yf_lag = yf_lags[i]
            answer = answers[i]
            response = IsPriceDatapointExpired(interval_start_dt, fetch_dt, max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
            try:
                self.assertEqual(response, answer)
            except:
                print("fetch_dt = {0}".format(fetch_dt))
                print("max_age = {0}".format(max_age))
                print("dt_now = {0}".format(dt_now))
                print("expire_on_candle_close = {0}".format(expire_on_candle_close))
                print("answer:")
                pprint(answer)
                print("response:")
                pprint(response)
                raise

    ## 1h interval end-of-day, fetched during interval, tested during | soon after | next morning
    def test_endOfDay(self):
        interval = Interval.Hours1
        interval_start_dt = datetime.combine(self.monday, time(hour=15, minute=30), self.market_tz)
        
        max_ages = []
        fetch_dts = []
        dt_nows = []
        expire_on_candle_closes = []
        yf_lags = []
        answers = []

        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=35), self.market_tz))
        max_ages.append(timedelta(minutes=20))
        dt_nows.append(  datetime.combine(self.monday, time(hour=15, minute=54), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=35), self.market_tz))
        max_ages.append(timedelta(minutes=20))
        dt_nows.append(  datetime.combine(self.monday, time(hour=15, minute=55), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(True)

        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=35), self.market_tz))
        max_ages.append(timedelta(minutes=60))
        dt_nows.append(  datetime.combine(self.monday, time(hour=15, minute=59), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(False)
        
        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=35), self.market_tz))
        max_ages.append(timedelta(minutes=60))
        dt_nows.append(  datetime.combine(self.monday, time(hour=16, minute=0), self.market_tz))
        expire_on_candle_closes.append(True)
        yf_lags.append(timedelta(minutes=1))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=35), self.market_tz))
        max_ages.append(timedelta(minutes=60))
        dt_nows.append(  datetime.combine(self.monday, time(hour=16, minute=0), self.market_tz))
        expire_on_candle_closes.append(True)
        yf_lags.append(timedelta(minutes=1))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=35), self.market_tz))
        max_ages.append(timedelta(minutes=60))
        dt_nows.append(  datetime.combine(self.monday, time(hour=16, minute=1), self.market_tz))
        expire_on_candle_closes.append(True)
        yf_lags.append(timedelta(minutes=1))
        answers.append(True)

        for i in range(len(fetch_dts)):
            fetch_dt = fetch_dts[i]
            max_age = max_ages[i]
            dt_now = dt_nows[i]
            expire_on_candle_close = expire_on_candle_closes[i]
            yf_lag = yf_lags[i]
            answer = answers[i]
            response = IsPriceDatapointExpired(interval_start_dt, fetch_dt, max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
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

    ## 1h interval end-of-Friday, fetched during interval, tested after market close | next Saturday
    def test_endOfFriday(self):
        interval = Interval.Hours1
        interval_start_dt = datetime.combine(self.friday, time(hour=15, minute=30), self.market_tz)
        
        max_ages = []
        fetch_dts = []
        dt_nows = []
        expire_on_candle_closes = []
        yf_lags = []
        answers = []

        fetch_dts.append(datetime.combine(self.friday, time(hour=15, minute=31), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.friday, time(hour=16, minute=1),  self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(False)

        fetch_dts.append(datetime.combine(self.friday, time(hour=15, minute=31), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.friday, time(hour=16, minute=0),  self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(False)

        fetch_dts.append(datetime.combine(self.friday, time(hour=15, minute=31), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.friday, time(hour=16, minute=0),  self.market_tz))
        expire_on_candle_closes.append(True)
        yf_lags.append(timedelta(minutes=1))
        answers.append(False)

        fetch_dts.append(datetime.combine(self.friday, time(hour=15, minute=31), self.market_tz))
        max_ages.append(timedelta(minutes=30))
        dt_nows.append(  datetime.combine(self.friday, time(hour=16, minute=1),  self.market_tz))
        expire_on_candle_closes.append(True)
        yf_lags.append(timedelta(minutes=1))
        answers.append(True)

        for i in range(len(fetch_dts)):
            fetch_dt = fetch_dts[i]
            max_age = max_ages[i]
            dt_now = dt_nows[i]
            expire_on_candle_close = expire_on_candle_closes[i]
            yf_lag = yf_lags[i]
            answer = answers[i]
            response = IsPriceDatapointExpired(interval_start_dt, fetch_dt, max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
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

    ## Batch test 1h interval, during interval
    def test_duringDay_dtDuringInterval_batch(self):
        interval = Interval.Hours1
        interval_start_dt = datetime.combine(self.monday, time(hour=12, minute=30), self.market_tz)
        
        max_age = timedelta(minutes=30)
        expire_on_candle_close = False ; yf_lag = None
        dt_now = datetime.combine(self.monday, time(hour=13, minute=15), self.market_tz)

        interval_start_dts = []
        fetch_dts = []
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=45), self.market_tz))
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=46), self.market_tz))

        responses = IsPriceDatapointExpired_batch(interval_start_dts, fetch_dts, max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
        for i in range(len(responses)):
            answer = IsPriceDatapointExpired(interval_start_dts[i], fetch_dts[i], max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
            try:
                self.assertEqual(responses[i], answer)
            except:
                print("interval = {0}".format(interval))
                print("max_age = {0}".format(max_age))
                print("interval_start_dt = {0}".format(interval_start_dts[i]))
                print("dt_now = {0}".format(dt_now))
                print("fetch_dt:")
                pprint(fetch_dts[i])
                print("answer:")
                pprint(answer)
                print("response:")
                pprint(responses[i])
                raise

    ## Batch test 1h interval, soon after interval
    def test_duringDay_dtAfterInterval_batch1(self):
        interval = Interval.Hours1
        interval_start_dt = datetime.combine(self.monday, time(hour=12, minute=30), self.market_tz)
        
        max_age = timedelta(minutes=30)
        expire_on_candle_close = False ; yf_lag = None
        dt_now = datetime.combine(self.monday, time(hour=13, minute=30), self.market_tz)

        interval_start_dts = []
        fetch_dts = []
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=40), self.market_tz))
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=13, minute=20), self.market_tz))

        responses = IsPriceDatapointExpired_batch(interval_start_dts, fetch_dts, max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
        for i in range(len(responses)):
            answer = IsPriceDatapointExpired(interval_start_dts[i], fetch_dts[i], max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
            try:
                self.assertEqual(responses[i], answer)
            except:
                print("interval = {0}".format(interval))
                print("max_age = {0}".format(max_age))
                print("interval_start_dt = {0}".format(interval_start_dts[i]))
                print("dt_now = {0}".format(dt_now))
                print("fetch_dt:")
                pprint(fetch_dts[i])
                print("answer:")
                pprint(answer)
                print("response:")
                pprint(responses[i])
                raise
    ## - candle close triggers expiry, no YF lag
    def test_duringDay_dtAfterInterval_batch2(self):
        interval = Interval.Hours1
        interval_start_dt = datetime.combine(self.monday, time(hour=12, minute=30), self.market_tz)
        
        max_age = timedelta(minutes=30)
        expire_on_candle_close = True ; yf_lag = timedelta(minutes=0)
        dt_now = datetime.combine(self.monday, time(hour=13, minute=30), self.market_tz)

        interval_start_dts = []
        fetch_dts = []
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=40), self.market_tz))
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=13, minute=20), self.market_tz))

        responses = IsPriceDatapointExpired_batch(interval_start_dts, fetch_dts, max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
        for i in range(len(responses)):
            answer = IsPriceDatapointExpired(interval_start_dts[i], fetch_dts[i], max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
            try:
                self.assertEqual(responses[i], answer)
            except:
                print("interval = {0}".format(interval))
                print("max_age = {0}".format(max_age))
                print("interval_start_dt = {0}".format(interval_start_dts[i]))
                print("dt_now = {0}".format(dt_now))
                print("fetch_dt:")
                pprint(fetch_dts[i])
                print("answer:")
                pprint(answer)
                print("response:")
                pprint(responses[i])
                raise
    ## - candle close triggers expiry, with 1 minute YF lag
    def test_duringDay_dtAfterInterval_batch3(self):
        interval = Interval.Hours1
        interval_start_dt = datetime.combine(self.monday, time(hour=12, minute=30), self.market_tz)
        
        max_age = timedelta(minutes=30)
        expire_on_candle_close = True ; yf_lag = timedelta(minutes=1)
        dt_now = datetime.combine(self.monday, time(hour=13, minute=30), self.market_tz)

        interval_start_dts = []
        fetch_dts = []
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=40), self.market_tz))
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=13, minute=20), self.market_tz))

        responses = IsPriceDatapointExpired_batch(interval_start_dts, fetch_dts, max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
        for i in range(len(responses)):
            answer = IsPriceDatapointExpired(interval_start_dts[i], fetch_dts[i], max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
            try:
                self.assertEqual(responses[i], answer)
            except:
                print("interval = {0}".format(interval))
                print("max_age = {0}".format(max_age))
                print("interval_start_dt = {0}".format(interval_start_dts[i]))
                print("dt_now = {0}".format(dt_now))
                print("fetch_dt:")
                pprint(fetch_dts[i])
                print("answer:")
                pprint(answer)
                print("response:")
                pprint(responses[i])
                raise

    ## Batch test 1h interval, after market close
    def test_endOfDay_evening_batch(self):
        interval = Interval.Hours1
        interval_start_dt = datetime.combine(self.monday, time(hour=15, minute=30), self.market_tz)

        max_age = timedelta(minutes=30)
        expire_on_candle_close = False ; yf_lag = None
        dt_now = datetime.combine(self.monday, time(hour=16, minute=0), self.market_tz)

        interval_start_dts = []
        fetch_dts = []

        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=30), self.market_tz))
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=59), self.market_tz))
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=16, minute=0), self.market_tz))

        responses = IsPriceDatapointExpired_batch(interval_start_dts, fetch_dts, max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
        for i in range(len(responses)):
            answer = IsPriceDatapointExpired(interval_start_dts[i], fetch_dts[i], max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
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

    ## Batch test 1h interval, next morning
    def test_endOfDay_nextMorning_batch(self):
        interval = Interval.Hours1
        interval_start_dt = datetime.combine(self.monday, time(hour=15, minute=30), self.market_tz)

        max_age = timedelta(minutes=30)
        expire_on_candle_close = False ; yf_lag = None
        dt_now = datetime.combine(self.tuesday, time(hour=10, minute=45), self.market_tz)

        interval_start_dts = []
        fetch_dts = []
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=30), self.market_tz))
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=15, minute=59), self.market_tz))
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.monday, time(hour=16, minute=0), self.market_tz))

        responses = IsPriceDatapointExpired_batch(interval_start_dts, fetch_dts, max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
        for i in range(len(responses)):
            answer = IsPriceDatapointExpired(interval_start_dts[i], fetch_dts[i], max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
            try:
                self.assertEqual(responses[i], answer)
            except:
                print("interval = {0}".format(interval))
                print("max_age = {0}".format(max_age))
                print("interval_start_dt = {0}".format(interval_start_dts[i]))
                print("dt_now = {0}".format(dt_now))
                print("fetch_dt:")
                pprint(fetch_dts[i])
                print("answer:")
                pprint(answer)
                print("response:")
                pprint(responses[i])
                raise

    ## Batch test 1h Friday interval, next Saturday
    def test_endOfFriday_batch(self):
        interval = Interval.Hours1
        interval_start_dt = datetime.combine(self.friday, time(hour=15, minute=30), self.market_tz)

        max_age = timedelta(minutes=30)
        expire_on_candle_close = False ; yf_lag = None
        dt_now = datetime.combine(self.friday+timedelta(days=1), time(hour=10, minute=45), self.market_tz)

        interval_start_dts = []
        fetch_dts = []
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.friday, time(hour=15, minute=30), self.market_tz))
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.friday, time(hour=15, minute=45), self.market_tz))
        #
        interval_start_dts.append(interval_start_dt)
        fetch_dts.append(datetime.combine(self.friday, time(hour=16, minute=0), self.market_tz))

        responses = IsPriceDatapointExpired_batch(interval_start_dts, fetch_dts, max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
        for i in range(len(responses)):
            answer = IsPriceDatapointExpired(interval_start_dts[i], fetch_dts[i], max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
            try:
                self.assertEqual(responses[i], answer)
            except:
                print("interval = {0}".format(interval))
                print("max_age = {0}".format(max_age))
                print("interval_start_dt = {0}".format(interval_start_dts[i]))
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
