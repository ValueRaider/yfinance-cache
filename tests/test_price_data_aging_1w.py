import unittest

from .context import yfc_dat as yfcd
from .context import yfc_time as yfct

from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

from pprint import pprint

## 2022 calendar:
## X* = day X is USA public holiday that closed NYSE
##  -- February --
##  Mo   Tu   We   Th   Fr   Sa   Su
##  7    8    9    10   11   12   13
##  14   15   16   17   18   19   20
##  21*  22   23   24   25   26   27
##  28

class Test_PriceDataAging_1W(unittest.TestCase):
    def setUp(self):
        self.market = "us_market"
        self.exchange = "NMS"
        self.market_tz = ZoneInfo('US/Eastern')
        self.market_open_time  = time(hour=9, minute=30)
        self.market_close_time = time(hour=16, minute=0)

        self.monday  = date(year=2022, month=2, day=14)
        self.tuesday = date(year=2022, month=2, day=15)
        self.wednday = date(year=2022, month=2, day=16)
        self.friday  = date(year=2022, month=2, day=18)
        self.saturday= date(year=2022, month=2, day=19)

    ## Test same day same week
    def test_sameWeek_sameDay(self):
        interval_start = self.monday
        interval = yfcd.Interval.Week

        max_ages = []
        fetch_dts = []
        dt_nows = []
        expire_on_candle_closes = []
        yf_lags = []
        answers = []

        max_ages.append(timedelta(hours=2))
        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=5), self.market_tz))
        dt_nows.append(  datetime.combine(self.monday, time(hour=14, minute=4), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(False)

        max_ages.append(timedelta(hours=2))
        fetch_dts.append(datetime.combine(self.monday, time(hour=12, minute=5), self.market_tz))
        dt_nows.append(  datetime.combine(self.monday, time(hour=14, minute=5), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(True)

        for i in range(len(fetch_dts)):
            fetch_dt = fetch_dts[i]
            max_age = max_ages[i]
            dt_now = dt_nows[i]
            expire_on_candle_close = expire_on_candle_closes[i]
            yf_lag = yf_lags[i]
            answer = answers[i]
            response = yfct.IsPriceDatapointExpired(interval_start, fetch_dt, max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
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

    ## Test different day same week
    def test_sameWeek_diffDay(self):
        interval_start = self.monday
        interval = yfcd.Interval.Week

        max_ages = []
        fetch_dts = []
        dt_nows = []
        expire_on_candle_closes = []
        yf_lags = []
        answers = []

        max_ages.append(timedelta(hours=2))
        fetch_dts.append(datetime.combine(self.monday,  time(hour=12, minute=5), self.market_tz))
        dt_nows.append(  datetime.combine(self.tuesday, time(hour=12, minute=5), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(True)

        max_ages.append(timedelta(hours=24))
        fetch_dts.append(datetime.combine(self.monday,  time(hour=12, minute=5), self.market_tz))
        dt_nows.append(  datetime.combine(self.tuesday, time(hour=12, minute=4), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(False)
        max_ages.append(timedelta(hours=24))
        fetch_dts.append(datetime.combine(self.monday,  time(hour=12, minute=5), self.market_tz))
        dt_nows.append(  datetime.combine(self.tuesday, time(hour=12, minute=5), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(True)

        max_ages.append(timedelta(days=1))
        fetch_dts.append(datetime.combine(self.monday,  time(hour=12, minute=5), self.market_tz))
        dt_nows.append(  datetime.combine(self.tuesday, time(hour=11, minute=4), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(False)
        max_ages.append(timedelta(days=1))
        fetch_dts.append(datetime.combine(self.monday,  time(hour=12, minute=5), self.market_tz))
        dt_nows.append(  datetime.combine(self.tuesday, time(hour=12, minute=5), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(True)

        max_ages.append(timedelta(days=2))
        fetch_dts.append(datetime.combine(self.monday,  time(hour=12, minute=5), self.market_tz))
        dt_nows.append(  datetime.combine(self.tuesday, time(hour=12, minute=5), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(False)

        max_ages.append(timedelta(days=2))
        fetch_dts.append(datetime.combine(self.monday,  time(hour=12, minute=5), self.market_tz))
        dt_nows.append(  datetime.combine(self.wednday, time(hour=12, minute=5), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(True)

        for i in range(len(fetch_dts)):
            fetch_dt = fetch_dts[i]
            max_age = max_ages[i]
            dt_now = dt_nows[i]
            expire_on_candle_close = expire_on_candle_closes[i]
            yf_lag = yf_lags[i]
            answer = answers[i]
            response = yfct.IsPriceDatapointExpired(interval_start, fetch_dt, max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
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

    ## Test different day same week with exchange data delay
    def test_sameWeek_diffDay_delay(self):
        exchange = "LSE" # 20min delay
        yf_lag = timedelta(minutes=20)
        market_tz = ZoneInfo("Europe/London")
        interval = yfcd.Interval.Week
        market_open = time(8)
        market_close = time(16, 30)

        ## Just before market close, but with data delay appears as after close:
        fetch_dt = datetime.combine(self.friday, market_close, market_tz)+timedelta(minutes=1)
        ## ... as interval is live then Yahoo returns current time as interval start:
        interval_start = self.monday
        max_age = timedelta(minutes=10)
        dt_now = datetime.combine(self.friday, market_close, market_tz)+timedelta(minutes=14)
        expire_on_candle_close = False
        answer = True
        result = yfct.IsPriceDatapointExpired(interval_start, fetch_dt, max_age, exchange, interval, expire_on_candle_close, yf_lag, dt_now)
        try:
            self.assertEqual(result, answer)
        except:
            print("interval_start: {}".format(interval_start))
            print("fetch_dt: {}".format(fetch_dt))
            print("dt_now: {}".format(dt_now))
            print("result: {}".format(result))
            print("answer: {}".format(answer))
            raise
        #
        dt_now = datetime.combine(self.friday, market_close, market_tz)+timedelta(minutes=2)
        answer = False
        result = yfct.IsPriceDatapointExpired(interval_start, fetch_dt, max_age, exchange, interval, expire_on_candle_close, yf_lag, dt_now)
        try:
            self.assertEqual(result, answer)
        except:
            print("result:")
            pprint(result)
            print("answer:")
            pprint(answer)
            raise

        ## Check that 'expire-on-candle-close' still works:
        dt_now = datetime.combine(self.friday, market_close, market_tz)+yf_lag+timedelta(minutes=1)
        max_age = timedelta(hours=1)
        expire_on_candle_close = True
        answer = True
        result = yfct.IsPriceDatapointExpired(interval_start, fetch_dt, max_age, exchange, interval, expire_on_candle_close, yf_lag, dt_now)
        try:
            self.assertEqual(result, answer)
        except:
            print("result:")
            pprint(result)
            print("answer:")
            pprint(answer)
            raise

        ## Just after market close + data delay:
        fetch_dt = datetime.combine(self.friday, market_close, market_tz)+yf_lag+timedelta(minutes=1)
        interval_start = self.monday
        max_age = timedelta(minutes=10)
        dt_now = datetime.combine(self.friday, market_close, market_tz)+yf_lag+timedelta(minutes=14)
        expire_on_candle_close = False
        answer = False
        result = yfct.IsPriceDatapointExpired(interval_start, fetch_dt, max_age, exchange, interval, expire_on_candle_close, yf_lag, dt_now)
        try:
            self.assertEqual(result, answer)
        except:
            print("result:")
            pprint(result)
            print("answer:")
            pprint(answer)
            raise

    ## Test weekend following week
    def test_sameWeek_weekend(self):
        interval_start = self.monday
        interval = yfcd.Interval.Week

        dt_now = datetime.combine(self.saturday, time(hour=9, minute=0), self.market_tz)
        max_ages = []
        fetch_dts = []
        expire_on_candle_closes = []
        yf_lags = []
        answers = []

        max_ages.append(timedelta(hours=2))
        fetch_dts.append(datetime.combine(self.monday, time(hour=13, minute=44), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(True)

        max_ages.append(timedelta(hours=2))
        fetch_dts.append(datetime.combine(self.monday, time(hour=13, minute=45), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(True)

        for i in range(len(fetch_dts)):
            fetch_dt = fetch_dts[i]
            max_age = max_ages[i]
            expire_on_candle_close = expire_on_candle_closes[i]
            yf_lag = yf_lags[i]
            answer = answers[i]
            response = yfct.IsPriceDatapointExpired(interval_start, fetch_dt, max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
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

    ## Test next week
    def test_nextWeek(self):
        interval_start = self.monday
        interval = yfcd.Interval.Week

        dt_now = datetime.combine(self.monday+timedelta(days=7), time(hour=13, minute=45), self.market_tz)
        max_ages = []
        fetch_dts = []
        expire_on_candle_closes = []
        yf_lags = []
        answers = []

        max_ages.append(timedelta(hours=2))
        fetch_dts.append(datetime.combine(self.monday, time(hour=13, minute=59), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(True)

        max_ages.append(timedelta(hours=2))
        fetch_dts.append(datetime.combine(self.monday, time(hour=14, minute=0), self.market_tz))
        expire_on_candle_closes.append(False) ; yf_lags.append(None)
        answers.append(True)

        for i in range(len(fetch_dts)):
            fetch_dt = fetch_dts[i]
            max_age = max_ages[i]
            expire_on_candle_close = expire_on_candle_closes[i]
            yf_lag = yf_lags[i]
            answer = answers[i]
            response = yfct.IsPriceDatapointExpired(interval_start, fetch_dt, max_age, self.exchange, interval, expire_on_candle_close, yf_lag, dt_now)
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

if __name__ == '__main__':
    unittest.main()
